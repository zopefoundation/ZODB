##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
import errno
import select
import socket
import sys
import threading
import time
import types

import ThreadedAsync
import zLOG

from ZEO.zrpc.log import log
from ZEO.zrpc.trigger import trigger
from ZEO.zrpc.connection import ManagedConnection

class ConnectionManager:
    """Keeps a connection up over time"""

    def __init__(self, addrs, client, tmin=1, tmax=180):
        self.addrlist = self._parse_addrs(addrs)
        self.client = client
        self.tmin = tmin
        self.tmax = tmax
        self.connected = 0
        self.connection = None
        self.closed = 0
        # If thread is not None, then there is a helper thread
        # attempting to connect.  thread is protected by thread_lock.
        self.thread = None
        self.thread_lock = threading.Lock()
        self.trigger = None
        self.thr_async = 0
        ThreadedAsync.register_loop_callback(self.set_async)

    def __repr__(self):
        return "<%s for %s>" % (self.__class__.__name__, self.addrlist)

    def _parse_addrs(self, addrs):
        # Return a list of (addr_type, addr) pairs.

        # For backwards compatibility (and simplicity?) the
        # constructor accepts a single address in the addrs argument --
        # a string for a Unix domain socket or a 2-tuple with a
        # hostname and port.  It can also accept a list of such addresses.

        addr_type = self._guess_type(addrs)
        if addr_type is not None:
            return [(addr_type, addrs)]
        else:
            addrlist = []
            for addr in addrs:
                addr_type = self._guess_type(addr)
                if addr_type is None:
                    raise ValueError, "unknown address in list: %s" % repr(a)
                addrlist.append((addr_type, addr))
            return addrlist

    def _guess_type(self, addr):
        if isinstance(addr, types.StringType):
            return socket.AF_UNIX

        if (len(addr) == 2
            and isinstance(addr[0], types.StringType)
            and isinstance(addr[1], types.IntType)):
            return socket.AF_INET

        # not anything I know about
        return None

    def close(self):
        """Prevent ConnectionManager from opening new connections"""
        self.closed = 1
        self.thread_lock.acquire()
        try:
            t = self.thread
            if t is not None:
                t.stop()
        finally:
            self.thread_lock.release()
        if t is not None:
            t.join(30)
            if t.isAlive():
                log("ConnectionManager.close(): self.thread.join() timed out")
        if self.connection:
            self.connection.close()
        if self.trigger is not None:
            self.trigger.close()

    def set_async(self, map):
        # This is the callback registered with ThreadedAsync.  The
        # callback might be called multiple times, so it shouldn't
        # create a trigger every time and should never do anything
        # after it's closed.

        # It may be that the only case where it is called multiple
        # times is in the test suite, where ThreadedAsync's loop can
        # be started in a child process after a fork.  Regardless,
        # it's good to be defensive.

        # XXX need each connection started with async==0 to have a
        # callback
        if not self.closed and self.trigger is None:
            self.trigger = trigger()
            self.thr_async = 1 # XXX needs to be set on the Connection

    def attempt_connect(self):
        """Attempt a connection to the server without blocking too long.

        There isn't a crisp definition for too long.  When a
        ClientStorage is created, it attempts to connect to the
        server.  If the server isn't immediately available, it can
        operate from the cache.  This method will start the background
        connection thread and wait a little while to see if it
        finishes quickly.
        """

        # XXX will a single attempt take too long?
        self.connect()
        self.thread_lock.acquire()
        try:
            t = self.thread
        finally:
            self.thread_lock.release()
        if t is not None:
            event = t.one_attempt
            event.wait()
        return self.connected

    def connect(self, sync=0):
        if self.connected == 1:
            return
        self.thread_lock.acquire()
        try:
            t = self.thread
            if t is None:
                log("starting thread to connect to server")
                self.thread = t = ConnectThread(self, self.client,
                                                self.addrlist,
                                                self.tmin, self.tmax)
                t.start()
        finally:
            self.thread_lock.release()
        if sync:
            t.join(30)
            while t.isAlive():
                log("ConnectionManager.connect(sync=1): "
                    "self.thread.join() timed out")
                t.join(30)

    def connect_done(self, c):
        log("connect_done()")
        self.connected = 1
        self.connection = c
        self.thread_lock.acquire()
        try:
            self.thread = None
        finally:
            self.thread_lock.release()

    def notify_closed(self):
        self.connected = 0
        self.connection = None
        self.client.notifyDisconnected()
        if not self.closed:
            self.connect()

# When trying to do a connect on a non-blocking socket, some outcomes
# are expected.  Set _CONNECT_IN_PROGRESS to the errno value(s) expected
# when an initial connect can't complete immediately.  Set _CONNECT_OK
# to the errno value(s) expected if the connect succeeds *or* if it's
# already connected (our code can attempt redundant connects).
if hasattr(errno, "WSAEWOULDBLOCK"):    # Windows
    _CONNECT_IN_PROGRESS = (errno.WSAEWOULDBLOCK,)
    # Win98: WSAEISCONN; Win2K: WSAEINVAL
    _CONNECT_OK          = (0, errno.WSAEISCONN, errno.WSAEINVAL)
else:                                   # Unix
    _CONNECT_IN_PROGRESS = (errno.EINPROGRESS,)
    _CONNECT_OK          = (0, errno.EISCONN)

class ConnectThread(threading.Thread):
    """Thread that tries to connect to server given one or more addresses.
    The thread is passed a ConnectionManager and the manager's client
    as arguments.  It calls notifyConnected() on the client when a
    socket connects.  If notifyConnected() returns without raising an
    exception, the thread is done; it calls connect_done() on the
    manager and exits.

    The thread will continue to run, attempting connections, until a
    successful notifyConnected() or stop() is called.
    """

    __super_init = threading.Thread.__init__

    # We don't expect clients to call any methods of this Thread other
    # than close() and those defined by the Thread API.

    def __init__(self, mgr, client, addrlist, tmin, tmax):
        self.__super_init(name="Connect(%s)" % addrlist)
        self.mgr = mgr
        self.client = client
        self.addrlist = addrlist
        self.tmin = tmin
        self.tmax = tmax
        self.stopped = 0
        self.one_attempt = threading.Event()
        self.fallback = None
        # A ConnectThread keeps track of whether it has finished a
        # call to attempt_connects().  This allows the
        # ConnectionManager to make an attempt to connect right away,
        # but not block for too long if the server isn't immediately
        # available.

    def stop(self):
        self.stopped = 1

    # Every method from run() to the end is used internally by the Thread.

    def run(self):
        delay = self.tmin
        while not self.stopped:
            success = self.attempt_connects()
            if not self.one_attempt.isSet():
                self.one_attempt.set()
            if success:
                break
            time.sleep(delay)
            delay *= 2
            if delay > self.tmax:
                delay = self.tmax
        log("thread exiting: %s" % self.getName())

    def close_sockets(self):
        for s in self.sockets.keys():
            s.close()

    def attempt_connects(self):
        """Try connecting to all self.addrlist addresses.

        If at least one succeeds, pick a success arbitrarily, close all other
        successes (if any), and return true.  If none succeed, return false.
        """

        self.sockets = {}  # {open socket:  connection address}

        log("attempting connection on %d sockets" % len(self.addrlist))
        ok = 0
        for domain, addr in self.addrlist:
            if __debug__:
                log("attempt connection to %s" % repr(addr),
                    level=zLOG.DEBUG)
            try:
                s = socket.socket(domain, socket.SOCK_STREAM)
            except socket.error, err:
                log("Failed to create socket with domain=%s: %s" % (
                    domain, err), level=zLOG.ERROR)
                continue
            s.setblocking(0)
            self.sockets[s] = addr
            # XXX can still block for a while if addr requires DNS
            if self.try_connect(s):
                ok = 1
                break

        # next wait until they actually connect
        while not ok and self.sockets:
            if self.stopped:
                self.close_sockets()
                return 0
            try:
                sockets = self.sockets.keys()
                r, w, x = select.select([], sockets, sockets, 1.0)
            except select.error:
                continue
            for s in x:
                del self.sockets[s]
                s.close()
            for s in w:
                if self.try_connect(s):
                    ok = 1
                    break

        if ok:
            del self.sockets[s] # don't close the newly connected socket
            self.close_sockets()
            return 1
        if self.fallback:
            (c, stub) = self.fallback
            self.fallback = None
            try:
                self.client.notifyConnected(stub)
            except:
                log("error in notifyConnected (%r)" % addr,
                    level=zLOG.ERROR, error=sys.exc_info())
                c.close()
                return 0
            else:
                self.mgr.connect_done(c)
                return 1
        return 0

    def try_connect(self, s):
        """Call s.connect_ex(addr); return true iff connection succeeds.

        We have to handle several possible return values from
        connect_ex().  If the socket is connected and the initial ZEO
        setup works, we're done.  Report success by raising an
        exception.  Yes, the is odd, but we need to bail out of the
        select() loop in the caller and an exception is a principled
        way to do the abort.

        If the socket sonnects and the initial ZEO setup
        (notifyConnected()) fails or the connect_ex() returns an
        error, we close the socket, remove it from self.sockets, and
        proceed with the other sockets.

        If connect_ex() returns EINPROGRESS, we need to try again later.
        """
        addr = self.sockets[s]
        try:
            e = s.connect_ex(addr)
        except socket.error, msg:
            log("failed to connect to %s: %s" % (addr, msg),
                level=zLOG.ERROR)
        else:
            log("connect_ex(%s) == %s" % (addr, e))
            if e in _CONNECT_IN_PROGRESS:
                return 0
            elif e in _CONNECT_OK:
                # special cases to deal with winsock oddities
                if sys.platform.startswith("win") and e == 0:

                    # It appears that winsock isn't behaving as
                    # expected on Win2k.  It's possible for connect_ex()
                    # to return 0, but the connection to have failed.
                    # In particular, in situations where I expect to
                    # get a Connection refused (10061), I'm seeing
                    # connect_ex() return 0.  OTOH, it looks like
                    # select() is a more reliable indicator on
                    # Windows.

                    r, w, x = select.select([s], [s], [s], 0.1)
                    if not (r or w or x):
                        return 0
                    if x:
                        # see comment at the end of the function
                        s.close()
                        del self.socket[s]
                c = self.test_connection(s, addr)
                if c:
                    log("connected to %s" % repr(addr), level=zLOG.DEBUG)
                    return 1
            else:
                log("error connecting to %s: %s" % (addr, errno.errorcode[e]),
                    level=zLOG.DEBUG)
        # Any execution that doesn't raise Connected() or return
        # because of CONNECT_IN_PROGRESS is an error.  Make sure the
        # socket is closed and remove it from the dict of pending
        # sockets.
        s.close()
        del self.sockets[s]
        return 0

    def test_connection(self, s, addr):
        # Establish a connection at the zrpc level and call the
        # client's notifyConnected(), giving the zrpc application a
        # chance to do app-level check of whether the connection is
        # okay.
        c = ManagedConnection(s, addr, self.client, self.mgr)
        try:
            (stub, preferred) = self.client.testConnection(c)
        except:
            log("error in testConnection (%r)" % (addr,),
                level=zLOG.ERROR, error=sys.exc_info())
            c.close()
            # Closing the ZRPC connection will eventually close the
            # socket, somewhere in asyncore.
            return 0
        if preferred:
            try:
                self.client.notifyConnected(stub)
            except:
                log("error in notifyConnected (%r)" % (addr,),
                    level=zLOG.ERROR, error=sys.exc_info())
                c.close()
                return 0
            else:
                self.mgr.connect_done(c)
                return 1
        if self.fallback is None:
            self.fallback = (c, stub)
        return 0
