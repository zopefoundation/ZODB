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

from ZODB.POSException import ReadOnlyError

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
        self.cond = threading.Condition(threading.Lock())
        self.connection = None # Protected by self.cond
        self.closed = 0
        # If thread is not None, then there is a helper thread
        # attempting to connect.
        self.thread = None # Protected by self.cond
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
                    raise ValueError, (
                        "unknown address in list: %s" % repr(addr))
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
        self.cond.acquire()
        try:
            t = self.thread
            self.thread = None
            conn = self.connection
        finally:
            self.cond.release()
        if t is not None:
            log("CM.close(): stopping and joining thread")
            t.stop()
            t.join(30)
            if t.isAlive():
                log("CM.close(): self.thread.join() timed out",
                    level=zLOG.WARNING)
        if conn is not None:
            # This will call close_conn() below which clears self.connection
            conn.close()
        if self.trigger is not None:
            self.trigger.close()
            self.trigger = None

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
        log("CM.set_async(%s)" % repr(map), level=zLOG.DEBUG)
        if not self.closed and self.trigger is None:
            log("CM.set_async(): first call")
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

        # XXX Will a single attempt take too long?
        # XXX Answer: it depends -- normally, you'll connect or get a
        # connection refused error very quickly.  Packet-eating
        # firewalls and other mishaps may cause the connect to take a
        # long time to time out though.  It's also possible that you
        # connect quickly to a slow server, and the attempt includes
        # at least one roundtrip to the server (the register() call).
        # But that's as fast as you can expect it to be.
        self.connect()
        self.cond.acquire()
        try:
            t = self.thread
            conn = self.connection
        finally:
            self.cond.release()
        if t is not None and conn is None:
            event = t.one_attempt
            event.wait()
            self.cond.acquire()
            try:
                conn = self.connection
            finally:
                self.cond.release()
        return conn is not None

    def connect(self, sync=0):
        self.cond.acquire()
        try:
            if self.connection is not None:
                return
            t = self.thread
            if t is None:
                log("CM.connect(): starting ConnectThread")
                self.thread = t = ConnectThread(self, self.client,
                                                self.addrlist,
                                                self.tmin, self.tmax)
                t.setDaemon(1)
                t.start()
            if sync:
                while self.connection is None:
                    self.cond.wait(30)
                    if self.connection is None:
                        log("CM.connect(sync=1): still waiting...")
        finally:
            self.cond.release()
        if sync:
            assert self.connection is not None

    def connect_done(self, conn, preferred):
        # Called by ConnectWrapper.notify_client() after notifying the client
        log("CM.connect_done(preferred=%s)" % preferred)
        self.cond.acquire()
        try:
            self.connection = conn
            if preferred:
                self.thread = None
            self.cond.notifyAll() # Wake up connect(sync=1)
        finally:
            self.cond.release()

    def close_conn(self, conn):
        # Called by the connection when it is closed
        self.cond.acquire()
        try:
            if conn is not self.connection:
                # Closing a non-current connection
                log("CM.close_conn() non-current", level=zLOG.BLATHER)
                return
            log("CM.close_conn()")
            self.connection = None
        finally:
            self.cond.release()
        self.client.notifyDisconnected()
        if not self.closed:
            self.connect()

    def is_connected(self):
        self.cond.acquire()
        try:
            return self.connection is not None
        finally:
            self.cond.release()

# When trying to do a connect on a non-blocking socket, some outcomes
# are expected.  Set _CONNECT_IN_PROGRESS to the errno value(s) expected
# when an initial connect can't complete immediately.  Set _CONNECT_OK
# to the errno value(s) expected if the connect succeeds *or* if it's
# already connected (our code can attempt redundant connects).
if hasattr(errno, "WSAEWOULDBLOCK"):    # Windows
    # XXX The official Winsock docs claim that WSAEALREADY should be
    # treated as yet another "in progress" indicator, but we've never
    # seen this.
    _CONNECT_IN_PROGRESS = (errno.WSAEWOULDBLOCK,)
    # Win98: WSAEISCONN; Win2K: WSAEINVAL
    _CONNECT_OK          = (0, errno.WSAEISCONN, errno.WSAEINVAL)
else:                                   # Unix
    _CONNECT_IN_PROGRESS = (errno.EINPROGRESS,)
    _CONNECT_OK          = (0, errno.EISCONN)

class ConnectThread(threading.Thread):
    """Thread that tries to connect to server given one or more addresses.

    The thread is passed a ConnectionManager and the manager's client
    as arguments.  It calls testConnection() on the client when a
    socket connects; that should return 1 or 0 indicating whether this
    is a preferred or a fallback connection.  It may also raise an
    exception, in which case the connection is abandoned.

    The thread will continue to run, attempting connections, until a
    preferred connection is seen and successfully handed over to the
    manager and client.

    As soon as testConnection() finds a preferred connection, or after
    all sockets have been tried and at least one fallback connection
    has been seen, notifyConnected(connection) is called on the client
    and connect_done() on the manager.  If this was a preferred
    connection, the thread then exits; otherwise, it keeps trying
    until it gets a preferred connection, and then reconnects the
    client using that connection.

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
        # A ConnectThread keeps track of whether it has finished a
        # call to try_connecting().  This allows the ConnectionManager
        # to make an attempt to connect right away, but not block for
        # too long if the server isn't immediately available.

    def stop(self):
        self.stopped = 1

    def run(self):
        delay = self.tmin
        success = 0
        # Don't wait too long the first time.
        # XXX make timeout configurable?
        attempt_timeout = 5
        while not self.stopped:
            success = self.try_connecting(attempt_timeout)
            if not self.one_attempt.isSet():
                self.one_attempt.set()
                attempt_timeout = 75
            if success > 0:
                break
            time.sleep(delay)
            if self.mgr.is_connected():
                log("CT: still trying to replace fallback connection",
                    level=zLOG.INFO)
            delay = min(delay*2, self.tmax)
        log("CT: exiting thread: %s" % self.getName())

    def try_connecting(self, timeout):
        """Try connecting to all self.addrlist addresses.

        Return 1 if a preferred connection was found; 0 if no
        connection was found; and -1 if a fallback connection was
        found.

        If no connection is found within timeout seconds, return 0.
        """
        log("CT: attempting to connect on %d sockets" % len(self.addrlist))
        deadline = time.time() + timeout
        wrappers = self._create_wrappers()
        for wrap in wrappers.keys():
            if wrap.state == "notified":
                return 1
        try:
            if time.time() > deadline:
                return 0
            r = self._connect_wrappers(wrappers, deadline)
            if r is not None:
                return r
            if time.time() > deadline:
                return 0
            r = self._fallback_wrappers(wrappers, deadline)
            if r is not None:
                return r
            # Alas, no luck.
            assert not wrappers
        finally:
            for wrap in wrappers.keys():
                wrap.close()
            del wrappers
        return 0

    def _create_wrappers(self):
        # Create socket wrappers
        wrappers = {}  # keys are active wrappers
        for domain, addr in self.addrlist:
            wrap = ConnectWrapper(domain, addr, self.mgr, self.client)
            wrap.connect_procedure()
            if wrap.state == "notified":
                for wrap in wrappers.keys():
                    wrap.close()
                wrappers[wrap] = wrap
                return wrappers
            if wrap.state != "closed":
                wrappers[wrap] = wrap
        return wrappers

    def _connect_wrappers(self, wrappers, deadline):
        # Next wait until they all actually connect (or fail)
        # The deadline is necessary, because we'd wait forever if a
        # sockets never connects or fails.
        while wrappers:
            if self.stopped:
                for wrap in wrappers.keys():
                    wrap.close()
                return 0
            # Select connecting wrappers
            connecting = [wrap
                          for wrap in wrappers.keys()
                          if wrap.state == "connecting"]
            if not connecting:
                break
            if time.time() > deadline:
                break
            try:
                r, w, x = select.select([], connecting, connecting, 1.0)
                log("CT: select() %d, %d, %d" % tuple(map(len, (r,w,x))))
            except select.error, msg:
                log("CT: select failed; msg=%s" % str(msg),
                    level=zLOG.WARNING) # XXX Is this the right level?
                continue
            # Exceptable wrappers are in trouble; close these suckers
            for wrap in x:
                log("CT: closing troubled socket %s" % str(wrap.addr))
                del wrappers[wrap]
                wrap.close()
            # Writable sockets are connected
            for wrap in w:
                wrap.connect_procedure()
                if wrap.state == "notified":
                    del wrappers[wrap] # Don't close this one
                    for wrap in wrappers.keys():
                        wrap.close()
                    return 1
                if wrap.state == "closed":
                    del wrappers[wrap]

    def _fallback_wrappers(self, wrappers, deadline):
        # If we've got wrappers left at this point, they're fallback
        # connections.  Try notifying them until one succeeds.
        for wrap in wrappers.keys():
            assert wrap.state == "tested" and wrap.preferred == 0
            if self.mgr.is_connected():
                wrap.close()
            else:
                wrap.notify_client()
                if wrap.state == "notified":
                    del wrappers[wrap] # Don't close this one
                    for wrap in wrappers.keys():
                        wrap.close()
                    return -1
            assert wrap.state == "closed"
            del wrappers[wrap]

            # XXX should check deadline
        

class ConnectWrapper:
    """An object that handles the connection procedure for one socket.

    This is a little state machine with states:
        closed
        opened
        connecting
        connected
        tested
        notified
    """

    def __init__(self, domain, addr, mgr, client):
        """Store arguments and create non-blocking socket."""
        self.domain = domain
        self.addr = addr
        self.mgr = mgr
        self.client = client
        # These attributes are part of the interface
        self.state = "closed"
        self.sock = None
        self.conn = None
        self.preferred = 0
        log("CW: attempt to connect to %s" % repr(addr))
        try:
            self.sock = socket.socket(domain, socket.SOCK_STREAM)
        except socket.error, err:
            log("CW: can't create socket, domain=%s: %s" % (domain, err),
                level=zLOG.ERROR)
            self.close()
            return
        self.sock.setblocking(0)
        self.state = "opened"

    def connect_procedure(self):
        """Call sock.connect_ex(addr) and interpret result."""
        if self.state in ("opened", "connecting"):
            try:
                err = self.sock.connect_ex(self.addr)
            except socket.error, msg:
                log("CW: connect_ex(%r) failed: %s" % (self.addr, msg),
                    level=zLOG.ERROR)
                self.close()
                return
            log("CW: connect_ex(%s) returned %s" %
                (self.addr, errno.errorcode.get(err) or str(err)))
            if err in _CONNECT_IN_PROGRESS:
                self.state = "connecting"
                return
            if err not in _CONNECT_OK:
                log("CW: error connecting to %s: %s" %
                    (self.addr, errno.errorcode.get(err) or str(err)),
                    level=zLOG.WARNING)
                self.close()
                return
            self.state = "connected"
        if self.state == "connected":
            self.test_connection()

    def test_connection(self):
        """Establish and test a connection at the zrpc level.

        Call the client's testConnection(), giving the client a chance
        to do app-level check of the connection.
        """
        self.conn = ManagedConnection(self.sock, self.addr,
                                      self.client, self.mgr)
        self.sock = None # The socket is now owned by the connection
        try:
            self.preferred = self.client.testConnection(self.conn)
            self.state = "tested"
        except ReadOnlyError:
            log("CW: ReadOnlyError in testConnection (%s)" % repr(self.addr))
            self.close()
            return
        except:
            log("CW: error in testConnection (%s)" % repr(self.addr),
                level=zLOG.ERROR, error=sys.exc_info())
            self.close()
            return
        if self.preferred:
            self.notify_client()

    def notify_client(self):
        """Call the client's notifyConnected().

        If this succeeds, call the manager's connect_done().

        If the client is already connected, we assume it's a fallback
        connection, and the new connection must be a preferred
        connection.  The client will close the old connection.
        """
        try:
            self.client.notifyConnected(self.conn)
        except:
            log("CW: error in notifyConnected (%s)" % repr(self.addr),
                level=zLOG.ERROR, error=sys.exc_info())
            self.close()
            return
        self.state = "notified"
        self.mgr.connect_done(self.conn, self.preferred)

    def close(self):
        """Close the socket and reset everything."""
        self.state = "closed"
        self.mgr = self.client = None
        self.preferred = 0
        if self.conn is not None:
            # Closing the ZRPC connection will eventually close the
            # socket, somewhere in asyncore.
            # XXX Why do we care? --Guido
            self.conn.close()
            self.conn = None
        if self.sock is not None:
            self.sock.close()
            self.sock = None

    def fileno(self):
        return self.sock.fileno()
