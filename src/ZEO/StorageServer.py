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
"""The StorageServer class and the exception that it may raise.

This server acts as a front-end for one or more real storages, like
file storage or Berkeley storage.

XXX Need some basic access control-- a declaration of the methods
exported for invocation by the server.
"""

from __future__ import nested_scopes

import asyncore
import cPickle
import os
import sys
import threading
import time

from ZEO import ClientStub
from ZEO.CommitLog import CommitLog
from ZEO.zrpc.server import Dispatcher
from ZEO.zrpc.connection import ManagedServerConnection, Delay, MTDelay
from ZEO.zrpc.trigger import trigger

import zLOG
from ZODB.POSException import StorageError, StorageTransactionError
from ZODB.POSException import TransactionError, ReadOnlyError
from ZODB.referencesf import referencesf
from ZODB.Transaction import Transaction
from ZODB.utils import u64

_label = "ZSS" # Default label used for logging.

def set_label():
    """Internal helper to reset the logging label (e.g. after fork())."""
    global _label
    _label = "ZSS:%s" % os.getpid()

def log(message, level=zLOG.INFO, label=None, error=None):
    """Internal helper to log a message using zLOG."""
    zLOG.LOG(label or _label, level, message, error=error)

class StorageServerError(StorageError):
    """Error reported when an unpickleable exception is raised."""

class StorageServer:

    """The server side implementation of ZEO.

    The StorageServer is the 'manager' for incoming connections.  Each
    connection is associated with its own ZEOStorage instance (defined
    below).  The StorageServer may handle multiple storages; each
    ZEOStorage instance only handles a single storage.
    """

    # Classes we instantiate.  A subclass might override.

    DispatcherClass = Dispatcher
    ZEOStorageClass = None # patched up later
    ManagedServerConnectionClass = ManagedServerConnection

    def __init__(self, addr, storages, read_only=0,
                 invalidation_queue_size=100,
                 transaction_timeout=None):
        """StorageServer constructor.

        This is typically invoked from the start.py script.

        Arguments (the first two are required and positional):

        addr -- the address at which the server should listen.  This
            can be a tuple (host, port) to signify a TCP/IP connection
            or a pathname string to signify a Unix domain socket
            connection.  A hostname may be a DNS name or a dotted IP
            address.

        storages -- a dictionary giving the storage(s) to handle.  The
            keys are the storage names, the values are the storage
            instances, typically FileStorage or Berkeley storage
            instances.  By convention, storage names are typically
            strings representing small integers starting at '1'.

        read_only -- an optional flag saying whether the server should
            operate in read-only mode.  Defaults to false.  Note that
            even if the server is operating in writable mode,
            individual storages may still be read-only.  But if the
            server is in read-only mode, no write operations are
            allowed, even if the storages are writable.  Note that
            pack() is considered a read-only operation.

        invalidation_queue_size -- The storage server keeps a queue
            of the objects modified by the last N transactions, where
            N == invalidation_queue_size.  This queue is used to
            speed client cache verification when a client disconnects
            for a short period of time.

        transaction_timout -- The maximum amount of time to wait for
            a transaction to commit after acquiring the storage lock.
            If the transaction takes too long, the client connection
            will be closed and the transaction aborted.
        """

        self.addr = addr
        self.storages = storages
        set_label()
        msg = ", ".join(
            ["%s:%s:%s" % (name, storage.isReadOnly() and "RO" or "RW",
                           storage.getName())
             for name, storage in storages.items()])
        log("%s created %s with storages: %s" %
            (self.__class__.__name__, read_only and "RO" or "RW", msg))
        for s in storages.values():
            s._waiting = []
        self.read_only = read_only
        # A list of at most invalidation_queue_size invalidations
        self.invq = []
        self.invq_bound = invalidation_queue_size
        self.connections = {}
        self.dispatcher = self.DispatcherClass(addr,
                                               factory=self.new_connection,
                                               reuse_addr=1)
        self.timeouts = {}
        for name in self.storages.keys():
            if transaction_timeout is None:
                # An object with no-op methods
                timeout = StubTimeoutThread()
            else:
                timeout = TimeoutThread(transaction_timeout)
                timeout.start()
            self.timeouts[name] = timeout

    def new_connection(self, sock, addr):
        """Internal: factory to create a new connection.

        This is called by the Dispatcher class in ZEO.zrpc.server
        whenever accept() returns a socket for a new incoming
        connection.
        """
        z = self.ZEOStorageClass(self, self.read_only)
        c = self.ManagedServerConnectionClass(sock, addr, z, self)
        log("new connection %s: %s" % (addr, `c`))
        return c

    def register_connection(self, storage_id, conn):
        """Internal: register a connection with a particular storage.

        This is called by ZEOStorage.register().

        The dictionary self.connections maps each storage name to a
        list of current connections for that storage; this information
        is needed to handle invalidation.  This function updates this
        dictionary.

        Returns the timeout object for the appropriate storage.
        """
        l = self.connections.get(storage_id)
        if l is None:
            l = self.connections[storage_id] = []
        l.append(conn)
        return self.timeouts[storage_id]

    def invalidate(self, conn, storage_id, tid, invalidated=(), info=None):
        """Internal: broadcast info and invalidations to clients.

        This is called from several ZEOStorage methods.

        This can do three different things:

        - If the invalidated argument is non-empty, it broadcasts
          invalidateTransaction() messages to all clients of the given
          storage except the current client (the conn argument).

        - If the invalidated argument is empty and the info argument
          is a non-empty dictionary, it broadcasts info() messages to
          all clients of the given storage, including the current
          client.

        - If both the invalidated argument and the info argument are
          non-empty, it broadcasts invalidateTransaction() messages to all
          clients except the current, and sends an info() message to
          the current client.

        """
        if invalidated:
            if len(self.invq) >= self.invq_bound:
                del self.invq[0]
            self.invq.append((tid, invalidated))
        for p in self.connections.get(storage_id, ()):
            if invalidated and p is not conn:
                p.client.invalidateTransaction(tid, invalidated)
            elif info is not None:
                p.client.info(info)

    def get_invalidations(self, tid):
        """Return a tid and list of all objects invalidation since tid.

        The tid is the most recent transaction id committed by the server.

        Returns None if it is unable to provide a complete list
        of invalidations for tid.  In this case, client should
        do full cache verification.
        """

        if not self.invq:
            log("invq empty")
            return None, []
        
        earliest_tid = self.invq[0][0]
        if earliest_tid > tid:
            log("tid to old for invq %s < %s" % (u64(tid), u64(earliest_tid)))
            return None, []
        
        oids = {}
        for tid, L in self.invq:
            for key in L:
                oids[key] = 1
        latest_tid = self.invq[-1][0]
        return latest_tid, oids.keys()

    def close_server(self):
        """Close the dispatcher so that there are no new connections.

        This is only called from the test suite, AFAICT.
        """
        for timeout in self.timeouts.values():
            timeout.stop()
        self.dispatcher.close()
        for storage in self.storages.values():
            storage.close()
        # Force the asyncore mainloop to exit by hackery, i.e. close
        # every socket in the map.  loop() will return when the map is
        # empty.
        for s in asyncore.socket_map.values():
            try:
                s.close()
            except:
                pass

    def close_conn(self, conn):
        """Internal: remove the given connection from self.connections.

        This is the inverse of register_connection().
        """
        for cl in self.connections.values():
            if conn.obj in cl:
                cl.remove(conn.obj)

class ZEOStorage:
    """Proxy to underlying storage for a single remote client."""

    # Classes we instantiate.  A subclass might override.

    ClientStorageStubClass = ClientStub.ClientStorage

    def __init__(self, server, read_only=0):
        self.server = server
        self.timeout = None
        self.connection = None
        self.client = None
        self.storage = None
        self.storage_id = "uninitialized"
        self.transaction = None
        self.read_only = read_only
        self.locked = 0
        self.log_label = _label

    def notifyConnected(self, conn):
        self.connection = conn # For restart_other() below
        self.client = self.ClientStorageStubClass(conn)
        addr = conn.addr
        if isinstance(addr, type("")):
            label = addr
        else:
            host, port = addr
            label = str(host) + ":" + str(port)
        self.log_label = _label + "/" + label

    def notifyDisconnected(self):
        # When this storage closes, we must ensure that it aborts
        # any pending transaction.
        if self.transaction is not None:
            self.log("disconnected during transaction %s" % self.transaction)
            self._abort()
        else:
            self.log("disconnected")

    def __repr__(self):
        tid = self.transaction and repr(self.transaction.id)
        if self.storage:
            stid = (self.storage._transaction and
                    repr(self.storage._transaction.id))
        else:
            stid = None
        name = self.__class__.__name__
        return "<%s %X trans=%s s_trans=%s>" % (name, id(self), tid, stid)

    def log(self, msg, level=zLOG.INFO, error=None):
        zLOG.LOG(self.log_label, level, msg, error=error)

    def setup_delegation(self):
        """Delegate several methods to the storage"""
        self.versionEmpty = self.storage.versionEmpty
        self.versions = self.storage.versions
        self.history = self.storage.history
        self.load = self.storage.load
        self.loadSerial = self.storage.loadSerial
        self.modifiedInVersion = self.storage.modifiedInVersion
        try:
            fn = self.storage.getExtensionMethods
        except AttributeError:
            # We must be running with a ZODB which
            # predates adding getExtensionMethods to
            # BaseStorage. Eventually this try/except
            # can be removed
            pass
        else:
            for name in fn().keys():
                if not hasattr(self,name):
                    setattr(self, name, getattr(self.storage, name))
        self.lastTransaction = self.storage.lastTransaction

    def check_tid(self, tid, exc=None):
        if self.read_only:
            raise ReadOnlyError()
        caller = sys._getframe().f_back.f_code.co_name
        if self.transaction is None:
            self.log("no current transaction: %s()" % caller, zLOG.PROBLEM)
            if exc is not None:
                raise exc(None, tid)
            else:
                return 0
        if self.transaction.id != tid:
            self.log("%s(%s) invalid; current transaction = %s" %
                     (caller, repr(tid), repr(self.transaction.id)),
                     zLOG.PROBLEM)
            if exc is not None:
                raise exc(self.transaction.id, tid)
            else:
                return 0
        return 1

    def register(self, storage_id, read_only):
        """Select the storage that this client will use

        This method must be the first one called by the client.
        """
        if self.storage is not None:
            self.log("duplicate register() call")
            raise ValueError, "duplicate register() call"
        storage = self.server.storages.get(storage_id)
        if storage is None:
            self.log("unknown storage_id: %s" % storage_id)
            raise ValueError, "unknown storage: %s" % storage_id

        if not read_only and (self.read_only or storage.isReadOnly()):
            raise ReadOnlyError()

        self.read_only = self.read_only or read_only
        self.storage_id = storage_id
        self.storage = storage
        self.setup_delegation()
        self.timeout = self.server.register_connection(storage_id, self)

    def get_info(self):
        return {'length': len(self.storage),
                'size': self.storage.getSize(),
                'name': self.storage.getName(),
                'supportsUndo': self.storage.supportsUndo(),
                'supportsVersions': self.storage.supportsVersions(),
                'supportsTransactionalUndo':
                self.storage.supportsTransactionalUndo(),
                'extensionMethods': self.getExtensionMethods(),
                }

    def get_size_info(self):
        return {'length': len(self.storage),
                'size': self.storage.getSize(),
                }

    def getExtensionMethods(self):
        try:
            e = self.storage.getExtensionMethods
        except AttributeError:
            return {}
        else:
            return e()

    def zeoLoad(self, oid):
        v = self.storage.modifiedInVersion(oid)
        if v:
            pv, sv = self.storage.load(oid, v)
        else:
            pv = sv = None
        try:
            p, s = self.storage.load(oid, '')
        except KeyError:
            if sv:
                # Created in version, no non-version data
                p = s = None
            else:
                raise
        return p, s, v, pv, sv

    def getInvalidations(self, tid):
        invtid, invlist = self.server.get_invalidations(tid)
        if invtid is None:
            return None
        self.log("Return %d invalidations up to tid %s"
                 % (len(invlist), u64(invtid)))
        return invtid, invlist

    def zeoVerify(self, oid, s, sv):
        try:
            os = self.storage.getSerial(oid)
        except KeyError:
            self.client.invalidateVerify((oid, ''))
            # XXX It's not clear what we should do now.  The KeyError
            # could be caused by an object uncreation, in which case
            # invalidation is right.  It could be an application bug
            # that left a dangling reference, in which case it's bad.
        else:
            # If the client has version data, the logic is a bit more
            # complicated.  If the current serial number matches the
            # client serial number, then the non-version data must
            # also be valid.  If the current serialno is for a
            # version, then the non-version data can't change.

            # If the version serialno isn't valid, then the
            # non-version serialno may or may not be valid.  Rather
            # than trying to figure it whether it is valid, we just
            # invalidate it.  Sending an invalidation for the
            # non-version data implies invalidating the version data
            # too, since an update to non-version data can only occur
            # after the version is aborted or committed.
            if sv:
                if sv != os:
                    self.client.invalidateVerify((oid, ''))
            else:
                if s != os:
                    self.client.invalidateVerify((oid, ''))

    def endZeoVerify(self):
        self.client.endVerify()

    def pack(self, time, wait=1):
        # Yes, you can pack a read-only server or storage!
        if wait:
            return run_in_thread(self._pack_impl, time)
        else:
            # If the client isn't waiting for a reply, start a thread
            # and forget about it.
            t = threading.Thread(target=self._pack_impl, args=(time,))
            t.start()
            return None

    def _pack_impl(self, time):
        self.log("pack(time=%s) started..." % repr(time))
        self.storage.pack(time, referencesf)
        self.log("pack(time=%s) complete" % repr(time))
        # Broadcast new size statistics
        self.server.invalidate(0, self.storage_id, None,
                               (), self.get_size_info())

    def new_oids(self, n=100):
        """Return a sequence of n new oids, where n defaults to 100"""
        if self.read_only:
            raise ReadOnlyError()
        if n <= 0:
            n = 1
        return [self.storage.new_oid() for i in range(n)]

    def undo(self, transaction_id):
        if self.read_only:
            raise ReadOnlyError()
        oids = self.storage.undo(transaction_id)
        if oids:
            self.server.invalidate(self, self.storage_id, None,
                                   map(lambda oid: (oid, ''), oids))
            return oids
        return ()

    # undoLog and undoInfo are potentially slow methods

    def undoInfo(self, first, last, spec):
        return run_in_thread(self.storage.undoInfo, first, last, spec)

    def undoLog(self, first, last):
        return run_in_thread(self.storage.undoLog, first, last)

    def tpc_begin(self, id, user, description, ext, tid, status):
        if self.read_only:
            raise ReadOnlyError()
        if self.transaction is not None:
            if self.transaction.id == id:
                self.log("duplicate tpc_begin(%s)" % repr(id))
                return
            else:
                raise StorageTransactionError("Multiple simultaneous tpc_begin"
                                              " requests from one client.")

        self.transaction = t = Transaction()
        t.id = id
        t.user = user
        t.description = description
        t._extension = ext

        self.serials = []
        self.invalidated = []
        self.txnlog = CommitLog()
        self.tid = tid
        self.status = status

    def tpc_finish(self, id):
        if not self.check_tid(id):
            return
        assert self.locked
        self.storage.tpc_finish(self.transaction)
        tid = self.storage.lastTransaction()
        if self.invalidated:
            self.server.invalidate(self, self.storage_id, tid,
                                   self.invalidated, self.get_size_info())
        self.transaction = None
        self.locked = 0
        self.timeout.end(self)
        # Return the tid, for cache invalidation optimization
        self._handle_waiting()
        return tid

    def tpc_abort(self, id):
        if not self.check_tid(id):
            return
        if self.locked:
            self.storage.tpc_abort(self.transaction)
        self.transaction = None
        self.locked = 0
        self.timeout.end(self)
        self._handle_waiting()

    def _abort(self):
        # called when a connection is closed unexpectedly
        if not self.locked:
            # Delete (d, zeo_storage) from the _waiting list, if found.
            waiting = self.storage._waiting
            for i in range(len(waiting)):
                d, z = waiting[i]
                if z is self:
                    del waiting[i]
                    self.log("Closed connection removed from waiting list."
                             " Clients waiting: %d." % len(waiting))
                    break

        if self.transaction:
            self.tpc_abort(self.transaction.id)

    # The public methods of the ZEO client API do not do the real work.
    # They defer work until after the storage lock has been acquired.
    # Most of the real implementations are in methods beginning with
    # an _.

    def storea(self, oid, serial, data, version, id):
        self.check_tid(id, exc=StorageTransactionError)
        self.txnlog.store(oid, serial, data, version)

    # The following four methods return values, so they must acquire
    # the storage lock and begin the transaction before returning.

    def vote(self, id):
        self.check_tid(id, exc=StorageTransactionError)
        if self.locked:
            return self._vote()
        else:
            return self._wait(lambda: self._vote())

    def abortVersion(self, src, id):
        self.check_tid(id, exc=StorageTransactionError)
        if self.locked:
            return self._abortVersion(src)
        else:
            return self._wait(lambda: self._abortVersion(src))

    def commitVersion(self, src, dest, id):
        self.check_tid(id, exc=StorageTransactionError)
        if self.locked:
            return self._commitVersion(src, dest)
        else:
            return self._wait(lambda: self._commitVersion(src, dest))

    def transactionalUndo(self, trans_id, id):
        self.check_tid(id, exc=StorageTransactionError)
        if self.locked:
            return self._transactionalUndo(trans_id)
        else:
            return self._wait(lambda: self._transactionalUndo(trans_id))

    def _tpc_begin(self, txn, tid, status):
        self.locked = 1
        self.storage.tpc_begin(txn, tid, status)
        self.timeout.begin(self)

    def _store(self, oid, serial, data, version):
        try:
            newserial = self.storage.store(oid, serial, data, version,
                                           self.transaction)
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception, err:
            if not isinstance(err, TransactionError):
                # Unexpected errors are logged and passed to the client
                exc_info = sys.exc_info()
                self.log("store error: %s, %s" % exc_info[:2],
                         zLOG.ERROR, error=exc_info)
                del exc_info
            # Try to pickle the exception.  If it can't be pickled,
            # the RPC response would fail, so use something else.
            pickler = cPickle.Pickler()
            pickler.fast = 1
            try:
                pickler.dump(err, 1)
            except:
                msg = "Couldn't pickle storage exception: %s" % repr(err)
                self.log(msg, zLOG.ERROR)
                err = StorageServerError(msg)
            # The exception is reported back as newserial for this oid
            newserial = err
        else:
            if serial != "\0\0\0\0\0\0\0\0":
                self.invalidated.append((oid, version))
        self.serials.append((oid, newserial))

    def _vote(self):
        self.client.serialnos(self.serials)
        return self.storage.tpc_vote(self.transaction)

    def _abortVersion(self, src):
        oids = self.storage.abortVersion(src, self.transaction)
        inv = [(oid, src) for oid in oids]
        self.invalidated.extend(inv)
        return oids

    def _commitVersion(self, src, dest):
        oids = self.storage.commitVersion(src, dest, self.transaction)
        inv = [(oid, dest) for oid in oids]
        self.invalidated.extend(inv)
        if dest:
            inv = [(oid, src) for oid in oids]
            self.invalidated.extend(inv)
        return oids

    def _transactionalUndo(self, trans_id):
        oids = self.storage.transactionalUndo(trans_id, self.transaction)
        inv = [(oid, None) for oid in oids]
        self.invalidated.extend(inv)
        return oids

    # When a delayed transaction is restarted, the dance is
    # complicated.  The restart occurs when one ZEOStorage instance
    # finishes as a transaction and finds another instance is in the
    # _waiting list.

    # XXX It might be better to have a mechanism to explicitly send
    # the finishing transaction's reply before restarting the waiting
    # transaction.  If the restart takes a long time, the previous
    # client will be blocked until it finishes.

    def _wait(self, thunk):
        # Wait for the storage lock to be acquired.
        self._thunk = thunk
        if self.storage._transaction:
            d = Delay()
            self.storage._waiting.append((d, self))
            self.log("Transaction blocked waiting for storage. "
                     "Clients waiting: %d." % len(self.storage._waiting))
            return d
        else:
            self.log("Transaction acquired storage lock.", zLOG.BLATHER)
            return self._restart()

    def _restart(self, delay=None):
        # Restart when the storage lock is available.
        self._tpc_begin(self.transaction, self.tid, self.status)
        loads, loader = self.txnlog.get_loader()
        for i in range(loads):
            # load oid, serial, data, version
            self._store(*loader.load())
        resp = self._thunk()
        if delay is not None:
            delay.reply(resp)
        else:
            return resp

    def _handle_waiting(self):
        # Restart any client waiting for the storage lock.
        while self.storage._waiting:
            delay, zeo_storage = self.storage._waiting.pop(0)
            if self._restart_other(zeo_storage, delay):
                if self.storage._waiting:
                    n = len(self.storage._waiting)
                    self.log("Blocked transaction restarted.  "
                             "Clients waiting: %d" % n)
                else:
                    self.log("Blocked transaction restarted.")
                return

    def _restart_other(self, zeo_storage, delay):
        # Return True if the server restarted.
        # call the restart() method on the appropriate server.
        try:
            zeo_storage._restart(delay)
        except:
            self.log("Unexpected error handling waiting transaction",
                     level=zLOG.WARNING, error=sys.exc_info())
            zeo_storage.connection.close()
            return 0
        else:
            return 1

class StubTimeoutThread:

    def begin(self, client):
        pass

    def end(self, client):
        pass

    def stop(self):
        pass

class TimeoutThread(threading.Thread):
    """Monitors transaction progress and generates timeouts."""

    def __init__(self, timeout):
        threading.Thread.__init__(self)
        self.setDaemon(1)
        self._timeout = timeout
        self._client = None
        self._deadline = None
        self._stop = 0
        self._active = threading.Event()
        self._lock = threading.Lock()
        self._trigger = trigger()

    def stop(self):
        self._stop = 1

    def begin(self, client):
        self._lock.acquire()
        try:
            self._active.set()
            self._client = client
            self._deadline = time.time() + self._timeout
        finally:
            self._lock.release()

    def end(self, client):
        # The ZEOStorage will call this message for every aborted
        # transaction, regardless of whether the transaction started
        # the 2PC.  Ignore here if 2PC never began.
        if client is not self._client:
            return
        self._lock.acquire()
        try:
            self._active.clear()
            self._client = None
            self._deadline = None
        finally:
            self._lock.release()

    def run(self):
        while not self._stop:
            self._active.wait()
            self._lock.acquire()
            try:
                deadline = self._deadline
                if deadline is None:
                    continue
                howlong = deadline - time.time()
            finally:
                self._lock.release()
            if howlong <= 0:
                self.timeout()
            else:
                time.sleep(howlong)
        self.trigger.close()

    def timeout(self):
        self._lock.acquire()
        try:
            client = self._client
            deadline = self._deadline
            self._active.clear()
            self._client = None
            self._deadline = None
        finally:
            self._lock.release()
        if client is None:
            return
        elapsed = time.time() - (deadline - self._timeout)
        client.log("Transaction timeout after %d seconds" % int(elapsed))
        self._trigger.pull_trigger(lambda: client.connection.close())

def run_in_thread(method, *args):
    t = SlowMethodThread(method, args)
    t.start()
    return t.delay

class SlowMethodThread(threading.Thread):
    """Thread to run potentially slow storage methods.

    Clients can use the delay attribute to access the MTDelay object
    used to send a zrpc response at the right time.
    """

    # Some storage methods can take a long time to complete.  If we
    # run these methods via a standard asyncore read handler, they
    # will block all other server activity until they complete.  To
    # avoid blocking, we spawn a separate thread, return an MTDelay()
    # object, and have the thread reply() when it finishes.

    def __init__(self, method, args):
        threading.Thread.__init__(self)
        self._method = method
        self._args = args
        self.delay = MTDelay()

    def run(self):
        try:
            result = self._method(*self._args)
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception:
            self.delay.error(sys.exc_info())
        else:
            self.delay.reply(result)

# Patch up class references
StorageServer.ZEOStorageClass = ZEOStorage
