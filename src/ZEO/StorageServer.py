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

import asyncore
import cPickle
import os
import sys
import threading

from ZEO import ClientStub
from ZEO.CommitLog import CommitLog
from ZEO.zrpc.server import Dispatcher
from ZEO.zrpc.connection import ManagedServerConnection, Delay, MTDelay

import zLOG
from ZODB.POSException import StorageError, StorageTransactionError
from ZODB.POSException import TransactionError, ReadOnlyError
from ZODB.referencesf import referencesf
from ZODB.Transaction import Transaction

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

    def __init__(self, addr, storages, read_only=0):

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

        """

        self.addr = addr
        self.storages = storages
        set_label()
        msg = ", ".join(
            ["%s:%s" % (name, storage.isReadOnly() and "RO" or "RW")
             for name, storage in storages.items()])
        log("%s created %s with storages: %s" %
            (self.__class__.__name__, read_only and "RO" or "RW", msg))
        for s in storages.values():
            s._waiting = []
        self.read_only = read_only
        self.connections = {}
        self.dispatcher = Dispatcher(addr, factory=self.new_connection,
                                     reuse_addr=1)

    def new_connection(self, sock, addr):
        """Internal: factory to create a new connection.

        This is called by the Dispatcher class in ZEO.zrpc.server
        whenever accept() returns a socket for a new incoming
        connection.
        """
        z = ZEOStorage(self, self.read_only)
        c = ManagedServerConnection(sock, addr, z, self)
        log("new connection %s: %s" % (addr, `c`))
        return c

    def register_connection(self, storage_id, conn):
        """Internal: register a connection with a particular storage.

        This is called by ZEOStorage.register().

        The dictionary self.connections maps each storage name to a
        list of current connections for that storage; this information
        is needed to handle invalidation.  This function updates this
        dictionary.
        """
        l = self.connections.get(storage_id)
        if l is None:
            l = self.connections[storage_id] = []
        l.append(conn)

    def invalidate(self, conn, storage_id, invalidated=(), info=None):
        """Internal: broadcast info and invalidations to clients.

        This is called from several ZEOStorage methods.

        This can do three different things:

        - If the invalidated argument is non-empty, it broadcasts
          invalidateTrans() messages to all clients of the given
          storage except the current client (the conn argument).

        - If the invalidated argument is empty and the info argument
          is a non-empty dictionary, it broadcasts info() messages to
          all clients of the given storage, including the current
          client.

        - If both the invalidated argument and the info argument are
          non-empty, it broadcasts invalidateTrans() messages to all
          clients except the current, and sends an info() message to
          the current client.

        """
        for p in self.connections.get(storage_id, ()):
            if invalidated and p is not conn:
                p.client.invalidateTrans(invalidated)
            elif info is not None:
                p.client.info(info)

    def close_server(self):
        """Close the dispatcher so that there are no new connections.

        This is only called from the test suite, AFAICT.
        """
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

    def __init__(self, server, read_only=0):
        self.server = server
        self.client = None
        self.storage = None
        self.storage_id = "uninitialized"
        self.transaction = None
        self.read_only = read_only

    def notifyConnected(self, conn):
        self.client = ClientStub.ClientStorage(conn)

    def notifyDisconnected(self):
        # When this storage closes, we must ensure that it aborts
        # any pending transaction.
        if self.transaction is not None:
            self.log("disconnected during transaction %s" % self.transaction)
            self.abort()
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
        zLOG.LOG("%s:%s" % (_label, self.storage_id), level, msg, error=error)

    def setup_delegation(self):
        """Delegate several methods to the storage"""
        self.versionEmpty = self.storage.versionEmpty
        self.versions = self.storage.versions
        self.history = self.storage.history
        self.load = self.storage.load
        self.loadSerial = self.storage.loadSerial
        self.modifiedInVersion = self.storage.modifiedInVersion

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
            log("duplicate register() call")
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
        self.server.register_connection(storage_id, self)

    def get_info(self):
        return {'length': len(self.storage),
                'size': self.storage.getSize(),
                'name': self.storage.getName(),
                'supportsUndo': self.storage.supportsUndo(),
                'supportsVersions': self.storage.supportsVersions(),
                'supportsTransactionalUndo':
                self.storage.supportsTransactionalUndo(),
                }

    def get_size_info(self):
        return {'length': len(self.storage),
                'size': self.storage.getSize(),
                }

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

    def beginZeoVerify(self):
        self.client.beginVerify()

    def zeoVerify(self, oid, s, sv):
        try:
            p, os, v, pv, osv = self.zeoLoad(oid)
        except: # except what?
            return None
        if os != s:
            self.client.invalidateVerify((oid, ''))
        elif osv != sv:
            self.client.invalidateVerify((oid, v))

    def endZeoVerify(self):
        self.client.endVerify()

    def pack(self, time, wait=1):
        # Yes, you can pack a read-only server or storage!
        if wait:
            return run_in_thread(self.pack_impl, time)
        else:
            # If the client isn't waiting for a reply, start a thread
            # and forget about it.
            t = threading.Thread(target=self.pack_impl, args=(time,))
            t.start()
            return None

    def pack_impl(self, time):
        self.log("pack(time=%s) started..." % repr(time))
        self.storage.pack(time, referencesf)
        self.log("pack(time=%s) complete" % repr(time))
        # Broadcast new size statistics
        self.server.invalidate(0, self.storage_id, (), self.get_size_info())

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
            self.server.invalidate(self, self.storage_id,
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

        # (This doesn't require a lock because we're using asyncore)
        if self.storage._transaction is None:
            self.strategy = ImmediateCommitStrategy(self.storage,
                                                    self.client)
        else:
            self.strategy = DelayedCommitStrategy(self.storage,
                                                  self.wait)

        t = Transaction()
        t.id = id
        t.user = user
        t.description = description
        t._extension = ext

        self.strategy.tpc_begin(t, tid, status)
        self.transaction = t

    def tpc_finish(self, id):
        if not self.check_tid(id):
            return
        invalidated = self.strategy.tpc_finish()
        if invalidated:
            self.server.invalidate(self, self.storage_id,
                                   invalidated, self.get_size_info())
        self.transaction = None
        self.strategy = None
        self.handle_waiting()

    def tpc_abort(self, id):
        if not self.check_tid(id):
            return
        strategy = self.strategy
        strategy.tpc_abort()
        self.transaction = None
        self.strategy = None
        self.handle_waiting()

    def abort(self):
        strategy = self.strategy
        self.transaction = None
        self.strategy = None
        strategy.abort(self)

    # XXX handle new serialnos

    def storea(self, oid, serial, data, version, id):
        self.check_tid(id, exc=StorageTransactionError)
        self.strategy.store(oid, serial, data, version)

    def vote(self, id):
        self.check_tid(id, exc=StorageTransactionError)
        return self.strategy.tpc_vote()

    def abortVersion(self, src, id):
        self.check_tid(id, exc=StorageTransactionError)
        return self.strategy.abortVersion(src)

    def commitVersion(self, src, dest, id):
        self.check_tid(id, exc=StorageTransactionError)
        return self.strategy.commitVersion(src, dest)

    def transactionalUndo(self, trans_id, id):
        self.check_tid(id, exc=StorageTransactionError)
        return self.strategy.transactionalUndo(trans_id)

    # When a delayed transaction is restarted, the dance is
    # complicated.  The restart occurs when one ZEOStorage instance
    # finishes as a transaction and finds another instance is in the
    # _waiting list.

    # XXX It might be better to have a mechanism to explicitly send
    # the finishing transaction's reply before restarting the waiting
    # transaction.  If the restart takes a long time, the previous
    # client will be blocked until it finishes.

    def wait(self):
        if self.storage._transaction:
            d = Delay()
            self.storage._waiting.append((d, self))
            self.log("Transaction blocked waiting for storage. "
                     "Clients waiting: %d." % len(self.storage._waiting))
            return d
        else:
            self.restart()
            return None

    def handle_waiting(self):
        while self.storage._waiting:
            delay, zeo_storage = self.storage._waiting.pop(0)
            if self.restart_other(zeo_storage, delay):
                if self.storage._waiting:
                    n = len(self.storage._waiting)
                    self.log("Blocked transaction restarted.  "
                             "Clients waiting: %d" % n)
                else:
                    self.log("Blocked transaction restarted.")
                return

    def restart_other(self, zeo_storage, delay):
        # Return True if the server restarted.
        # call the restart() method on the appropriate server.
        try:
            zeo_storage.restart(delay)
        except:
            self.log("Unexpected error handling waiting transaction",
                     level=zLOG.WARNING, error=sys.exc_info())
            zeo_storage._conn.close()
            return 0
        else:
            return 1

    def restart(self, delay=None):
        old_strategy = self.strategy
        assert isinstance(old_strategy, DelayedCommitStrategy)
        self.strategy = ImmediateCommitStrategy(self.storage,
                                                self.client)
        resp = old_strategy.restart(self.strategy)
        if delay is not None:
            delay.reply(resp)

# A ZEOStorage instance can use different strategies to commit a
# transaction.  The current implementation uses different strategies
# depending on whether the underlying storage is available.  These
# strategies implement the distributed commit lock.

# If the underlying storage is availabe, start the commit immediately
# using the ImmediateCommitStrategy.  If the underlying storage is not
# available because another client is committing a transaction, delay
# the commit as long as possible.  At some point it will no longer be
# possible to delay; either the transaction will reach the vote stage
# or a synchronous method like transactionalUndo() will be called.
# When it is no longer possible to delay, the client must block until
# the storage is ready.  Then we switch back to the immediate strategy.

class ICommitStrategy:
    """A class that describes that commit strategy interface.

    The commit strategy interface does not require the transaction
    argument, except for tpc_begin().  The storage interface requires
    the client to pass a transaction object/id to each transactional
    method.  The strategy does not; it requires the caller to only
    call methods for a single transaction.
    """
    # This isn't a proper Zope interface, because I don't want to
    # introduce a dependency between ZODB and Zope interfaces.

    def tpc_begin(self, trans, tid, status): pass

    def store(self, oid, serial, data, version): pass

    def abortVersion(self, src): pass

    def commitVersion(self, src, dest): pass

    # the trans_id arg to transactionalUndo is not the current txn's id
    def transactionalUndo(self, trans_id): pass

    def tpc_vote(self): pass

    def tpc_abort(self): pass

    def tpc_finish(self): pass

    # What to do if a connection is closed in mid-transaction
    def abort(self, zeo_storage): pass

class ImmediateCommitStrategy:
    """The storage is available so do a normal commit."""

    def __init__(self, storage, client):
        self.storage = storage
        self.client = client
        self.invalidated = []
        self.serials = []

    def tpc_begin(self, txn, tid, status):
        self.txn = txn
        self.storage.tpc_begin(txn, tid, status)

    def tpc_vote(self):
        # send all the serialnos as a batch
        self.client.serialnos(self.serials)
        return self.storage.tpc_vote(self.txn)

    def tpc_finish(self):
        self.storage.tpc_finish(self.txn)
        return self.invalidated

    def tpc_abort(self):
        self.storage.tpc_abort(self.txn)

    def store(self, oid, serial, data, version):
        try:
            newserial = self.storage.store(oid, serial, data, version,
                                           self.txn)
        except Exception, err:
            if not isinstance(err, TransactionError):
                # Unexpected errors are logged and passed to the client
                exc_info = sys.exc_info()
                log("store error: %s, %s" % exc_info[:2],
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
                log(msg, zLOG.ERROR)
                err = StorageServerError(msg)
            # The exception is reported back as newserial for this oid
            newserial = err
        else:
            if serial != "\0\0\0\0\0\0\0\0":
                self.invalidated.append((oid, version))
        self.serials.append((oid, newserial))

    def commitVersion(self, src, dest):
        oids = self.storage.commitVersion(src, dest, self.txn)
        inv = [(oid, dest) for oid in oids]
        self.invalidated.extend(inv)
        if dest:
            inv = [(oid, src) for oid in oids]
            self.invalidated.extend(inv)
        return oids

    def abortVersion(self, src):
        oids = self.storage.abortVersion(src, self.txn)
        inv = [(oid, src) for oid in oids]
        self.invalidated.extend(inv)
        return oids

    def transactionalUndo(self, trans_id):
        oids = self.storage.transactionalUndo(trans_id, self.txn)
        inv = [(oid, None) for oid in oids]
        self.invalidated.extend(inv)
        return oids

    def abort(self, zeo_storage):
        self.tpc_abort()
        zeo_storage.handle_waiting()

class DelayedCommitStrategy:
    """The storage is unavailable, so log to a file."""

    def __init__(self, storage, block):
        # the block argument is called when we can't delay any longer
        self.storage = storage
        self.block = block
        self.log = CommitLog()

        # Store information about the call that blocks
        self.name = None
        self.args = None

    def tpc_begin(self, txn, tid, status):
        self.txn = txn
        self.tid = tid
        self.status = status

    def store(self, oid, serial, data, version):
        self.log.store(oid, serial, data, version)

    def tpc_abort(self):
        pass # just forget about this strategy

    def tpc_finish(self):
        # There has to be a tpc_vote() call before tpc_finish() is
        # called, and tpc_vote() always blocks, so a proper
        # tpc_finish() call will always be sent to the immediate
        # commit strategy object.  So, if we get here, it means no
        # call to tpc_vote() was made, which is a bug in the caller.
        raise RuntimeError, "Logic error.  This method must not be called."

    def tpc_vote(self):
        self.name = "tpc_vote"
        self.args = ()
        return self.block()

    def commitVersion(self, src, dest):
        self.name = "commitVersion"
        self.args = src, dest
        return self.block()

    def abortVersion(self, src):
        self.name = "abortVersion"
        self.args = src,
        return self.block()

    def transactionalUndo(self, trans_id):
        self.name = "transactionalUndo"
        self.args = trans_id,
        return self.block()

    def restart(self, new_strategy):
        # called by the storage when the storage is available
        assert isinstance(new_strategy, ImmediateCommitStrategy)
        new_strategy.tpc_begin(self.txn, self.tid, self.status)
        loads, loader = self.log.get_loader()
        for i in range(loads):
            oid, serial, data, version = loader.load()
            new_strategy.store(oid, serial, data, version)
        meth = getattr(new_strategy, self.name)
        return meth(*self.args)

    def abort(self, zeo_storage):
        # Delete (d, zeo_storage) from the _waiting list, if found.
        waiting = self.storage._waiting
        for i in range(len(waiting)):
            d, z = waiting[i]
            if z is zeo_storage:
                del waiting[i]
                break

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
        except Exception:
            self.delay.error(sys.exc_info())
        else:
            self.delay.reply(result)
