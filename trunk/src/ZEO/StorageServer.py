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
"""Network ZODB storage server

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
from ZODB.POSException import StorageError, StorageTransactionError, \
     TransactionError, ReadOnlyError
from ZODB.referencesf import referencesf
from ZODB.Transaction import Transaction
from ZODB.TmpStore import TmpStore

# We create a special fast pickler! This allows us
# to create slightly more efficient pickles and
# to create them a tad faster.
pickler = cPickle.Pickler()
pickler.fast = 1 # Don't use the memo
dump = pickler.dump

def log(message, level=zLOG.INFO, label="ZEO Server:%s" % os.getpid(),
        error=None):
    zLOG.LOG(label, level, message, error=error)

class StorageServerError(StorageError):
    pass

class StorageServer:
    def __init__(self, addr, storages, read_only=0):
        # XXX should read_only be a per-storage option? not yet...
        self.addr = addr
        self.storages = storages
        for s in storages.values():
            s._waiting = []
        self.read_only = read_only
        self.connections = {}
        self.dispatcher = Dispatcher(addr, factory=self.newConnection,
                                     reuse_addr=1)

    def newConnection(self, sock, addr):
        c = ManagedServerConnection(sock, addr, ZEOStorage(self), self)
        log("new connection %s: %s" % (addr, `c`))
        return c

    def register(self, storage_id, proxy):
        """Register a connection's use with a particular storage.

        This information is needed to handle invalidation.
        """
        l = self.connections.get(storage_id)
        if l is None:
            l = self.connections[storage_id] = []
        l.append(proxy)

    def invalidate(self, conn, storage_id, invalidated=(), info=None):
        for p in self.connections.get(storage_id, ()):
            if invalidated and p is not conn:
                p.client.invalidateTrans(invalidated)
            elif info is not None:
                p.client.info(info)

    def close_server(self):
        # Close the dispatcher so that there are no new connections.
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
        for sid, cl in self.connections.items():
            if conn.obj in cl:
                cl.remove(conn.obj)

class ZEOStorage:
    """Proxy to underlying storage for a single remote client."""

    def __init__(self, server):
        self.server = server
        self.client = None
        self._conn = None # the connection associated with client
        self.__storage = None
        self.__storage_id = "uninitialized"
        self._transaction = None

    def notifyConnected(self, conn):
        self._conn = conn
        self.client = ClientStub.ClientStorage(conn)

    def notifyDisconnected(self):
        # When this storage closes, we must ensure that it aborts
        # any pending transaction.  Not sure if this is the clearest way.
        if self._transaction is not None:
            self._log("disconnected during transaction %s" % self._transaction,
                      zLOG.BLATHER)
            self.tpc_abort(self._transaction.id)
        else:
            self._log("disconnected", zLOG.BLATHER)

    def __repr__(self):
        tid = self._transaction and repr(self._transaction.id)
        if self.__storage:
            stid = self.__storage._transaction and \
                   repr(self.__storage._transaction.id)
        else:
            stid = None
        name = self.__class__.__name__
        return "<%s %X trans=%s s_trans=%s>" % (name, id(self), tid, stid)

    def _log(self, msg, level=zLOG.INFO, error=None, pid=os.getpid()):
        name = getattr(self.__storage, '__name__', None)
        if name is None:
            name = str(self.__storage)
        zLOG.LOG("ZEO Server:%s:%s" % (pid, name), level, msg, error=error)

    def setup_delegation(self):
        """Delegate several methods to the storage"""
        self.undoInfo = self.__storage.undoInfo
        self.undoLog = self.__storage.undoLog
        self.versionEmpty = self.__storage.versionEmpty
        self.versions = self.__storage.versions
        self.history = self.__storage.history
        self.load = self.__storage.load
        self.loadSerial = self.__storage.loadSerial
        self.modifiedInVersion = self.__storage.modifiedInVersion

    def _check_tid(self, tid, exc=None):
        caller = sys._getframe().f_back.f_code.co_name
        if self._transaction is None:
            self._log("no current transaction: %s()" % caller, zLOG.PROBLEM)
            if exc is not None:
                raise exc(None, tid)
            else:
                return 0
        if self._transaction.id != tid:
            self._log("%s(%s) invalid; current transaction = %s" % \
                 (caller, repr(tid), repr(self._transaction.id)), zLOG.PROBLEM)
            if exc is not None:
                raise exc(self._transaction.id, tid)
            else:
                return 0
        return 1

    def register(self, storage_id, read_only):
        """Select the storage that this client will use

        This method must be the first one called by the client.
        """
        self._log("register(%s, %s)" % (storage_id, read_only))
        storage = self.server.storages.get(storage_id)
        if storage is None:
            self._log("unknown storage_id: %s" % storage_id)
            raise ValueError, "unknown storage: %s" % storage_id

        if not read_only and (self.server.read_only or storage.isReadOnly()):
            raise ReadOnlyError()

        self.__storage_id = storage_id
        self.__storage = storage
        self.setup_delegation()
        self.server.register(storage_id, self)
        self._log("registered storage %s: %s" % (storage_id, storage))

    def get_info(self):
        return {'length': len(self.__storage),
                'size': self.__storage.getSize(),
                'name': self.__storage.getName(),
                'supportsUndo': self.__storage.supportsUndo(),
                'supportsVersions': self.__storage.supportsVersions(),
                'supportsTransactionalUndo':
                self.__storage.supportsTransactionalUndo(),
                }

    def get_size_info(self):
        return {'length': len(self.__storage),
                'size': self.__storage.getSize(),
                }

    def zeoLoad(self, oid):
        v = self.__storage.modifiedInVersion(oid)
        if v:
            pv, sv = self.__storage.load(oid, v)
        else:
            pv = sv = None
        try:
            p, s = self.__storage.load(oid, '')
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

    def pack(self, t, wait=None):
        if wait is not None:
            wait = MTDelay()
        t = threading.Thread(target=self._pack, args=(t, wait))
        t.start()
        if wait is not None:
            return wait
        else:
            return None

    def _pack(self, t, delay):
        try:
            self.__storage.pack(t, referencesf)
        except:
            self._log('Pack failed for %s' % self.__storage_id,
                      zLOG.ERROR,
                      error=sys.exc_info())
            if delay is not None:
                raise
        else:
            if delay is None:
                # Broadcast new size statistics
                self.server.invalidate(0, self.__storage_id, (),
                                       self.get_size_info())
            else:
                delay.reply(None)

    def new_oids(self, n=100):
        """Return a sequence of n new oids, where n defaults to 100"""
        if n <= 0:
            # Always return at least one
            n = 1
        return [self.__storage.new_oid() for i in range(n)]

    def undo(self, transaction_id):
        oids = self.__storage.undo(transaction_id)
        if oids:
            self.server.invalidate(self, self.__storage_id,
                                   map(lambda oid: (oid, ''), oids))
            return oids
        return ()

    def tpc_begin(self, id, user, description, ext, tid, status):
        if self._transaction is not None:
            if self._transaction.id == id:
                self._log("duplicate tpc_begin(%s)" % repr(id))
                return
            else:
                raise StorageTransactionError("Multiple simultaneous tpc_begin"
                                              " requests from one client.")

        # (This doesn't require a lock because we're using asyncore)
        if self.__storage._transaction is None:
            self.strategy = ImmediateCommitStrategy(self.__storage,
                                                    self.client)
        else:
            self.strategy = DelayedCommitStrategy(self.__storage,
                                                  self.wait)

        t = Transaction()
        t.id = id
        t.user = user
        t.description = description
        t._extension = ext

        self.strategy.tpc_begin(t, tid, status)
        self._transaction = t

    def tpc_finish(self, id):
        if not self._check_tid(id):
            return
        invalidated = self.strategy.tpc_finish()
        if invalidated:
            self.server.invalidate(self, self.__storage_id,
                                   invalidated, self.get_size_info())
        self._transaction = None
        self.strategy = None
        self._handle_waiting()

    def tpc_abort(self, id):
        if not self._check_tid(id):
            return
        self.strategy.tpc_abort()
        self._transaction = None
        self.strategy = None
        self._handle_waiting()

    # XXX handle new serialnos

    def storea(self, oid, serial, data, version, id):
        self._check_tid(id, exc=StorageTransactionError)
        self.strategy.store(oid, serial, data, version)

    def vote(self, id):
        self._check_tid(id, exc=StorageTransactionError)
        return self.strategy.tpc_vote()

    def abortVersion(self, src, id):
        self._check_tid(id, exc=StorageTransactionError)
        return self.strategy.abortVersion(src)

    def commitVersion(self, src, dest, id):
        self._check_tid(id, exc=StorageTransactionError)
        return self.strategy.commitVersion(src, dest)

    def transactionalUndo(self, trans_id, id):
        self._check_tid(id, exc=StorageTransactionError)
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
        if self.__storage._transaction:
            d = Delay()
            self.__storage._waiting.append((d, self))
            self._log("Transaction blocked waiting for storage. "
                      "Clients waiting: %d." % len(self.__storage._waiting))
            return d
        else:
            self.restart()
            return None

    def _handle_waiting(self):
        while self.__storage._waiting:
            delay, zeo_storage = self.__storage._waiting.pop(0)
            if self._restart(zeo_storage, delay):
                if self.__storage._waiting:
                    n = len(self.__storage._waiting)
                    self._log("Blocked transaction restarted.  "
                              "Clients waiting: %d" % n)
                else:
                    self._log("Blocked transaction restarted.")
                return

    def _restart(self, zeo_storage, delay):
        # Return True if the server restarted.
        # call the restart() method on the appropriate server.
        try:
            zeo_storage.restart(delay)
        except:
            self._log("Unexpected error handling waiting transaction",
                      level=zLOG.WARNING, error=sys.exc_info())
            zeo_storage._conn.close()
            return 0
        else:
            return 1

    def restart(self, delay=None):
        old_strategy = self.strategy
        assert isinstance(old_strategy, DelayedCommitStrategy)
        self.strategy = ImmediateCommitStrategy(self.__storage,
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
        except TransactionError, err:
            # Storage errors are passed to the client
            newserial = err
        except Exception:
            # Unexpected storage errors are logged and passed to the client
            exc_info = sys.exc_info()
            log("store error: %s, %s" % exc_info[:2],
                zLOG.ERROR, error=exc_info)
            newserial = exc_info[1]
            del exc_info
        else:
            if serial != "\0\0\0\0\0\0\0\0":
                self.invalidated.append((oid, version))

        try:
            dump(newserial, 1)
        except:
            msg = "Couldn't pickle storage exception: %s" % repr(newserial)
            log(msg, zLOG.ERROR)
            dump('', 1) # clear pickler
            r = StorageServerError(msg)
            newserial = r
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
