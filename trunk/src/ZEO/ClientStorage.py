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
"""Network ZODB storage client

$Id: ClientStorage.py,v 1.61 2002/09/18 21:17:48 gvanrossum Exp $
"""

# XXX TO DO
# get rid of beginVerify, set up _tfile in verify_cache
# set self._storage = stub later, in endVerify
# if wait is given, wait until verify is complete
# get rid of _basic_init

import cPickle
import os
import tempfile
import threading
import time

from ZEO import ClientCache, ServerStub
from ZEO.TransactionBuffer import TransactionBuffer
from ZEO.Exceptions import Disconnected
from ZEO.zrpc.client import ConnectionManager

from ZODB import POSException
from ZODB.TimeStamp import TimeStamp
from zLOG import LOG, PROBLEM, INFO, BLATHER

def log2(type, msg, subsys="ClientStorage:%d" % os.getpid()):
    LOG(subsys, type, msg)

try:
    from ZODB.ConflictResolution import ResolvedSerial
except ImportError:
    ResolvedSerial = 'rs'

class ClientStorageError(POSException.StorageError):
    """An error occured in the ZEO Client Storage"""

class UnrecognizedResult(ClientStorageError):
    """A server call returned an unrecognized result"""

class ClientDisconnected(ClientStorageError, Disconnected):
    """The database storage is disconnected from the storage."""

def get_timestamp(prev_ts=None):
    t = time.time()
    t = apply(TimeStamp, (time.gmtime(t)[:5] + (t % 60,)))
    if prev_ts is not None:
        t = t.laterThan(prev_ts)
    return t

class DisconnectedServerStub:
    """Raise ClientDisconnected on all attribute access."""

    def __getattr__(self, attr):
        raise ClientDisconnected()

disconnected_stub = DisconnectedServerStub()

class ClientStorage:

    def __init__(self, addr, storage='1', cache_size=20000000,
                 name='', client=None, var=None,
                 min_disconnect_poll=5, max_disconnect_poll=300,
                 wait=0, read_only=0, read_only_fallback=0):

        log2(INFO, "ClientStorage (pid=%d) created %s/%s for storage: %r" %
             (os.getpid(),
              read_only and "RO" or "RW",
              read_only_fallback and "fallback" or "normal",
              storage))

        self._addr = addr # For tests
        self._server = disconnected_stub
        self._is_read_only = read_only
        self._storage = storage
        self._read_only_fallback = read_only_fallback

        self._info = {'length': 0, 'size': 0, 'name': 'ZEO Client',
                      'supportsUndo':0, 'supportsVersions': 0,
                      'supportsTransactionalUndo': 0}

        self._tbuf = TransactionBuffer()
        self._db = None
        self._oids = []
        # _serials: stores (oid, serialno) as returned by server
        # _seriald: _check_serials() moves from _serials to _seriald,
        #           which maps oid to serialno
        self._serials = []
        self._seriald = {}

        self._basic_init(name or str(addr))

        # Decide whether to use non-temporary files
        client = client or os.environ.get('ZEO_CLIENT')
        self._cache = ClientCache.ClientCache(storage, cache_size,
                                              client=client, var=var)

        self._rpc_mgr = ConnectionManager(addr, self,
                                          tmin=min_disconnect_poll,
                                          tmax=max_disconnect_poll)

        if wait:
            self._rpc_mgr.connect(sync=1)
        else:
            if not self._rpc_mgr.attempt_connect():
                self._rpc_mgr.connect()

        # If we're connected at this point, the cache is opened as a
        # side effect of verify_cache().  If not, open it now.
        if not self.is_connected():
            self._cache.open()

    def _basic_init(self, name):
        """Handle initialization activites of BaseStorage"""

        self.__name__ = name # A standard convention among storages

        # A ClientStorage only allows one thread to commit at a time.
        # Mutual exclusion is achieved using _tpc_cond, which
        # protects _transaction.  A thread that wants to assign to
        # self._transaction must acquire _tpc_cond first.  A thread
        # that decides it's done with a transaction (whether via success
        # or failure) must set _transaction to None and do
        # _tpc_cond.notify() before releasing _tpc_cond.
        self._tpc_cond = threading.Condition()
        self._transaction = None

        # Prevent multiple new_oid calls from going out.  The _oids
        # variable should only be modified while holding the
        # _oid_lock.
        self._oid_lock = threading.Lock()

        commit_lock = threading.Lock()
        self._commit_lock_acquire = commit_lock.acquire
        self._commit_lock_release = commit_lock.release

        t = self._ts = get_timestamp()
        self._serial = `t`
        self._oid='\0\0\0\0\0\0\0\0'

    def close(self):
        if self._tbuf is not None:
            self._tbuf.close()
            self._tbuf = None
        if self._cache is not None:
            self._cache.close()
            self._cache = None
        if self._rpc_mgr is not None:
            self._rpc_mgr.close()
            self._rpc_mgr = None

    def registerDB(self, db, limit):
        """Register that the storage is controlled by the given DB."""
        # This is called by ZODB.DB (and by some tests).
        # The storage isn't really ready to use until after this call.
        log2(INFO, "registerDB(%s, %s)" % (repr(db), repr(limit)))
        self._db = db

    def is_connected(self):
        if self._server is disconnected_stub:
            return 0
        else:
            return 1

    def update(self):
        """Handle any pending invalidation messages."""
        self._server._update()

    def testConnection(self, conn):
        """Return a pair (stub, preferred).

        Where:
        - stub is an RPC stub
        - preferred is: 1 if the connection is an optimal match,
                        0 if it is a suboptimal but acceptable match
        It can also raise DisconnectedError or ReadOnlyError.

        This is called by ConnectionManager to decide which connection
        to use in case there are multiple, and some are read-only and
        others are read-write.
        """
        log2(INFO, "Testing connection %r" % conn)
        # XXX Check the protocol version here?
        stub = ServerStub.StorageServer(conn)
        try:
            stub.register(str(self._storage), self._is_read_only)
            return (stub, 1)
        except POSException.ReadOnlyError:
            if not self._read_only_fallback or self.is_connected():
                raise
            log2(INFO, "Got ReadOnlyError; trying again with read_only=1")
            stub.register(str(self._storage), read_only=1)
            return (stub, 0)

    def notifyConnected(self, stub):
        """Start using the given RPC stub.

        This is called by ConnectionManager after it has decided which
        connection should be used.  The stub is one returned by a
        previous testConnection() call.
        """
        log2(INFO, "Connected to storage")
        self._oids = []
        self._info.update(stub.get_info())
        self.verify_cache(stub)

        # XXX The stub should be saved here and set in endVerify() below.
        self._server = stub

    def verify_cache(self, server):
        # XXX beginZeoVerify ends up calling back to beginVerify() below.
        # That whole exchange is rather unnecessary.
        server.beginZeoVerify()
        self._cache.verify(server.zeoVerify)
        server.endZeoVerify()

    ### Is there a race condition between notifyConnected and
    ### notifyDisconnected? In Particular, what if we get
    ### notifyDisconnected in the middle of notifyConnected?
    ### The danger is that we'll proceed as if we were connected
    ### without worrying if we were, but this would happen any way if
    ### notifyDisconnected had to get the instance lock.  There's
    ### nothing to gain by getting the instance lock.

    def notifyDisconnected(self):
        log2(PROBLEM, "Disconnected from storage")
        self._server = disconnected_stub

    def __len__(self):
        return self._info['length']

    def getName(self):
        return "%s (%s)" % (self.__name__, "XXX")

    def getSize(self):
        return self._info['size']

    def supportsUndo(self):
        return self._info['supportsUndo']

    def supportsVersions(self):
        return self._info['supportsVersions']

    def supportsTransactionalUndo(self):
        return self._info['supportsTransactionalUndo']

    def isReadOnly(self):
        return self._is_read_only

    def _check_trans(self, trans):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        if self._transaction is not trans:
            raise POSException.StorageTransactionError(self._transaction,
                                                       trans)

    def abortVersion(self, src, transaction):
        self._check_trans(transaction)
        oids = self._server.abortVersion(src, self._serial)
        # When a version aborts, invalidate the version and
        # non-version data.  The non-version data should still be
        # valid, but older versions of ZODB will change the
        # non-version serialno on an abort version.  With those
        # versions of ZODB, you'd get a conflict error if you tried to
        # commit a transaction with the cached data.

        # XXX If we could guarantee that ZODB gave the right answer,
        # we could just invalidate the version data.
        for oid in oids:
            self._tbuf.invalidate(oid, '')
        return oids

    def commitVersion(self, src, dest, transaction):
        self._check_trans(transaction)
        oids = self._server.commitVersion(src, dest, self._serial)
        if dest:
            # just invalidate our version data
            for oid in oids:
                self._tbuf.invalidate(oid, src)
        else:
            # dest is '', so invalidate version and non-version
            for oid in oids:
                self._tbuf.invalidate(oid, dest)
        return oids

    def history(self, oid, version, length=1):
        return self._server.history(oid, version, length)

    def loadSerial(self, oid, serial):
        return self._server.loadSerial(oid, serial)

    def load(self, oid, version, _stuff=None):
        p = self._cache.load(oid, version)
        if p:
            return p
        if self._server is None:
            raise ClientDisconnected()
        p, s, v, pv, sv = self._server.zeoLoad(oid)
        self._cache.checkSize(0)
        self._cache.store(oid, p, s, v, pv, sv)
        if v and version and v == version:
            return pv, sv
        else:
            if s:
                return p, s
            raise KeyError, oid # no non-version data for this

    def modifiedInVersion(self, oid):
        v = self._cache.modifiedInVersion(oid)
        if v is not None:
            return v
        return self._server.modifiedInVersion(oid)

    def new_oid(self, last=None):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        # avoid multiple oid requests to server at the same time
        self._oid_lock.acquire()
        try:
            if not self._oids:
                self._oids = self._server.new_oids()
                self._oids.reverse()
            return self._oids.pop()
        finally:
            self._oid_lock.release()

    def pack(self, t=None, rf=None, wait=0, days=0):
        # XXX Is it okay that read-only connections allow pack()?
        # rf argument ignored; server will provide it's own implementation
        if t is None:
            t = time.time()
        t = t - (days * 86400)
        return self._server.pack(t, wait)

    def _check_serials(self):
        if self._serials:
            l = len(self._serials)
            r = self._serials[:l]
            del self._serials[:l]
            for oid, s in r:
                if isinstance(s, Exception):
                    raise s
                self._seriald[oid] = s
            return r

    def store(self, oid, serial, data, version, transaction):
        self._check_trans(transaction)
        self._server.storea(oid, serial, data, version, self._serial)
        self._tbuf.store(oid, version, data)
        return self._check_serials()

    def tpc_vote(self, transaction):
        if transaction is not self._transaction:
            return
        self._server.vote(self._serial)
        return self._check_serials()

    def tpc_begin(self, transaction, tid=None, status=' '):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self._tpc_cond.acquire()
        while self._transaction is not None:
            # It is allowable for a client to call two tpc_begins in a
            # row with the same transaction, and the second of these
            # must be ignored.
            if self._transaction == transaction:
                self._tpc_cond.release()
                return
            self._tpc_cond.wait(30)
        self._transaction = transaction
        self._tpc_cond.release()

        if tid is None:
            self._ts = get_timestamp(self._ts)
            id = `self._ts`
        else:
            self._ts = TimeStamp(tid)
            id = tid

        try:
            r = self._server.tpc_begin(id,
                                       transaction.user,
                                       transaction.description,
                                       transaction._extension,
                                       tid, status)
        except:
            # Client may have disconnected during the tpc_begin().
            if self._server is not disconnected_stub:
                self.end_transaction()
            raise

        self._serial = id
        self._seriald.clear()
        del self._serials[:]

    def end_transaction(self):
        # the right way to set self._transaction to None
        # calls notify() on _tpc_cond in case there are waiting threads
        self._tpc_cond.acquire()
        self._transaction = None
        self._tpc_cond.notify()
        self._tpc_cond.release()

    def tpc_abort(self, transaction):
        if transaction is not self._transaction:
            return
        try:
            self._server.tpc_abort(self._serial)
            self._tbuf.clear()
            self._seriald.clear()
            del self._serials[:]
        finally:
            self.end_transaction()

    def tpc_finish(self, transaction, f=None):
        if transaction is not self._transaction:
            return
        try:
            if f is not None:
                f()

            self._server.tpc_finish(self._serial)

            r = self._check_serials()
            assert r is None or len(r) == 0, "unhandled serialnos: %s" % r

            self._update_cache()
        finally:
            self.end_transaction()

    def _update_cache(self):
        # Iterate over the objects in the transaction buffer and
        # update or invalidate the cache.
        self._cache.checkSize(self._tbuf.get_size())
        try:
            self._tbuf.begin_iterate()
        except ValueError, msg:
            raise ClientStorageError, (
                "Unexpected error reading temporary file in "
                "client storage: %s" % msg)
        while 1:
            try:
                t = self._tbuf.next()
            except ValueError, msg:
                raise ClientStorageError, (
                    "Unexpected error reading temporary file in "
                    "client storage: %s" % msg)
            if t is None:
                break
            oid, v, p = t
            if p is None: # an invalidation
                s = None
            else:
                s = self._seriald[oid]
            if s == ResolvedSerial or s is None:
                self._cache.invalidate(oid, v)
            else:
                self._cache.update(oid, s, v, p)
        self._tbuf.clear()

    def transactionalUndo(self, trans_id, trans):
        self._check_trans(trans)
        oids = self._server.transactionalUndo(trans_id, self._serial)
        for oid in oids:
            self._tbuf.invalidate(oid, '')
        return oids

    def undo(self, transaction_id):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        # XXX what are the sync issues here?
        oids = self._server.undo(transaction_id)
        for oid in oids:
            self._cache.invalidate(oid, '')
        return oids

    def undoInfo(self, first=0, last=-20, specification=None):
        return self._server.undoInfo(first, last, specification)

    def undoLog(self, first, last, filter=None):
        if filter is not None:
            return () # can't pass a filter to server

        return self._server.undoLog(first, last) # Eek!

    def versionEmpty(self, version):
        return self._server.versionEmpty(version)

    def versions(self, max=None):
        return self._server.versions(max)

    # below are methods invoked by the StorageServer

    def serialnos(self, args):
        self._serials.extend(args)

    def info(self, dict):
        self._info.update(dict)

    def beginVerify(self):
        self._tfile = tempfile.TemporaryFile(suffix=".inv")
        self._pickler = cPickle.Pickler(self._tfile, 1)
        self._pickler.fast = 1 # Don't use the memo

    def invalidateVerify(self, args):
        # Invalidation as result of verify_cache().
        # Queue an invalidate for the end the verification procedure.
        if self._pickler is None:
            # XXX This should never happen
            return
        self._pickler.dump(args)

    def endVerify(self):
        if self._pickler is None:
            return
        self._pickler.dump((0,0))
        self._tfile.seek(0)
        unpick = cPickle.Unpickler(self._tfile)
        f = self._tfile
        self._tfile = None

        while 1:
            oid, version = unpick.load()
            if not oid:
                break
            self._cache.invalidate(oid, version=version)
            self._db.invalidate(oid, version=version)
        f.close()

    def invalidateTrans(self, args):
        # Invalidation as a result of a transaction.
        for oid, version in args:
            self._cache.invalidate(oid, version=version)
            try:
                self._db.invalidate(oid, version=version)
            except AttributeError, msg:
                log2(PROBLEM,
                    "Invalidate(%s, %s) failed for _db: %s" % (repr(oid),
                                                               repr(version),
                                                               msg))

    # Unfortunately, the ZEO 2 wire protocol uses different names for
    # several of the callback methods invoked by the StorageServer.
    # We can't change the wire protocol at this point because that
    # would require synchronized updates of clients and servers and we
    # don't want that.  So here we alias the old names to their new
    # implementations.

    begin = beginVerify
    invalidate = invalidateVerify
    end = endVerify
    Invalidate = invalidateTrans
