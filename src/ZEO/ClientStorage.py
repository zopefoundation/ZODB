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

$Id: ClientStorage.py,v 1.46 2002/08/16 18:15:04 bwarsaw Exp $
"""

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

def log2(type, msg, subsys="ClientStorage %d" % os.getpid()):
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
                 wait=0, read_only=0):

        self._server = disconnected_stub
        self._is_read_only = read_only
        self._storage = storage

        self._info = {'length': 0, 'size': 0, 'name': 'ZEO Client',
                      'supportsUndo':0, 'supportsVersions': 0}

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
        self._cache.open() # XXX open now? or later?

        self._rpc_mgr = ConnectionManager(addr, self,
                                          tmin=min_disconnect_poll,
                                          tmax=max_disconnect_poll)

        # XXX What if we can only get a read-only connection and we
        # want a read-write connection?  Looks like the current code
        # will block forever.  (Future feature)
        if wait:
            self._rpc_mgr.connect(sync=1)
        else:
            if not self._rpc_mgr.attempt_connect():
                self._rpc_mgr.connect()

    def _basic_init(self, name):
        """Handle initialization activites of BaseStorage"""

        # XXX does anything depend on attr being __name__
        self.__name__ = name

        # A ClientStorage only allows one client to commit at a time.
        # Mutual exclusion is achieved using tpc_cond(), which
        # protects _transaction.  A thread that wants to assign to
        # self._transaction must acquire tpc_cond() first.
        
        # Invariant: If self._transaction is not None, then tpc_cond()
        # must be acquired.
        self.tpc_cond = threading.Condition()
        self._transaction = None

        # Prevent multiple new_oid calls from going out.  The _oids
        # variable should only be modified while holding the
        # oid_cond.
        self.oid_cond = threading.Condition()

        commit_lock = threading.Lock()
        self._commit_lock_acquire = commit_lock.acquire
        self._commit_lock_release = commit_lock.release

        t = self._ts = get_timestamp()
        self._serial = `t`
        self._oid='\0\0\0\0\0\0\0\0'

    def close(self):
        if self._tbuf is not None:
            self._tbuf.close()
        if self._cache is not None:
            self._cache.close()
        self._rpc_mgr.close()

    def registerDB(self, db, limit):
        """Register that the storage is controlled by the given DB."""
        log2(INFO, "registerDB(%s, %s)" % (repr(db), repr(limit)))
        self._db = db

    def is_connected(self):
        if self._server is disconnected_stub:
            return 0
        else:
            return 1

    def notifyConnected(self, c):
        log2(INFO, "Connected to storage via %s" % repr(c))

        # check the protocol version here?

        stub = ServerStub.StorageServer(c)

        self._oids = []

        # XXX Why is this synchronous?  If it were async, verification
        # would start faster.
        stub.register(str(self._storage), self._is_read_only)
        self._info.update(stub.get_info())
        self.verify_cache(stub)

        # Don't make the server available to clients until after
        # validating the cache
        self._server = stub

    def verify_cache(self, server):
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
        try:
            return self._info['supportsTransactionalUndo']
        except KeyError:
            return 0

    def isReadOnly(self):
        return self._is_read_only

    def _check_trans(self, trans, exc=None):
        if self._transaction is not trans:
            if exc is None:
                return 0
            else:
                raise exc(self._transaction, trans)
        return 1

    def abortVersion(self, src, transaction):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self._check_trans(transaction,
                          POSException.StorageTransactionError)
        oids = self._server.abortVersion(src, self._serial)
        for oid in oids:
            self._tbuf.invalidate(oid, src)
        return oids

    def commitVersion(self, src, dest, transaction):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self._check_trans(transaction,
                          POSException.StorageTransactionError)
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
        self.oid_cond.acquire()
        if not self._oids:
            self._oids = self._server.new_oids()
            self._oids.reverse()
            self.oid_cond.notifyAll()
        oid = self._oids.pop()
        self.oid_cond.release()
        return oid

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
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self._check_trans(transaction, POSException.StorageTransactionError)
        self._server.storea(oid, serial, data, version, self._serial)
        self._tbuf.store(oid, version, data)
        return self._check_serials()

    def tpc_vote(self, transaction):
        if transaction is not self._transaction:
            return
        self._server.vote(self._serial)
        return self._check_serials()

    def tpc_abort(self, transaction):
        if transaction is not self._transaction:
            return
        try:
            self._server.tpc_abort(self._serial)
            self._tbuf.clear()
            self._seriald.clear()
            del self._serials[:]
        finally:
            self.tpc_cond.acquire()
            self._transaction = None
            self.tpc_cond.notify()
            self.tpc_cond.release()

    def tpc_begin(self, transaction, tid=None, status=' '):
        self.tpc_cond.acquire()
        while self._transaction is not None:
            if self._transaction == transaction:
                # Our tpc_cond lock is re-entrant.  It is allowable for a
                # client to call two tpc_begins in a row with the same
                # transaction, and the second of these must be ignored.  Our
                # locking is safe because the acquire() above gives us a
                # second lock on tpc_cond, and the following release() brings
                # us back to owning just the one tpc_cond lock (acquired
                # during the first of two consecutive tpc_begins).
                self.tpc_cond.release()
                return
            self.tpc_cond.wait()
        self.tpc_cond.release()

        if self._server is None:
            self.tpc_cond.release()
            self._transaction = None
            raise ClientDisconnected()

        if tid is None:
            self._ts = get_timestamp(self._ts)
            id = `self._ts`
        else:
            self._ts = TimeStamp(tid)
            id = tid
        self._transaction = transaction

        try:
            r = self._server.tpc_begin(id,
                                       transaction.user,
                                       transaction.description,
                                       transaction._extension,
                                       tid, status)
        except:
            # Client may have disconnected during the tpc_begin().
            # Then notifyDisconnected() will have released the lock.
            if self._server is not disconnected_stub:
                self.tpc_cond.acquire()
                self._transaction = None
                self.tpc_cond.release()
            raise

        self._serial = id
        self._seriald.clear()
        del self._serials[:]

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
            self.tpc_cond.acquire()
            self._transaction = None
            self.tpc_cond.notify()
            self.tpc_cond.release()

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
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self._check_trans(trans, POSException.StorageTransactionError)
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

    def begin(self):
        self._tfile = tempfile.TemporaryFile(suffix=".inv")
        self._pickler = cPickle.Pickler(self._tfile, 1)
        self._pickler.fast = 1 # Don't use the memo

    def invalidate(self, args):
        # Queue an invalidate for the end the transaction
        if self._pickler is None:
            return
        self._pickler.dump(args)

    def end(self):
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

    def Invalidate(self, args):
        for oid, version in args:
            self._cache.invalidate(oid, version=version)
            try:
                self._db.invalidate(oid, version=version)
            except AttributeError, msg:
                log2(PROBLEM,
                    "Invalidate(%s, %s) failed for _db: %s" % (repr(oid),
                                                               repr(version),
                                                               msg))
