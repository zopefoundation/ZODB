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
"""Handy standard storage machinery

$Id: BaseStorage.py,v 1.42 2004/02/19 18:51:03 jeremy Exp $
"""
import cPickle
import threading
import time

import UndoLogCompatible
import POSException
from persistent.TimeStamp import TimeStamp

import zLOG
from ZODB import POSException
from ZODB.UndoLogCompatible import UndoLogCompatible
from ZODB.utils import z64

class BaseStorage(UndoLogCompatible):
    _transaction=None # Transaction that is being committed
    _tstatus=' '      # Transaction status, used for copying data
    _is_read_only = 0

    def __init__(self, name, base=None):
        self.__name__= name
        zLOG.LOG(self.__class__.__name__, zLOG.DEBUG,
                 "create storage %s" % self.__name__)

        # Allocate locks:
        l = threading.RLock()
        self._lock_acquire = l.acquire
        self._lock_release = l.release
        l = threading.Lock()
        self._commit_lock_acquire = l.acquire
        self._commit_lock_release = l.release

        t=time.time()
        t=self._ts=apply(TimeStamp,(time.gmtime(t)[:5]+(t%60,)))
        self._tid = `t`
        if base is None:
            self._oid='\0\0\0\0\0\0\0\0'
        else:
            self._oid=base._oid

    def abortVersion(self, src, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        return self._tid, []

    def commitVersion(self, src, dest, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        return self._tid, []

    def close(self):
        pass

    def cleanup(self):
        pass

    def sortKey(self):
        """Return a string that can be used to sort storage instances.

        The key must uniquely identify a storage and must be the same
        across multiple instantiations of the same storage.
        """
        # name may not be sufficient, e.g. ZEO has a user-definable name.
        return self.__name__

    def getName(self):
        return self.__name__

    def getSize(self):
        return len(self)*300 # WAG!

    def history(self, oid, version, length=1, filter=None):
        pass

    def modifiedInVersion(self, oid):
        return ''

    def new_oid(self, last=None):
        # 'last' is only for internal use, not part of the public API
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        if last is None:
            self._lock_acquire()
            try:
                last=self._oid
                d=ord(last[-1])
                if d < 255: last=last[:-1]+chr(d+1)
                else:       last=self.new_oid(last[:-1])
                self._oid=last
                return last
            finally: self._lock_release()
        else:
            d=ord(last[-1])
            if d < 255: return last[:-1]+chr(d+1)+'\0'*(8-len(last))
            else:       return self.new_oid(last[:-1])

    def registerDB(self, db, limit):
        pass # we don't care

    def isReadOnly(self):
        return self._is_read_only

    def supportsUndo(self):
        return 0

    def supportsVersions(self):
        return 0

    def tpc_abort(self, transaction):
        self._lock_acquire()
        try:
            if transaction is not self._transaction:
                return
            self._abort()
            self._clear_temp()
            self._transaction = None
            self._commit_lock_release()
        finally:
            self._lock_release()

    def _abort(self):
        """Subclasses should redefine this to supply abort actions"""
        pass

    def tpc_begin(self, transaction, tid=None, status=' '):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self._lock_acquire()
        try:
            if self._transaction is transaction:
                return
            self._lock_release()
            self._commit_lock_acquire()
            self._lock_acquire()
            self._transaction = transaction
            self._clear_temp()

            user = transaction.user
            desc = transaction.description
            ext = transaction._extension
            if ext:
                ext = cPickle.dumps(ext, 1)
            else:
                ext = ""
            self._ude = user, desc, ext

            if tid is None:
                now = time.time()
                t = TimeStamp(*(time.gmtime(now)[:5] + (now % 60,)))
                self._ts = t = t.laterThan(self._ts)
                self._tid = `t`
            else:
                self._ts = TimeStamp(tid)
                self._tid = tid

            self._tstatus = status
            self._begin(self._tid, user, desc, ext)
        finally:
            self._lock_release()

    def _begin(self, tid, u, d, e):
        """Subclasses should redefine this to supply transaction start actions.
        """
        pass

    def tpc_vote(self, transaction):
        self._lock_acquire()
        try:
            if transaction is not self._transaction:
                return
            self._vote()
        finally:
            self._lock_release()

    def _vote(self):
        """Subclasses should redefine this to supply transaction vote actions.
        """
        pass

    def tpc_finish(self, transaction, f=None):
        self._lock_acquire()
        try:
            if transaction is not self._transaction:
                return
            try:
                if f is not None:
                    f(self._tid)
                u, d, e = self._ude
                self._finish(self._tid, u, d, e)
                self._clear_temp()
                return self._tid
            finally:
                self._ude = None
                self._transaction = None
                self._commit_lock_release()
        finally:
            self._lock_release()

    def _finish(self, tid, u, d, e):
        """Subclasses should redefine this to supply transaction finish actions
        """
        pass

    def undo(self, transaction_id, txn):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        raise POSException.UndoError, 'non-undoable transaction'

    def undoLog(self, first, last, filter=None):
        return ()

    def versionEmpty(self, version):
        return 1

    def versions(self, max=None):
        return ()

    def pack(self, t, referencesf):
        if self._is_read_only:
            raise POSException.ReadOnlyError()

    def getSerial(self, oid):
        self._lock_acquire()
        try:
            v = self.modifiedInVersion(oid)
            pickledata, serial = self.load(oid, v)
            return serial
        finally:
            self._lock_release()

    def loadSerial(self, oid, serial):
        raise POSException.Unsupported, (
            "Retrieval of historical revisions is not supported")

    def loadBefore(self, oid, tid):
        """Return most recent revision of oid before tid committed."""

        # XXX Is it okay for loadBefore() to return current data?
        # There doesn't seem to be a good reason to forbid it, even
        # though the typical use of this method will never find
        # current data.  But maybe we should call it loadByTid()?

        n = 2
        start_time = None
        end_time = None
        while start_time is None:
            # The history() approach is a hack, because the dict
            # returned by history() doesn't contain a tid.  It
            # contains a serialno, which is often the same, but isn't
            # required to be.  We'll pretend it is for now.

            # A second problem is that history() doesn't say anything
            # about whether the transaction status.  If it falls before
            # the pack time, we can't honor the MVCC request.

            # Note: history() returns the most recent record first.

            # XXX The filter argument to history() only appears to be
            # supported by FileStorage.  Perhaps it shouldn't be used.
            L = self.history(oid, "", n, lambda d: not d["version"])
            if not L:
                return
            for d in L:
                if d["serial"] < tid:
                    start_time = d["serial"]
                    break
                else:
                    end_time = d["serial"]
            if len(L) < n:
                break
            n *= 2
        if start_time is None:
            return None
        data = self.loadSerial(oid, start_time)
        return data, start_time, end_time

    def getExtensionMethods(self):
        """getExtensionMethods

        This returns a dictionary whose keys are names of extra methods
        provided by this storage. Storage proxies (such as ZEO) should
        call this method to determine the extra methods that they need
        to proxy in addition to the standard storage methods.
        Dictionary values should be None; this will be a handy place
        for extra marshalling information, should we need it
        """
        return {}

    def copyTransactionsFrom(self, other, verbose=0):
        """Copy transactions from another storage.

        This is typically used for converting data from one storage to
        another.  `other' must have an .iterator() method.
        """
        _ts=None
        ok=1
        preindex={};
        preget=preindex.get   # waaaa
        # restore() is a new storage API method which has an identical
        # signature to store() except that it does not return anything.
        # Semantically, restore() is also identical to store() except that it
        # doesn't do the ConflictError or VersionLockError consistency
        # checks.  The reason to use restore() over store() in this method is
        # that store() cannot be used to copy transactions spanning a version
        # commit or abort, or over transactional undos.
        #
        # We'll use restore() if it's available, otherwise we'll fall back to
        # using store().  However, if we use store, then
        # copyTransactionsFrom() may fail with VersionLockError or
        # ConflictError.
        if hasattr(self, 'restore'):
            restoring = 1
        else:
            restoring = 0
        fiter = other.iterator()
        for transaction in fiter:
            tid=transaction.tid
            if _ts is None:
                _ts=TimeStamp(tid)
            else:
                t=TimeStamp(tid)
                if t <= _ts:
                    if ok: print ('Time stamps out of order %s, %s' % (_ts, t))
                    ok=0
                    _ts=t.laterThan(_ts)
                    tid=`_ts`
                else:
                    _ts = t
                    if not ok:
                        print ('Time stamps back in order %s' % (t))
                        ok=1

            if verbose:
                print _ts

            self.tpc_begin(transaction, tid, transaction.status)
            for r in transaction:
                oid=r.oid
                if verbose: print oid_repr(oid), r.version, len(r.data)
                if restoring:
                    self.restore(oid, r.tid, r.data, r.version,
                                 r.data_txn, transaction)
                else:
                    pre=preget(oid, None)
                    s=self.store(oid, pre, r.data, r.version, transaction)
                    preindex[oid]=s

            self.tpc_vote(transaction)
            self.tpc_finish(transaction)

        fiter.close()

class TransactionRecord:
    """Abstract base class for iterator protocol"""

class DataRecord:
    """Abstract base class for iterator protocol"""
