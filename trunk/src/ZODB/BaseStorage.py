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
"""
# Do this portably in the face of checking out with -kv
import string
__version__ = string.split('$Revision: 1.28 $')[-2:][0]

import cPickle
import ThreadLock, bpthread
import time, UndoLogCompatible
import POSException
from TimeStamp import TimeStamp
z64='\0'*8

class BaseStorage(UndoLogCompatible.UndoLogCompatible):
    _transaction=None # Transaction that is being committed
    _serial=z64       # Transaction serial number
    _tstatus=' '      # Transaction status, used for copying data
    _is_read_only = 0

    def __init__(self, name, base=None):

        self.__name__=name

        # Allocate locks:
        l=ThreadLock.allocate_lock()
        self._lock_acquire=l.acquire
        self._lock_release=l.release
        l=bpthread.allocate_lock()
        self._commit_lock_acquire=l.acquire
        self._commit_lock_release=l.release

        t=time.time()
        t=self._ts=apply(TimeStamp,(time.gmtime(t)[:5]+(t%60,)))
        self._serial=`t`
        if base is None:
            self._oid='\0\0\0\0\0\0\0\0'
        else:
            self._oid=base._oid

    def abortVersion(self, src, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        return []

    def commitVersion(self, src, dest, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        return []

    def close(self):
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

    def history(self, oid, version, length=1):
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
                self._serial = `t`
            else:
                self._ts = TimeStamp(tid)
                self._serial = tid

            self._tstatus = status
            self._begin(self._serial, user, desc, ext)
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
                    f()
                u, d, e = self._ude
                self._finish(self._serial, u, d, e)
                self._clear_temp()
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

    def undo(self, transaction_id):
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

        This is typically used for converting data from one storage to another.
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
        for transaction in other.iterator():

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

            if verbose: print _ts

            self.tpc_begin(transaction, tid, transaction.status)
            for r in transaction:
                oid=r.oid
                if verbose: print `oid`, r.version, len(r.data)
                if restoring:
                    self.restore(oid, r.serial, r.data, r.version,
                                 r.data_txn, transaction)
                else:
                    pre=preget(oid, None)
                    s=self.store(oid, pre, r.data, r.version, transaction)
                    preindex[oid]=s

            self.tpc_vote(transaction)
            self.tpc_finish(transaction)

class TransactionRecord:
    """Abstract base class for iterator protocol"""

class DataRecord:
    """Abstract base class for iterator protocol"""
