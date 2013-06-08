##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Storage base class that is mostly a mistake

The base class here is tightly coupled with its subclasses and
its use is not recommended.  It's still here for historical reasons.
"""
from __future__ import print_function

import threading
import time
import logging
import sys
from struct import pack as _structpack, unpack as _structunpack

import zope.interface
from persistent.TimeStamp import TimeStamp

import ZODB.interfaces
from ZODB import POSException
from ZODB.utils import z64, oid_repr, byte_ord, byte_chr
from ZODB.UndoLogCompatible import UndoLogCompatible
from ZODB._compat import dumps, _protocol, py2_hasattr

log = logging.getLogger("ZODB.BaseStorage")



class BaseStorage(UndoLogCompatible):
    """Base class that supports storage implementations.

    XXX Base classes like this are an attractive nuisance. They often
    introduce more complexity than they save.  While important logic
    is implemented here, we should consider exposing it as utility
    functions or as objects that can be used through composition.

    A subclass must define the following methods:
    load()
    store()
    close()
    cleanup()
    lastTransaction()

    It must override these hooks:
    _begin()
    _vote()
    _abort()
    _finish()
    _clear_temp()

    If it stores multiple revisions, it should implement
    loadSerial()
    loadBefore()

    Each storage will have two locks that are accessed via lock
    acquire and release methods bound to the instance.  (Yuck.)
    _lock_acquire / _lock_release (reentrant)
    _commit_lock_acquire / _commit_lock_release

    The commit lock is acquired in tpc_begin() and released in
    tpc_abort() and tpc_finish().  It is never acquired with the other
    lock held.

    The other lock appears to protect _oid and _transaction and
    perhaps other things.  It is always held when load() is called, so
    presumably the load() implementation should also acquire the lock.
    """
    _transaction=None # Transaction that is being committed
    _tstatus=' '      # Transaction status, used for copying data
    _is_read_only = False

    def __init__(self, name, base=None):
        self.__name__= name
        log.debug("create storage %s", self.__name__)

        # Allocate locks:
        self._lock = threading.RLock()
        self.__commit_lock = threading.Lock()

        # Comment out the following 4 lines to debug locking:
        self._lock_acquire = self._lock.acquire
        self._lock_release = self._lock.release
        self._commit_lock_acquire = self.__commit_lock.acquire
        self._commit_lock_release = self.__commit_lock.release

        t = time.time()
        t = self._ts = TimeStamp(*(time.gmtime(t)[:5] + (t%60,)))
        self._tid = t.raw()

        # ._oid is the highest oid in use (0 is always in use -- it's
        # a reserved oid for the root object).  Our new_oid() method
        # increments it by 1, and returns the result.  It's really a
        # 64-bit integer stored as an 8-byte big-endian string.
        oid = getattr(base, '_oid', None)
        if oid is None:
            self._oid = z64
        else:
            self._oid = oid

    ########################################################################
    # The following methods are normally overridden on instances,
    # except when debugging:

    def _lock_acquire(self, *args):
        f = sys._getframe(1)
        sys.stdout.write("[la(%s:%s)\n" % (f.f_code.co_filename, f.f_lineno))
        sys.stdout.flush()
        self._lock.acquire(*args)
        sys.stdout.write("la(%s:%s)]\n" % (f.f_code.co_filename, f.f_lineno))
        sys.stdout.flush()

    def _lock_release(self, *args):
        f = sys._getframe(1)
        sys.stdout.write("[lr(%s:%s)\n" % (f.f_code.co_filename, f.f_lineno))
        sys.stdout.flush()
        self._lock.release(*args)
        sys.stdout.write("lr(%s:%s)]\n" % (f.f_code.co_filename, f.f_lineno))
        sys.stdout.flush()

    def _commit_lock_acquire(self, *args):
        f = sys._getframe(1)
        sys.stdout.write("[ca(%s:%s)\n" % (f.f_code.co_filename, f.f_lineno))
        sys.stdout.flush()
        self.__commit_lock.acquire(*args)
        sys.stdout.write("ca(%s:%s)]\n" % (f.f_code.co_filename, f.f_lineno))
        sys.stdout.flush()

    def _commit_lock_release(self, *args):
        f = sys._getframe(1)
        sys.stdout.write("[cr(%s:%s)\n" % (f.f_code.co_filename, f.f_lineno))
        sys.stdout.flush()
        self.__commit_lock.release(*args)
        sys.stdout.write("cr(%s:%s)]\n" % (f.f_code.co_filename, f.f_lineno))
        sys.stdout.flush()

    #
    ########################################################################

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
        return ()

    def new_oid(self):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self._lock_acquire()
        try:
            last = self._oid
            d = byte_ord(last[-1])
            if d < 255:  # fast path for the usual case
                last = last[:-1] + byte_chr(d+1)
            else:        # there's a carry out of the last byte
                last_as_long, = _structunpack(">Q", last)
                last = _structpack(">Q", last_as_long + 1)
            self._oid = last
            return last
        finally:
            self._lock_release()

    # Update the maximum oid in use, under protection of a lock.  The
    # maximum-in-use attribute is changed only if possible_new_max_oid is
    # larger than its current value.
    def set_max_oid(self, possible_new_max_oid):
        self._lock_acquire()
        try:
            if possible_new_max_oid > self._oid:
                self._oid = possible_new_max_oid
        finally:
            self._lock_release()

    def registerDB(self, db):
        pass # we don't care

    def isReadOnly(self):
        return self._is_read_only

    def tpc_abort(self, transaction):
        self._lock_acquire()
        try:
            if transaction is not self._transaction:
                return
            try:
                self._abort()
                self._clear_temp()
                self._transaction = None
            finally:
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
                raise POSException.StorageTransactionError(
                    "Duplicate tpc_begin calls for same transaction")
            self._lock_release()
            self._commit_lock_acquire()
            self._lock_acquire()
            self._transaction = transaction
            self._clear_temp()

            user = transaction.user
            desc = transaction.description
            ext = transaction._extension
            if ext:
                ext = dumps(ext, _protocol)
            else:
                ext = ""

            self._ude = user, desc, ext

            if tid is None:
                now = time.time()
                t = TimeStamp(*(time.gmtime(now)[:5] + (now % 60,)))
                self._ts = t = t.laterThan(self._ts)
                self._tid = t.raw()
            else:
                self._ts = TimeStamp(tid)
                self._tid = tid

            self._tstatus = status
            self._begin(self._tid, user, desc, ext)
        finally:
            self._lock_release()

    def tpc_transaction(self):
        return self._transaction

    def _begin(self, tid, u, d, e):
        """Subclasses should redefine this to supply transaction start actions.
        """
        pass

    def tpc_vote(self, transaction):
        self._lock_acquire()
        try:
            if transaction is not self._transaction:
                raise POSException.StorageTransactionError(
                    "tpc_vote called with wrong transaction")
            self._vote()
        finally:
            self._lock_release()

    def _vote(self):
        """Subclasses should redefine this to supply transaction vote actions.
        """
        pass

    def tpc_finish(self, transaction, f=None):
        # It's important that the storage calls the function we pass
        # while it still has its lock.  We don't want another thread
        # to be able to read any updated data until we've had a chance
        # to send an invalidation message to all of the other
        # connections!

        self._lock_acquire()
        try:
            if transaction is not self._transaction:
                raise POSException.StorageTransactionError(
                    "tpc_finish called with wrong transaction")
            try:
                if f is not None:
                    f(self._tid)
                u, d, e = self._ude
                self._finish(self._tid, u, d, e)
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

    def lastTransaction(self):
        with self._lock:
            return self._ltid

    def getTid(self, oid):
        self._lock_acquire()
        try:
            v = ''
            try:
                supportsVersions = self.supportsVersions
            except AttributeError:
                pass
            else:
                if supportsVersions():
                    v = self.modifiedInVersion(oid)
            pickledata, serial = self.load(oid, v)
            return serial
        finally:
            self._lock_release()

    def loadSerial(self, oid, serial):
        raise POSException.Unsupported(
            "Retrieval of historical revisions is not supported")

    def loadBefore(self, oid, tid):
        """Return most recent revision of oid before tid committed."""
        return None

    def copyTransactionsFrom(self, other, verbose=0):
        """Copy transactions from another storage.

        This is typically used for converting data from one storage to
        another.  `other` must have an .iterator() method.
        """
        copy(other, self, verbose)

def copy(source, dest, verbose=0):
    """Copy transactions from a source to a destination storage

    This is typically used for converting data from one storage to
    another.  `source` must have an .iterator() method.
    """
    _ts = None
    ok = 1
    preindex = {};
    preget = preindex.get
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
    restoring = py2_hasattr(dest, 'restore')
    fiter = source.iterator()
    for transaction in fiter:
        tid = transaction.tid
        if _ts is None:
            _ts = TimeStamp(tid)
        else:
            t = TimeStamp(tid)
            if t <= _ts:
                if ok: print(('Time stamps out of order %s, %s' % (_ts, t)))
                ok = 0
                _ts = t.laterThan(_ts)
                tid = _ts.raw()
            else:
                _ts = t
                if not ok:
                    print(('Time stamps back in order %s' % (t)))
                    ok = 1

        if verbose:
            print(_ts)

        dest.tpc_begin(transaction, tid, transaction.status)
        for r in transaction:
            oid = r.oid
            if verbose:
                print(oid_repr(oid), r.version, len(r.data))
            if restoring:
                dest.restore(oid, r.tid, r.data, r.version,
                             r.data_txn, transaction)
            else:
                pre = preget(oid, None)
                s = dest.store(oid, pre, r.data, r.version, transaction)
                preindex[oid] = s

        dest.tpc_vote(transaction)
        dest.tpc_finish(transaction)


# defined outside of BaseStorage to facilitate independent reuse.
# just depends on _transaction attr and getTid method.
def checkCurrentSerialInTransaction(self, oid, serial, transaction):
    if transaction is not self._transaction:
        raise POSException.StorageTransactionError(self, transaction)

    committed_tid = self.getTid(oid)
    if committed_tid != serial:
        raise POSException.ReadConflictError(
            oid=oid, serials=(committed_tid, serial))

BaseStorage.checkCurrentSerialInTransaction = checkCurrentSerialInTransaction

@zope.interface.implementer(ZODB.interfaces.IStorageTransactionInformation)
class TransactionRecord(object):
    """Abstract base class for iterator protocol"""


    def __init__(self, tid, status, user, description, extension):
        self.tid = tid
        self.status = status
        self.user = user
        self.description = description
        self.extension = extension

    # XXX This is a workaround to make the TransactionRecord compatible with a
    # transaction object because it is passed to tpc_begin().
    def _ext_set(self, value):
        self.extension = value
    def _ext_get(self):
        return self.extension
    _extension = property(fset=_ext_set, fget=_ext_get)


@zope.interface.implementer(ZODB.interfaces.IStorageRecordInformation)
class DataRecord(object):
    """Abstract base class for iterator protocol"""


    version = ''

    def __init__(self, oid, tid, data, prev):
        self.oid = oid
        self.tid = tid
        self.data = data
        self.data_txn = prev
