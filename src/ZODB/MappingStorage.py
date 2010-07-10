##############################################################################
#
# Copyright (c) Zope Foundation and Contributors.
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
"""A simple in-memory mapping-based ZODB storage

This storage provides an example implementation of a fairly full
storage without distracting storage details.
"""

import BTrees
import cPickle
import time
import threading
import ZODB.interfaces
import ZODB.POSException
import ZODB.TimeStamp
import ZODB.utils
import zope.interface

class MappingStorage(object):

    zope.interface.implements(
        ZODB.interfaces.IStorage,
        ZODB.interfaces.IStorageIteration,
        )

    def __init__(self, name='MappingStorage'):
        self.__name__ = name
        self._data = {}                               # {oid->{tid->pickle}}
        self._transactions = BTrees.OOBTree.OOBTree() # {tid->TransactionRecord}
        self._ltid = None
        self._last_pack = None
        _lock = threading.RLock()
        self._lock_acquire = _lock.acquire
        self._lock_release = _lock.release
        self._commit_lock = threading.Lock()
        self._opened = True
        self._transaction = None
        self._oid = 0

    ######################################################################
    # Preconditions:

    def opened(self):
        """The storage is open
        """
        return self._opened

    def not_in_transaction(self):
        """The storage is not committing a transaction
        """
        return self._transaction is None

    #
    ######################################################################

    # testing framework (lame)
    def cleanup(self):
        pass

    # ZODB.interfaces.IStorage
    @ZODB.utils.locked
    def close(self):
        self._opened = False

    # ZODB.interfaces.IStorage
    def getName(self):
        return self.__name__

    # ZODB.interfaces.IStorage
    @ZODB.utils.locked(opened)
    def getSize(self):
        size = 0
        for oid, tid_data in self._data.items():
            size += 50
            for tid, pickle in tid_data.items():
                size += 100+len(pickle)
        return size

    # ZEO.interfaces.IServeable
    @ZODB.utils.locked(opened)
    def getTid(self, oid):
        tid_data = self._data.get(oid)
        if tid_data:
            return tid_data.maxKey()
        raise ZODB.POSException.POSKeyError(oid)

    # ZODB.interfaces.IStorage
    @ZODB.utils.locked(opened)
    def history(self, oid, size=1):
        tid_data = self._data.get(oid)
        if not tid_data:
            raise ZODB.POSException.POSKeyError(oid)

        tids = tid_data.keys()[-size:]
        tids.reverse()
        return [
            dict(
                time = ZODB.TimeStamp.TimeStamp(tid),
                tid = tid,
                serial = tid,
                user_name = self._transactions[tid].user,
                description = self._transactions[tid].description,
                extension = self._transactions[tid].extension,
                size = len(tid_data[tid])
                )
            for tid in tids]

    # ZODB.interfaces.IStorage
    def isReadOnly(self):
        return False

    # ZODB.interfaces.IStorageIteration
    def iterator(self, start=None, end=None):
        for transaction_record in self._transactions.values(start, end):
            yield transaction_record

    # ZODB.interfaces.IStorage
    @ZODB.utils.locked(opened)
    def lastTransaction(self):
        if self._ltid is not None:
            return self._ltid

    # ZODB.interfaces.IStorage
    @ZODB.utils.locked(opened)
    def __len__(self):
        return len(self._data)

    # ZODB.interfaces.IStorage
    @ZODB.utils.locked(opened)
    def load(self, oid, version=''):
        assert not version, "Versions are not supported"
        tid_data = self._data.get(oid)
        if tid_data:
            tid = tid_data.maxKey()
            return tid_data[tid], tid
        raise ZODB.POSException.POSKeyError(oid)

    # ZODB.interfaces.IStorage
    @ZODB.utils.locked(opened)
    def loadBefore(self, oid, tid):
        tid_data = self._data.get(oid)
        if tid_data:
            before = ZODB.utils.u64(tid)
            if not before:
                return None
            before = ZODB.utils.p64(before-1)
            tids_before = tid_data.keys(None, before)
            if tids_before:
                tids_after = tid_data.keys(tid, None)
                tid = tids_before[-1]
                return (tid_data[tid], tid,
                        (tids_after and tids_after[0] or None)
                        )
        else:
            raise ZODB.POSException.POSKeyError(oid)


    # ZODB.interfaces.IStorage
    @ZODB.utils.locked(opened)
    def loadSerial(self, oid, serial):
        tid_data = self._data.get(oid)
        if tid_data:
            try:
                return tid_data[serial]
            except KeyError:
                pass

        raise ZODB.POSException.POSKeyError(oid, serial)

    # ZODB.interfaces.IStorage
    @ZODB.utils.locked(opened)
    def new_oid(self):
        self._oid += 1
        return ZODB.utils.p64(self._oid)

    # ZODB.interfaces.IStorage
    @ZODB.utils.locked(opened)
    def pack(self, t, referencesf, gc=True):
        if not self._data:
            return

        stop = `ZODB.TimeStamp.TimeStamp(*time.gmtime(t)[:5]+(t%60,))`
        if self._last_pack is not None and self._last_pack >= stop:
            if self._last_pack == stop:
                return
            raise ValueError("Already packed to a later time")

        self._last_pack = stop
        transactions = self._transactions

        # Step 1, remove old non-current records
        for oid, tid_data in self._data.items():
            tids_to_remove = tid_data.keys(None, stop)
            if tids_to_remove:
                tids_to_remove.pop()    # Keep the last, if any

                if tids_to_remove:
                    for tid in tids_to_remove:
                        del tid_data[tid]
                        if transactions[tid].pack(oid):
                            del transactions[tid]

        if gc:
            # Step 2, GC.  A simple sweep+copy
            new_data = BTrees.OOBTree.OOBTree()
            to_copy = set([ZODB.utils.z64])
            while to_copy:
                oid = to_copy.pop()
                tid_data = self._data.pop(oid)
                new_data[oid] = tid_data
                for pickle in tid_data.values():
                    for oid in referencesf(pickle):
                        if oid in new_data:
                            continue
                        to_copy.add(oid)

            # Remove left over data from transactions
            for oid, tid_data in self._data.items():
                for tid in tid_data:
                    if transactions[tid].pack(oid):
                        del transactions[tid]

            self._data.clear()
            self._data.update(new_data)

    # ZODB.interfaces.IStorage
    def registerDB(self, db):
        pass

    # ZODB.interfaces.IStorage
    def sortKey(self):
        return self.__name__

    # ZODB.interfaces.IStorage
    @ZODB.utils.locked(opened)
    def store(self, oid, serial, data, version, transaction):
        assert not version, "Versions are not supported"
        if transaction is not self._transaction:
            raise ZODB.POSException.StorageTransactionError(self, transaction)

        old_tid = None
        tid_data = self._data.get(oid)
        if tid_data:
            old_tid = tid_data.maxKey()
            if serial != old_tid:
                raise ZODB.POSException.ConflictError(
                    oid=oid, serials=(old_tid, serial), data=data)

        self._tdata[oid] = data

        return self._tid

    # ZODB.interfaces.IStorage
    @ZODB.utils.locked(opened)
    def tpc_abort(self, transaction):
        if transaction is not self._transaction:
            return
        self._transaction = None
        self._commit_lock.release()

    # ZODB.interfaces.IStorage
    @ZODB.utils.locked(opened)
    def tpc_begin(self, transaction, tid=None):
        # The tid argument exists to support testing.
        if transaction is self._transaction:
            raise ZODB.POSException.StorageTransactionError(
                "Duplicate tpc_begin calls for same transaction")
        self._lock_release()
        self._commit_lock.acquire()
        self._lock_acquire()
        self._transaction = transaction
        self._tdata = {}
        if tid is None:
            if self._transactions:
                old_tid = self._transactions.maxKey()
            else:
                old_tid = None
            tid = ZODB.utils.newTid(old_tid)
        self._tid = tid

    # ZODB.interfaces.IStorage
    @ZODB.utils.locked(opened)
    def tpc_finish(self, transaction, func = lambda tid: None):
        if (transaction is not self._transaction):
            raise ZODB.POSException.StorageTransactionError(
                "tpc_finish called with wrong transaction")

        tid = self._tid
        func(tid)

        tdata = self._tdata
        for oid in tdata:
            tid_data = self._data.get(oid)
            if tid_data is None:
                tid_data = BTrees.OOBTree.OOBucket()
                self._data[oid] = tid_data
            tid_data[tid] = tdata[oid]

        self._ltid = tid
        self._transactions[tid] = TransactionRecord(tid, transaction, tdata)
        self._transaction = None
        del self._tdata
        self._commit_lock.release()

    # ZEO.interfaces.IServeable
    @ZODB.utils.locked(opened)
    def tpc_transaction(self):
        return self._transaction

    # ZODB.interfaces.IStorage
    def tpc_vote(self, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(
                "tpc_vote called with wrong transaction")

class TransactionRecord:

    status = ' '

    def __init__(self, tid, transaction, data):
        self.tid = tid
        self.user = transaction.user
        self.description = transaction.description
        extension = transaction._extension
        self.extension = extension
        self.data = data

    _extension = property(lambda self: self._extension,
                          lambda self, v: setattr(self, '_extension', v),
                          )

    def __iter__(self):
        for oid, data in self.data.items():
            yield DataRecord(oid, self.tid, data)

    def pack(self, oid):
        self.status = 'p'
        del self.data[oid]
        return not self.data

class DataRecord(object):
    """Abstract base class for iterator protocol"""

    zope.interface.implements(ZODB.interfaces.IStorageRecordInformation)

    version = ''
    data_txn = None

    def __init__(self, oid, tid, data):
        self.oid = oid
        self.tid = tid
        self.data = data

def DB(*args, **kw):
    return ZODB.DB(MappingStorage(), *args, **kw)
