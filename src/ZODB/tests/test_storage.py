##############################################################################
#
# Copyright (c) 2004 Zope Foundation and Contributors.
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
"""A storage used for unittests.

The primary purpose of this module is to have a minimal multi-version
storage to use for unit tests.  MappingStorage isn't sufficient.
Since even a minimal storage has some complexity, we run standard
storage tests against the test storage.
"""
import bisect
import unittest
import warnings

from ZODB.BaseStorage import BaseStorage
from ZODB import POSException
from ZODB.utils import p64, u64, z64

from ZODB.tests import StorageTestBase
from ZODB.tests import BasicStorage, MTStorage, Synchronization
from ZODB.tests import RevisionStorage

class Transaction(object):
    """Hold data for current transaction for MinimalMemoryStorage."""

    def __init__(self, tid):
        self.index = {}
        self.tid = tid

    def store(self, oid, data):
        self.index[(oid, self.tid)] = data

    def cur(self):
        return dict.fromkeys([oid for oid, tid in self.index.keys()], self.tid)

class MinimalMemoryStorage(BaseStorage, object):
    """Simple in-memory storage that supports revisions.

    This storage is needed to test multi-version concurrency control.
    It is similar to MappingStorage, but keeps multiple revisions.  It
    does not support versions.  It doesn't implement operations like
    pack(), because they aren't necessary for testing.
    """

    def __init__(self):
        super(MinimalMemoryStorage, self).__init__("name")
        # _index maps oid, tid pairs to data records
        self._index = {}
        # _cur maps oid to current tid
        self._cur = {}

        self._ltid = z64

    def isCurrent(self, oid, serial):
        return serial == self._cur[oid]

    def hook(self, oid, tid, version):
        # A hook for testing
        pass

    def __len__(self):
        return len(self._index)

    def _clear_temp(self):
        pass

    def load(self, oid, version=''):
        assert version == ''
        with self._lock:
            assert not version
            tid = self._cur[oid]
            self.hook(oid, tid, '')
            return self._index[(oid, tid)], tid

    def _begin(self, tid, u, d, e):
        self._txn = Transaction(tid)

    def store(self, oid, serial, data, v, txn):
        if txn is not self._transaction:
            raise POSException.StorageTransactionError(self, txn)
        assert not v
        if self._cur.get(oid) != serial:
            if not (serial is None or self._cur.get(oid) in [None, z64]):
                raise POSException.ConflictError(
                    oid=oid, serials=(self._cur.get(oid), serial), data=data)
        self._txn.store(oid, data)
        return self._tid

    def _abort(self):
        del self._txn

    def _finish(self, tid, u, d, e):
        with self._lock:
            self._index.update(self._txn.index)
            self._cur.update(self._txn.cur())
            self._ltid = self._tid

    def loadBefore(self, the_oid, the_tid):
        warnings.warn("loadBefore is deprecated - use loadAt instead",
                DeprecationWarning, stacklevel=2)
        return self._loadBefore(the_oid, the_tid)

    def _loadBefore(self, the_oid, the_tid):
        # It's okay if loadBefore() is really expensive, because this
        # storage is just used for testing.
        with self._lock:
            tids = [tid for oid, tid in self._index if oid == the_oid]
            if not tids:
                raise KeyError(the_oid)
            tids.sort()
            i = bisect.bisect_left(tids, the_tid) - 1
            if i == -1:
                return None
            tid = tids[i]
            j = i + 1
            if j == len(tids):
                end_tid = None
            else:
                end_tid = tids[j]

            self.hook(the_oid, self._cur[the_oid], '')

            return self._index[(the_oid, tid)], tid, end_tid

    def loadAt(self, oid, at):
        try:
            r = self._loadBefore(oid, p64(u64(at)+1))
        except KeyError:
            return None, z64
        if r is None:
            # not-yet created (deleteObject not supported -> serial=0)
            return None, z64
        data, serial, _ = r
        return data, serial

    def loadSerial(self, oid, serial):
        return self._index[(oid, serial)]

    def close(self):
        pass

    cleanup = close

class MinimalTestSuite(StorageTestBase.StorageTestBase,
                       BasicStorage.BasicStorage,
                       MTStorage.MTStorage,
                       Synchronization.SynchronizedStorage,
                       RevisionStorage.RevisionStorage,
                       ):

    def setUp(self):
        StorageTestBase.StorageTestBase.setUp(self)
        self._storage = MinimalMemoryStorage()

    # we don't implement undo

    def checkLoadBeforeUndo(self):
        pass

def test_suite():
    return unittest.makeSuite(MinimalTestSuite, "check")
