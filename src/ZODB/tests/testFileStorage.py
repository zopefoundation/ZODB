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
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
import ZODB.FileStorage
import sys, os, unittest
import errno
import filecmp
import StringIO
from ZODB.Transaction import Transaction
from ZODB import POSException
from ZODB.fsrecover import recover

from ZODB.tests import StorageTestBase, BasicStorage, \
     TransactionalUndoStorage, VersionStorage, \
     TransactionalUndoVersionStorage, PackableStorage, \
     Synchronization, ConflictResolution, HistoryStorage, \
     IteratorStorage, Corruption, RevisionStorage, PersistentStorage, \
     MTStorage, ReadOnlyStorage, RecoveryStorage
from ZODB.tests.StorageTestBase import MinPO, zodb_unpickle, zodb_pickle

class BaseFileStorageTests(StorageTestBase.StorageTestBase):

    def open(self, **kwargs):
        self._storage = ZODB.FileStorage.FileStorage('FileStorageTests.fs',
                                                     **kwargs)

    def setUp(self):
        self.open(create=1)

    def tearDown(self):
        self._storage.close()
        self._storage.cleanup()

class FileStorageTests(
    BaseFileStorageTests,
    BasicStorage.BasicStorage,
    TransactionalUndoStorage.TransactionalUndoStorage,
    RevisionStorage.RevisionStorage,
    VersionStorage.VersionStorage,
    TransactionalUndoVersionStorage.TransactionalUndoVersionStorage,
    PackableStorage.PackableStorage,
    PackableStorage.PackableUndoStorage,
    Synchronization.SynchronizedStorage,
    ConflictResolution.ConflictResolvingStorage,
    ConflictResolution.ConflictResolvingTransUndoStorage,
    HistoryStorage.HistoryStorage,
    IteratorStorage.IteratorStorage,
    IteratorStorage.ExtendedIteratorStorage,
    PersistentStorage.PersistentStorage,
    MTStorage.MTStorage,
    ReadOnlyStorage.ReadOnlyStorage
    ):

    def checkLongMetadata(self):
        s = "X" * 75000
        try:
            self._dostore(user=s)
        except POSException.StorageError:
            pass
        else:
            self.fail("expect long user field to raise error")
        try:
            self._dostore(description=s)
        except POSException.StorageError:
            pass
        else:
            self.fail("expect long user field to raise error")

    def check_use_fsIndex(self):
        from ZODB.fsIndex import fsIndex

        self.assertEqual(self._storage._index.__class__, fsIndex)

    # XXX We could really use some tests for sanity checking

    def check_conversion_to_fsIndex_not_if_readonly(self):

        self.tearDown()

        class OldFileStorage(ZODB.FileStorage.FileStorage):
            def _newIndexes(self):
                return {}, {}, {}, {}, {}, {}, {}


        from ZODB.fsIndex import fsIndex

        # Hack FileStorage to create dictionary indexes
        self._storage = OldFileStorage('FileStorageTests.fs')

        self.assertEqual(type(self._storage._index), type({}))
        for i in range(10):
            self._dostore()

        # Should save the index
        self._storage.close()

        self._storage = ZODB.FileStorage.FileStorage(
            'FileStorageTests.fs', read_only=1)
        self.assertEqual(type(self._storage._index), type({}))

    def check_conversion_to_fsIndex(self):

        self.tearDown()

        class OldFileStorage(ZODB.FileStorage.FileStorage):
            def _newIndexes(self):
                return {}, {}, {}, {}, {}, {}, {}


        from ZODB.fsIndex import fsIndex

        # Hack FileStorage to create dictionary indexes
        self._storage = OldFileStorage('FileStorageTests.fs')

        self.assertEqual(type(self._storage._index), type({}))
        for i in range(10):
            self._dostore()

        oldindex = self._storage._index.copy()

        # Should save the index
        self._storage.close()

        self._storage = ZODB.FileStorage.FileStorage('FileStorageTests.fs')
        self.assertEqual(self._storage._index.__class__, fsIndex)
        self.failUnless(self._storage._used_index)

        index = {}
        for k, v in self._storage._index.items():
            index[k] = v

        self.assertEqual(index, oldindex)


    def check_save_after_load_with_no_index(self):
        for i in range(10):
            self._dostore()
        self._storage.close()
        os.remove('FileStorageTests.fs.index')
        self.open()
        self.assertEqual(self._storage._saved, 1)


    # This would make the unit tests too slow
    # check_save_after_load_that_worked_hard(self)

    def check_periodic_save_index(self):

        # Check the basic algorithm
        oldsaved = self._storage._saved
        self._storage._records_before_save = 10
        for i in range(4):
            self._dostore()
        self.assertEqual(self._storage._saved, oldsaved)
        self._dostore()
        self.assertEqual(self._storage._saved, oldsaved+1)

        # Now make sure the parameter changes as we get bigger
        for i in range(20):
            self._dostore()

        self.failUnless(self._storage._records_before_save > 20)


class FileStorageRecoveryTest(
    StorageTestBase.StorageTestBase,
    RecoveryStorage.RecoveryStorage,
    ):

    def setUp(self):
        self._storage = ZODB.FileStorage.FileStorage("Source.fs", create=True)
        self._dst = ZODB.FileStorage.FileStorage("Dest.fs", create=True)

    def tearDown(self):
        self._storage.close()
        self._dst.close()
        self._storage.cleanup()
        self._dst.cleanup()

    def new_dest(self):
        return ZODB.FileStorage.FileStorage('Dest.fs')

class SlowFileStorageTest(BaseFileStorageTests):

    level = 2

    def check10Kstores(self):
        # The _get_cached_serial() method has a special case
        # every 8000 calls.  Make sure it gets minimal coverage.
        oids = [[self._storage.new_oid(), None] for i in range(100)]
        for i in range(100):
            t = Transaction()
            self._storage.tpc_begin(t)
            for j in range(100):
                o = MinPO(j)
                oid, revid = oids[j]
                serial = self._storage.store(oid, revid, zodb_pickle(o), "", t)
                oids[j][1] = serial
            self._storage.tpc_vote(t)
            self._storage.tpc_finish(t)


def test_suite():
    suite = unittest.TestSuite()
    for klass in [FileStorageTests, Corruption.FileStorageCorruptTests,
                  FileStorageRecoveryTest, SlowFileStorageTest]:
        suite.addTest(unittest.makeSuite(klass, "check"))
    return suite

if __name__=='__main__':
    unittest.main()
