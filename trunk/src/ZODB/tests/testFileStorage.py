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
from __future__ import nested_scopes

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
from ZODB.tests.StorageTestBase import MinPO, zodb_unpickle

class FileStorageTests(
    StorageTestBase.StorageTestBase,
    BasicStorage.BasicStorage,
    TransactionalUndoStorage.TransactionalUndoStorage,
    RevisionStorage.RevisionStorage,
    VersionStorage.VersionStorage,
    TransactionalUndoVersionStorage.TransactionalUndoVersionStorage,
    PackableStorage.PackableStorage,
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

    def open(self, **kwargs):
        self._storage = ZODB.FileStorage.FileStorage('FileStorageTests.fs',
                                                     **kwargs)

    def setUp(self):
        self.open(create=1)

    def tearDown(self):
        self._storage.close()
        StorageTestBase.removefs("FileStorageTests.fs")

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
                return {}, {}, {}, {}


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
                return {}, {}, {}, {}


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

    # There are a bunch of tests that the current pack() implementation
    # does not past.  We need to fix pack(), but don't want tests to
    # fail until then.

    def checkPackVersionsInPast(self):
        pass

    def checkPackAfterUndoDeletion(self):
        pass

class FileStorageRecoveryTest(
    StorageTestBase.StorageTestBase,
    RecoveryStorage.RecoveryStorage,
    ):

    def setUp(self):
        StorageTestBase.removefs("Source.fs")
        StorageTestBase.removefs("Dest.fs")
        self._storage = ZODB.FileStorage.FileStorage('Source.fs')
        self._dst = ZODB.FileStorage.FileStorage('Dest.fs')

    def tearDown(self):
        self._storage.close()
        self._dst.close()
        StorageTestBase.removefs("Source.fs")
        StorageTestBase.removefs("Dest.fs")

    def new_dest(self):
        StorageTestBase.removefs('Dest.fs')
        return ZODB.FileStorage.FileStorage('Dest.fs')


def test_suite():
    suite = unittest.makeSuite(FileStorageTests, 'check')
    suite2 = unittest.makeSuite(Corruption.FileStorageCorruptTests, 'check')
    suite3 = unittest.makeSuite(FileStorageRecoveryTest, 'check')
    suite.addTest(suite2)
    suite.addTest(suite3)
    return suite

if __name__=='__main__':
    unittest.main()
