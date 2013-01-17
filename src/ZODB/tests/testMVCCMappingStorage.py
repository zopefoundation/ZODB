##############################################################################
#
# Copyright (c) Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

import unittest


# Used as base classes for testcases
from ZODB.tests.BasicStorage import BasicStorage
from ZODB.tests.HistoryStorage import HistoryStorage
from ZODB.tests.IteratorStorage import ExtendedIteratorStorage
from ZODB.tests.IteratorStorage import IteratorStorage
from ZODB.tests.MTStorage import MTStorage
from ZODB.tests.PackableStorage import PackableStorageWithOptionalGC
from ZODB.tests.RevisionStorage import RevisionStorage
from ZODB.tests import StorageTestBase
from ZODB.tests.Synchronization import SynchronizedStorage

class MVCCTests(object):

    def testCrossConnectionInvalidation(self):
        # Verify connections see updated state at txn boundaries.
        # This will fail if the Connection doesn't poll for changes.
        import transaction
        from ZODB.db import DB
        db = DB(self._storage)
        try:
            c1 = db.open(transaction.TransactionManager())
            r1 = c1.root()
            r1['myobj'] = 'yes'
            c2 = db.open(transaction.TransactionManager())
            r2 = c2.root()
            self.assert_('myobj' not in r2)

            c1.transaction_manager.commit()
            self.assert_('myobj' not in r2)

            c2.sync()
            self.assert_('myobj' in r2)
            self.assert_(r2['myobj'] == 'yes')
        finally:
            db.close()

    def testCrossConnectionIsolation(self):
        # Verify MVCC isolates connections.
        # This will fail if Connection doesn't poll for changes.
        from persistent.mapping import PersistentMapping
        import transaction
        from ZODB.db import DB
        db = DB(self._storage)
        try:
            c1 = db.open()
            r1 = c1.root()
            r1['alpha'] = PersistentMapping()
            r1['gamma'] = PersistentMapping()
            transaction.commit()

            # Open a second connection but don't load root['alpha'] yet
            c2 = db.open()
            r2 = c2.root()

            r1['alpha']['beta'] = 'yes'

            storage = c1._storage
            t = transaction.Transaction()
            t.description = 'isolation test 1'
            storage.tpc_begin(t)
            c1.commit(t)
            storage.tpc_vote(t)
            storage.tpc_finish(t)

            # The second connection will now load root['alpha'], but due to
            # MVCC, it should continue to see the old state.
            self.assert_(r2['alpha']._p_changed is None)  # A ghost
            self.assert_(not r2['alpha'])
            self.assert_(r2['alpha']._p_changed == 0)

            # make root['alpha'] visible to the second connection
            c2.sync()

            # Now it should be in sync
            self.assert_(r2['alpha']._p_changed is None)  # A ghost
            self.assert_(r2['alpha'])
            self.assert_(r2['alpha']._p_changed == 0)
            self.assert_(r2['alpha']['beta'] == 'yes')

            # Repeat the test with root['gamma']
            r1['gamma']['delta'] = 'yes'

            storage = c1._storage
            t = transaction.Transaction()
            t.description = 'isolation test 2'
            storage.tpc_begin(t)
            c1.commit(t)
            storage.tpc_vote(t)
            storage.tpc_finish(t)

            # The second connection will now load root[3], but due to MVCC,
            # it should continue to see the old state.
            self.assert_(r2['gamma']._p_changed is None)  # A ghost
            self.assert_(not r2['gamma'])
            self.assert_(r2['gamma']._p_changed == 0)

            # make root[3] visible to the second connection
            c2.sync()

            # Now it should be in sync
            self.assert_(r2['gamma']._p_changed is None)  # A ghost
            self.assert_(r2['gamma'])
            self.assert_(r2['gamma']._p_changed == 0)
            self.assert_(r2['gamma']['delta'] == 'yes')
        finally:
            db.close()
    

class MVCCMappingStorageTests(
    StorageTestBase.StorageTestBase,
    BasicStorage,
    HistoryStorage,
    ExtendedIteratorStorage,
    IteratorStorage,
    MTStorage,
    PackableStorageWithOptionalGC,
    RevisionStorage,
    SynchronizedStorage,
    MVCCTests
    ):

    def setUp(self):
        from ZODB.tests.MVCCMappingStorage import MVCCMappingStorage
        self._storage = MVCCMappingStorage()

    def tearDown(self):
        self._storage.close()

    def testLoadBeforeUndo(self):
        pass # we don't support undo yet
    testUndoZombie = testLoadBeforeUndo

    def testTransactionIdIncreases(self):
        import transaction
        t = transaction.Transaction()
        self._storage.tpc_begin(t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)

        # Add a fake transaction
        transactions = self._storage._transactions
        self.assertEqual(1, len(transactions))
        fake_timestamp = 'zzzzzzzy'  # the year 5735 ;-)
        transactions[fake_timestamp] = transactions.values()[0]

        # Verify the next transaction comes after the fake transaction
        t = transaction.Transaction()
        self._storage.tpc_begin(t)
        self.assertEqual(self._storage._tid, 'zzzzzzzz')

def create_blob_storage(name, blob_dir):
    from ZODB.blob import BlobStorage
    from ZODB.tests.MVCCMappingStorage import MVCCMappingStorage
    s = MVCCMappingStorage(name)
    return BlobStorage(blob_dir, s)

def test_suite():
    from ZODB.tests.testblob import storage_reusable_suite
    return unittest.TestSuite((
        unittest.makeSuite(MVCCMappingStorageTests),
    # Note: test_packing doesn't work because even though MVCCMappingStorage
    # retains history, it does not provide undo methods, so the
    # BlobStorage wrapper calls _packNonUndoing instead of _packUndoing,
    # causing blobs to get deleted even though object states are retained.
        storage_reusable_suite('MVCCMapping', create_blob_storage,
                               test_undo=False,),
    ))
