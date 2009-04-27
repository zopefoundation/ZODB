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

from persistent.mapping import PersistentMapping
import transaction
from ZODB.DB import DB
from ZODB.tests.MVCCMappingStorage import MVCCMappingStorage


from ZODB.tests import (
    BasicStorage,
    HistoryStorage,
    IteratorStorage,
    MTStorage,
    PackableStorage,
    RevisionStorage,
    StorageTestBase,
    Synchronization,
    )

class MVCCTests:

    def checkCrossConnectionInvalidation(self):
        # Verify connections see updated state at txn boundaries.
        # This will fail if the Connection doesn't poll for changes.
        db = DB(self._storage)
        try:
            c1 = db.open()
            r1 = c1.root()
            r1['myobj'] = 'yes'
            c2 = db.open()
            r2 = c2.root()
            self.assert_('myobj' not in r2)

            storage = c1._storage
            t = transaction.Transaction()
            t.description = 'invalidation test'
            storage.tpc_begin(t)
            c1.commit(t)
            storage.tpc_vote(t)
            storage.tpc_finish(t)

            self.assert_('myobj' not in r2)
            c2.sync()
            self.assert_('myobj' in r2)
            self.assert_(r2['myobj'] == 'yes')
        finally:
            db.close()

    def checkCrossConnectionIsolation(self):
        # Verify MVCC isolates connections.
        # This will fail if Connection doesn't poll for changes.
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
    BasicStorage.BasicStorage,

    HistoryStorage.HistoryStorage,
    IteratorStorage.ExtendedIteratorStorage,
    IteratorStorage.IteratorStorage,
    MTStorage.MTStorage,
    PackableStorage.PackableStorageWithOptionalGC,
    RevisionStorage.RevisionStorage,
    Synchronization.SynchronizedStorage,
    MVCCTests
    ):

    def setUp(self):
        self._storage = MVCCMappingStorage()

    def tearDown(self):
        self._storage.close()

    def checkLoadBeforeUndo(self):
        pass # we don't support undo yet
    checkUndoZombie = checkLoadBeforeUndo


def test_suite():
    suite = unittest.makeSuite(MVCCMappingStorageTests, 'check')
    return suite

if __name__ == "__main__":
    loader = unittest.TestLoader()
    loader.testMethodPrefix = "check"
    unittest.main(testLoader=loader)
