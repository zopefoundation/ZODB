from __future__ import nested_scopes

import ZODB.FileStorage
import sys, os, unittest
import errno
from ZODB.Transaction import Transaction
from ZODB import POSException

from ZODB.tests import StorageTestBase, BasicStorage, \
     TransactionalUndoStorage, VersionStorage, \
     TransactionalUndoVersionStorage, PackableStorage, \
     Synchronization, ConflictResolution, HistoryStorage, \
     IteratorStorage, Corruption, RevisionStorage, PersistentStorage, \
     MTStorage, ReadOnlyStorage

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
    HistoryStorage.HistoryStorage,
    IteratorStorage.IteratorStorage,
    IteratorStorage.ExtendedIteratorStorage,
    PersistentStorage.PersistentStorage,
    MTStorage.MTStorage,
    ReadOnlyStorage.ReadOnlyStorage
    ):

    def open(self, **kwargs):
        if kwargs:
            self._storage = apply(ZODB.FileStorage.FileStorage,
                                  ('FileStorageTests.fs',), kwargs)
        else:
            self._storage = ZODB.FileStorage.FileStorage(
                'FileStorageTests.fs', **kwargs)

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

class FileStorageRecoveryTest(
    StorageTestBase.StorageTestBase,
    IteratorStorage.IteratorDeepCompare,
    ):

    def setUp(self):
        self._storage = ZODB.FileStorage.FileStorage('Source.fs')
        self._dst = ZODB.FileStorage.FileStorage('Dest.fs')

    def tearDown(self):
        self._storage.close()
        self._dst.close()
        StorageTestBase.removefs("Source.fs")
        StorageTestBase.removefs("Dest.fs")

    def checkSimpleRecovery(self):
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=11)
        revid = self._dostore(oid, revid=revid, data=12)
        revid = self._dostore(oid, revid=revid, data=13)
        self._dst.copyTransactionsFrom(self._storage)
        self.compare(self._storage, self._dst)

    def checkRecoveryAcrossVersions(self):
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=21)
        revid = self._dostore(oid, revid=revid, data=22)
        revid = self._dostore(oid, revid=revid, data=23, version='one')
        revid = self._dostore(oid, revid=revid, data=34, version='one')
        # Now commit the version
        t = Transaction()
        self._storage.tpc_begin(t)
        self._storage.commitVersion('one', '', t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        self._dst.copyTransactionsFrom(self._storage)
        self.compare(self._storage, self._dst)

    def checkRecoverAbortVersion(self):
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=21, version="one")
        revid = self._dostore(oid, revid=revid, data=23, version='one')
        revid = self._dostore(oid, revid=revid, data=34, version='one')
        # Now abort the version and the creation
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.abortVersion('one', t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        self.assertEqual(oids, [oid])
        self._dst.copyTransactionsFrom(self._storage)
        self.compare(self._storage, self._dst)
        # Also make sure the the last transaction has a data record
        # with None for its data attribute, because we've undone the
        # object.
        for s in self._storage, self._dst:
            iter = s.iterator()
            for trans in iter:
                pass # iterate until we get the last one
            data = trans[0]
            self.assertRaises(IndexError, lambda i:trans[i], 1)
            self.assertEqual(data.oid, oid)
            self.assertEqual(data.data, None)


def test_suite():
    suite = unittest.makeSuite(FileStorageTests, 'check')
    suite2 = unittest.makeSuite(Corruption.FileStorageCorruptTests, 'check')
    suite3 = unittest.makeSuite(FileStorageRecoveryTest, 'check')
    suite.addTest(suite2)
    suite.addTest(suite3)
    return suite

def main():
    alltests=test_suite()
    runner = unittest.TextTestRunner()
    runner.run(alltests)

def debug():
    test_suite().debug()

def pdebug():
    import pdb
    pdb.run('debug()')

if __name__=='__main__':
    if len(sys.argv) > 1:
        globals()[sys.argv[1]]()
    else:
        main()
