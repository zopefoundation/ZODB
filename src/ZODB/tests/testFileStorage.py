import ZODB.FileStorage
import os, unittest

from ZODB.tests import StorageTestBase, BasicStorage, \
     TransactionalUndoStorage, VersionStorage, \
     TransactionalUndoVersionStorage, PackableStorage, \
     Synchronization, ConflictResolution, HistoryStorage, \
     IteratorStorage, Corruption, RevisionStorage

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
    IteratorStorage.IteratorStorage
    ):

    def setUp(self):
        self._storage = ZODB.FileStorage.FileStorage(
            'FileStorageTests.fs', create=1)
        StorageTestBase.StorageTestBase.setUp(self)

    def tearDown(self):
        StorageTestBase.StorageTestBase.tearDown(self)
        for ext in '', '.old', '.tmp', '.lock', '.index':
            path = 'FileStorageTests.fs' + ext
            if os.path.exists(path):
                os.remove(path)

def test_suite():
    suite = unittest.makeSuite(FileStorageTests, 'check')
    suite2 = unittest.makeSuite(Corruption.FileStorageCorruptTests, 'check')
    suite.addTest(suite2)
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
