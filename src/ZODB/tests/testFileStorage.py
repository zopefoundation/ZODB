import sys
sys.path.insert(0, '.')

import ZODB.FileStorage
import os, unittest
import StorageTestBase, BasicStorage, TransactionalUndoStorage
import VersionStorage, TransactionalUndoVersionStorage
import PackableStorage
import Synchronization
import ConflictResolution

class FileStorageTests(
    StorageTestBase.StorageTestBase,
    BasicStorage.BasicStorage,
    TransactionalUndoStorage.TransactionalUndoStorage,
    VersionStorage.VersionStorage,
    TransactionalUndoVersionStorage.TransactionalUndoVersionStorage,
    PackableStorage.PackableStorage,
    Synchronization.SynchronizedStorage,
    ConflictResolution.ConflictResolvingStorage,
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
    return unittest.makeSuite(FileStorageTests, 'check')

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
