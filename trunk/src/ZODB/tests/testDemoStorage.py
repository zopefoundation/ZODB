import ZODB.DemoStorage
import os, unittest

from ZODB.tests import StorageTestBase, BasicStorage, \
     TransactionalUndoStorage, VersionStorage, \
     TransactionalUndoVersionStorage, PackableStorage, \
     Synchronization, ConflictResolution, HistoryStorage, \
     IteratorStorage, Corruption

class DemoStorageTests(StorageTestBase.StorageTestBase,
                       BasicStorage.BasicStorage,
                       VersionStorage.VersionStorage,
                       Synchronization.SynchronizedStorage,
                       ):

    def setUp(self):
        self._storage = ZODB.DemoStorage.DemoStorage()
        StorageTestBase.StorageTestBase.setUp(self)

def test_suite():
    suite = unittest.makeSuite(DemoStorageTests, 'check')
    return suite

if __name__ == "__main__":
    loader = unittest.TestLoader()
    loader.testMethodPrefix = "check"
    unittest.main(testLoader=loader)
    
