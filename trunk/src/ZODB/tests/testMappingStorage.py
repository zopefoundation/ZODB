import ZODB.MappingStorage
import os, unittest

from ZODB.tests import StorageTestBase, BasicStorage, Synchronization

class MappingStorageTests(StorageTestBase.StorageTestBase,
                       BasicStorage.BasicStorage,
                       Synchronization.SynchronizedStorage,
                       ):

    def setUp(self):
        self._storage = ZODB.MappingStorage.MappingStorage()
        StorageTestBase.StorageTestBase.setUp(self)

def test_suite():
    suite = unittest.makeSuite(MappingStorageTests, 'check')
    return suite

if __name__ == "__main__":
    loader = unittest.TestLoader()
    loader.testMethodPrefix = "check"
    unittest.main(testLoader=loader)
    
