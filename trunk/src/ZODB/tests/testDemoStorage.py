import ZODB.DemoStorage
import os, unittest

from ZODB.tests import StorageTestBase, BasicStorage, \
     VersionStorage, Synchronization

class DemoStorageTests(StorageTestBase.StorageTestBase,
                       BasicStorage.BasicStorage,
                       VersionStorage.VersionStorage,
                       Synchronization.SynchronizedStorage,
                       ):

    def setUp(self):
        StorageTestBase.StorageTestBase.setUp(self)
        self._storage = ZODB.DemoStorage.DemoStorage()

def test_suite():
    suite = unittest.makeSuite(DemoStorageTests, 'check')
    return suite

if __name__ == "__main__":
    loader = unittest.TestLoader()
    loader.testMethodPrefix = "check"
    unittest.main(testLoader=loader)
    
