import os
import shutil
import tempfile
import unittest
from StringIO import StringIO

import ZConfig

from ZODB import StorageConfig

class StorageTestCase(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.tmpfn = tempfile.mktemp()
        self.storage = None

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        storage = self.storage
        self.storage = None
        try:
            if storage is not None:
                storage.close()
        except:
            pass
        try:
            # Full storage creates a directory
            if os.path.isdir(self.tmpfn):
                shutil.rmtree(self.tmpfn)
            else:
                os.remove(self.tmpfn)
        except os.error:
            pass

    def testFileStorage(self):
        from ZODB.FileStorage import FileStorage
        sample = """
        <Storage>
        type       FileStorage
        file_name  %s
        create     yes
        </Storage>
        """ % self.tmpfn
        io = StringIO(sample)
        rootconf = ZConfig.loadfile(io)
        storageconf = rootconf.getSection("Storage")
        cls, args = StorageConfig.getStorageInfo(storageconf)
        self.assertEqual(cls, FileStorage)
        self.assertEqual(args, {"file_name": self.tmpfn, "create": 1})
        self.storage = StorageConfig.createStorage(storageconf)
        self.assert_(isinstance(self.storage, FileStorage))

    def testZEOStorage(self):
        from ZEO.ClientStorage import ClientStorage
        sample = """
        <Storage>
        type       ClientStorage
        addr       zeo://www.python.org:9001
        wait       no
        </Storage>
        """
        io = StringIO(sample)
        rootconf = ZConfig.loadfile(io)
        storageconf = rootconf.getSection("Storage")
        cls, args = StorageConfig.getStorageInfo(storageconf)
        self.assertEqual(cls, ClientStorage)
        self.assertEqual(args, {"addr": [("www.python.org", 9001)], "wait": 0})
        self.storage = StorageConfig.createStorage(storageconf)
        self.assert_(isinstance(self.storage, ClientStorage))

    def testDemoStorage(self):
        from ZODB.DemoStorage import DemoStorage
        sample = """
        <Storage>
        type       DemoStorage
        </Storage>
        """
        io = StringIO(sample)
        rootconf = ZConfig.loadfile(io)
        storageconf = rootconf.getSection("Storage")
        cls, args = StorageConfig.getStorageInfo(storageconf)
        self.assertEqual(cls, DemoStorage)
        self.assertEqual(args, {})
        self.storage = StorageConfig.createStorage(storageconf)
        self.assert_(isinstance(self.storage, DemoStorage))

    def testModuleStorage(self):
        # Test explicit module+class
        from ZODB.DemoStorage import DemoStorage
        sample = """
        <Storage>
        type       ZODB.DemoStorage.DemoStorage
        </Storage>
        """
        io = StringIO(sample)
        rootconf = ZConfig.loadfile(io)
        storageconf = rootconf.getSection("Storage")
        cls, args = StorageConfig.getStorageInfo(storageconf)
        self.assertEqual(cls, DemoStorage)
        self.assertEqual(args, {})
        self.storage = StorageConfig.createStorage(storageconf)
        self.assert_(isinstance(self.storage, DemoStorage))

    def testFullStorage(self):
        try:
            from bsddb3Storage.Full import Full
        except ImportError:
            return
        sample = """
        <Storage>
        type       Full
        name       %s
        cachesize  1000
        </Storage>
        """ % self.tmpfn
        os.mkdir(self.tmpfn)
        io = StringIO(sample)
        rootconf = ZConfig.loadfile(io)
        storageconf = rootconf.getSection("Storage")
        cls, args = StorageConfig.getStorageInfo(storageconf)
        self.assertEqual(cls, Full)
        # It's too hard to test the config instance equality
        args = args.copy()
        del args['config']
        self.assertEqual(args, {"name": self.tmpfn})
        self.storage = StorageConfig.createStorage(storageconf)
        self.assert_(isinstance(self.storage, Full))
        # XXX _config isn't public
        self.assert_(self.storage._config.cachesize, 1000)

    def testMinimalStorage(self):
        try:
            from bsddb3Storage.Minimal import Minimal
        except ImportError:
            return
        sample = """
        <Storage>
        type       Minimal
        name       %s
        cachesize  1000
        </Storage>
        """ % self.tmpfn
        os.mkdir(self.tmpfn)
        io = StringIO(sample)
        rootconf = ZConfig.loadfile(io)
        storageconf = rootconf.getSection("Storage")
        cls, args = StorageConfig.getStorageInfo(storageconf)
        self.assertEqual(cls, Minimal)
        # It's too hard to test the config instance equality
        args = args.copy()
        del args['config']
        self.assertEqual(args, {"name": self.tmpfn})
        self.storage = StorageConfig.createStorage(storageconf)
        self.assert_(isinstance(self.storage, Minimal))
        # XXX _config isn't public
        self.assert_(self.storage._config.cachesize, 1000)

def test_suite():
    return unittest.makeSuite(StorageTestCase)

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
