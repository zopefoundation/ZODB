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
            # BDBFullStorage storage creates a directory
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
            from BDBStorage.BDBFullStorage import BDBFullStorage
        except ImportError:
            return
        sample = """
        <Storage>
        type       BDBFullStorage
        name       %s
        cachesize  1000
        </Storage>
        """ % self.tmpfn
        os.mkdir(self.tmpfn)
        io = StringIO(sample)
        rootconf = ZConfig.loadfile(io)
        storageconf = rootconf.getSection("Storage")
        cls, args = StorageConfig.getStorageInfo(storageconf)
        self.assertEqual(cls, BDBFullStorage)
        # It's too hard to test the config instance equality
        args = args.copy()
        del args['config']
        self.assertEqual(args, {"name": self.tmpfn})
        self.storage = StorageConfig.createStorage(storageconf)
        self.assert_(isinstance(self.storage, BDBFullStorage))
        # XXX _config isn't public
        self.assert_(self.storage._config.cachesize, 1000)

    def testMinimalStorage(self):
        try:
            from BDBStorage.BDBMinimalStorage import BDBMinimalStorage
        except ImportError:
            return
        sample = """
        <Storage>
        type       BDBMinimalStorage
        name       %s
        cachesize  1000
        </Storage>
        """ % self.tmpfn
        os.mkdir(self.tmpfn)
        io = StringIO(sample)
        rootconf = ZConfig.loadfile(io)
        storageconf = rootconf.getSection("Storage")
        cls, args = StorageConfig.getStorageInfo(storageconf)
        self.assertEqual(cls, BDBMinimalStorage)
        # It's too hard to test the config instance equality
        args = args.copy()
        del args['config']
        self.assertEqual(args, {"name": self.tmpfn})
        self.storage = StorageConfig.createStorage(storageconf)
        self.assert_(isinstance(self.storage, BDBMinimalStorage))
        # XXX _config isn't public
        self.assert_(self.storage._config.cachesize, 1000)

def test_suite():
    return unittest.makeSuite(StorageTestCase)

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
