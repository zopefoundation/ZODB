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
        self._storage = ZODB.DemoStorage.DemoStorage()

    def tearDown(self):
        self._storage.close()

def test_suite():
    suite = unittest.makeSuite(DemoStorageTests, 'check')
    return suite

if __name__ == "__main__":
    loader = unittest.TestLoader()
    loader.testMethodPrefix = "check"
    unittest.main(testLoader=loader)
