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
import ZODB.MappingStorage
import os, unittest

from ZODB.tests import StorageTestBase
from ZODB.tests \
     import BasicStorage, MTStorage, Synchronization, PackableStorage

class MappingStorageTests(StorageTestBase.StorageTestBase,
                          BasicStorage.BasicStorage,
                          MTStorage.MTStorage,
                          PackableStorage.PackableStorage,
                          Synchronization.SynchronizedStorage,
                          ):

    def setUp(self):
        self._storage = ZODB.MappingStorage.MappingStorage()

    def tearDown(self):
        self._storage.close()

    def checkOversizeNote(self):
        # This base class test checks for the common case where a storage
        # doesnt support huge transaction metadata. This storage doesnt
        # have this limit, so we inhibit this test here.
        pass

def test_suite():
    suite = unittest.makeSuite(MappingStorageTests, 'check')
    return suite

if __name__ == "__main__":
    loader = unittest.TestLoader()
    loader.testMethodPrefix = "check"
    unittest.main(testLoader=loader)
