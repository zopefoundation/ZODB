##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
import ZODB.MappingStorage
import unittest

from ZODB.tests import StorageTestBase
from ZODB.tests import BasicStorage, MTStorage, Synchronization
from ZODB.tests import PackableStorage, IteratorStorage

class MappingStorageTests(StorageTestBase.StorageTestBase,
                          BasicStorage.BasicStorage,
                          MTStorage.MTStorage,
                          PackableStorage.PackableStorage,
                          Synchronization.SynchronizedStorage,
                          IteratorStorage.IteratorStorage
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

    def checkSimpleIteration(self):
        # The test base class IteratorStorage assumes that we keep undo data
        # to construct our iterator, which we don't, so we disable this test.
        pass

    def checkUndoZombie(self):
        # The test base class IteratorStorage assumes that we keep undo data
        # to construct our iterator, which we don't, so we disable this test.
        pass

def test_suite():
    suite = unittest.makeSuite(MappingStorageTests, 'check')
    return suite

if __name__ == "__main__":
    loader = unittest.TestLoader()
    loader.testMethodPrefix = "check"
    unittest.main(testLoader=loader)
