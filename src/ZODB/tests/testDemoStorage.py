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
import unittest

import transaction
from ZODB.DB import DB
import ZODB.utils
import ZODB.DemoStorage
from ZODB.tests import StorageTestBase, BasicStorage
from ZODB.tests import Synchronization, IteratorStorage

class DemoStorageTests(StorageTestBase.StorageTestBase,
                       BasicStorage.BasicStorage,
                       Synchronization.SynchronizedStorage,
                       IteratorStorage.IteratorStorage
                       ):

    def setUp(self):
        self._storage = ZODB.DemoStorage.DemoStorage()

    def tearDown(self):
        self._storage.close()

    def checkOversizeNote(self):
        # This base class test checks for the common case where a storage
        # doesnt support huge transaction metadata. This storage doesnt
        # have this limit, so we inhibit this test here.
        pass

    def checkLoadDelegation(self):
        # Minimal test of loadEX w/o version -- ironically
        db = DB(self._storage) # creates object 0. :)
        s2 = ZODB.DemoStorage.DemoStorage(base=self._storage)
        self.assertEqual(s2.load(ZODB.utils.z64, ''),
                         self._storage.load(ZODB.utils.z64, ''))

    def checkUndoZombie(self):
        # The test base class IteratorStorage assumes that we keep undo data
        # to construct our iterator, which we don't, so we disable this test.
        pass

class DemoStorageWrappedBase(DemoStorageTests):

    def setUp(self):
        import ZODB.DemoStorage
        self._base = self._makeBaseStorage()
        self._storage = ZODB.DemoStorage.DemoStorage(base=self._base)

    def tearDown(self):
        self._storage.close()
        self._base.close()

    def _makeBaseStorage(self):
        raise NotImplementedError

class DemoStorageWrappedAroundFileStorage(DemoStorageWrappedBase):

    def _makeBaseStorage(self):
        from ZODB.MappingStorage import MappingStorage
        return MappingStorage()

class DemoStorageWrappedAroundMappingStorage(DemoStorageWrappedBase):

    def _makeBaseStorage(self):
        from ZODB.FileStorage import FileStorage
        return FileStorage('FileStorageTests.fs')
                       

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(DemoStorageTests, 'check'))
    suite.addTest(unittest.makeSuite(DemoStorageWrappedAroundFileStorage,
                                     'check'))
    suite.addTest(unittest.makeSuite(DemoStorageWrappedAroundMappingStorage,
                                     'check'))
    return suite

if __name__ == "__main__":
    loader = unittest.TestLoader()
    loader.testMethodPrefix = "check"
    unittest.main(testLoader=loader)
