##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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
from ZODB.tests import StorageTestBase, BasicStorage, VersionStorage
from ZODB.tests import Synchronization

class DemoStorageTests(StorageTestBase.StorageTestBase,
                       BasicStorage.BasicStorage,
                       VersionStorage.VersionStorage,
                       Synchronization.SynchronizedStorage,
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

    def checkAbortVersionNonCurrent(self):
        # TODO:  Need to implement a real loadBefore for DemoStorage?
        pass

    def checkLoadBeforeVersion(self):
        # TODO:  Need to implement a real loadBefore for DemoStorage?
        pass

    # the next three pack tests depend on undo

    def checkPackVersionReachable(self):
        pass

    def checkPackVersions(self):
        pass

    def checkPackVersionsInPast(self):
        pass

    def checkLoadDelegation(self):
        # Minimal test of loadEX w/o version -- ironically
        db = DB(self._storage) # creates object 0. :)
        s2 = ZODB.DemoStorage.DemoStorage(base=self._storage)
        self.assertEqual(s2.load(ZODB.utils.z64, ''),
                         self._storage.load(ZODB.utils.z64, ''))

    def checkOmitVersionOnLoadAndHistory(self):
        db = DB(self._storage)
        self.assertEqual(self._storage.load('\0'*8),
                         self._storage.load('\0'*8, ''))
        self._storage.history('\0'*8)


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
