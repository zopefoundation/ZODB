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
import ZODB.DemoStorage
import unittest

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


def test_suite():
    suite = unittest.makeSuite(DemoStorageTests, 'check')
    return suite

if __name__ == "__main__":
    loader = unittest.TestLoader()
    loader.testMethodPrefix = "check"
    unittest.main(testLoader=loader)
