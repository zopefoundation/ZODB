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

# Used as base classes for test cases
from ZODB.tests.BasicStorage import BasicStorage
from ZODB.tests.HistoryStorage import HistoryStorage
from ZODB.tests.IteratorStorage import ExtendedIteratorStorage
from ZODB.tests.IteratorStorage import IteratorStorage
from ZODB.tests.MTStorage import MTStorage
from ZODB.tests.PackableStorage import PackableStorageWithOptionalGC
from ZODB.tests.RevisionStorage import RevisionStorage
from ZODB.tests.StorageTestBase import StorageTestBase
from ZODB.tests.Synchronization import SynchronizedStorage

class MappingStorageTests(
    StorageTestBase,
    BasicStorage,
    HistoryStorage,
    ExtendedIteratorStorage,
    IteratorStorage,
    MTStorage,
    PackableStorageWithOptionalGC,
    RevisionStorage,
    SynchronizedStorage,
    ):

    def setUp(self):
        from ZODB.MappingStorage import MappingStorage
        StorageTestBase.setUp(self, )
        self._storage = MappingStorage()

    def checkOversizeNote(self):
        # This base class test checks for the common case where a storage
        # doesnt support huge transaction metadata. This storage doesnt
        # have this limit, so we inhibit this test here.
        pass

    def checkLoadBeforeUndo(self):
        pass # we don't support undo yet
    checkUndoZombie = checkLoadBeforeUndo


class MappingStorageHexTests(MappingStorageTests):

    def setUp(self):
        from ZODB.MappingStorage import MappingStorage
        from ZODB.tests.hexstorage import HexStorage
        StorageTestBase.setUp(self, )
        self._storage = HexStorage(MappingStorage())


def test_suite():
    return unittest.TestSuite((
        unittest.makeSuite(MappingStorageTests, 'check'),
        unittest.makeSuite(MappingStorageHexTests, 'check'),
    ))
