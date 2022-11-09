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
from collections import namedtuple

import ZODB.MappingStorage
import ZODB.tests.hexstorage
from ZODB.tests import BasicStorage
from ZODB.tests import HistoryStorage
from ZODB.tests import IteratorStorage
from ZODB.tests import MTStorage
from ZODB.tests import PackableStorage
from ZODB.tests import RevisionStorage
from ZODB.tests import StorageTestBase
from ZODB.tests import Synchronization


class MappingStorageTests(
    StorageTestBase.StorageTestBase,
    BasicStorage.BasicStorage,

    HistoryStorage.HistoryStorage,
    IteratorStorage.ExtendedIteratorStorage,
    IteratorStorage.IteratorStorage,
    MTStorage.MTStorage,
    PackableStorage.PackableStorageWithOptionalGC,
    RevisionStorage.RevisionStorage,
    Synchronization.SynchronizedStorage,
):

    def setUp(self):
        StorageTestBase.StorageTestBase.setUp(self, )
        self._storage = ZODB.MappingStorage.MappingStorage()

    def checkOversizeNote(self):
        # This base class test checks for the common case where a storage
        # doesnt support huge transaction metadata. This storage doesnt
        # have this limit, so we inhibit this test here.
        pass

    def checkLoadBeforeUndo(self):
        pass  # we don't support undo yet
    checkUndoZombie = checkLoadBeforeUndo


class MappingStorageHexTests(MappingStorageTests):

    def setUp(self):
        StorageTestBase.StorageTestBase.setUp(self, )
        self._storage = ZODB.tests.hexstorage.HexStorage(
            ZODB.MappingStorage.MappingStorage())


MockTransaction = namedtuple(
    'transaction',
    ['user', 'description', 'extension']
)


class MappingStorageTransactionRecordTests(unittest.TestCase):

    def setUp(self):
        self._transaction_record = ZODB.MappingStorage.TransactionRecord(
            0,
            MockTransaction('user', 'description', 'extension'),
            ''
        )

    def check_set__extension(self):
        self._transaction_record._extension = 'new'
        self.assertEqual(self._transaction_record.extension, 'new')

    def check_get__extension(self):
        self.assertEqual(
            self._transaction_record.extension,
            self._transaction_record._extension
        )


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(MappingStorageTests, 'check'))
    suite.addTest(unittest.makeSuite(MappingStorageHexTests, 'check'))
    suite.addTest(unittest.makeSuite(
        MappingStorageTransactionRecordTests, 'check'))
    return suite
