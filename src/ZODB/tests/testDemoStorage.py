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
from ZODB.tests.PackableStorage import PackableStorage
from ZODB.tests.RevisionStorage import RevisionStorage
from ZODB.tests import StorageTestBase
from ZODB.tests.Synchronization import SynchronizedStorage


class DemoStorageTests(
    StorageTestBase.StorageTestBase,
    BasicStorage,
    HistoryStorage,
    ExtendedIteratorStorage,
    IteratorStorage,
    MTStorage,
    PackableStorage,
    RevisionStorage,
    SynchronizedStorage,
    ):

    def setUp(self):
        from ZODB.DemoStorage import DemoStorage
        StorageTestBase.StorageTestBase.setUp(self)
        self._storage = DemoStorage()

    def checkOversizeNote(self):
        # This base class test checks for the common case where a storage
        # doesnt support huge transaction metadata. This storage doesnt
        # have this limit, so we inhibit this test here.
        pass

    def checkLoadDelegation(self):
        # Minimal test of loadEX w/o version -- ironically
        from ZODB.DB import DB
        from ZODB.DemoStorage import DemoStorage
        from ZODB.utils import z64
        db = DB(self._storage) # creates object 0. :)
        s2 = DemoStorage(base=self._storage)
        self.assertEqual(s2.load(z64, ''), self._storage.load(z64, ''))

    def checkLengthAndBool(self):
        import transaction
        from ZODB.DB import DB
        self.assertEqual(len(self._storage), 0)
        self.assert_(not self._storage)
        db = DB(self._storage) # creates object 0. :)
        self.assertEqual(len(self._storage), 1)
        self.assert_(self._storage)
        conn = db.open()
        for i in range(10):
            conn.root()[i] = conn.root().__class__()
        transaction.commit()
        self.assertEqual(len(self._storage), 11)
        self.assert_(self._storage)

    def checkLoadBeforeUndo(self):
        pass # we don't support undo yet
    checkUndoZombie = checkLoadBeforeUndo


class DemoStorageHexTests(DemoStorageTests):

    def setUp(self):
        from ZODB.DemoStorage import DemoStorage
        from ZODB.tests.hexstorage import HexStorage
        StorageTestBase.StorageTestBase.setUp(self)
        self._storage = HexStorage(DemoStorage())

class DemoStorageWrappedBase(DemoStorageTests):

    def setUp(self):
        from ZODB.DemoStorage import DemoStorage
        StorageTestBase.StorageTestBase.setUp(self)
        self._base = self._makeBaseStorage()
        self._storage = DemoStorage(base=self._base)

    def tearDown(self):
        self._base.close()
        StorageTestBase.StorageTestBase.tearDown(self)

    def _makeBaseStorage(self):
        raise NotImplementedError

    def checkPackOnlyOneObject(self):
        pass # Wrapping demo storages don't do gc

    def checkPackWithMultiDatabaseReferences(self):
        pass # we never do gc
    checkPackAllRevisions = checkPackWithMultiDatabaseReferences

class DemoStorageWrappedAroundMappingStorage(DemoStorageWrappedBase):

    def _makeBaseStorage(self):
        from ZODB.MappingStorage import MappingStorage
        return MappingStorage()

class DemoStorageWrappedAroundFileStorage(DemoStorageWrappedBase):

    def _makeBaseStorage(self):
        from ZODB.FileStorage import FileStorage
        return FileStorage('FileStorageTests.fs')

class DemoStorageWrappedAroundHexMappingStorage(DemoStorageWrappedBase):

    def _makeBaseStorage(self):
        from ZODB.MappingStorage import MappingStorage
        from ZODB.tests.hexstorage import HexStorage
        return HexStorage(MappingStorage())


def setUp(test):
    import random
    from ZODB.tests.util import setUp
    random.seed(0)
    setUp(test)

def testSomeDelegation():
    r"""
    >>> class S:
    ...     def __init__(self, name):
    ...         self.name = name
    ...     def registerDB(self, db):
    ...         print self.name, db
    ...     def close(self):
    ...         print self.name, 'closed'
    ...     sortKey = getSize = __len__ = history = getTid = None
    ...     tpc_finish = tpc_vote = tpc_transaction = None
    ...     _lock_acquire = _lock_release = lambda self: None
    ...     getName = lambda self: 'S'
    ...     isReadOnly = tpc_transaction = None
    ...     supportsUndo = undo = undoLog = undoInfo = None
    ...     supportsTransactionalUndo = None
    ...     def new_oid(self):
    ...         return '\0' * 8
    ...     def tpc_begin(self, t, tid, status):
    ...         print 'begin', tid, status
    ...     def tpc_abort(self, t):
    ...         pass

    >>> from ZODB.DemoStorage import DemoStorage
    >>> storage = DemoStorage(base=S(1), changes=S(2))

    >>> storage.registerDB(1)
    2 1

    >>> storage.close()
    1 closed
    2 closed

    >>> storage.tpc_begin(1, 2, 3)
    begin 2 3
    >>> storage.tpc_abort(1)

    """

def blob_pos_key_error_with_non_blob_base():
    """
    >>> from ZODB.DemoStorage import DemoStorage
    >>> from ZODB.utils import p64
    >>> storage = DemoStorage()
    >>> storage.loadBlob(p64(1), p64(1))
    Traceback (most recent call last):
    ...
    POSKeyError: 0x01

    >>> storage.openCommittedBlobFile(p64(1), p64(1))
    Traceback (most recent call last):
    ...
    POSKeyError: 0x01

    """

def load_before_base_storage_current():
    """
    Here we'll exercise that DemoStorage's loadBefore method works
    properly when deferring to a record that is current in the
    base storage.

    >>> import time
    >>> import transaction
    >>> import ZODB.MappingStorage
    >>> from ZODB.utils import p64
    >>> from ZODB.utils import u64
    >>> from ZODB.utils import z64

    >>> from ZODB.DB import DB
    >>> from ZODB.DemoStorage import DemoStorage
    >>> base = ZODB.MappingStorage.MappingStorage()
    >>> basedb = DB(base)
    >>> conn = basedb.open()
    >>> conn.root()['foo'] = 'bar'
    >>> transaction.commit()
    >>> conn.close()
    >>> storage = DemoStorage(base=base)
    >>> db = DB(storage)
    >>> conn = db.open()
    >>> conn.root()['foo'] = 'baz'
    >>> time.sleep(.1) # Windows has a low-resolution clock
    >>> transaction.commit()

    >>> oid = z64
    >>> base_current = storage.base.load(oid)
    >>> tid = p64(u64(base_current[1]) + 1)
    >>> base_record = storage.base.loadBefore(oid, tid)
    >>> base_record[-1] is None
    True
    >>> base_current == base_record[:2]
    True

    >>> t = storage.loadBefore(oid, tid)

    The data and tid are the values from the base storage, but the
    next tid is from changes.

    >>> t[:2] == base_record[:2]
    True
    >>> t[-1] == storage.changes.load(oid)[1]
    True

    >>> conn.close()
    >>> db.close()
    >>> base.close()
    """

def test_suite():
    import os
    if os.environ.get('USE_ZOPE_TESTING_DOCTEST'):
        from zope.testing import doctest
    else:
        import doctest
    from ZODB.tests.util import tearDown
    return unittest.TestSuite((
        doctest.DocTestSuite(setUp=setUp, tearDown=tearDown),
        doctest.DocFileSuite('../DemoStorage.test',
                             setUp=setUp, tearDown=tearDown),
        unittest.makeSuite(DemoStorageTests, 'check'),
        unittest.makeSuite(DemoStorageHexTests, 'check'),
        unittest.makeSuite(DemoStorageWrappedAroundFileStorage, 'check'),
        unittest.makeSuite(DemoStorageWrappedAroundMappingStorage, 'check'),
        unittest.makeSuite(DemoStorageWrappedAroundHexMappingStorage, 'check'),
    ))
