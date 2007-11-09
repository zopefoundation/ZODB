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
"""Unit tests for the Connection class."""

from zope.testing import doctest
import unittest
import warnings

from persistent import Persistent
import transaction
from ZODB.config import databaseFromString
from ZODB.utils import p64, u64
from ZODB.tests.warnhook import WarningsHook
from zope.interface.verify import verifyObject

class ConnectionDotAdd(unittest.TestCase):

    def setUp(self):
        from ZODB.Connection import Connection
        self.db = StubDatabase()
        self.datamgr = Connection(self.db)
        self.datamgr.open()
        self.transaction = StubTransaction()

    def tearDown(self):
        transaction.abort()

    def check_add(self):
        from ZODB.POSException import InvalidObjectReference
        obj = StubObject()
        self.assert_(obj._p_oid is None)
        self.assert_(obj._p_jar is None)
        self.datamgr.add(obj)
        self.assert_(obj._p_oid is not None)
        self.assert_(obj._p_jar is self.datamgr)
        self.assert_(self.datamgr.get(obj._p_oid) is obj)

        # Only first-class persistent objects may be added.
        self.assertRaises(TypeError, self.datamgr.add, object())

        # Adding to the same connection does not fail. Object keeps the
        # same oid.
        oid = obj._p_oid
        self.datamgr.add(obj)
        self.assertEqual(obj._p_oid, oid)

        # Cannot add an object from a different connection.
        obj2 = StubObject()
        obj2._p_jar = object()
        self.assertRaises(InvalidObjectReference, self.datamgr.add, obj2)

    def checkResetOnAbort(self):
        # Check that _p_oid and _p_jar are reset when a transaction is
        # aborted.
        obj = StubObject()
        self.datamgr.add(obj)
        oid = obj._p_oid
        self.datamgr.abort(self.transaction)
        self.assert_(obj._p_oid is None)
        self.assert_(obj._p_jar is None)
        self.assertRaises(KeyError, self.datamgr.get, oid)

    def checkResetOnTpcAbort(self):
        obj = StubObject()
        self.datamgr.add(obj)
        oid = obj._p_oid

        # Simulate an error while committing some other object.

        self.datamgr.tpc_begin(self.transaction)
        # Let's pretend something bad happens here.
        # Call tpc_abort, clearing everything.
        self.datamgr.tpc_abort(self.transaction)
        self.assert_(obj._p_oid is None)
        self.assert_(obj._p_jar is None)
        self.assertRaises(KeyError, self.datamgr.get, oid)

    def checkTpcAbortAfterCommit(self):
        obj = StubObject()
        self.datamgr.add(obj)
        oid = obj._p_oid
        self.datamgr.tpc_begin(self.transaction)
        self.datamgr.commit(self.transaction)
        # Let's pretend something bad happened here.
        self.datamgr.tpc_abort(self.transaction)
        self.assert_(obj._p_oid is None)
        self.assert_(obj._p_jar is None)
        self.assertRaises(KeyError, self.datamgr.get, oid)
        self.assertEquals(self.db._storage._stored, [oid])

    def checkCommit(self):
        obj = StubObject()
        self.datamgr.add(obj)
        oid = obj._p_oid
        self.datamgr.tpc_begin(self.transaction)
        self.datamgr.commit(self.transaction)
        self.datamgr.tpc_finish(self.transaction)
        self.assert_(obj._p_oid is oid)
        self.assert_(obj._p_jar is self.datamgr)

        # This next assert_ is covered by an assert in tpc_finish.
        ##self.assert_(not self.datamgr._added)

        self.assertEquals(self.db._storage._stored, [oid])
        self.assertEquals(self.db._storage._finished, [oid])

    def checkModifyOnGetstate(self):
        member = StubObject()
        subobj = StubObject()
        subobj.member = member
        obj = ModifyOnGetStateObject(subobj)
        self.datamgr.add(obj)
        self.datamgr.tpc_begin(self.transaction)
        self.datamgr.commit(self.transaction)
        self.datamgr.tpc_finish(self.transaction)
        storage = self.db._storage
        self.assert_(obj._p_oid in storage._stored, "object was not stored")
        self.assert_(subobj._p_oid in storage._stored,
                "subobject was not stored")
        self.assert_(member._p_oid in storage._stored, "member was not stored")
        self.assert_(self.datamgr._added_during_commit is None)

    def checkUnusedAddWorks(self):
        # When an object is added, but not committed, it shouldn't be stored,
        # but also it should be an error.
        obj = StubObject()
        self.datamgr.add(obj)
        self.datamgr.tpc_begin(self.transaction)
        self.datamgr.tpc_finish(self.transaction)
        self.assert_(obj._p_oid not in self.datamgr._storage._stored)

class UserMethodTests(unittest.TestCase):

    # add isn't tested here, because there are a bunch of traditional
    # unit tests for it.

    # The version tests would require a storage that supports versions
    # which is a bit more work.

    def test_root(self):
        r"""doctest of root() method

        The root() method is simple, and the tests are pretty minimal.
        Ensure that a new database has a root and that it is a
        PersistentMapping.

        >>> db = databaseFromString("<zodb>\n<mappingstorage/>\n</zodb>")
        >>> cn = db.open()
        >>> root = cn.root()
        >>> type(root).__name__
        'PersistentMapping'
        >>> root._p_oid
        '\x00\x00\x00\x00\x00\x00\x00\x00'
        >>> root._p_jar is cn
        True
        >>> db.close()
        """

    def test_get(self):
        r"""doctest of get() method

        The get() method return the persistent object corresponding to
        an oid.

        >>> db = databaseFromString("<zodb>\n<mappingstorage/>\n</zodb>")
        >>> cn = db.open()
        >>> obj = cn.get(p64(0))
        >>> obj._p_oid
        '\x00\x00\x00\x00\x00\x00\x00\x00'

        The object is a ghost.

        >>> obj._p_state
        -1

        And multiple calls with the same oid, return the same object.

        >>> obj2 = cn.get(p64(0))
        >>> obj is obj2
        True

        If all references to the object are released, then a new
        object will be returned. The cache doesn't keep unreferenced
        ghosts alive.  (The next object returned my still have the
        same id, because Python may re-use the same memory.)

        >>> del obj, obj2
        >>> cn._cache.get(p64(0), None)

        If the object is unghosted, then it will stay in the cache
        after the last reference is released.  (This is true only if
        there is room in the cache and the object is recently used.)

        >>> obj = cn.get(p64(0))
        >>> obj._p_activate()
        >>> y = id(obj)
        >>> del obj
        >>> obj = cn.get(p64(0))
        >>> id(obj) == y
        True
        >>> obj._p_state
        0

        A request for an object that doesn't exist will raise a KeyError.

        >>> cn.get(p64(1))
        Traceback (most recent call last):
          ...
        KeyError: '\x00\x00\x00\x00\x00\x00\x00\x01'
        """

    def test_close(self):
        r"""doctest of close() method

        This is a minimal test, because most of the interesting
        effects on closing a connection involve its interaction with the
        database and the transaction.

        >>> db = databaseFromString("<zodb>\n<mappingstorage/>\n</zodb>")
        >>> cn = db.open()

        It's safe to close a connection multiple times.
        >>> cn.close()
        >>> cn.close()
        >>> cn.close()

        It's not possible to load or store objects once the storage is closed.

        >>> cn.get(p64(0))
        Traceback (most recent call last):
          ...
        ConnectionStateError: The database connection is closed
        >>> p = Persistent()
        >>> cn.add(p)
        Traceback (most recent call last):
          ...
        ConnectionStateError: The database connection is closed
        """

    def test_close_with_pending_changes(self):
        r"""doctest to ensure close() w/ pending changes complains

        >>> import transaction

        Just opening and closing is fine.
        >>> db = databaseFromString("<zodb>\n<mappingstorage/>\n</zodb>")
        >>> cn = db.open()
        >>> cn.close()

        Opening, making a change, committing, and closing is fine.
        >>> cn = db.open()
        >>> cn.root()['a'] = 1
        >>> transaction.commit()
        >>> cn.close()

        Opening, making a change, and aborting is fine.
        >>> cn = db.open()
        >>> cn.root()['a'] = 1
        >>> transaction.abort()
        >>> cn.close()

        But trying to close with a change pending complains.
        >>> cn = db.open()
        >>> cn.root()['a'] = 10
        >>> cn.close()
        Traceback (most recent call last):
          ...
        ConnectionStateError: Cannot close a connection joined to a transaction

        This leaves the connection as it was, so we can still commit
        the change.
        >>> transaction.commit()
        >>> cn2 = db.open()
        >>> cn2.root()['a']
        10
        >>> cn.close(); cn2.close()

        >>> db.close()
        """

    def test_onCloseCallbacks(self):
        r"""doctest of onCloseCallback() method

        >>> db = databaseFromString("<zodb>\n<mappingstorage/>\n</zodb>")
        >>> cn = db.open()

        Every function registered is called, even if it raises an
        exception.  They are only called once.

        >>> L = []
        >>> def f():
        ...     L.append("f")
        >>> def g():
        ...     L.append("g")
        ...     return 1 / 0
        >>> cn.onCloseCallback(g)
        >>> cn.onCloseCallback(f)
        >>> cn.close()
        >>> L
        ['g', 'f']
        >>> del L[:]
        >>> cn.close()
        >>> L
        []

        The implementation keeps a list of callbacks that is reset
        to a class variable (which is bound to None) after the connection
        is closed.

        >>> cn._Connection__onCloseCallbacks
        """

    def test_db(self):
        r"""doctest of db() method

        >>> db = databaseFromString("<zodb>\n<mappingstorage/>\n</zodb>")
        >>> cn = db.open()
        >>> cn.db() is db
        True
        >>> cn.close()
        >>> cn.db() is db
        True
        """

    def test_isReadOnly(self):
        r"""doctest of isReadOnly() method

        >>> db = databaseFromString("<zodb>\n<mappingstorage/>\n</zodb>")
        >>> cn = db.open()
        >>> cn.isReadOnly()
        False
        >>> cn.close()
        >>> cn.isReadOnly()
        Traceback (most recent call last):
          ...
        ConnectionStateError: The database connection is closed

        An expedient way to create a read-only storage:

        >>> db._storage._is_read_only = True
        >>> cn = db.open()
        >>> cn.isReadOnly()
        True
        """

    def test_cache(self):
        r"""doctest of cacheMinimize().

        Thus test us minimal, just verifying that the method can be called
        and has some effect.  We need other tests that verify the cache works
        as intended.

        >>> db = databaseFromString("<zodb>\n<mappingstorage/>\n</zodb>")
        >>> cn = db.open()
        >>> r = cn.root()
        >>> cn.cacheMinimize()
        >>> r._p_state
        -1

        >>> r._p_activate()
        >>> r._p_state  # up to date
        0
        >>> cn.cacheMinimize()
        >>> r._p_state  # ghost again
        -1
        """

class InvalidationTests(unittest.TestCase):

    # It's harder to write serious tests, because some of the critical
    # correctness issues relate to concurrency.  We'll have to depend
    # on the various concurrent updates and NZODBThreads tests to
    # handle these.

    def test_invalidate(self):
        r"""

        This test initializes the database with several persistent
        objects, then manually delivers invalidations and verifies that
        they have the expected effect.

        >>> db = databaseFromString("<zodb>\n<mappingstorage/>\n</zodb>")
        >>> cn = db.open()
        >>> p1 = Persistent()
        >>> p2 = Persistent()
        >>> p3 = Persistent()
        >>> r = cn.root()
        >>> r.update(dict(p1=p1, p2=p2, p3=p3))
        >>> transaction.commit()

        Transaction ids are 8-byte strings, just like oids; p64() will
        create one from an int.

        >>> cn.invalidate(p64(1), {p1._p_oid: 1})
        >>> cn._txn_time
        '\x00\x00\x00\x00\x00\x00\x00\x01'
        >>> p1._p_oid in cn._invalidated
        True
        >>> p2._p_oid in cn._invalidated
        False

        >>> cn.invalidate(p64(10), {p2._p_oid: 1, p64(76): 1})
        >>> cn._txn_time
        '\x00\x00\x00\x00\x00\x00\x00\x01'
        >>> p1._p_oid in cn._invalidated
        True
        >>> p2._p_oid in cn._invalidated
        True

        Calling invalidate() doesn't affect the object state until
        a transaction boundary.

        >>> p1._p_state
        0
        >>> p2._p_state
        0
        >>> p3._p_state
        0

        The sync() method will abort the current transaction and
        process any pending invalidations.

        >>> cn.sync()
        >>> p1._p_state
        -1
        >>> p2._p_state
        -1
        >>> p3._p_state
        0
        >>> cn._invalidated
        set([])

        """

def test_invalidateCache():
    """The invalidateCache method invalidates a connection's cache.  It also
    prevents reads until the end of a transaction::

        >>> from ZODB.tests.util import DB
        >>> import transaction
        >>> db = DB()
        >>> tm = transaction.TransactionManager()
        >>> connection = db.open(transaction_manager=tm)
        >>> connection.root()['a'] = StubObject()
        >>> connection.root()['a'].x = 1
        >>> connection.root()['b'] = StubObject()
        >>> connection.root()['b'].x = 1
        >>> connection.root()['c'] = StubObject()
        >>> connection.root()['c'].x = 1
        >>> tm.commit()
        >>> connection.root()['b']._p_deactivate()
        >>> connection.root()['c'].x = 2

    So we have a connection and an active transaction with some modifications.
    Lets call invalidateCache:

        >>> connection.invalidateCache()

    Now, if we try to load an object, we'll get a read conflict:

        >>> connection.root()['b'].x
        Traceback (most recent call last):
        ...
        ReadConflictError: database read conflict error

    If we try to commit the transaction, we'll get a conflict error:

        >>> tm.commit()
        Traceback (most recent call last):
        ...
        ConflictError: database conflict error

    and the cache will have been cleared:

        >>> print connection.root()['a']._p_changed
        None
        >>> print connection.root()['b']._p_changed
        None
        >>> print connection.root()['c']._p_changed
        None

    But we'll be able to access data again:

        >>> connection.root()['b'].x
        1

    Aborting a transaction after a read conflict also lets us read data and go
    on about our business:

        >>> connection.invalidateCache()

        >>> connection.root()['c'].x
        Traceback (most recent call last):
        ...
        ReadConflictError: database read conflict error

        >>> tm.abort()
        >>> connection.root()['c'].x
        1

        >>> connection.root()['c'].x = 2
        >>> tm.commit()

        >>> db.close()
    """

# ---- stubs

class StubObject(Persistent):
    pass

class StubTransaction:
    pass

class ErrorOnGetstateException(Exception):
    pass

class ErrorOnGetstateObject(Persistent):

    def __getstate__(self):
        raise ErrorOnGetstateException

class ModifyOnGetStateObject(Persistent):

    def __init__(self, p):
        self._v_p = p

    def __getstate__(self):
        self._p_jar.add(self._v_p)
        self.p = self._v_p
        return Persistent.__getstate__(self)


class StubStorage:
    """Very simple in-memory storage that does *just* enough to support tests.

    Only one concurrent transaction is supported.
    Voting is not supported.
    Versions are not supported.

    Inspect self._stored and self._finished to see how the storage has been
    used during a unit test. Whenever an object is stored in the store()
    method, its oid is appended to self._stored. When a transaction is
    finished, the oids that have been stored during the transaction are
    appended to self._finished.
    """

    # internal
    _oid = 1
    _transaction = None

    def __init__(self):
        # internal
        self._stored = []
        self._finished = []
        self._data = {}
        self._transdata = {}
        self._transstored = []

    def new_oid(self):
        oid = str(self._oid)
        self._oid += 1
        return oid

    def sortKey(self):
        return 'StubStorage sortKey'

    def tpc_begin(self, transaction):
        if transaction is None:
            raise TypeError('transaction may not be None')
        elif self._transaction is None:
            self._transaction = transaction
        elif self._transaction != transaction:
            raise RuntimeError(
                'StubStorage uses only one transaction at a time')

    def tpc_abort(self, transaction):
        if transaction is None:
            raise TypeError('transaction may not be None')
        elif self._transaction != transaction:
            raise RuntimeError(
                'StubStorage uses only one transaction at a time')
        del self._transaction
        self._transdata.clear()

    def tpc_finish(self, transaction, callback):
        if transaction is None:
            raise TypeError('transaction may not be None')
        elif self._transaction != transaction:
            raise RuntimeError(
                'StubStorage uses only one transaction at a time')
        self._finished.extend(self._transstored)
        self._data.update(self._transdata)
        callback(transaction)
        del self._transaction
        self._transdata.clear()
        self._transstored = []

    def load(self, oid, version):
        if version != '':
            raise TypeError('StubStorage does not support versions.')
        return self._data[oid]

    def store(self, oid, serial, p, version, transaction):
        if version != '':
            raise TypeError('StubStorage does not support versions.')
        if transaction is None:
            raise TypeError('transaction may not be None')
        elif self._transaction != transaction:
            raise RuntimeError(
                'StubStorage uses only one transaction at a time')
        self._stored.append(oid)
        self._transstored.append(oid)
        self._transdata[oid] = (p, serial)
        # Explicitly returing None, as we're not pretending to be a ZEO
        # storage
        return None


class TestConnectionInterface(unittest.TestCase):

    def test_connection_interface(self):
        from ZODB.interfaces import IConnection
        db = databaseFromString("<zodb>\n<mappingstorage/>\n</zodb>")
        cn = db.open()
        verifyObject(IConnection, cn)


class StubDatabase:

    def __init__(self):
        self._storage = StubStorage()

    classFactory = None
    database_name = 'stubdatabase'
    databases = {'stubdatabase': database_name}

    def invalidate(self, transaction, dict_with_oid_keys, connection):
        pass

def test_suite():
    s = unittest.makeSuite(ConnectionDotAdd, 'check')
    s.addTest(doctest.DocTestSuite())
    s.addTest(unittest.makeSuite(TestConnectionInterface))
    return s
