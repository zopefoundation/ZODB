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
"""Unit tests for the Connection class."""
from __future__ import print_function

import doctest
import re
import sys
import unittest
from contextlib import contextmanager

import six

import transaction
from persistent import Persistent
from transaction import Transaction
from zope.interface.verify import verifyObject
from zope.testing import loggingsupport
from zope.testing import renormalizing

import ZODB.tests.util
from ZODB.config import databaseFromString
from ZODB.utils import p64
from ZODB.utils import u64
from ZODB.utils import z64

from .. import mvccadapter


checker = renormalizing.RENormalizing([
    # Python 3 bytes add a "b".
    (re.compile("b('.*?')"), r"\1"),
    # Python 3 removes empty list representation.
    (re.compile(r"set\(\[\]\)"), r"set()"),
    # Python 3 adds module name to exceptions.
    (re.compile("ZODB.POSException.POSKeyError"), r"POSKeyError"),
    (re.compile("ZODB.POSException.ReadConflictError"), r"ReadConflictError"),
    (re.compile("ZODB.POSException.ConflictError"), r"ConflictError"),
    (re.compile("ZODB.POSException.ConnectionStateError"),
     r"ConnectionStateError"),
])


class ConnectionDotAdd(ZODB.tests.util.TestCase):

    def setUp(self):
        ZODB.tests.util.TestCase.setUp(self)
        from ZODB.Connection import Connection
        self.db = StubDatabase()
        self.datamgr = Connection(self.db)
        self.datamgr.open()
        self.transaction = Transaction()

    def test_add(self):
        from ZODB.POSException import InvalidObjectReference
        obj = StubObject()
        self.assertTrue(obj._p_oid is None)
        self.assertTrue(obj._p_jar is None)
        self.datamgr.add(obj)
        self.assertTrue(obj._p_oid is not None)
        self.assertTrue(obj._p_jar is self.datamgr)
        self.assertTrue(self.datamgr.get(obj._p_oid) is obj)

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

    def testResetOnAbort(self):
        # Check that _p_oid and _p_jar are reset when a transaction is
        # aborted.
        obj = StubObject()
        self.datamgr.add(obj)
        oid = obj._p_oid
        self.datamgr.abort(self.transaction)
        self.assertTrue(obj._p_oid is None)
        self.assertTrue(obj._p_jar is None)
        self.assertRaises(KeyError, self.datamgr.get, oid)

    def testResetOnTpcAbort(self):
        obj = StubObject()
        self.datamgr.add(obj)
        oid = obj._p_oid

        # Simulate an error while committing some other object.

        self.datamgr.tpc_begin(self.transaction)
        # Let's pretend something bad happens here.
        # Call tpc_abort, clearing everything.
        self.datamgr.tpc_abort(self.transaction)
        self.assertTrue(obj._p_oid is None)
        self.assertTrue(obj._p_jar is None)
        self.assertRaises(KeyError, self.datamgr.get, oid)

    def testTpcAbortAfterCommit(self):
        obj = StubObject()
        self.datamgr.add(obj)
        oid = obj._p_oid
        self.datamgr.tpc_begin(self.transaction)
        self.datamgr.commit(self.transaction)
        # Let's pretend something bad happened here.
        self.datamgr.tpc_abort(self.transaction)
        self.assertTrue(obj._p_oid is None)
        self.assertTrue(obj._p_jar is None)
        self.assertRaises(KeyError, self.datamgr.get, oid)
        self.assertEqual(self.db.storage._stored, [oid])

    def testCommit(self):
        obj = StubObject()
        self.datamgr.add(obj)
        oid = obj._p_oid
        self.datamgr.tpc_begin(self.transaction)
        self.datamgr.commit(self.transaction)
        self.datamgr.tpc_finish(self.transaction)
        self.assertTrue(obj._p_oid is oid)
        self.assertTrue(obj._p_jar is self.datamgr)

        # This next assertTrue is covered by an assert in tpc_finish.
        # self.assertTrue(not self.datamgr._added)

        self.assertEqual(self.db.storage._stored, [oid])
        self.assertEqual(self.db.storage._finished, [oid])

    def testModifyOnGetstate(self):
        member = StubObject()
        subobj = StubObject()
        subobj.member = member
        obj = ModifyOnGetStateObject(subobj)
        self.datamgr.add(obj)
        self.datamgr.tpc_begin(self.transaction)
        self.datamgr.commit(self.transaction)
        self.datamgr.tpc_finish(self.transaction)
        storage = self.db.storage
        self.assertTrue(obj._p_oid in storage._stored, "object was not stored")
        self.assertTrue(subobj._p_oid in storage._stored,
                        "subobject was not stored")
        self.assertTrue(member._p_oid in storage._stored,
                        "member was not stored")
        self.assertTrue(self.datamgr._added_during_commit is None)

    def testUnusedAddWorks(self):
        # When an object is added, but not committed, it shouldn't be stored,
        # but also it should be an error.
        obj = StubObject()
        self.datamgr.add(obj)
        self.datamgr.tpc_begin(self.transaction)
        self.datamgr.tpc_finish(self.transaction)
        self.assertTrue(obj._p_oid not in
                        self.datamgr._storage._storage._stored)

    def test__resetCacheResetsReader(self):
        # https://bugs.launchpad.net/zodb/+bug/142667
        old_cache = self.datamgr._cache
        self.datamgr._resetCache()
        new_cache = self.datamgr._cache
        self.assertFalse(new_cache is old_cache)
        self.assertTrue(self.datamgr._reader._cache is new_cache)


class SetstateErrorLoggingTests(ZODB.tests.util.TestCase):

    def setUp(self):
        ZODB.tests.util.TestCase.setUp(self)
        self.db = databaseFromString("<zodb>\n<mappingstorage/>\n</zodb>")
        self.datamgr = self.db.open()
        self.object = StubObject()
        self.datamgr.add(self.object)
        transaction.commit()
        self.handler = loggingsupport.InstalledHandler("ZODB")

    def tearDown(self):
        self.handler.uninstall()

    def test_closed_connection_wont_setstate(self):
        self.object._p_deactivate()
        self.datamgr.close()
        self.assertRaises(
            ZODB.POSException.ConnectionStateError,
            self.datamgr.setstate, self.object)
        record, = self.handler.records
        self.assertEqual(
            record.msg,
            "Shouldn't load state for ZODB.tests.testConnection.StubObject"
            " 0x01 when the connection is closed")
        self.assertTrue(record.exc_info)


class UserMethodTests(unittest.TestCase):

    # add isn't tested here, because there are a bunch of traditional
    # unit tests for it.

    def doctest_root(self):
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

    def doctest_get(self):
        r"""doctest of get() method

        The get() method return the persistent object corresponding to
        an oid.

        >>> db = databaseFromString("<zodb>\n<mappingstorage/>\n</zodb>")
        >>> cn = db.open()
        >>> cn.cacheMinimize() # See fix84.rst
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
        ghosts alive, although on some implementations like PyPy we
        need to run a garbage collection to be sure they go away. (The
        next object returned may still have the same id, because Python
        may re-use the same memory.)

        >>> del obj, obj2
        >>> import gc
        >>> _ = gc.collect()
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

        A request for an object that doesn't exist will raise a POSKeyError.

        >>> cn.get(p64(1))
        Traceback (most recent call last):
          ...
        POSKeyError: 0x01
        """

    def doctest_close(self):
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

    def doctest_close_with_pending_changes(self):
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

    def doctest_onCloseCallbacks(self):
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

    def doctest_close_dispatches_to_activity_monitors(self):
        r"""doctest that connection close updates activity monitors

        Set up a multi-database:

            >>> db1 = ZODB.DB(None)
            >>> db2 = ZODB.DB(None, databases=db1.databases, database_name='2',
            ...               cache_size=10)
            >>> conn1 = db1.open()
            >>> conn2 = conn1.get_connection('2')

        Add activity monitors to both dbs:

            >>> from ZODB.ActivityMonitor import ActivityMonitor
            >>> db1.setActivityMonitor(ActivityMonitor())
            >>> db2.setActivityMonitor(ActivityMonitor())

        Commit a transaction that affects both connections:

            >>> conn1.root()[0] = conn1.root().__class__()
            >>> conn2.root()[0] = conn2.root().__class__()
            >>> transaction.commit()

        After closing the primary connection, both monitors should be up to
        date:

            >>> conn1.close()
            >>> len(db1.getActivityMonitor().log)
            1
            >>> len(db2.getActivityMonitor().log)
            1
        """

    def doctest_db(self):
        r"""doctest of db() method

        >>> db = databaseFromString("<zodb>\n<mappingstorage/>\n</zodb>")
        >>> cn = db.open()
        >>> cn.db() is db
        True
        >>> cn.close()
        >>> cn.db() is db
        True
        """

    def doctest_isReadOnly(self):
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

        >>> db.close()

        An expedient way to create a read-only storage:

        >>> db = databaseFromString("<zodb>\n<mappingstorage/>\n</zodb>")
        >>> db.storage.isReadOnly = lambda: True
        >>> cn = db.open()
        >>> cn.isReadOnly()
        True
        """

    def doctest_cache(self):
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


def doctest_transaction_retry_convenience():
    """
    Simple test to verify integration with the transaction retry
    helper my verifying that we can raise ConflictError and have it
    handled properly.

    This is an adaptation of the convenience tests in transaction.

    >>> db = ZODB.tests.util.DB()
    >>> conn = db.open()
    >>> dm = conn.root()

    >>> ntry = 0
    >>> with transaction.manager:
    ...      dm['ntry'] = 0

    >>> import ZODB.POSException
    >>> for attempt in transaction.manager.attempts():
    ...     with attempt as t:
    ...         t.note(u'test')
    ...         six.print_(dm['ntry'], ntry)
    ...         ntry += 1
    ...         dm['ntry'] = ntry
    ...         if ntry % 3:
    ...             raise ZODB.POSException.ConflictError()
    0 0
    0 1
    0 2
    """


class InvalidationTests(unittest.TestCase):

    # It's harder to write serious tests, because some of the critical
    # correctness issues relate to concurrency.  We'll have to depend
    # on the various concurrent updates and NZODBThreads tests to
    # handle these.

    def doctest_invalidate(self):
        r"""

        This test initializes the database with several persistent
        objects, then manually delivers invalidations and verifies that
        they have the expected effect.

        >>> db = databaseFromString("<zodb>\n<mappingstorage/>\n</zodb>")
        >>> mvcc_storage = db._mvcc_storage
        >>> cn = db.open()
        >>> mvcc_instance = cn._storage
        >>> p1 = Persistent()
        >>> p2 = Persistent()
        >>> p3 = Persistent()
        >>> r = cn.root()
        >>> r.update(dict(p1=p1, p2=p2, p3=p3))
        >>> transaction.commit()

        Transaction ids are 8-byte strings, just like oids; p64() will
        create one from an int.

        >>> mvcc_storage.invalidate(p64(1), {p1._p_oid: 1})

        Transaction start times are based on storage's last
        transaction. (Previousely, they were based on the first
        invalidation seen in a transaction.)

        >>> mvcc_instance.poll_invalidations() == [p1._p_oid]
        True
        >>> mvcc_instance._start == p64(u64(db.storage.lastTransaction()) + 1)
        True

        >>> mvcc_storage.invalidate(p64(10), {p2._p_oid: 1, p64(76): 1})

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
        0
        >>> p2._p_state
        -1
        >>> p3._p_state
        0

        >>> db.close()
        """

    def test_mvccadapterNewTransactionVsInvalidations(self):
        """
        Check that polled invalidations are consistent with the TID at which
        the transaction operates. Otherwise, it's like we miss invalidations.
        """
        db = databaseFromString("<zodb>\n<mappingstorage/>\n</zodb>")
        try:
            t1 = transaction.TransactionManager()
            c1 = db.open(t1)
            r1 = c1.root()
            r1['a'] = 1
            t1.commit()
            t2 = transaction.TransactionManager()
            c2 = db.open(t2)
            c2.root()['b'] = 1
            s1 = c1._storage
            l1 = s1._lock

            @contextmanager
            def beforeLock1():
                s1._lock = l1
                t2.commit()
                with l1:
                    yield
            s1._lock = beforeLock1()
            t1.begin()
            self.assertIs(s1._lock, l1)
            self.assertIn('b', r1)
        finally:
            db.close()


def doctest_invalidateCache():
    """The invalidateCache method invalidates a connection's cache.

    It also prevents reads until the end of a transaction::

        >>> from ZODB.tests.util import DB
        >>> import transaction
        >>> db = DB()
        >>> mvcc_storage = db._mvcc_storage
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

        >>> mvcc_storage.invalidateCache()

    This won't have any effect until the next transaction:

        >>> connection.root()['a']._p_changed
        0
        >>> connection.root()['b']._p_changed
        >>> connection.root()['c']._p_changed
        1

    But if we sync():

        >>> connection.sync()

    All of our data was invalidated:

        >>> connection.root()['a']._p_changed
        >>> connection.root()['b']._p_changed
        >>> connection.root()['c']._p_changed

    But we can load data as usual:

    Now, if we try to load an object, we'll get a read conflict:

        >>> connection.root()['b'].x
        1

        >>> db.close()
    """


def doctest_connection_root_convenience():
    """Connection root attributes can now be used as objects with attributes

    >>> db = ZODB.tests.util.DB()
    >>> conn = db.open()
    >>> conn.root.x
    Traceback (most recent call last):
    ...
    AttributeError: x

    >>> del conn.root.x
    Traceback (most recent call last):
    ...
    AttributeError: x

    >>> conn.root()['x'] = 1
    >>> conn.root.x
    1
    >>> conn.root.y = 2
    >>> sorted(conn.root().items())
    [('x', 1), ('y', 2)]

    >>> conn.root
    <root: x y>

    >>> del conn.root.x
    >>> sorted(conn.root().items())
    [('y', 2)]

    >>> conn.root.rather_long_name = 1
    >>> conn.root.rather_long_name2 = 1
    >>> conn.root.rather_long_name4 = 1
    >>> conn.root.rather_long_name5 = 1
    >>> conn.root
    <root: rather_long_name rather_long_name2 rather_long_name4 ...>
    """


class proper_ghost_initialization_with_empty__p_deactivate_class(Persistent):
    def _p_deactivate(self):
        pass


def doctest_proper_ghost_initialization_with_empty__p_deactivate():
    """
    See https://bugs.launchpad.net/zodb/+bug/185066

    >>> db = ZODB.tests.util.DB()
    >>> conn = db.open()
    >>> C = proper_ghost_initialization_with_empty__p_deactivate_class
    >>> conn.root.x = x = C()
    >>> conn.root.x.y = 1
    >>> transaction.commit()

    >>> conn2 = db.open()
    >>> bool(conn2.root.x._p_changed)
    False
    >>> conn2.root.x.y
    1

    """


def doctest_readCurrent():
    r"""
    The connection's readCurrent method is called to provide a higher
    level of consistency in cases where an object is read to compute an
    update to a separate object.  When this is used, the
    checkCurrentSerialInTransaction method on the storage is called in
    2-phase commit.

    To demonstrate this, we'll create a storage and give it a test
    implementation of checkCurrentSerialInTransaction.

    >>> import ZODB.MappingStorage
    >>> store = ZODB.MappingStorage.MappingStorage()

    >>> from ZODB.POSException import ReadConflictError
    >>> bad = set()
    >>> def checkCurrentSerialInTransaction(oid, serial, trans):
    ...     six.print_('checkCurrentSerialInTransaction', repr(oid))
    ...     if oid in bad:
    ...         raise ReadConflictError(oid=oid)

    >>> store.checkCurrentSerialInTransaction = checkCurrentSerialInTransaction

    Now, we'll use the storage as usual.  checkCurrentSerialInTransaction
    won't normally be called:

    >>> db = ZODB.DB(store)
    >>> conn = db.open()
    >>> conn.root.a = ZODB.tests.util.P('a')
    >>> transaction.commit()
    >>> conn.root.b = ZODB.tests.util.P('b')
    >>> transaction.commit()

    >>> conn.root.a._p_oid
    b'\x00\x00\x00\x00\x00\x00\x00\x01'
    >>> conn.root.b._p_oid
    b'\x00\x00\x00\x00\x00\x00\x00\x02'

    If we call readCurrent for an object and we modify another object,
    then checkCurrentSerialInTransaction will be called for the object
    readCurrent was called on.

    >>> conn.readCurrent(conn.root.a)
    >>> conn.root.b.x = 0
    >>> transaction.commit()
    checkCurrentSerialInTransaction '\x00\x00\x00\x00\x00\x00\x00\x01'

    It doesn't matter how often we call readCurrent,
    checkCurrentSerialInTransaction will be called only once:

    >>> conn.readCurrent(conn.root.a)
    >>> conn.readCurrent(conn.root.a)
    >>> conn.readCurrent(conn.root.a)
    >>> conn.readCurrent(conn.root.a)
    >>> conn.root.b.x += 1
    >>> transaction.commit()
    checkCurrentSerialInTransaction '\x00\x00\x00\x00\x00\x00\x00\x01'

    checkCurrentSerialInTransaction won't be called if another object
    isn't modified:


    >>> conn.readCurrent(conn.root.a)
    >>> transaction.commit()

    Or if the object it was called on is modified:

    >>> conn.readCurrent(conn.root.a)
    >>> conn.root.a.x = 0
    >>> conn.root.b.x += 1
    >>> transaction.commit()

    If the storage raises a conflict error, it'll be propagated:

    >>> _ = str(conn.root.a) # do read
    >>> bad.add(conn.root.a._p_oid)
    >>> conn.readCurrent(conn.root.a)
    >>> conn.root.b.x += 1
    >>> transaction.commit()
    Traceback (most recent call last):
    ...
    ReadConflictError: database read conflict error (oid 0x01)

    >>> transaction.abort()

    The conflict error will cause the affected object to be invalidated:

    >>> conn.root.a._p_changed

    The storage may raise it later:

    >>> def checkCurrentSerialInTransaction(oid, serial, trans):
    ...     if not trans == transaction.get(): print('oops')
    ...     six.print_('checkCurrentSerialInTransaction', repr(oid))
    ...     store.badness = ReadConflictError(oid=oid)

    >>> def tpc_vote(t):
    ...     if store.badness:
    ...        badness = store.badness
    ...        store.badness = None
    ...        raise badness

    >>> store.checkCurrentSerialInTransaction = checkCurrentSerialInTransaction
    >>> store.badness = None
    >>> store.tpc_vote = tpc_vote

    It will still be propagated:

    >>> _ = str(conn.root.a) # do read
    >>> conn.readCurrent(conn.root.a)
    >>> conn.root.b.x = +1
    >>> transaction.commit()
    Traceback (most recent call last):
    ...
    ReadConflictError: database read conflict error (oid 0x01)

    >>> transaction.abort()

    The conflict error will cause the affected object to be invalidated:

    >>> conn.root.a._p_changed

    Read checks don't leak across transactions:

    >>> conn.readCurrent(conn.root.a)
    >>> transaction.commit()
    >>> conn.root.b.x = +1
    >>> transaction.commit()

    Read checks to work across savepoints.

    >>> conn.readCurrent(conn.root.a)
    >>> conn.root.b.x = +1
    >>> _ = transaction.savepoint()
    >>> transaction.commit()
    Traceback (most recent call last):
    ...
    ReadConflictError: database read conflict error (oid 0x01)

    >>> transaction.abort()

    >>> conn.readCurrent(conn.root.a)
    >>> _ = transaction.savepoint()
    >>> conn.root.b.x = +1
    >>> transaction.commit()
    Traceback (most recent call last):
    ...
    ReadConflictError: database read conflict error (oid 0x01)

    >>> transaction.abort()

    """


def doctest_cache_management_of_subconnections():
    """Make that cache management works for subconnections.

    When we use multi-databases, we open a connection in one database and
    access connections to other databases through it.  This test verifies
    that cache management is applied to all of the connections.

    Set up a multi-database:

    >>> db1 = ZODB.DB(None)
    >>> db2 = ZODB.DB(None, databases=db1.databases, database_name='2',
    ...               cache_size=10)
    >>> conn1 = db1.open()
    >>> conn2 = conn1.get_connection('2')

    Populate it with some data, more than will fit in the cache:

    >>> for i in range(100):
    ...     conn2.root()[i] = conn2.root().__class__()

    Upon commit, the cache is reduced to the cache size:

    >>> transaction.commit()
    >>> conn2._cache.cache_non_ghost_count
    10

    Fill it back up:

    >>> for i in range(100):
    ...     _ = str(conn2.root()[i])
    >>> conn2._cache.cache_non_ghost_count
    101

    Doing cache GC on the primary also does it on the secondary:

    >>> conn1.cacheGC()
    >>> conn2._cache.cache_non_ghost_count
    10

    Ditto for cache minimize:

    >>> conn1.cacheMinimize()
    >>> conn2._cache.cache_non_ghost_count
    0


    Fill it back up:

    >>> for i in range(100):
    ...     _ = str(conn2.root()[i])
    >>> conn2._cache.cache_non_ghost_count
    101

    GC is done on reopen:

    >>> conn1.close()
    >>> db1.open() is conn1
    True
    >>> conn2 is conn1.get_connection('2')
    True

    >>> conn2._cache.cache_non_ghost_count
    10

    """


class C_invalidations_of_new_objects_work_after_savepoint(Persistent):
    def __init__(self):
        self.settings = 1

    def _p_invalidate(self):
        print('INVALIDATE', self.settings)
        Persistent._p_invalidate(self)
        print(self.settings)   # POSKeyError here


def doctest_abort_of_savepoint_creating_new_objects_w_exotic_invalidate_doesnt_break():  # noqa: E501 line too long
    r"""
    Before, the following would fail with a POSKeyError, which was
    somewhat surprising, in a very edgy sort of way. :)

    Really, when an object add is aborted, the object should be "removed" from
    the db and its invalidation method shouldn't even be called:

    >>> conn = ZODB.connection(None)
    >>> conn.root.x = x = C_invalidations_of_new_objects_work_after_savepoint()
    >>> _ = transaction.savepoint()
    >>> x._p_oid
    '\x00\x00\x00\x00\x00\x00\x00\x01'

    >>> x._p_jar is conn
    True

    >>> transaction.abort()

    After the abort, the oid and jar are None:

    >>> x._p_oid
    >>> x._p_jar

    """


class Clp9460655(Persistent):
    def __init__(self, word, id):
        super(Clp9460655, self).__init__()
        self.id = id
        self._word = word


def doctest_lp9460655():
    r"""
    >>> conn = ZODB.connection(None)
    >>> root = conn.root()
    >>> Word = Clp9460655

    >>> from BTrees.OOBTree import OOBTree
    >>> data = root['data'] = OOBTree()

    >>> commonWords = []
    >>> count = "0"
    >>> for x in ('hello', 'world', 'how', 'are', 'you'):
    ...         commonWords.append(Word(x, count))
    ...         count = str(int(count) + 1)

    >>> sv = transaction.savepoint()
    >>> for word in commonWords:
    ...         sv2 = transaction.savepoint()
    ...         data[word.id] = word

    >>> sv.rollback()
    >>> print(commonWords[1].id)  # raises POSKeyError
    1

    """


def doctest_lp615758_transaction_abort_Incomplete_cleanup_for_new_objects():
    r"""

    As the following doctest demonstrates, "abort" forgets to
    reset "_p_changed" for new (i.e. "added") objects.

    >>> class P(Persistent): pass
    ...
    >>> c = ZODB.connection(None)
    >>> obj = P()
    >>> c.add(obj)
    >>> obj.x = 1
    >>> obj._p_changed
    True
    >>> transaction.abort()
    >>> obj._p_changed
    False

    >>> c.close()
    """


class Clp485456_setattr_in_getstate_doesnt_cause_multiple_stores(Persistent):

    def __getstate__(self):
        self.got = 1
        return self.__dict__.copy()


def doctest_lp485456_setattr_in_setstate_doesnt_cause_multiple_stores():
    r"""
    >>> C = Clp485456_setattr_in_getstate_doesnt_cause_multiple_stores
    >>> conn = ZODB.connection(None)
    >>> oldstore = conn._storage.store
    >>> def store(oid, *args):
    ...     six.print_('storing', repr(oid))
    ...     return oldstore(oid, *args)
    >>> conn._storage.store = store

    When we commit a change, we only get a single store call

    >>> conn.root.x = C()
    >>> transaction.commit()
    storing '\x00\x00\x00\x00\x00\x00\x00\x00'
    storing '\x00\x00\x00\x00\x00\x00\x00\x01'

    Retry with the new object registered before its referrer.

    >>> z = C()
    >>> conn.add(z)
    >>> conn.root.z = z
    >>> transaction.commit()
    storing '\x00\x00\x00\x00\x00\x00\x00\x02'
    storing '\x00\x00\x00\x00\x00\x00\x00\x00'

    We still see updates:

    >>> conn.root.x.y = 1
    >>> transaction.commit()
    storing '\x00\x00\x00\x00\x00\x00\x00\x01'

    Not not non-updates:

    >>> transaction.commit()

    Let's try some combinations with savepoints:

    >>> conn.root.n = 0
    >>> _ = transaction.savepoint()

    >>> oldspstore = conn._storage.store
    >>> def store(oid, *args):
    ...     six.print_('savepoint storing', repr(oid))
    ...     return oldspstore(oid, *args)
    >>> conn._storage.store = store

    >>> conn.root.y = C()
    >>> _ = transaction.savepoint()
    savepoint storing '\x00\x00\x00\x00\x00\x00\x00\x00'
    savepoint storing '\x00\x00\x00\x00\x00\x00\x00\x03'

    >>> conn.root.y.x = 1
    >>> _  = transaction.savepoint()
    savepoint storing '\x00\x00\x00\x00\x00\x00\x00\x03'

    >>> transaction.commit()
    storing '\x00\x00\x00\x00\x00\x00\x00\x00'
    storing '\x00\x00\x00\x00\x00\x00\x00\x03'

    >>> conn.close()
    """


class _PlayPersistent(Persistent):
    def setValueWithSize(self, size=0): self.value = size*' '
    __init__ = setValueWithSize


class EstimatedSizeTests(ZODB.tests.util.TestCase):
    """check that size estimations are handled correctly."""

    def setUp(self):
        ZODB.tests.util.TestCase.setUp(self)
        self.db = db = databaseFromString("<zodb>\n<mappingstorage/>\n</zodb>")
        self.conn = c = db.open()
        self.obj = obj = _PlayPersistent()
        c.root()['obj'] = obj
        transaction.commit()

    def test_size_set_on_write_commit(self):
        obj, cache = self.obj, self.conn._cache
        # we have just written "obj". Its size should not be zero
        size, cache_size = obj._p_estimated_size, cache.total_estimated_size
        self.assertTrue(size > 0)
        self.assertTrue(cache_size > size)
        # increase the size, write again and check that the size changed
        obj.setValueWithSize(1000)
        transaction.commit()
        new_size = obj._p_estimated_size
        self.assertTrue(new_size > size)
        self.assertEqual(cache.total_estimated_size,
                         cache_size + new_size - size)

    def test_size_set_on_write_savepoint(self):
        obj, cache = self.obj, self.conn._cache
        # we have just written "obj". Its size should not be zero
        size, cache_size = obj._p_estimated_size, cache.total_estimated_size
        # increase the size, write again and check that the size changed
        obj.setValueWithSize(1000)
        transaction.savepoint()
        new_size = obj._p_estimated_size
        self.assertTrue(new_size > size)
        self.assertEqual(cache.total_estimated_size,
                         cache_size + new_size - size)

    def test_size_set_on_load(self):
        c = self.db.open()  # new connection
        obj = c.root()['obj']
        # the object is still a ghost and '_p_estimated_size' not yet set
        # access to unghost
        cache = c._cache
        cache_size = cache.total_estimated_size
        obj.value
        size = obj._p_estimated_size
        self.assertTrue(size > 0)
        self.assertEqual(cache.total_estimated_size, cache_size + size)
        # we test here as well that the deactivation works reduced the cache
        # size
        obj._p_deactivate()
        self.assertEqual(cache.total_estimated_size, cache_size)

    def test_configuration(self):
        # verify defaults ....
        expected = 0
        # ... on db
        db = self.db
        self.assertEqual(db.getCacheSizeBytes(), expected)
        self.assertEqual(db.getHistoricalCacheSizeBytes(), expected)
        # ... on connection
        conn = self.conn
        self.assertEqual(conn._cache.cache_size_bytes, expected)
        # verify explicit setting ...
        expected = 10000
        # ... on db
        db = databaseFromString("<zodb>\n"
                                "  cache-size-bytes %d\n"
                                "  historical-cache-size-bytes %d\n"
                                "  <mappingstorage />\n"
                                "</zodb>"
                                % (expected, expected+1)
                                )
        self.assertEqual(db.getCacheSizeBytes(), expected)
        self.assertEqual(db.getHistoricalCacheSizeBytes(), expected+1)
        # ... on connectionB
        conn = db.open()
        self.assertEqual(conn._cache.cache_size_bytes, expected)
        # test huge (larger than 4 byte) size limit (if possible)
        if sys.maxsize > (0x1 << 33):
            db = databaseFromString("<zodb>\n"
                                    "  cache-size-bytes 8GB\n"
                                    "  <mappingstorage />\n"
                                    "</zodb>"
                                    )
            self.assertEqual(db.getCacheSizeBytes(), 0x1 << 33)

    def test_cache_garbage_collection(self):
        db = self.db
        # activate size based cache garbage collection
        db.setCacheSizeBytes(1)
        conn = self.conn
        cache = conn._cache
        # verify the change worked as expected
        self.assertEqual(cache.cache_size_bytes, 1)
        # verify our entrance assumption is fulfilled
        self.assertTrue(cache.total_estimated_size > 1)
        conn.cacheGC()
        self.assertTrue(cache.total_estimated_size <= 1)
        # sanity check
        self.assertTrue(cache.total_estimated_size >= 0)

    def test_cache_garbage_collection_shrinking_object(self):
        db = self.db
        # activate size based cache garbage collection
        db.setCacheSizeBytes(1000)
        obj, cache = self.obj, self.conn._cache
        # verify the change worked as expected
        self.assertEqual(cache.cache_size_bytes, 1000)
        # verify our entrance assumption is fulfilled
        self.assertTrue(cache.total_estimated_size > 1)
        # give the objects some size
        obj.setValueWithSize(500)
        transaction.savepoint()
        self.assertTrue(cache.total_estimated_size > 500)
        # make the object smaller
        obj.setValueWithSize(100)
        transaction.savepoint()
        # make sure there was no overflow
        self.assertTrue(cache.total_estimated_size != 0)
        # the size is not larger than the allowed maximum
        self.assertTrue(cache.total_estimated_size <= 1000)

# ---- stubs


class StubObject(Persistent):
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


class StubStorage(object):
    """Very simple in-memory storage that does *just* enough to support tests.

    Only one concurrent transaction is supported.
    Voting is not supported.

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
        return str(oid).encode()

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
        return z64

    def load(self, oid, version=''):
        if version != '':
            raise TypeError('StubStorage does not support versions.')
        return self._data[oid]

    def loadBefore(self, oid, tid):
        return self._data[oid] + (None, )

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

    def lastTransaction(self):
        return z64


class TestConnection(unittest.TestCase):

    def test_connection_interface(self):
        from ZODB.interfaces import IConnection
        db = databaseFromString("<zodb>\n<mappingstorage/>\n</zodb>")
        cn = db.open()
        verifyObject(IConnection, cn)
        db.close()

    def test_storage_afterCompletionCalled(self):
        db = ZODB.DB(None)
        conn = db.open()
        data = []
        conn._storage.afterCompletion = lambda: data.append(None)
        conn.transaction_manager.commit()
        self.assertEqual(len(data), 1)
        conn.close()
        self.assertEqual(len(data), 2)
        db.close()

    def test_explicit_transactions_no_newTransactuon_on_afterCompletion(self):
        syncs = []
        from .MVCCMappingStorage import MVCCMappingStorage
        storage = MVCCMappingStorage()

        new_instance = storage.new_instance

        def new_instance2():
            inst = new_instance()
            sync = inst.sync

            def sync2(*args):
                sync()
                syncs.append(1)
            inst.sync = sync2
            return inst

        storage.new_instance = new_instance2

        db = ZODB.DB(storage)
        del syncs[:]  # Need to do this to clear effect of getting the
        # root object

        # We don't want to depend on latest transaction package, so
        # just set attr for test:
        tm = transaction.TransactionManager()
        tm.explicit = True

        conn = db.open(tm)
        self.assertEqual(len(syncs), 0)
        conn.transaction_manager.begin()
        self.assertEqual(len(syncs), 1)
        conn.transaction_manager.commit()
        self.assertEqual(len(syncs), 1)
        conn.transaction_manager.begin()
        self.assertEqual(len(syncs), 2)
        conn.transaction_manager.abort()
        self.assertEqual(len(syncs), 2)
        conn.close()
        self.assertEqual(len(syncs), 2)

        # For reference, in non-explicit mode:
        conn = db.open()
        self.assertEqual(len(syncs), 3)
        conn._storage.sync = syncs.append
        conn.transaction_manager.begin()
        self.assertEqual(len(syncs), 4)
        conn.transaction_manager.abort()
        self.assertEqual(len(syncs), 5)
        conn.close()

        db.close()


class StubDatabase(object):

    def __init__(self):
        self.storage = StubStorage()
        self._mvcc_storage = mvccadapter.MVCCAdapter(self.storage)
        self.new_oid = self.storage.new_oid

    classFactory = None
    database_name = 'stubdatabase'
    databases = {'stubdatabase': database_name}

    def invalidate(self, transaction, dict_with_oid_keys, connection):
        pass

    large_record_size = 1 << 30


def test_suite():
    s = unittest.makeSuite(ConnectionDotAdd)
    s.addTest(unittest.makeSuite(SetstateErrorLoggingTests))
    s.addTest(doctest.DocTestSuite(checker=checker))
    s.addTest(unittest.makeSuite(TestConnection))
    s.addTest(unittest.makeSuite(EstimatedSizeTests))
    s.addTest(unittest.makeSuite(InvalidationTests))
    return s
