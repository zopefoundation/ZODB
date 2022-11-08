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
import doctest
import re
import time
import unittest

from six import PY2

import transaction
from zope.testing import renormalizing

import ZODB
import ZODB.tests.util
from ZODB.tests.MinPO import MinPO


checker = renormalizing.RENormalizing([
    # Python 3 bytes add a "b".
    (re.compile("b('.*?')"),
     r"\1"),
    # Python 3 adds module name to exceptions.
    (re.compile("ZODB.POSException.ReadConflictError"), r"ReadConflictError"),
])


# Return total number of connections across all pools in a db._pools.
def nconn(pools):
    return sum([len(pool.all) for pool in pools.values()])


class DBTests(ZODB.tests.util.TestCase):

    def setUp(self):
        ZODB.tests.util.TestCase.setUp(self)
        self.db = ZODB.DB('test.fs')

    def tearDown(self):
        self.db.close()
        ZODB.tests.util.TestCase.tearDown(self)

    def dowork(self):
        c = self.db.open()
        r = c.root()
        o = r[time.time()] = MinPO(0)
        transaction.commit()
        for i in range(25):
            o.value = MinPO(i)
            transaction.commit()
            o = o.value
        serial = o._p_serial
        root_serial = r._p_serial
        c.close()
        return serial, root_serial

    # make sure the basic methods are callable

    def testSets(self):
        self.db.setCacheSize(15)
        self.db.setHistoricalCacheSize(15)

    def test_references(self):

        # TODO: For now test that we're using referencesf.  We really should
        #       have tests of referencesf.

        import ZODB.serialize
        self.assertTrue(self.db.references is ZODB.serialize.referencesf)

    def test_history_and_undo_meta_data_text_handlinf(self):
        db = self.db
        conn = db.open()
        for i in range(3):
            with conn.transaction_manager as t:
                t.note(u'work %s' % i)
                t.setUser(u'user%s' % i)
                conn.root()[i] = 42

        conn.close()

        from ZODB.utils import z64

        def check(info, text):
            for i, h in enumerate(reversed(info)):
                for (name, expect) in (('description', 'work %s'),
                                       ('user_name', '/ user%s')):
                    expect = expect % i
                    if not text:
                        expect = expect.encode('ascii')
                    self.assertEqual(h[name], expect)

                if PY2:
                    expect = unicode if text else str  # noqa: F821 undef name
                    for name in 'description', 'user_name':
                        self.assertTrue(isinstance(h[name], expect))

        check(db.storage.history(z64, 3), False)
        check(db.storage.undoLog(0, 3), False)
        check(db.storage.undoInfo(0, 3), False)
        check(db.history(z64, 3), True)
        check(db.undoLog(0, 3), True)
        check(db.undoInfo(0, 3), True)


class TransactionalUndoTests(unittest.TestCase):

    def _makeOne(self):
        from ZODB.DB import TransactionalUndo

        class MockStorage(object):
            instance_count = 0
            close_count = 0
            release_count = 0
            begin_count = 0
            abort_count = 0

            def new_instance(self):
                self.instance_count += 1
                return self

            def tpc_begin(self, tx):
                self.begin_count += 1

            def close(self):
                self.close_count += 1

            def release(self):
                self.release_count += 1

            def sortKey(self):
                return 'MockStorage'

        class MockDB(object):

            def __init__(self):
                self.storage = self._mvcc_storage = MockStorage()

        return TransactionalUndo(MockDB(), [1])

    def test_only_new_instance_on_begin(self):
        undo = self._makeOne()
        self.assertIsNone(undo._storage)
        self.assertEqual(0, undo._db.storage.instance_count)

        undo.tpc_begin(transaction.get())
        self.assertIsNotNone(undo._storage)
        self.assertEqual(1, undo._db.storage.instance_count)
        self.assertEqual(1, undo._db.storage.begin_count)
        self.assertIsNotNone(undo._storage)

        # And we can't begin again
        with self.assertRaises(AssertionError):
            undo.tpc_begin(transaction.get())

    def test_close_many(self):
        undo = self._makeOne()
        self.assertIsNone(undo._storage)
        self.assertEqual(0, undo._db.storage.instance_count)

        undo.close()
        # Not open, didn't go through
        self.assertEqual(0, undo._db.storage.close_count)
        self.assertEqual(0, undo._db.storage.release_count)

        undo.tpc_begin(transaction.get())
        undo.close()
        undo.close()
        self.assertEqual(0, undo._db.storage.close_count)
        self.assertEqual(1, undo._db.storage.release_count)
        self.assertIsNone(undo._storage)

    def test_sortKey(self):
        # We get the same key whether or not we're open
        undo = self._makeOne()
        key = undo.sortKey()
        self.assertIn('MockStorage', key)

        undo.tpc_begin(transaction.get())
        key2 = undo.sortKey()
        self.assertEqual(key, key2)

    def test_tpc_abort_closes(self):
        undo = self._makeOne()
        undo.tpc_begin(transaction.get())
        undo._db.storage.tpc_abort = lambda tx: None
        undo.tpc_abort(transaction.get())
        self.assertEqual(0, undo._db.storage.close_count)
        self.assertEqual(1, undo._db.storage.release_count)

    def test_tpc_abort_closes_on_exception(self):
        undo = self._makeOne()
        undo.tpc_begin(transaction.get())
        with self.assertRaises(AttributeError):
            undo.tpc_abort(transaction.get())
        self.assertEqual(0, undo._db.storage.close_count)
        self.assertEqual(1, undo._db.storage.release_count)

    def test_tpc_finish_closes(self):
        undo = self._makeOne()
        undo.tpc_begin(transaction.get())
        undo._db.storage.tpc_finish = lambda tx: None
        undo.tpc_finish(transaction.get())
        self.assertEqual(0, undo._db.storage.close_count)
        self.assertEqual(1, undo._db.storage.release_count)

    def test_tpc_finish_closes_on_exception(self):
        undo = self._makeOne()
        undo.tpc_begin(transaction.get())
        with self.assertRaises(AttributeError):
            undo.tpc_finish(transaction.get())
        self.assertEqual(0, undo._db.storage.close_count)
        self.assertEqual(1, undo._db.storage.release_count)


def test_invalidateCache():
    """The invalidateCache method invalidates a connection caches for all of
    the connections attached to a database::

        >>> from ZODB.tests.util import DB
        >>> import transaction
        >>> db = DB()
        >>> mvcc_storage = db._mvcc_storage
        >>> tm1 = transaction.TransactionManager()
        >>> c1 = db.open(transaction_manager=tm1)
        >>> c1.root()['a'] = MinPO(1)
        >>> tm1.commit()
        >>> tm2 = transaction.TransactionManager()
        >>> c2 = db.open(transaction_manager=tm2)
        >>> c2.root()['a'].value
        1
        >>> tm3 = transaction.TransactionManager()
        >>> c3 = db.open(transaction_manager=tm3)
        >>> c3.root()['a'].value
        1
        >>> c3.close()

        >>> mvcc_storage.invalidateCache()
        >>> c1.root.a._p_changed
        0
        >>> c1.sync()
        >>> c1.root.a._p_changed
        >>> c2.root.a._p_changed
        0
        >>> c2.sync()
        >>> c2.root.a._p_changed
        >>> c3 is db.open(transaction_manager=tm3)
        True
        >>> c3.root.a._p_changed

        >>> c1.root()['a'].value
        1
        >>> c2.root()['a'].value
        1
        >>> c3.root()['a'].value
        1

        >>> db.close()
    """


def connectionDebugInfo():
    r"""DB.connectionDebugInfo provides information about connections.

    >>> import time
    >>> now = 1228423244.1
    >>> def faux_time():
    ...     global now
    ...     now += .1
    ...     return now
    >>> real_time = time.time
    >>> if isinstance(time, type):
    ...    time.time = staticmethod(faux_time) # Jython
    ... else:
    ...     time.time = faux_time

    >>> from ZODB.tests.util import DB
    >>> import transaction
    >>> db = DB()
    >>> c1 = db.open()
    >>> c1.setDebugInfo('test info')
    >>> c1.root()['a'] = MinPO(1)
    >>> transaction.commit()
    >>> c2 = db.open()
    >>> _ = c1.root()['a']
    >>> c2.close()

    >>> c3 = db.open(before=c1.root()._p_serial)

    >>> info = db.connectionDebugInfo()
    >>> info = sorted(info, key=lambda i: str(i['opened']))
    >>> before = [x['before'] for x in info]
    >>> opened = [x['opened'] for x in info]
    >>> infos = [x['info'] for x in info]
    >>> before == [None, c1.root()._p_serial, None]
    True
    >>> opened
    ['2008-12-04T20:40:44Z (1.30s)', '2008-12-04T20:40:46Z (0.10s)', None]
    >>> infos
    ['test info (2)', ' (0)', ' (0)']

    >>> time.time = real_time

    """


def passing_a_file_name_to_DB():
    """You can pass a file-storage file name to DB.

    (Also note that we can access DB in ZODB.)

    >>> import os
    >>> db = ZODB.DB('data.fs')
    >>> db.storage # doctest: +ELLIPSIS
    <ZODB.FileStorage.FileStorage.FileStorage object at ...
    >>> os.path.exists('data.fs')
    True

    >>> db.close()
    """


def passing_None_to_DB():
    """You can pass None DB to get a MappingStorage.

    (Also note that we can access DB in ZODB.)

    >>> db = ZODB.DB(None)
    >>> db.storage # doctest: +ELLIPSIS
    <ZODB.MappingStorage.MappingStorage object at ...
    >>> db.close()
    """


def open_convenience():
    """Often, we just want to open a single connection.

    >>> conn = ZODB.connection('data.fs')
    >>> conn.root()
    {}

    >>> conn.root()['x'] = 1
    >>> transaction.commit()
    >>> conn.close()

    Let's make sure the database was cloased when we closed the
    connection, and that the data is there.

    >>> db = ZODB.DB('data.fs')
    >>> conn = db.open()
    >>> conn.root()
    {'x': 1}
    >>> db.close()


    We can pass storage-specific arguments if they don't conflict with
    DB arguments.

    >>> conn = ZODB.connection('data.fs', blob_dir='blobs')
    >>> conn.root()['b'] = ZODB.blob.Blob(b'test')
    >>> transaction.commit()
    >>> conn.close()

    >>> db = ZODB.DB('data.fs', blob_dir='blobs')
    >>> conn = db.open()
    >>> with conn.root()['b'].open() as fp: fp.read()
    'test'
    >>> db.close()

    """


def db_with_transaction():
    """Using databases with with

    The transaction method returns a context manager that when entered
    starts a transaction with a private transaction manager.  To
    illustrate this, we start a trasnaction using a regular connection
    and see that it isn't automatically committed or aborted as we use
    the transaction context manager.

    >>> db = ZODB.tests.util.DB()
    >>> conn = db.open()
    >>> conn.root()['x'] = conn.root().__class__()
    >>> transaction.commit()
    >>> conn.root()['x']['x'] = 1

    >>> with db.transaction() as conn2:
    ...     conn2.root()['y'] = 1

    >>> conn2.opened

Now, we'll open a 3rd connection a verify that

    >>> conn3 = db.open()
    >>> conn3.root()['x']
    {}
    >>> conn3.root()['y']
    1
    >>> conn3.close()

Let's try again, but this time, we'll have an exception:

    >>> with db.transaction() as conn2:
    ...     conn2.root()['y'] = 2
    ...     XXX  # noqa: F821 undefined name
    Traceback (most recent call last):
    ...
    NameError: name 'XXX' is not defined

    >>> conn2.opened

    >>> conn3 = db.open()
    >>> conn3.root()['x']
    {}
    >>> conn3.root()['y']
    1
    >>> conn3.close()

    >>> transaction.commit()

    >>> conn3 = db.open()
    >>> conn3.root()['x']
    {'x': 1}


    >>> db.close()
    """


def connection_allows_empty_version_for_idiots():
    r"""
    >>> db = ZODB.DB('t.fs')
    >>> c = ZODB.tests.util.assert_deprecated(
    ...       (lambda : db.open('')),
    ...       'A version string was passed to open')
    >>> c.root()
    {}
    >>> db.close()
    """


def warn_when_data_records_are_big():
    """
When data records are large, a warning is issued to try to prevent new
users from shooting themselves in the foot.

    >>> db = ZODB.DB('t.fs', create=True)
    >>> conn = db.open()
    >>> conn.root.x = 'x'*(1<<24)
    >>> ZODB.tests.util.assert_warning(UserWarning, transaction.commit,
    ...    "object you're saving is large.")
    >>> db.close()

The large_record_size option can be used to control the record size:

    >>> db = ZODB.DB('t.fs', create=True, large_record_size=999)
    >>> conn = db.open()
    >>> conn.root.x = 'x'
    >>> transaction.commit()

    >>> conn.root.x = 'x'*999
    >>> ZODB.tests.util.assert_warning(UserWarning, transaction.commit,
    ...    "object you're saving is large.")

    >>> db.close()

We can also specify it using a configuration option:

    >>> import ZODB.config
    >>> db = ZODB.config.databaseFromString('''
    ...     <zodb>
    ...         large-record-size 1MB
    ...         <filestorage>
    ...             path t.fs
    ...             create true
    ...         </filestorage>
    ...     </zodb>
    ... ''')
    >>> conn = db.open()
    >>> conn.root.x = 'x'
    >>> transaction.commit()

    >>> conn.root.x = 'x'*(1<<20)
    >>> ZODB.tests.util.assert_warning(UserWarning, transaction.commit,
    ...    "object you're saving is large.")

    >>> db.close()
    """  # '


def minimally_test_connection_timeout():
    """There's a mechanism to discard old connections.

    Make sure it doesn't error. :)

    >>> db = ZODB.DB(None, pool_timeout=.01)
    >>> c1 = db.open()
    >>> c1.cacheMinimize() # See fix84.rst
    >>> c2 = db.open()
    >>> c1.close()
    >>> c2.close()
    >>> time.sleep(.02)
    >>> db.open() is c2
    True

    >>> db.pool.available
    []

    """


def cleanup_on_close():
    """Verify that various references are cleared on close

    >>> db = ZODB.DB(None)

    >>> conn = db.open()
    >>> conn.root.x = 'x'
    >>> transaction.commit()
    >>> conn.close()

    >>> historical_conn = db.open(at=db.lastTransaction())
    >>> historical_conn.close()

    >>> db.close()

    >>> db.databases
    {}

    >>> db.pool.pop() is None
    True

    >>> [pool is None for pool in db.historical_pool.pools.values()]
    []
"""


def test_suite():
    s = unittest.defaultTestLoader.loadTestsFromName(__name__)
    s.addTest(doctest.DocTestSuite(
        setUp=ZODB.tests.util.setUp, tearDown=ZODB.tests.util.tearDown,
        checker=checker, optionflags=doctest.IGNORE_EXCEPTION_DETAIL
    ))
    return s
