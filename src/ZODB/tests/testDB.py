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

# used as a base class
from ZODB.tests.util import TestCase as utilTestCase


class DBTests(utilTestCase):

    def setUp(self):
        from ZODB.DB import DB
        utilTestCase.setUp(self)
        self.db = DB('test.fs')

    def tearDown(self):
        self.db.close()
        utilTestCase.tearDown(self)

    def dowork(self):
        import time
        import transaction
        from ZODB.tests.MinPO import MinPO
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


def test_invalidateCache():
    """The invalidateCache method invalidates a connection caches for all of
    the connections attached to a database::

    >>> from ZODB.DB import DB
    >>> from ZODB.tests.MinPO import MinPO
    >>> import transaction
    >>> db = DB(None)
    >>> tm1 = transaction.TransactionManager()
    >>> c1 = db.open(transaction_manager=tm1)
    >>> c1.root()['a'] = MinPO(1)
    >>> tm1.commit()
    >>> tm2 = transaction.TransactionManager()
    >>> c2 = db.open(transaction_manager=tm2)
    >>> c1.root()['a']._p_deactivate()
    >>> tm3 = transaction.TransactionManager()
    >>> c3 = db.open(transaction_manager=tm3)
    >>> c3.root()['a'].value
    1
    >>> c3.close()
    >>> db.invalidateCache()

    >>> c1.root()['a'].value
    Traceback (most recent call last):
    ...
    ReadConflictError: database read conflict error

    >>> c2.root()['a'].value
    Traceback (most recent call last):
    ...
    ReadConflictError: database read conflict error

    >>> c3 is db.open(transaction_manager=tm3)
    True
    >>> print c3.root()['a']._p_changed
    None

    >>> db.close()
    """

def connectionDebugInfo():
    r"""DB.connectionDebugInfo provides information about connections.

    >>> import time
    >>> from ZODB.tests.MinPO import MinPO
    >>> now = 1228423244.5
    >>> def faux_time():
    ...     global now
    ...     now += .1
    ...     return now
    >>> real_time = time.time
    >>> time.time = faux_time

    >>> from ZODB.DB import DB
    >>> import transaction
    >>> db = DB(None)
    >>> c1 = db.open()
    >>> c1.setDebugInfo('test info')
    >>> c1.root()['a'] = MinPO(1)
    >>> transaction.commit()
    >>> c2 = db.open()
    >>> _ = c1.root()['a']
    >>> c2.close()

    >>> c3 = db.open(before=c1.root()._p_serial)

    >>> info = db.connectionDebugInfo()
    >>> import pprint
    >>> pprint.pprint(sorted(info, key=lambda i: str(i['opened'])), width=1)
    [{'before': None,
      'info': 'test info (2)',
      'opened': '2008-12-04T20:40:44Z (1.40s)'},
     {'before': '\x03zY\xd8\xc0m9\xdd',
      'info': ' (0)',
      'opened': '2008-12-04T20:40:45Z (0.30s)'},
     {'before': None,
      'info': ' (0)',
      'opened': None}]

    >>> time.time = real_time

    """

def passing_a_file_name_to_DB():
    """You can pass a file-storage file name to DB.

    (Also note that we can access DB in ZODB.)

    >>> import os
    >>> from ZODB.DB import DB
    >>> db = DB('data.fs')
    >>> db.storage # doctest: +ELLIPSIS
    <ZODB.FileStorage.FileStorage.FileStorage object at ...
    >>> os.path.exists('data.fs')
    True

    >>> db.close()
    """

def passing_None_to_DB():
    """You can pass None DB to get a MappingStorage.

    (Also note that we can access DB in ZODB.)

    >>> from ZODB.DB import DB
    >>> db = DB(None)
    >>> db.storage # doctest: +ELLIPSIS
    <ZODB.MappingStorage.MappingStorage object at ...
    >>> db.close()
    """

def open_convenience():
    """Often, we just want to open a single connection.

    >>> import transaction
    >>> from ZODB import connection
    >>> conn = connection('data.fs')
    >>> conn.root()
    {}

    >>> conn.root()['x'] = 1
    >>> transaction.commit()
    >>> conn.close()

    Let's make sure the database was cloased when we closed the
    connection, and that the data is there.

    >>> from ZODB.DB import DB
    >>> db = DB('data.fs')
    >>> conn = db.open()
    >>> conn.root()
    {'x': 1}
    >>> db.close()


    We can pass storage-specific arguments if they don't conflict with
    DB arguments.

    >>> from ZODB.blob import Blob
    >>> conn = connection('data.fs', blob_dir='blobs')
    >>> conn.root()['b'] = Blob('test')
    >>> transaction.commit()
    >>> conn.close()

    >>> db = DB('data.fs', blob_dir='blobs')
    >>> conn = db.open()
    >>> conn.root()['b'].open().read()
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

    >>> import transaction
    >>> from ZODB.DB import DB
    >>> db = DB(None)
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
    ...     XXX
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
    >>> from ZODB.DB import DB
    >>> db = DB('t.fs')
    >>> from ZODB.tests.util import assert_deprecated
    >>> c = assert_deprecated(
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

    >>> import transaction
    >>> from ZODB.DB import DB
    >>> from ZODB.tests.util import assert_warning
    >>> db = DB('t.fs', create=True)
    >>> conn = db.open()
    >>> conn.root.x = 'x'*(1<<24)
    >>> assert_warning(UserWarning, transaction.commit,
    ...    "object you're saving is large.")
    >>> db.close()

The large_record_size option can be used to control the record size:

    >>> from ZODB.DB import DB
    >>> db = DB('t.fs', create=True, large_record_size=999)
    >>> conn = db.open()
    >>> conn.root.x = 'x'
    >>> transaction.commit()

    >>> conn.root.x = 'x'*999
    >>> assert_warning(UserWarning, transaction.commit,
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
    >>> assert_warning(UserWarning, transaction.commit,
    ...    "object you're saving is large.")

    >>> db.close()
    """ # '

def minimally_test_connection_timeout():
    """There's a mechanism to discard old connections.

    Make sure it doesn't error. :)

    >>> import time
    >>> from ZODB.DB import DB
    >>> db = DB(None, pool_timeout=.01)
    >>> c1 = db.open()
    >>> c2 = db.open()
    >>> c1.close()
    >>> c2.close()
    >>> time.sleep(.02)
    >>> db.open() is c2
    True

    >>> db.pool.available
    []

    """

def test_suite():
    import doctest
    from ZODB.tests.util import setUp
    from ZODB.tests.util import tearDown
    return unittest.TestSuite((
        unittest.makeSuite(DBTests),
        doctest.DocTestSuite(setUp=setUp, tearDown=tearDown),
    ))
