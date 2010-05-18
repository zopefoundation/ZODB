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

from ZODB.tests.MinPO import MinPO
import datetime
import doctest
import os
import sys
import time
import transaction
import unittest
import ZODB
import ZODB.tests.util

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
        self.assert_(self.db.references is ZODB.serialize.referencesf)


def test_invalidateCache():
    """The invalidateCache method invalidates a connection caches for all of
    the connections attached to a database::

        >>> from ZODB.tests.util import DB
        >>> import transaction
        >>> db = DB()
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
    >>> now = 1228423244.5
    >>> def faux_time():
    ...     global now
    ...     now += .1
    ...     return now
    >>> real_time = time.time
    >>> time.time = faux_time

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
    >>> conn.root()['b'] = ZODB.blob.Blob('test')
    >>> transaction.commit()
    >>> conn.close()

    >>> db = ZODB.DB('data.fs', blob_dir='blobs')
    >>> conn = db.open()
    >>> conn.root()['b'].open().read()
    'test'
    >>> db.close()

    """

if sys.version_info >= (2, 6):
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
    >>> import sys, StringIO
    >>> stderr = sys.stderr
    >>> sys.stderr = StringIO.StringIO()
    >>> db = ZODB.DB('t.fs')
    >>> c = db.open('')
    >>> sys.stderr.getvalue() # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    '...: DeprecationWarning: A version string was passed to
    open.\nThe first argument is a transaction manager...

    >>> sys.stderr = stderr
    >>> c.root()
    {}
    >>> db.close()
    """

def warn_when_data_records_are_big():
    """
When data records are large, a warning is issued to try to prevent new
users from shooting themselves in the foot.

    >>> import warnings
    >>> old_warn = warnings.warn
    >>> def faux_warn(message, *args):
    ...     print message,
    ...     if args: print args
    >>> warnings.warn = faux_warn

    >>> db = ZODB.DB('t.fs', create=True)
    >>> conn = db.open()
    >>> conn.root.x = 'x'*(1<<24)
    >>> transaction.commit()
    The <class 'persistent.mapping.PersistentMapping'>
    object you're saving is large. (16777284 bytes.)
    <BLANKLINE>
    Perhaps you're storing media which should be stored in blobs.
    <BLANKLINE>
    Perhaps you're using a non-scalable data structure, such as a
    PersistentMapping or PersistentList.
    <BLANKLINE>
    Perhaps you're storing data in objects that aren't persistent at
    all. In cases like that, the data is stored in the record of the
    containing persistent object.
    <BLANKLINE>
    In any case, storing records this big is probably a bad idea.
    <BLANKLINE>
    If you insist and want to get rid of this warning, use the
    large_record_size option of the ZODB.DB constructor (or the
    large-record-size option in a configuration file) to specify a larger
    size.

    >>> db.close()

The large_record_size option can be used to control the record size:

    >>> db = ZODB.DB('t.fs', create=True, large_record_size=999)
    >>> conn = db.open()
    >>> conn.root.x = 'x'
    >>> transaction.commit()

    >>> conn.root.x = 'x'*999
    >>> transaction.commit() # doctest: +ELLIPSIS
    The <class 'persistent.mapping.PersistentMapping'>
    object you're saving is large. (1067 bytes.)
    ...

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
    >>> transaction.commit() # doctest: +ELLIPSIS
    The <class 'persistent.mapping.PersistentMapping'>
    object you're saving is large. (1048644 bytes.)
    ...

    >>> db.close()

    >>> warnings.warn = old_warn
    """

def test_suite():
    s = unittest.makeSuite(DBTests)
    s.addTest(doctest.DocTestSuite(
        setUp=ZODB.tests.util.setUp, tearDown=ZODB.tests.util.tearDown,
        ))
    return s
