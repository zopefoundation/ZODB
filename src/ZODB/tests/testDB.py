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
from zope.testing import doctest
import datetime
import os
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
      'opened': '2008-12-04T15:40:44 (1.40s)'},
     {'before': '\x03zY\xd8\xc0m9\xdd',
      'info': ' (0)',
      'opened': '2008-12-04T15:40:45 (0.30s)'},
     {'before': None,
      'info': ' (0)',
      'opened': None}]

    >>> time.time = real_time

    """


def test_suite():
    s = unittest.makeSuite(DBTests)
    s.addTest(doctest.DocTestSuite())
    return s
