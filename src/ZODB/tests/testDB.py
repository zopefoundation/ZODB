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
import os
import time
import unittest
import warnings

import transaction

from zope.testing import doctest

import ZODB
import ZODB.FileStorage

from ZODB.tests.MinPO import MinPO

# Return total number of connections across all pools in a db._pools.
def nconn(pools):
    return sum([len(pool.all) for pool in pools.values()])

class DBTests(unittest.TestCase):

    def setUp(self):
        self.__path = os.path.abspath('test.fs')
        store = ZODB.FileStorage.FileStorage(self.__path)
        self.db = ZODB.DB(store)
        warnings.filterwarnings(
            'ignore', message='Versions are deprecated', module=__name__)

    def tearDown(self):
        self.db.close()
        for s in ('', '.index', '.lock', '.tmp'):
            if os.path.exists(self.__path+s):
                os.remove(self.__path+s)

    def dowork(self, version=''):
        c = self.db.open(version)
        r = c.root()
        o = r[time.time()] = MinPO(0)
        transaction.commit()
        for i in range(25):
            o.value = MinPO(i)
            transaction.commit()
            o = o.value
        c.close()

    # make sure the basic methods are callable

    def testSets(self):
        self.db.setCacheSize(15)
        self.db.setVersionCacheSize(15)

    def test_removeVersionPool(self):
        # Test that we can remove a version pool

        # This is white box because we check some internal data structures

        self.dowork()
        self.dowork('v2')
        c1 = self.db.open('v1')
        c1.close() # return to pool
        c12 = self.db.open('v1')
        c12.close() # return to pool
        self.assert_(c1 is c12) # should be same

        pools = self.db._pools

        self.assertEqual(len(pools), 3)
        self.assertEqual(nconn(pools), 3)

        self.db.removeVersionPool('v1')

        self.assertEqual(len(pools), 2)
        self.assertEqual(nconn(pools), 2)

        c12 = self.db.open('v1')
        c12.close() # return to pool
        self.assert_(c1 is not c12) # should be different

        self.assertEqual(len(pools), 3)
        self.assertEqual(nconn(pools), 3)

    def _test_for_leak(self):
        self.dowork()
        self.dowork('v2')
        while 1:
            c1 = self.db.open('v1')
            self.db.removeVersionPool('v1')
            c1.close() # return to pool

    def test_removeVersionPool_while_connection_open(self):
        # Test that we can remove a version pool

        # This is white box because we check some internal data structures

        self.dowork()
        self.dowork('v2')
        c1 = self.db.open('v1')
        c1.close() # return to pool
        c12 = self.db.open('v1')
        self.assert_(c1 is c12) # should be same

        pools = self.db._pools

        self.assertEqual(len(pools), 3)
        self.assertEqual(nconn(pools), 3)

        self.db.removeVersionPool('v1')

        self.assertEqual(len(pools), 2)
        self.assertEqual(nconn(pools), 2)

        c12.close() # should leave pools alone

        self.assertEqual(len(pools), 2)
        self.assertEqual(nconn(pools), 2)

        c12 = self.db.open('v1')
        c12.close() # return to pool
        self.assert_(c1 is not c12) # should be different

        self.assertEqual(len(pools), 3)
        self.assertEqual(nconn(pools), 3)

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


def test_suite():
    s = unittest.makeSuite(DBTests)
    s.addTest(doctest.DocTestSuite())
    return s
