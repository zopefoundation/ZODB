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
import os
import time
import unittest
import datetime

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

    def tearDown(self):
        self.db.close()
        for s in ('', '.index', '.lock', '.tmp'):
            if os.path.exists(self.__path+s):
                os.remove(self.__path+s)

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

    def test_removeHistoricalPool(self):
        # Test that we can remove a historical pool

        # This is white box because we check some internal data structures

        serial1, root_serial1 = self.dowork()
        now = datetime.datetime.utcnow()
        serial2, root_serial2 = self.dowork()
        self.failUnless(root_serial1 < root_serial2)
        c1 = self.db.open(at=now)
        root = c1.root()
        root.keys() # wake up object to get proper serial set
        self.assertEqual(root._p_serial, root_serial1)
        c1.close() # return to pool
        c12 = self.db.open(at=now)
        c12.close() # return to pool
        self.assert_(c1 is c12) # should be same

        pools = self.db._pools

        self.assertEqual(len(pools), 2)
        self.assertEqual(nconn(pools), 2)

        self.db.removeHistoricalPool(at=now)

        self.assertEqual(len(pools), 1)
        self.assertEqual(nconn(pools), 1)

        c12 = self.db.open(at=now)
        c12.close() # return to pool
        self.assert_(c1 is not c12) # should be different

        self.assertEqual(len(pools), 2)
        self.assertEqual(nconn(pools), 2)

    def _test_for_leak(self):
        self.dowork()
        now = datetime.datetime.utcnow()
        self.dowork()
        while 1:
            c1 = self.db.open(at=now)
            self.db.removeHistoricalPool(at=now)
            c1.close() # return to pool

    def test_removeHistoricalPool_while_connection_open(self):
        # Test that we can remove a version pool

        # This is white box because we check some internal data structures

        self.dowork()
        now = datetime.datetime.utcnow()
        self.dowork()
        c1 = self.db.open(at=now)
        c1.close() # return to pool
        c12 = self.db.open(at=now)
        self.assert_(c1 is c12) # should be same

        pools = self.db._pools

        self.assertEqual(len(pools), 2)
        self.assertEqual(nconn(pools), 2)

        self.db.removeHistoricalPool(at=now)

        self.assertEqual(len(pools), 1)
        self.assertEqual(nconn(pools), 1)

        c12.close() # should leave pools alone

        self.assertEqual(len(pools), 1)
        self.assertEqual(nconn(pools), 1)

        c12 = self.db.open(at=now)
        c12.close() # return to pool
        self.assert_(c1 is not c12) # should be different

        self.assertEqual(len(pools), 2)
        self.assertEqual(nconn(pools), 2)

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
