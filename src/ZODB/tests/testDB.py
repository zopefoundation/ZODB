##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
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

import ZODB
import ZODB.FileStorage

from ZODB.tests.MinPO import MinPO

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

    def dowork(self, version=''):
        c = self.db.open(version)
        r = c.root()
        o = r[time.time()] = MinPO(0)
        get_transaction().commit()
        for i in range(25):
            o.value = MinPO(i)
            get_transaction().commit()
            o = o.value
        c.close()

    # make sure the basic methods are callable

    def testSets(self):
        # test set methods that have non-trivial implementations
        warnings.filterwarnings("error", category=DeprecationWarning)
        self.assertRaises(DeprecationWarning,
                          self.db.setCacheDeactivateAfter, 12)
        self.assertRaises(DeprecationWarning,
                          self.db.setVersionCacheDeactivateAfter, 12)
        # XXX There is no API call for removing the warning we just
        # added, but filters appears to be a public variable.
        del warnings.filters[0]
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

        pools, pooll = self.db._pools

        self.assertEqual(len(pools), 3)
        self.assertEqual(len(pooll), 3)

        self.db.removeVersionPool('v1')

        self.assertEqual(len(pools), 2)
        self.assertEqual(len(pooll), 2)

        c12 = self.db.open('v1')
        c12.close() # return to pool
        self.assert_(c1 is not c12) # should be different

        self.assertEqual(len(pools), 3)
        self.assertEqual(len(pooll), 3)

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

        pools, pooll = self.db._pools

        self.assertEqual(len(pools), 3)
        self.assertEqual(len(pooll), 3)

        self.db.removeVersionPool('v1')

        self.assertEqual(len(pools), 2)
        self.assertEqual(len(pooll), 2)

        c12.close() # should leave pools alone

        self.assertEqual(len(pools), 2)
        self.assertEqual(len(pooll), 2)

        c12 = self.db.open('v1')
        c12.close() # return to pool
        self.assert_(c1 is not c12) # should be different

        self.assertEqual(len(pools), 3)
        self.assertEqual(len(pooll), 3)


def test_suite():
    return unittest.makeSuite(DBTests)
