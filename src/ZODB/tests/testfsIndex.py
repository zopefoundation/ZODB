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
import unittest
import random

from ZODB.fsIndex import fsIndex
from ZODB.utils import p64, z64


class Test(unittest.TestCase):

    def setUp(self):
        self.index = fsIndex()

        for i in range(200):
            self.index[p64(i * 1000)] = (i * 1000L + 1)

    def test__del__(self):
        index = self.index
        self.assert_(p64(1000) in index)
        self.assert_(p64(100*1000) in index)
        
        del self.index[p64(1000)]
        del self.index[p64(100*1000)]

        self.assert_(p64(1000) not in index)
        self.assert_(p64(100*1000) not in index)

        for key in list(self.index):
            del index[key]
        self.assert_(not index)

        # Whitebox. Make sure empty buckets are removed
        self.assert_(not index._data)

    def testInserts(self):
        index = self.index

        for i in range(0,200):
            self.assertEqual((i,index[p64(i*1000)]), (i,(i*1000L+1)))

        self.assertEqual(len(index), 200)

        key=p64(2000)

        self.assertEqual(index.get(key), 2001)

        key=p64(2001)
        self.assertEqual(index.get(key), None)
        self.assertEqual(index.get(key, ''), '')

        # self.failUnless(len(index._data) > 1)

    def testUpdate(self):
        index = self.index
        d={}

        for i in range(200):
            d[p64(i*1000)]=(i*1000L+1)

        index.update(d)

        for i in range(400,600):
            d[p64(i*1000)]=(i*1000L+1)

        index.update(d)

        for i in range(100, 500):
            d[p64(i*1000)]=(i*1000L+2)

        index.update(d)

        self.assertEqual(index.get(p64(2000)), 2001)
        self.assertEqual(index.get(p64(599000)), 599001)
        self.assertEqual(index.get(p64(399000)), 399002)
        self.assertEqual(len(index), 600)

    def testKeys(self):
        keys = list(iter(self.index))
        keys.sort()

        for i, k in enumerate(keys):
            self.assertEqual(k, p64(i * 1000))

        keys = list(self.index.iterkeys())
        keys.sort()

        for i, k in enumerate(keys):
            self.assertEqual(k, p64(i * 1000))

        keys = self.index.keys()
        keys.sort()

        for i, k in enumerate(keys):
            self.assertEqual(k, p64(i * 1000))

    def testValues(self):
        values = list(self.index.itervalues())
        values.sort()

        for i, v in enumerate(values):
            self.assertEqual(v, (i * 1000L + 1))

        values = self.index.values()
        values.sort()

        for i, v in enumerate(values):
            self.assertEqual(v, (i * 1000L + 1))

    def testItems(self):
        items = list(self.index.iteritems())
        items.sort()

        for i, item in enumerate(items):
            self.assertEqual(item, (p64(i * 1000), (i * 1000L + 1)))

        items = self.index.items()
        items.sort()

        for i, item in enumerate(items):
            self.assertEqual(item, (p64(i * 1000), (i * 1000L + 1)))

    def testMaxKey(self):
        index = self.index
        index.clear()

        # An empty index should complain.
        self.assertRaises(ValueError, index.maxKey)

        # Now build up a tree with random values, and check maxKey at each
        # step.
        correct_max = ""   # smaller than anything we'll add
        for i in range(1000):
            key = p64(random.randrange(100000000))
            index[key] = i
            correct_max = max(correct_max, key)
            index_max = index.maxKey()
            self.assertEqual(index_max, correct_max)

        index.clear()
        a = '\000\000\000\000\000\001\000\000'
        b = '\000\000\000\000\000\002\000\000'
        c = '\000\000\000\000\000\003\000\000'
        d = '\000\000\000\000\000\004\000\000'
        index[a] = 1
        index[c] = 2
        self.assertEqual(index.maxKey(b), a)
        self.assertEqual(index.maxKey(d), c)
        self.assertRaises(ValueError, index.maxKey, z64)

    def testMinKey(self):
        index = self.index
        index.clear()

        # An empty index should complain.
        self.assertRaises(ValueError, index.minKey)

        # Now build up a tree with random values, and check minKey at each
        # step.
        correct_min = "\xff" * 8   # bigger than anything we'll add
        for i in range(1000):
            key = p64(random.randrange(100000000))
            index[key] = i
            correct_min = min(correct_min, key)
            index_min = index.minKey()
            self.assertEqual(index_min, correct_min)

        index.clear()
        a = '\000\000\000\000\000\001\000\000'
        b = '\000\000\000\000\000\002\000\000'
        c = '\000\000\000\000\000\003\000\000'
        d = '\000\000\000\000\000\004\000\000'
        index[a] = 1
        index[c] = 2
        self.assertEqual(index.minKey(b), c)
        self.assertRaises(ValueError, index.minKey, d)

def test_suite():
    loader=unittest.TestLoader()
    return loader.loadTestsFromTestCase(Test)

if __name__=='__main__':
    unittest.TextTestRunner().run(test_suite())
