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
import unittest, sys
from ZODB.fsIndex import fsIndex
from ZODB.utils import p64


class Test(unittest.TestCase):

    def testInserts(self):
        index=fsIndex()

        for i in range(200):
            index[p64(i*1000)]=(i*1000L+1)

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
        index=fsIndex()
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


def test_suite():
    loader=unittest.TestLoader()
    return loader.loadTestsFromTestCase(Test)

if __name__=='__main__':
    unittest.TextTestRunner().run(test_suite())
