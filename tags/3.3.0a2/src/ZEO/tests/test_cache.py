##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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
"""Basic unit tests for a multi-version client cache."""

import os
import tempfile
import unittest

import ZEO.cache
from ZODB.utils import p64

n1 = p64(1)
n2 = p64(2)
n3 = p64(3)
n4 = p64(4)
n5 = p64(5)

class CacheTests(unittest.TestCase):

    def setUp(self):
        self.cache = ZEO.cache.ClientCache()
        self.cache.open()

    def tearDown(self):
        if self.cache.path:
            os.remove(self.cache.path)

    def testLastTid(self):
        self.assertEqual(self.cache.getLastTid(), None)
        self.cache.setLastTid(n2)
        self.assertEqual(self.cache.getLastTid(), n2)
        self.cache.invalidate(None, "", n1)
        self.assertEqual(self.cache.getLastTid(), n2)
        self.cache.invalidate(None, "", n3)
        self.assertEqual(self.cache.getLastTid(), n3)
        self.assertRaises(ValueError, self.cache.setLastTid, n2)

    def testLoad(self):
        data1 = "data for n1"
        self.assertEqual(self.cache.load(n1, ""), None)
        self.assertEqual(self.cache.load(n1, "version"), None)
        self.cache.store(n1, "", n3, None, data1)
        self.assertEqual(self.cache.load(n1, ""), (data1, n3, ""))
        # The cache doesn't know whether version exists, because it
        # only has non-version data.
        self.assertEqual(self.cache.load(n1, "version"), None)
        self.assertEqual(self.cache.modifiedInVersion(n1), None)

    def testInvalidate(self):
        data1 = "data for n1"
        self.cache.store(n1, "", n3, None, data1)
        self.cache.invalidate(n1, "", n4)
        self.cache.invalidate(n2, "", n2)
        self.assertEqual(self.cache.load(n1, ""), None)
        self.assertEqual(self.cache.loadBefore(n1, n4),
                         (data1, n3, n4))

    def testVersion(self):
        data1 = "data for n1"
        data1v = "data for n1 in version"
        self.cache.store(n1, "version", n3, None, data1v)
        self.assertEqual(self.cache.load(n1, ""), None)
        self.assertEqual(self.cache.load(n1, "version"),
                         (data1v, n3, "version"))
        self.assertEqual(self.cache.load(n1, "random"), None)
        self.assertEqual(self.cache.modifiedInVersion(n1), "version")
        self.cache.invalidate(n1, "version", n4)
        self.assertEqual(self.cache.load(n1, "version"), None)

    def testNonCurrent(self):
        data1 = "data for n1"
        data2 = "data for n2"
        self.cache.store(n1, "", n4, None, data1)
        self.cache.store(n1, "", n2, n3, data2)
        # can't say anything about state before n2
        self.assertEqual(self.cache.loadBefore(n1, n2), None)
        # n3 is the upper bound of non-current record n2
        self.assertEqual(self.cache.loadBefore(n1, n3), (data2, n2, n3))
        # no data for between n2 and n3
        self.assertEqual(self.cache.loadBefore(n1, n4), None)
        self.cache.invalidate(n1, "", n5)
        self.assertEqual(self.cache.loadBefore(n1, n5), (data1, n4, n5))
        self.assertEqual(self.cache.loadBefore(n2, n4), None)

    def testException(self):
        self.assertRaises(ValueError,
                          self.cache.store,
                          n1, "version", n2, n3, "data")
        self.cache.store(n1, "", n2, None, "data")
        self.assertRaises(ValueError,
                          self.cache.store,
                          n1, "", n3, None, "data")

    def testEviction(self):
        # Manually override the current maxsize
        maxsize = self.cache.size = self.cache.fc.maxsize = 3395 # 1245
        self.cache.fc = ZEO.cache.FileCache(3395, None, self.cache)

        # Trivial test of eviction code.  Doesn't test non-current
        # eviction.
        data = ["z" * i for i in range(100)]
        for i in range(50):
            n = p64(i)
            self.cache.store(n, "", n, None, data[i])
            self.assertEquals(len(self.cache), i + 1)
            self.assert_(self.cache.fc.currentsize < maxsize)
        # The cache now uses 1225 bytes.  The next insert
        # should delete some objects.
        n = p64(50)
        self.cache.store(n, "", n, None, data[51])
        self.assert_(len(self.cache) < 51)
        self.assert_(self.cache.fc.currentsize <= maxsize)

        # XXX Need to make sure eviction of non-current data
        # and of version data are handled correctly.

    def testSerialization(self):
        self.cache.store(n1, "", n2, None, "data for n1")
        self.cache.store(n2, "version", n2, None, "version data for n2")
        self.cache.store(n3, "", n3, n4, "non-current data for n3")
        self.cache.store(n3, "", n4, n5, "more non-current data for n3")

        path = tempfile.mktemp()
        # Copy data from self.cache into path, reaching into the cache
        # guts to make the copy.
        dst = open(path, "wb+")
        src = self.cache.fc.f
        src.seek(0)
        dst.write(src.read(self.cache.fc.maxsize))
        dst.close()
        copy = ZEO.cache.ClientCache(path)
        copy.open()

        # Verify that internals of both objects are the same.
        # Could also test that external API produces the same results.
        eq = self.assertEqual
        eq(copy.tid, self.cache.tid)
        eq(len(copy), len(self.cache))
        eq(copy.version, self.cache.version)
        eq(copy.current, self.cache.current)
        eq(copy.noncurrent, self.cache.noncurrent)

def test_suite():
    return unittest.makeSuite(CacheTests)
