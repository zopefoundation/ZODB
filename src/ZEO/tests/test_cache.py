##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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
"""Basic unit tests for a multi-version client cache."""

import os
import random
import tempfile
import unittest
import doctest
import string
import sys

import ZEO.cache
from ZODB.utils import p64, repr_to_oid


n1 = p64(1)
n2 = p64(2)
n3 = p64(3)
n4 = p64(4)
n5 = p64(5)


def hexprint(file):
    file.seek(0)
    data = file.read()
    offset = 0
    while data:
        line, data = data[:16], data[16:]
        printable = ""
        hex = ""
        for character in line:
            if character in string.printable and not ord(character) in [12,13,9]:
                printable += character
            else:
                printable += '.'
            hex += character.encode('hex') + ' '
        hex = hex[:24] + ' ' + hex[24:]
        hex = hex.ljust(49)
        printable = printable.ljust(16)
        print '%08x  %s |%s|' % (offset, hex, printable)
        offset += 16


class ClientCacheDummy(object):

    def __init__(self):
        self.objects = {}

    def _evicted(self, o):
        if o.key in self.objects:
            del self.objects[o.key]


def oid(o):
    repr = '%016x' % o
    return repr_to_oid(repr)
tid = oid


class FileCacheFuzzing(unittest.TestCase):

    def testFileCacheFuzzing(self):
        cache_dummy = ClientCacheDummy()
        fc = ZEO.cache.FileCache(maxsize=5000, fpath=None,
                                      parent=cache_dummy)
        for i in xrange(10000):
            size = random.randint(0,5500)
            obj = ZEO.cache.Object(key=(oid(i), oid(1)), version='',
                                   data='*'*size, start_tid=oid(1),
                                   end_tid=None)
            fc.add(obj)
        hexprint(fc.f)
        fc.close()


class CacheTests(unittest.TestCase):

    def setUp(self):
        # The default cache size is much larger than we need here.  Since
        # testSerialization reads the entire file into a string, it's not
        # good to leave it that big.
        self.cache = ZEO.cache.ClientCache(size=1024**2)
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
        # The cache now uses 1225 bytes.  The next insert
        # should delete some objects.
        n = p64(50)
        self.cache.store(n, "", n, None, data[51])
        self.assert_(len(self.cache) < 51)

        # TODO:  Need to make sure eviction of non-current data
        # and of version data are handled correctly.

    def _run_fuzzing(self):
        current_tid = 1
        current_oid = 1
        def log(*args):
            #print args
            pass
        cache = self.fuzzy_cache
        objects = self.fuzzy_cache_client.objects
        for operation in xrange(10000):
            op = random.choice(['add', 'access', 'remove', 'update', 'settid'])
            if not objects:
                op = 'add'
            log(op)
            if op == 'add':
                current_oid += 1
                key = (oid(current_oid), tid(current_tid))
                object = ZEO.cache.Object(
                    key=key, version='', data='*'*random.randint(1,60*1024),
                    start_tid=tid(current_tid), end_tid=None)
                assert key not in objects
                log(key, len(object.data), current_tid)
                cache.add(object)
                if (object.size + ZEO.cache.OBJECT_HEADER_SIZE >
                    cache.maxsize - ZEO.cache.ZEC3_HEADER_SIZE):
                    assert key not in cache
                else:
                    objects[key] = object
                    assert key in cache, key
            elif op == 'access':
                key = random.choice(objects.keys())
                log(key)
                object = objects[key]
                found = cache.access(key)
                assert object.data == found.data
                assert object.key == found.key
                assert object.size == found.size == (len(object.data)+object.TOTAL_FIXED_SIZE)
            elif op == 'remove':
                key = random.choice(objects.keys())
                log(key)
                cache.remove(key)
                assert key not in cache
                assert key not in objects
            elif op == 'update':
                key = random.choice(objects.keys())
                object = objects[key]
                log(key, object.key)
                if not object.end_tid:
                    object.end_tid = tid(current_tid)
                    log(key, current_tid)
                    cache.update(object)
            elif op == 'settid':
                current_tid += 1
                log(current_tid)
                cache.settid(tid(current_tid))
        cache.close()

    def testFuzzing(self):
        random.seed()
        seed = random.randint(0, sys.maxint)
        random.seed(seed)
        self.fuzzy_cache_client = ClientCacheDummy()
        self.fuzzy_cache = ZEO.cache.FileCache(
            random.randint(100, 50*1024), None, self.fuzzy_cache_client)
        try:
            self._run_fuzzing()
        except:
            print "Error in fuzzing with seed", seed
            hexprint(self.fuzzy_cache.f)
            raise

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
        eq(copy.getLastTid(), self.cache.getLastTid())
        eq(len(copy), len(self.cache))
        eq(copy.version, self.cache.version)
        eq(copy.current, self.cache.current)
        eq(copy.noncurrent, self.cache.noncurrent)


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(CacheTests))
    suite.addTest(unittest.makeSuite(FileCacheFuzzing))
    suite.addTest(doctest.DocFileSuite('filecache.txt'))
    return suite
