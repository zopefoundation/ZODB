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
"""Basic unit tests for a client cache."""

from ZODB.utils import p64, repr_to_oid
from zope.testing import doctest
import os
import random
import string
import struct
import sys
import tempfile
import unittest
import ZEO.cache
import ZODB.tests.util
import zope.testing.setupstack

import ZEO.cache
from ZODB.utils import p64, u64

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
            if (character in string.printable
                and not ord(character) in [12,13,9]):
                printable += character
            else:
                printable += '.'
            hex += character.encode('hex') + ' '
        hex = hex[:24] + ' ' + hex[24:]
        hex = hex.ljust(49)
        printable = printable.ljust(16)
        print '%08x  %s |%s|' % (offset, hex, printable)
        offset += 16


def oid(o):
    repr = '%016x' % o
    return repr_to_oid(repr)
tid = oid

class CacheTests(ZODB.tests.util.TestCase):

    def setUp(self):
        # The default cache size is much larger than we need here.  Since
        # testSerialization reads the entire file into a string, it's not
        # good to leave it that big.
        ZODB.tests.util.TestCase.setUp(self)
        self.cache = ZEO.cache.ClientCache(size=1024**2)

    def tearDown(self):
        if self.cache.path:
            os.remove(self.cache.path)
        ZODB.tests.util.TestCase.tearDown(self)

    def testLastTid(self):
        self.assertEqual(self.cache.getLastTid(), None)
        self.cache.setLastTid(n2)
        self.assertEqual(self.cache.getLastTid(), n2)
        self.assertEqual(self.cache.getLastTid(), n2)
        self.cache.invalidate(n1, n3)
        self.assertEqual(self.cache.getLastTid(), n3)
        self.assertRaises(ValueError, self.cache.setLastTid, n2)

    def testLoad(self):
        data1 = "data for n1"
        self.assertEqual(self.cache.load(n1), None)
        self.cache.store(n1, n3, None, data1)
        self.assertEqual(self.cache.load(n1), (data1, n3))

    def testInvalidate(self):
        data1 = "data for n1"
        self.cache.store(n1, n3, None, data1)
        self.cache.invalidate(n2, n2)
        self.cache.invalidate(n1, n4)
        self.assertEqual(self.cache.load(n1), None)
        self.assertEqual(self.cache.loadBefore(n1, n4),
                         (data1, n3, n4))

    def testNonCurrent(self):
        data1 = "data for n1"
        data2 = "data for n2"
        self.cache.store(n1, n4, None, data1)
        self.cache.store(n1, n2, n3, data2)
        # can't say anything about state before n2
        self.assertEqual(self.cache.loadBefore(n1, n2), None)
        # n3 is the upper bound of non-current record n2
        self.assertEqual(self.cache.loadBefore(n1, n3), (data2, n2, n3))
        # no data for between n2 and n3
        self.assertEqual(self.cache.loadBefore(n1, n4), None)
        self.cache.invalidate(n1, n5)
        self.assertEqual(self.cache.loadBefore(n1, n5), (data1, n4, n5))
        self.assertEqual(self.cache.loadBefore(n2, n4), None)

    def testException(self):
        self.cache.store(n1, n2, None, "data")
        self.cache.store(n1, n2, None, "data")
        self.assertRaises(ValueError,
                          self.cache.store,
                          n1, n3, None, "data")

    def testEviction(self):
        # Manually override the current maxsize
        cache = ZEO.cache.ClientCache(None, 3395)

        # Trivial test of eviction code.  Doesn't test non-current
        # eviction.
        data = ["z" * i for i in range(100)]
        for i in range(50):
            n = p64(i)
            cache.store(n, n, None, data[i])
            self.assertEquals(len(cache), i + 1)
        # The cache now uses 3287 bytes.  The next insert
        # should delete some objects.
        n = p64(50)
        cache.store(n, n, None, data[51])
        self.assert_(len(cache) < 51)

        # TODO:  Need to make sure eviction of non-current data
        # are handled correctly.

    def testSerialization(self):
        self.cache.store(n1, n2, None, "data for n1")
        self.cache.store(n3, n3, n4, "non-current data for n3")
        self.cache.store(n3, n4, n5, "more non-current data for n3")

        path = tempfile.mktemp()
        # Copy data from self.cache into path, reaching into the cache
        # guts to make the copy.
        dst = open(path, "wb+")
        src = self.cache.f
        src.seek(0)
        dst.write(src.read(self.cache.maxsize))
        dst.close()
        copy = ZEO.cache.ClientCache(path)

        # Verify that internals of both objects are the same.
        # Could also test that external API produces the same results.
        eq = self.assertEqual
        eq(copy.getLastTid(), self.cache.getLastTid())
        eq(len(copy), len(self.cache))
        eq(dict(copy.current), dict(self.cache.current))
        eq(dict([(k, dict(v)) for (k, v) in copy.noncurrent.items()]),
           dict([(k, dict(v)) for (k, v) in self.cache.noncurrent.items()]),
           )

    def testCurrentObjectLargerThanCache(self):
        if self.cache.path:
            os.remove(self.cache.path)
        self.cache = ZEO.cache.ClientCache(size=50)

        # We store an object that is a bit larger than the cache can handle.
        self.cache.store(n1, n2, None, "x"*64)
        # We can see that it was not stored.
        self.assertEquals(None, self.cache.load(n1))
        # If an object cannot be stored in the cache, it must not be
        # recorded as current.
        self.assert_(n1 not in self.cache.current)
        # Regression test: invalidation must still work.
        self.cache.invalidate(n1, n2)

    def testOldObjectLargerThanCache(self):
        if self.cache.path:
            os.remove(self.cache.path)
        cache = ZEO.cache.ClientCache(size=50)

        # We store an object that is a bit larger than the cache can handle.
        cache.store(n1, n2, n3, "x"*64)
        # We can see that it was not stored.
        self.assertEquals(None, cache.load(n1))
        # If an object cannot be stored in the cache, it must not be
        # recorded as non-current.
        self.assert_(1 not in cache.noncurrent)

    def testVeryLargeCaches(self):
        cache = ZEO.cache.ClientCache('cache', size=(1<<33))
        cache.store(n1, n2, None, "x")
        cache.close()
        cache = ZEO.cache.ClientCache('cache', size=(1<<33))
        self.assertEquals(cache.load(n1), ('x', n2))
        cache.close()

    def testConversionOfLargeFreeBlocks(self):
        f = open('cache', 'wb')
        f.write(ZEO.cache.magic+
                '\0'*8 +
                'f'+struct.pack(">I", (1<<32)-12)
                )
        f.seek((1<<32)-1)
        f.write('x')
        f.close()
        cache = ZEO.cache.ClientCache('cache', size=1<<32)
        cache.close()
        cache = ZEO.cache.ClientCache('cache', size=1<<32)
        cache.close()
        f = open('cache', 'rb')
        f.seek(12)
        self.assertEquals(f.read(1), 'f')
        self.assertEquals(struct.unpack(">I", f.read(4))[0],
                          ZEO.cache.max_block_size)
        f.close()
        

__test__ = dict(
    kill_does_not_cause_cache_corruption =
    r"""

    If we kill a process while a cache is being written to, the cache
    isn't corrupted.  To see this, we'll write a little script that
    writes records to a cache file repeatedly.

    >>> import os, random, sys, time
    >>> open('t', 'w').write('''
    ... import os, random, sys, thread, time
    ... sys.path = %r
    ... 
    ... def suicide():
    ...     time.sleep(random.random()/10)
    ...     os._exit(0)
    ... 
    ... import ZEO.cache
    ... from ZODB.utils import p64
    ... cache = ZEO.cache.ClientCache('cache')
    ... oid = 0
    ... t = 0
    ... thread.start_new_thread(suicide, ())
    ... while 1:
    ...     oid += 1
    ...     t += 1
    ...     data = 'X' * random.randint(5000,25000)
    ...     cache.store(p64(oid), p64(t), None, data)
    ... 
    ... ''' % sys.path)

    >>> for i in range(10):
    ...     _ = os.spawnl(os.P_WAIT, sys.executable, sys.executable, 't')
    ...     if os.path.exists('cache'):
    ...         cache = ZEO.cache.ClientCache('cache')
    ...         cache.close()
    ...         os.remove('cache')
    ...         os.remove('cache.lock')
       
    
    """,

    full_cache_is_valid =
    r"""

    If we fill up the cache without any free space, the cache can
    still be used.

    >>> import ZEO.cache
    >>> cache = ZEO.cache.ClientCache('cache', 1000)
    >>> data = 'X' * (1000 - ZEO.cache.ZEC_HEADER_SIZE - 41)
    >>> cache.store(p64(1), p64(1), None, data)
    >>> cache.close()
    >>> cache = ZEO.cache.ClientCache('cache', 1000)
    >>> cache.store(p64(2), p64(2), None, 'XXX')

    >>> cache.close()
    """,

    cannot_open_same_cache_file_twice =
    r"""
    >>> import ZEO.cache
    >>> cache = ZEO.cache.ClientCache('cache', 1000)
    >>> cache2 = ZEO.cache.ClientCache('cache', 1000)
    Traceback (most recent call last):
    ...
    LockError: Couldn't lock 'cache.lock'

    >>> cache.close()
    """,

    thread_safe =
    r"""

    >>> import ZEO.cache, ZODB.utils
    >>> cache = ZEO.cache.ClientCache('cache', 1000000)

    >>> for i in range(100):
    ...     cache.store(ZODB.utils.p64(i), ZODB.utils.p64(1), None, '0')

    >>> import random, sys, threading
    >>> random = random.Random(0)
    >>> stop = False
    >>> read_failure = None

    >>> def read_thread():
    ...     def pick_oid():
    ...         return ZODB.utils.p64(random.randint(0,99))
    ...
    ...     try:
    ...         while not stop:
    ...             cache.load(pick_oid())
    ...             cache.loadBefore(pick_oid(), ZODB.utils.p64(2))
    ...     except:
    ...         global read_failure
    ...         read_failure = sys.exc_info()

    >>> thread = threading.Thread(target=read_thread)
    >>> thread.start()

    >>> for tid in range(2,10):
    ...     for oid in range(100):
    ...         oid = ZODB.utils.p64(oid)
    ...         cache.invalidate(oid, ZODB.utils.p64(tid))
    ...         cache.store(oid, ZODB.utils.p64(tid), None, str(tid))

    >>> stop = True
    >>> thread.join()
    >>> if read_failure:
    ...    print 'Read failure:'
    ...    import traceback
    ...    traceback.print_exception(*read_failure)

    >>> expected = '9', ZODB.utils.p64(9)
    >>> for oid in range(100):
    ...     loaded = cache.load(ZODB.utils.p64(oid))
    ...     if loaded != expected:
    ...         print oid, loaded

    >>> cache.close()
    
    """,

    broken_non_current =
    r"""

    In production, we saw a situation where an _del_noncurrent raused
    a key error when trying to free space, causing the cache to become
    unusable.  I can't see why this would occur, but added a logging
    exception handler so, in the future, we'll still see cases in the
    log, but will ignore the error and keep going.
    
    >>> import ZEO.cache, ZODB.utils, logging, sys
    >>> logger = logging.getLogger('ZEO.cache')
    >>> logger.setLevel(logging.ERROR)
    >>> handler = logging.StreamHandler(sys.stdout)
    >>> logger.addHandler(handler)
    >>> cache = ZEO.cache.ClientCache('cache', 1000)
    >>> cache.store(ZODB.utils.p64(1), ZODB.utils.p64(1), None, '0')
    >>> cache.invalidate(ZODB.utils.p64(1), ZODB.utils.p64(2))
    >>> cache._del_noncurrent(ZODB.utils.p64(1), ZODB.utils.p64(2))
    ... # doctest: +NORMALIZE_WHITESPACE
    Couldn't find non-current
    ('\x00\x00\x00\x00\x00\x00\x00\x01', '\x00\x00\x00\x00\x00\x00\x00\x02')
    >>> cache._del_noncurrent(ZODB.utils.p64(1), ZODB.utils.p64(1))
    >>> cache._del_noncurrent(ZODB.utils.p64(1), ZODB.utils.p64(1)) # 
    ... # doctest: +NORMALIZE_WHITESPACE
    Couldn't find non-current
    ('\x00\x00\x00\x00\x00\x00\x00\x01', '\x00\x00\x00\x00\x00\x00\x00\x01')

    >>> logger.setLevel(logging.NOTSET)
    >>> logger.removeHandler(handler)

    >>> cache.close()
    """
    )

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(CacheTests))
    suite.addTest(
        doctest.DocTestSuite(
            setUp=zope.testing.setupstack.setUpDirectory,
            tearDown=zope.testing.setupstack.tearDown,
            )
        )
    return suite
