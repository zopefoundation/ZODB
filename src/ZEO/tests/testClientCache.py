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
# FOR A PARTICULAR PURPOSE
# 
##############################################################################
"""Test suite for the ZEO.ClientCache module.

At times, we do 'white box' testing, i.e. we know about the internals
of the ClientCache object.
"""
from __future__ import nested_scopes

import os
import time
import tempfile
import unittest

from ZEO.ClientCache import ClientCache

class ClientCacheTests(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.cachesize = 10*1000*1000
        self.cache = ClientCache(size=self.cachesize)
        self.cache.open()

    def tearDown(self):
        self.cache.close()
        unittest.TestCase.tearDown(self)

    def testOpenClose(self):
        pass # All the work is done by setUp() / tearDown()

    def testStoreLoad(self):
        cache = self.cache
        oid = 'abcdefgh'
        data = '1234'*100
        serial = 'ABCDEFGH'
        cache.store(oid, data, serial, '', '', '')
        loaded = cache.load(oid, '')
        self.assertEqual(loaded, (data, serial))

    def testMissingLoad(self):
        cache = self.cache
        oid = 'abcdefgh'
        data = '1234'*100
        serial = 'ABCDEFGH'
        cache.store(oid, data, serial, '', '', '')
        loaded = cache.load('garbage1', '')
        self.assertEqual(loaded, None)

    def testInvalidate(self):
        cache = self.cache
        oid = 'abcdefgh'
        data = '1234'*100
        serial = 'ABCDEFGH'
        cache.store(oid, data, serial, '', '', '')
        loaded = cache.load(oid, '')
        self.assertEqual(loaded, (data, serial))
        cache.invalidate(oid, '')
        loaded = cache.load(oid, '')
        self.assertEqual(loaded, None)

    def testVersion(self):
        cache = self.cache
        oid = 'abcdefgh'
        data = '1234'*100
        serial = 'ABCDEFGH'
        vname = 'myversion'
        vdata = '5678'*200
        vserial = 'IJKLMNOP'
        cache.store(oid, data, serial, vname, vdata, vserial)
        loaded = cache.load(oid, '')
        self.assertEqual(loaded, (data, serial))
        vloaded = cache.load(oid, vname)
        self.assertEqual(vloaded, (vdata, vserial))

    def testVersionOnly(self):
        cache = self.cache
        oid = 'abcdefgh'
        data = ''
        serial = ''
        vname = 'myversion'
        vdata = '5678'*200
        vserial = 'IJKLMNOP'
        cache.store(oid, data, serial, vname, vdata, vserial)
        loaded = cache.load(oid, '')
        self.assertEqual(loaded, None)
        vloaded = cache.load(oid, vname)
        self.assertEqual(vloaded, (vdata, vserial))

    def testInvalidateNonVersion(self):
        cache = self.cache
        oid = 'abcdefgh'
        data = '1234'*100
        serial = 'ABCDEFGH'
        vname = 'myversion'
        vdata = '5678'*200
        vserial = 'IJKLMNOP'
        cache.store(oid, data, serial, vname, vdata, vserial)
        loaded = cache.load(oid, '')
        self.assertEqual(loaded, (data, serial))
        vloaded = cache.load(oid, vname)
        self.assertEqual(vloaded, (vdata, vserial))
        cache.invalidate(oid, '')
        loaded = cache.load(oid, '')
        self.assertEqual(loaded, None)
        # The version data is also invalidated at this point
        vloaded = cache.load(oid, vname)
        self.assertEqual(vloaded, None)

    def testInvalidateVersion(self):
        # Invalidating a version should not invalidate the non-version data.
        # (This tests for the same bug as testInvalidatePersists below.)
        cache = self.cache
        oid = 'abcdefgh'
        data = '1234'*100
        serial = 'ABCDEFGH'
        cache.store(oid, data, serial, '', '', '')
        loaded = cache.load(oid, '')
        self.assertEqual(loaded, (data, serial))
        cache.invalidate(oid, 'bogus')
        loaded = cache.load(oid, '')
        self.assertEqual(loaded, (data, serial))

    def testVerify(self):
        cache = self.cache
        results = []
        def verifier(oid, serial, vserial):
            results.append((oid, serial, vserial))
        cache.verify(verifier)
        self.assertEqual(results, [])
        oid = 'abcdefgh'
        data = '1234'*100
        serial = 'ABCDEFGH'
        cache.store(oid, data, serial, '', '', '')
        results = []
        cache.verify(verifier)
        self.assertEqual(results, [(oid, serial, None)])

    def testCheckSize(self):
        # Make sure that cache._index[oid] is erased for oids that are
        # stored in the cache file that's rewritten after a flip.
        cache = self.cache
        oid = 'abcdefgh'
        data = '1234'*100
        serial = 'ABCDEFGH'
        cache.store(oid, data, serial, '', '', '')
        cache.checkSize(10*self.cachesize) # Force a file flip
        oid2 = 'abcdefgz'
        data2 = '1234'*10
        serial2 = 'ABCDEFGZ'
        cache.store(oid2, data2, serial2, '', '', '')
        cache.checkSize(10*self.cachesize) # Force another file flip
        self.assertNotEqual(cache._index.get(oid2), None)
        self.assertEqual(cache._index.get(oid), None)

class PersistentClientCacheTests(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.vardir = os.getcwd() # Don't use /tmp, it's a security risk
        self.cachesize = 10*1000*1000
        self.storagename = 'foo'
        self.clientname = 'test'
        # Predict file names
        fn0 = 'c%s-%s-0.zec' % (self.storagename, self.clientname)
        fn1 = 'c%s-%s-1.zec' % (self.storagename, self.clientname)
        for fn in fn0, fn1:
            fn = os.path.join(self.vardir, fn)
            try:
                os.unlink(fn)
            except os.error:
                pass
        self.openCache()

    def openCache(self):
        self.cache = ClientCache(storage=self.storagename,
                                 size=self.cachesize,
                                 client=self.clientname,
                                 var=self.vardir)
        self.cache.open()

    def reopenCache(self):
        self.cache.close()
        self.openCache()
        return self.cache

    def tearDown(self):
        self.cache.close()
        for filename in self.cache._p:
            if filename is not None:
                try:
                    os.unlink(filename)
                except os.error:
                    pass
        unittest.TestCase.tearDown(self)

    def testCacheFileSelection(self):
        # A bug in __init__ read the wrong slice of the file to determine
        # the serial number of the first record, reading the
        # last byte of the data size plus the first seven bytes of the
        # serial number.  This caused random selection of the proper
        # 'current' file when a persistent cache was opened.
        cache = self.cache
        self.assertEqual(cache._current, 0) # Check that file 0 is current
        oid = 'abcdefgh'
        data = '1234'
        serial = 'ABCDEFGH'
        cache.store(oid, data, serial, '', '', '')
        cache.checkSize(10*self.cachesize) # Force a file flip
        self.assertEqual(cache._current, 1) # Check that the flip worked
        oid = 'abcdefgh'
        data = '123'
        serial = 'ABCDEFGZ'
        cache.store(oid, data, serial, '', '', '')
        cache = self.reopenCache()
        loaded = cache.load(oid, '')
        # Check that we got the most recent data:
        self.assertEqual(loaded, (data, serial))
        self.assertEqual(cache._current, 1) # Double check that 1 is current

    def testInvalidationPersists(self):
        # A bug in invalidate() caused invalidation to overwrite the
        # 2nd byte of the data size on disk, rather rather than
        # overwriting the status byte.  For certain data sizes this
        # can be observed by reopening a persistent cache: the
        # invalidated data will appear valid (but with altered size).
        cache = self.cache
        magicsize = (ord('i') + 1) << 16
        cache = self.cache
        oid = 'abcdefgh'
        data = '!'*magicsize
        serial = 'ABCDEFGH'
        cache.store(oid, data, serial, '', '', '')
        loaded = cache.load(oid, '')
        self.assertEqual(loaded, (data, serial))
        cache.invalidate(oid, '')
        cache = self.reopenCache()
        loaded = cache.load(oid, '')
        if loaded != None:
            self.fail("invalidated data resurrected, size %d, was %d" %
                      (len(loaded[0]), len(data)))

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ClientCacheTests))
    suite.addTest(unittest.makeSuite(PersistentClientCacheTests))
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
