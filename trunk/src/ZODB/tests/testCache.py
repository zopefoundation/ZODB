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
"""A few simple tests of the public cache API.

Each DB Connection has a separate PickleCache.  The Cache serves two
purposes. It acts like a memo for unpickling.  It also keeps recent
objects in memory under the assumption that they may be used again.
"""
from __future__ import nested_scopes

import time
import types
import unittest

import ZODB
import ZODB.MappingStorage
from ZODB.cPickleCache import PickleCache
from ZODB.POSException import ConflictError
from ZODB.PersistentMapping import PersistentMapping
from ZODB.tests.MinPO import MinPO
from ZODB.utils import p64

from Persistence import Persistent

class CacheTestBase(unittest.TestCase):

    def setUp(self):
        store = ZODB.MappingStorage.MappingStorage()
        self.db = ZODB.DB(store,
                          cache_size = self.CACHE_SIZE)
        self.conns = []

    def tearDown(self):
        for conn in self.conns:
            conn.close()
        self.db.close()

    CACHE_SIZE = 20

    def noodle_new_connection(self):
        """Do some reads and writes on a new connection."""

        c = self.db.open()
        self.conns.append(c)
        self.noodle_connection(c)

    def noodle_connection(self, c):
        r = c.root()

        i = len(self.conns)
        d = r.get(i)
        if d is None:
            d = r[i] = PersistentMapping()
            get_transaction().commit()

        for i in range(15):
            o = d.get(i)
            if o is None:
                o = d[i] = MinPO(i)
            o.value += 1
        get_transaction().commit()

class DBMethods(CacheTestBase):

    __super_setUp = CacheTestBase.setUp

    def setUp(self):
        self.__super_setUp()
        for i in range(4):
            self.noodle_new_connection()

    def checkCacheDetail(self):
        for name, count in self.db.cacheDetail():
            self.assert_(isinstance(name, types.StringType))
            self.assert_(isinstance(count, types.IntType))

    def checkCacheExtremeDetail(self):
        expected = ['conn_no', 'id', 'oid', 'rc', 'klass', 'state']
        for dict in self.db.cacheExtremeDetail():
            for k, v in dict.items():
                self.assert_(k in expected)

    # XXX not really sure how to do a black box test of the cache.
    # should the full sweep and minimize calls always remove things?

    # The sleep(3) call is based on the implementation of the cache.
    # It's measures time in units of three seconds, so something used
    # within the last three seconds looks like something that is
    # currently being used.  Three seconds old is the youngest
    # something can be and still be collected.

    def checkFullSweep(self):
        old_size = self.db.cacheSize()
        time.sleep(3)
        self.db.cacheFullSweep(0)
        new_size = self.db.cacheSize()
        self.assert_(new_size < old_size, "%s < %s" % (old_size, new_size))

    def checkMinimize(self):
        old_size = self.db.cacheSize()
        time.sleep(3)
        self.db.cacheMinimize(0)
        new_size = self.db.cacheSize()
        self.assert_(new_size < old_size, "%s < %s" % (old_size, new_size))

    # XXX don't have an explicit test for incrgc, because the
    # connection and database call it internally

    # XXX same for the get and invalidate methods

    def checkLRUitems(self):
        # get a cache
        c = self.conns[0]._cache
        c.lru_items()

    def checkClassItems(self):
        c = self.conns[0]._cache
        c.klass_items()

class LRUCacheTests(CacheTestBase):

    def checkLRU(self):
        # verify the LRU behavior of the cache
        dataset_size = 5
        CACHE_SIZE = dataset_size*2+1
        # a cache big enough to hold the objects added in two
        # transactions, plus the root object
        self.db.setCacheSize(CACHE_SIZE)
        c = self.db.open()
        r = c.root()
        l = {}
        # the root is the only thing in the cache, because all the
        # other objects are new
        self.assertEqual(len(c._cache), 1)
        # run several transactions
        for t in range(5):
            for i in range(dataset_size):
                l[(t,i)] = r[i] = MinPO(i)
            get_transaction().commit()
            # commit() will register the objects, placing them in the
            # cache.  at the end of commit, the cache will be reduced
            # down to CACHE_SIZE items
            if len(l)>CACHE_SIZE:
                self.assertEqual(c._cache.ringlen(), CACHE_SIZE)
        for i in range(dataset_size):
            # Check objects added in the first two transactions.
            # They must all be ghostified.
            self.assertEqual(l[(0,i)]._p_changed, None)
            self.assertEqual(l[(1,i)]._p_changed, None)
            # Check objects added in the last two transactions.
            # They must all still exist in memory, but have
            # had their changes flushed
            self.assertEqual(l[(3,i)]._p_changed, 0)
            self.assertEqual(l[(4,i)]._p_changed, 0)
            # Of the objects added in the middle transaction, most
            # will have been ghostified. There is one cache slot
            # that may be occupied by either one of those objects or
            # the root, depending on precise order of access. We do
            # not bother to check this

    def checkSize(self):
        # XXX need to fix
        return
    
        self.assertEqual(self.db.cacheSize(), 0)
        self.assertEqual(self.db.cacheDetailSize(), [])

        CACHE_SIZE = 10
        self.db.setCacheSize(CACHE_SIZE)

        CONNS = 3
        for i in range(CONNS):
            self.noodle_new_connection()

        self.assertEquals(self.db.cacheSize(), CACHE_SIZE * CONNS)
        details = self.db.cacheDetailSize()
        self.assertEquals(len(details), CONNS)
        for d in details:
            self.assertEquals(d['ngsize'], CACHE_SIZE)
            # the root is also in the cache as ghost, because
            # the connection holds a reference to it
            self.assertEquals(d['size'], CACHE_SIZE + 1)

    def checkDetail(self):
        # XXX need to fix
        return
    
        CACHE_SIZE = 10
        self.db.setCacheSize(CACHE_SIZE)

        CONNS = 3
        for i in range(CONNS):
            self.noodle_new_connection()

        for klass, count in self.db.cacheDetail():
            print klass, count
            if klass.endswith('MinPO'):
                self.assertEqual(count, CONNS * CACHE_SIZE)
            if klass.endswith('PersistentMapping'):
                # one root per connection
                self.assertEqual(count, CONNS)

        for details in self.db.cacheExtremeDetail():
            print details
            # one dict per object.  keys:
            if details['klass'].endswith('PersistentMapping'):
                self.assertEqual(details['state'], None)
            else:
                self.assert_(details['klass'].endswith('MinPO'))
                self.assertEqual(details['state'], 0)

class StubDataManager:
    def setklassstate(self, object):
        pass

class StubObject(Persistent):
    pass

class CacheErrors(unittest.TestCase):

    def setUp(self):
        self.jar = StubDataManager()
        self.cache = PickleCache(self.jar)

    def checkGetBogusKey(self):
        self.assertRaises(KeyError, self.cache.get, p64(0))
        try:
            self.cache[12]
        except KeyError:
            pass
        else:
            self.fail("expected KeyError")
        try:
            self.cache[12] = 12
        except TypeError:
            pass
        else:
            self.fail("expected TyepError")
        try:
            del self.cache[12]
        except TypeError:
            pass
        else:
            self.fail("expected TypeError")

    def checkBogusObject(self):
        def add(key, obj):
            self.cache[key] = obj

        key = p64(2)
        # value isn't persistent
        self.assertRaises(TypeError, add, key, 12)

        o = StubObject()
        # o._p_oid == None
        self.assertRaises(TypeError, add, key, o)

        o._p_oid = p64(3)
        self.assertRaises(ValueError, add, key, o)

        o._p_oid = key
        # o._p_jar == None
        self.assertRaises(Exception, add, key, o)

        o._p_jar = self.jar
        self.cache[key] = o
        # make sure it can be added multiple times
        self.cache[key] = o

        # same object, different keys
        self.assertRaises(ValueError, add, p64(0), o)

    def checkTwoCaches(self):
        jar2 = StubDataManager()
        cache2 = PickleCache(jar2)

        o = StubObject()
        key = o._p_oid = p64(1)
        o._p_jar = jar2

        cache2[key] = o

        try:
            self.cache[key] = o
        except ValueError:
            pass
        else:
            self.fail("expected ValueError because object already in cache")

    def checkReadOnlyAttrsWhenCached(self):
        o = StubObject()
        key = o._p_oid = p64(1)
        o._p_jar = self.jar
        self.cache[key] = o
        try:
            o._p_oid = p64(2)
        except ValueError:
            pass
        else:
            self.fail("expect that you can't change oid of cached object")
        try:
            del o._p_jar
        except ValueError:
            pass
        else:
            self.fail("expect that you can't delete jar of cached object")

def test_suite():
    s = unittest.makeSuite(DBMethods, 'check')
    s.addTest(unittest.makeSuite(LRUCacheTests, 'check'))
    s.addTest(unittest.makeSuite(CacheErrors, 'check'))
    return s
