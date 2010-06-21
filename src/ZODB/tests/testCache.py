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
"""A few simple tests of the public cache API.

Each DB Connection has a separate PickleCache.  The Cache serves two
purposes. It acts like a memo for unpickling.  It also keeps recent
objects in memory under the assumption that they may be used again.
"""

from persistent.cPickleCache import PickleCache
from persistent import Persistent
from persistent.mapping import PersistentMapping
from ZODB.tests.MinPO import MinPO
from ZODB.utils import p64
from zope.testing import doctest
import gc
import threading
import time
import transaction
import unittest
import ZODB
import ZODB.MappingStorage
import ZODB.tests.util

class CacheTestBase(ZODB.tests.util.TestCase):

    def setUp(self):
        ZODB.tests.util.TestCase.setUp(self)
        store = ZODB.MappingStorage.MappingStorage()
        self.db = ZODB.DB(store,
                          cache_size = self.CACHE_SIZE)
        self.conns = []

    def tearDown(self):
        self.db.close()
        ZODB.tests.util.TestCase.tearDown(self)

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
            transaction.commit()

        for i in range(15):
            o = d.get(i)
            if o is None:
                o = d[i] = MinPO(i)
            o.value += 1
        transaction.commit()



# CantGetRidOfMe is used by checkMinimizeTerminates.
make_trouble = True
class CantGetRidOfMe(MinPO):
    def __init__(self, value):
        MinPO.__init__(self, value)
        self.an_attribute = 42

    def __del__(self):
        # Referencing an attribute of self causes self to be
        # loaded into the cache again, which also resurrects
        # self.
        if make_trouble:
            self.an_attribute

class DBMethods(CacheTestBase):

    def setUp(self):
        CacheTestBase.setUp(self)
        for i in range(4):
            self.noodle_new_connection()

    def checkCacheDetail(self):
        for name, count in self.db.cacheDetail():
            self.assert_(isinstance(name, str))
            self.assert_(isinstance(count, int))

    def checkCacheExtremeDetail(self):
        expected = ['conn_no', 'id', 'oid', 'rc', 'klass', 'state']
        for dict in self.db.cacheExtremeDetail():
            for k, v in dict.items():
                self.assert_(k in expected)

    # TODO:  not really sure how to do a black box test of the cache.
    # Should the full sweep and minimize calls always remove things?

    def checkFullSweep(self):
        old_size = self.db.cacheSize()
        self.db.cacheFullSweep()
        new_size = self.db.cacheSize()
        self.assert_(new_size < old_size, "%s < %s" % (old_size, new_size))

    def checkMinimize(self):
        old_size = self.db.cacheSize()
        self.db.cacheMinimize()
        new_size = self.db.cacheSize()
        self.assert_(new_size < old_size, "%s < %s" % (old_size, new_size))

    def checkMinimizeTerminates(self):
        # This is tricky.  cPickleCache had a case where it could get into
        # an infinite loop, but we don't want the test suite to hang
        # if this bug reappears.  So this test spawns a thread to run the
        # dangerous operation, and the main thread complains if the worker
        # thread hasn't finished in 30 seconds (arbitrary, but way more
        # than enough).  In that case, the worker thread will continue
        # running forever (until killed externally), but at least the
        # test suite will move on.
        #
        # The bug was triggered by having a persistent object whose __del__
        # method references an attribute of the object.  An attempt to
        # ghostify such an object will clear the attribute, and if the
        # cache also releases the last Python reference to the object then
        # (due to ghostifying it), the __del__ method gets invoked.
        # Referencing the attribute loads the object again, and also
        # puts it back into the cPickleCache.  If the cache implementation
        # isn't looking out for this, it can get into an infinite loop
        # then, endlessly trying to ghostify an object that in turn keeps
        # unghostifying itself again.
        class Worker(threading.Thread):

            def __init__(self, testcase):
                threading.Thread.__init__(self)
                self.testcase = testcase

            def run(self):
                global make_trouble
                # Make CantGetRidOfMe.__del__ dangerous.
                make_trouble = True

                conn = self.testcase.conns[0]
                r = conn.root()
                d = r[1]
                for i in range(len(d)):
                    d[i] = CantGetRidOfMe(i)
                transaction.commit()

                self.testcase.db.cacheMinimize()

                # Defang the nasty objects.  Else, because they're
                # immortal now, they hang around and create trouble
                # for subsequent tests.
                make_trouble = False
                self.testcase.db.cacheMinimize()

        w = Worker(self)
        w.start()
        w.join(30)
        if w.isAlive():
            self.fail("cacheMinimize still running after 30 seconds -- "
                      "almost certainly in an infinite loop")

    # TODO:  don't have an explicit test for incrgc, because the
    # connection and database call it internally.
    # Same for the get and invalidate methods.

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
            transaction.commit()
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

            # The assertion below is non-sensical
            # The (poorly named) cache size is a target for non-ghosts.
            # The cache *usually* contains non-ghosts, so that the
            # size normally exceeds the target size.

            #self.assertEquals(d['size'], CACHE_SIZE)

    def checkDetail(self):
        CACHE_SIZE = 10
        self.db.setCacheSize(CACHE_SIZE)

        CONNS = 3
        for i in range(CONNS):
            self.noodle_new_connection()

        gc.collect()

        # Obscure:  The above gc.collect call is necessary to make this test
        # pass.
        #
        # This test then only works because the order of computations
        # and object accesses in the "noodle" calls is such that the
        # persistent mapping containing the MinPO objects is
        # deactivated before the MinPO objects.
        #
        # - Without the gc call, the cache will contain ghost MinPOs
        #   and the check of the MinPO count below will fail. That's
        #   because the counts returned by cacheDetail include ghosts.
        #
        # - If the mapping object containing the MinPOs isn't
        #   deactivated, there will be one fewer non-ghost MinPO and
        #   the test will fail anyway.
        #
        # This test really needs to be thought through and documented
        # better.


        for klass, count in self.db.cacheDetail():
            if klass.endswith('MinPO'):
                self.assertEqual(count, CONNS * CACHE_SIZE)
            if klass.endswith('PersistentMapping'):
                # one root per connection
                self.assertEqual(count, CONNS)

        for details in self.db.cacheExtremeDetail():
            # one 'details' dict per object
            if details['klass'].endswith('PersistentMapping'):
                self.assertEqual(details['state'], None)
            else:
                self.assert_(details['klass'].endswith('MinPO'))
                self.assertEqual(details['state'], 0)
            # The cache should never hold an unreferenced ghost.
            if details['state'] is None:    # i.e., it's a ghost
                self.assert_(details['rc'] > 0)

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
        self.assertEqual(self.cache.get(p64(0)), None)
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

    def checkTwoObjsSameOid(self):
        # Try to add two distinct objects with the same oid to the cache.
        # This has always been an error, but the error message prior to
        # ZODB 3.2.6 didn't make sense.  This test verifies that (a) an
        # exception is raised; and, (b) the error message is the intended
        # one.
        obj1 = StubObject()
        key = obj1._p_oid = p64(1)
        obj1._p_jar = self.jar
        self.cache[key] = obj1

        obj2 = StubObject()
        obj2._p_oid = key
        obj2._p_jar = self.jar
        try:
            self.cache[key] = obj2
        except ValueError, detail:
            self.assertEqual(str(detail),
                             "A different object already has the same oid")
        else:
            self.fail("two objects with the same oid should have failed")

def check_basic_cache_size_estimation():
    """Make sure the basic accounting is correct:

    >>> import ZODB.MappingStorage
    >>> db = ZODB.MappingStorage.DB()
    >>> conn = db.open()

The cache is empty initially:

    >>> conn._cache.total_estimated_size
    0

We force the root to be loaded and the cache grows:

    >>> getattr(conn.root, 'z', None)
    >>> conn._cache.total_estimated_size
    64

We add some data and the cache grows:

    >>> conn.root.z = ZODB.tests.util.P('x'*100)
    >>> import transaction
    >>> transaction.commit()
    >>> conn._cache.total_estimated_size
    320

Loading the objects in another connection gets the same sizes:

    >>> conn2 = db.open()
    >>> conn2._cache.total_estimated_size
    0
    >>> getattr(conn2.root, 'x', None)
    >>> conn2._cache.total_estimated_size
    128
    >>> _ = conn2.root.z.name
    >>> conn2._cache.total_estimated_size
    320

If we deactivate, the size goes down:

    >>> conn2.root.z._p_deactivate()
    >>> conn2._cache.total_estimated_size
    128

Loading data directly, rather than through traversal updates the cache
size correctly:

    >>> conn3 = db.open()
    >>> _ = conn3.get(conn2.root.z._p_oid).name
    >>> conn3._cache.total_estimated_size
    192

    """


def test_suite():
    s = unittest.makeSuite(DBMethods, 'check')
    s.addTest(unittest.makeSuite(LRUCacheTests, 'check'))
    s.addTest(unittest.makeSuite(CacheErrors, 'check'))
    s.addTest(doctest.DocTestSuite())
    return s
