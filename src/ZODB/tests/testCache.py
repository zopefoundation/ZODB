"""A few simple tests of the public cache API.

Each DB Connection has a separate PickleCache.  The Cache serves two
purposes. It acts like a memo for unpickling.  It also keeps recent
objects in memory under the assumption that they may be used again.
"""

import random
import time
import types
import unittest

import ZODB
import ZODB.MappingStorage
from ZODB.PersistentMapping import PersistentMapping
from ZODB.tests.MinPO import MinPO
from ZODB.utils import p64

class CacheTestBase(unittest.TestCase):

    def setUp(self):
        store = ZODB.MappingStorage.MappingStorage()
        self.db = ZODB.DB(store,
                          cache_size = self.CACHE_SIZE,
                          cache_deactivate_after = self.CACHE_DEACTIVATE_AFTER)
        self.conns = []

    def tearDown(self):
        for conn in self.conns:
            conn.close()
        self.db.close()

    NUM_COLLECTIONS = 10
    MAX_OBJECTS = 100
    CACHE_SIZE = 20
    CACHE_DEACTIVATE_AFTER = 5

    def noodle_new_connection(self):
        """Do some reads and writes on a new connection."""

        c = self.db.open()
        self.noodle_connection(c)

    def noodle_connection(self, c):
        r = c.root()

        i = random.randrange(0, self.NUM_COLLECTIONS)
        d = r.get(i)
        if d is None:
            d = r[i] = PersistentMapping()
            get_transaction().commit()
            
        for i in range(random.randrange(10, 20)):
            j = random.randrange(0, self.MAX_OBJECTS)
            o = d.get(j)
            if o is None:
                o = d[j] = MinPO(j)
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

def test_suite():
    return unittest.makeSuite(DBMethods, 'check')
