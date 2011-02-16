##############################################################################
#
# Copyright (c) 2009 Zope Corporation and Contributors.
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

class PickleCacheTests(unittest.TestCase):

    def _getTargetClass(self):
        from persistent.picklecache import PickleCache
        return PickleCache

    def _makeOne(self, jar=None, target_size=10):
        if jar is None:
            jar = DummyConnection()
        return self._getTargetClass()(jar, target_size)

    def _makePersist(self, state=None, oid='foo', jar=None):
        if state is None:
            from persistent.interfaces import GHOST
            state = GHOST
        if jar is None:
            jar = DummyConnection()
        persist = DummyPersistent()
        persist._p_state = state
        persist._p_oid = oid
        persist._p_jar = jar
        return persist

    def test_class_conforms_to_IPickleCache(self):
        from zope.interface.verify import verifyClass
        from persistent.interfaces import IPickleCache
        verifyClass(IPickleCache, self._getTargetClass())

    def test_instance_conforms_to_IPickleCache(self):
        from zope.interface.verify import verifyObject
        from persistent.interfaces import IPickleCache
        verifyObject(IPickleCache, self._makeOne())

    def test_empty(self):
        cache = self._makeOne()

        self.assertEqual(len(cache), 0)
        self.assertEqual(len(cache.items()), 0)
        self.assertEqual(len(cache.klass_items()), 0)
        self.assertEqual(cache.ringlen(), 0)
        self.assertEqual(len(cache.lru_items()), 0)
        self.assertEqual(cache.cache_size, 10)
        self.assertEqual(cache.cache_drain_resistance, 0)
        self.assertEqual(cache.cache_non_ghost_count, 0)
        self.assertEqual(dict(cache.cache_data), {})
        self.assertEqual(cache.cache_klass_count, 0)

    def test___getitem___nonesuch_raises_KeyError(self):
        cache = self._makeOne()

        self.assertRaises(KeyError, lambda: cache['nonesuch'])

    def test_get_nonesuch_no_default(self):
        cache = self._makeOne()

        self.assertEqual(cache.get('nonesuch'), None)

    def test_get_nonesuch_w_default(self):
        cache = self._makeOne()
        default = object

        self.failUnless(cache.get('nonesuch', default) is default)

    def test___setitem___non_string_oid_raises_ValueError(self):
        cache = self._makeOne()

        try:
            cache[object()] = self._makePersist()
        except ValueError:
            pass
        else:
            self.fail("Didn't raise ValueError with non-string OID.")

    def test___setitem___duplicate_oid_raises_KeyError(self):
        cache = self._makeOne()
        original = self._makePersist()
        cache['original'] = original
        duplicate = self._makePersist()

        try:
            cache['original'] = duplicate
        except KeyError:
            pass
        else:
            self.fail("Didn't raise KeyError with duplicate OID.")

    def test___setitem___ghost(self):
        from persistent.interfaces import GHOST
        cache = self._makeOne()
        ghost = self._makePersist(state=GHOST)

        cache['ghost'] = ghost

        self.assertEqual(len(cache), 1)
        self.assertEqual(len(cache.items()), 1)
        self.assertEqual(len(cache.klass_items()), 0)
        self.assertEqual(cache.items()[0][0], 'ghost')
        self.assertEqual(cache.ringlen(), 0)
        self.failUnless(cache.items()[0][1] is ghost)
        self.failUnless(cache['ghost'] is ghost)

    def test___setitem___non_ghost(self):
        from persistent.interfaces import UPTODATE
        cache = self._makeOne()
        uptodate = self._makePersist(state=UPTODATE)

        cache['uptodate'] = uptodate

        self.assertEqual(len(cache), 1)
        self.assertEqual(len(cache.items()), 1)
        self.assertEqual(len(cache.klass_items()), 0)
        self.assertEqual(cache.items()[0][0], 'uptodate')
        self.assertEqual(cache.ringlen(), 1)
        self.failUnless(cache.items()[0][1] is uptodate)
        self.failUnless(cache['uptodate'] is uptodate)
        self.failUnless(cache.get('uptodate') is uptodate)

    def test___setitem___persistent_class(self):
        class pclass(object):
            pass
        cache = self._makeOne()

        cache['pclass'] = pclass

        self.assertEqual(len(cache), 1)
        self.assertEqual(len(cache.items()), 0)
        self.assertEqual(len(cache.klass_items()), 1)
        self.assertEqual(cache.klass_items()[0][0], 'pclass')
        self.failUnless(cache.klass_items()[0][1] is pclass)
        self.failUnless(cache['pclass'] is pclass)
        self.failUnless(cache.get('pclass') is pclass)

    def test___delitem___non_string_oid_raises_ValueError(self):
        cache = self._makeOne()

        try:
            del cache[object()]
        except ValueError:
            pass
        else:
            self.fail("Didn't raise ValueError with non-string OID.")

    def test___delitem___nonesuch_raises_KeyError(self):
        cache = self._makeOne()
        original = self._makePersist()

        try:
            del cache['nonesuch']
        except KeyError:
            pass
        else:
            self.fail("Didn't raise KeyError with nonesuch OID.")

    def test_lruitems(self):
        from persistent.interfaces import UPTODATE
        cache = self._makeOne()
        cache['one'] = self._makePersist(oid='one', state=UPTODATE)
        cache['two'] = self._makePersist(oid='two', state=UPTODATE)
        cache['three'] = self._makePersist(oid='three', state=UPTODATE)

        items = cache.lru_items()
        self.assertEqual(len(items), 3)
        self.assertEqual(items[0][0], 'one')
        self.assertEqual(items[1][0], 'two')
        self.assertEqual(items[2][0], 'three')

    def test_mru_nonesuch_raises_KeyError(self):
        cache = self._makeOne()

        try:
            cache.mru('nonesuch')
        except KeyError:
            pass
        else:
            self.fail("Didn't raise KeyError with nonesuch OID.")

    def test_mru_normal(self):
        from persistent.interfaces import UPTODATE
        cache = self._makeOne()
        cache['one'] = self._makePersist(oid='one', state=UPTODATE)
        cache['two'] = self._makePersist(oid='two', state=UPTODATE)
        cache['three'] = self._makePersist(oid='three', state=UPTODATE)

        cache.mru('two')

        self.assertEqual(cache.ringlen(), 3)
        items = cache.lru_items()
        self.assertEqual(len(items), 3)
        self.assertEqual(items[0][0], 'one')
        self.assertEqual(items[1][0], 'three')
        self.assertEqual(items[2][0], 'two')

    def test_mru_ghost(self):
        from persistent.interfaces import UPTODATE
        from persistent.interfaces import GHOST
        cache = self._makeOne()
        cache['one'] = self._makePersist(oid='one', state=UPTODATE)
        two = cache['two'] = self._makePersist(oid='two', state=GHOST)
        cache['three'] = self._makePersist(oid='three', state=UPTODATE)

        cache.mru('two')

        self.assertEqual(cache.ringlen(), 2)
        items = cache.lru_items()
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0][0], 'one')
        self.assertEqual(items[1][0], 'three')

    def test_mru_was_ghost_now_active(self):
        from persistent.interfaces import UPTODATE
        from persistent.interfaces import GHOST
        cache = self._makeOne()
        cache['one'] = self._makePersist(oid='one', state=UPTODATE)
        two = cache['two'] = self._makePersist(oid='two', state=GHOST)
        cache['three'] = self._makePersist(oid='three', state=UPTODATE)

        two._p_state = UPTODATE
        cache.mru('two')

        self.assertEqual(cache.ringlen(), 3)
        items = cache.lru_items()
        self.assertEqual(len(items), 3)
        self.assertEqual(items[0][0], 'one')
        self.assertEqual(items[1][0], 'three')
        self.assertEqual(items[2][0], 'two')

    def test_mru_first(self):
        from persistent.interfaces import UPTODATE
        cache = self._makeOne()
        cache['one'] = self._makePersist(oid='one', state=UPTODATE)
        cache['two'] = self._makePersist(oid='two', state=UPTODATE)
        cache['three'] = self._makePersist(oid='three', state=UPTODATE)

        cache.mru('one')

        self.assertEqual(cache.ringlen(), 3)
        items = cache.lru_items()
        self.assertEqual(len(items), 3)
        self.assertEqual(items[0][0], 'two')
        self.assertEqual(items[1][0], 'three')
        self.assertEqual(items[2][0], 'one')

    def test_mru_last(self):
        from persistent.interfaces import UPTODATE
        cache = self._makeOne()
        cache['one'] = self._makePersist(oid='one', state=UPTODATE)
        cache['two'] = self._makePersist(oid='two', state=UPTODATE)
        cache['three'] = self._makePersist(oid='three', state=UPTODATE)

        cache.mru('three')

        self.assertEqual(cache.ringlen(), 3)
        items = cache.lru_items()
        self.assertEqual(len(items), 3)
        self.assertEqual(items[0][0], 'one')
        self.assertEqual(items[1][0], 'two')
        self.assertEqual(items[2][0], 'three')

    def test_incrgc_simple(self):
        from persistent.interfaces import UPTODATE
        cache = self._makeOne()
        oids = []
        for i in range(100):
            oid = 'oid_%04d' % i
            oids.append(oid)
            cache[oid] = self._makePersist(oid=oid, state=UPTODATE)
        self.assertEqual(cache.cache_non_ghost_count, 100)

        cache.incrgc()

        self.assertEqual(cache.cache_non_ghost_count, 10)
        items = cache.lru_items()
        self.assertEqual(len(items), 10)
        self.assertEqual(items[0][0], 'oid_0090')
        self.assertEqual(items[1][0], 'oid_0091')
        self.assertEqual(items[2][0], 'oid_0092')
        self.assertEqual(items[3][0], 'oid_0093')
        self.assertEqual(items[4][0], 'oid_0094')
        self.assertEqual(items[5][0], 'oid_0095')
        self.assertEqual(items[6][0], 'oid_0096')
        self.assertEqual(items[7][0], 'oid_0097')
        self.assertEqual(items[8][0], 'oid_0098')
        self.assertEqual(items[9][0], 'oid_0099')

        for oid in oids[:90]:
            self.failUnless(cache.get(oid) is None)

        for oid in oids[90:]:
            self.failIf(cache.get(oid) is None)


class DummyPersistent(object):
    pass

class DummyConnection:

    def setklassstate(self, obj):
        """Method used by PickleCache."""

def test_suite():
    return unittest.TestSuite((
        unittest.makeSuite(PickleCacheTests),
        ))

if __name__ == '__main__':
    unittest.main()
