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

_marker = object()

class PickleCacheTests(unittest.TestCase):

    def _getTargetClass(self):
        from persistent.picklecache import PickleCache
        return PickleCache

    def _makeOne(self, jar=None, target_size=10):
        if jar is None:
            jar = DummyConnection()
        return self._getTargetClass()(jar, target_size)

    def _makePersist(self, state=None, oid='foo', jar=_marker):
        from persistent.interfaces import GHOST
        if state is None:
            state = GHOST
        if jar is _marker:
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
        cache['original'] = original # doesn't raise
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
        import gc
        from persistent.interfaces import UPTODATE
        cache = self._makeOne()
        oids = []
        for i in range(100):
            oid = 'oid_%04d' % i
            oids.append(oid)
            cache[oid] = self._makePersist(oid=oid, state=UPTODATE)
        self.assertEqual(cache.cache_non_ghost_count, 100)

        cache.incrgc()
        gc.collect() # banish the ghosts who are no longer in the ring

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

    def test_incrgc_w_smaller_drain_resistance(self):
        from persistent.interfaces import UPTODATE
        cache = self._makeOne()
        cache.drain_resistance = 2
        oids = []
        for i in range(100):
            oid = 'oid_%04d' % i
            oids.append(oid)
            cache[oid] = self._makePersist(oid=oid, state=UPTODATE)
        self.assertEqual(cache.cache_non_ghost_count, 100)

        cache.incrgc()

        self.assertEqual(cache.cache_non_ghost_count, 10)

    def test_incrgc_w_larger_drain_resistance(self):
        from persistent.interfaces import UPTODATE
        cache = self._makeOne()
        cache.drain_resistance = 2
        cache.target_size = 90
        oids = []
        for i in range(100):
            oid = 'oid_%04d' % i
            oids.append(oid)
            cache[oid] = self._makePersist(oid=oid, state=UPTODATE)
        self.assertEqual(cache.cache_non_ghost_count, 100)

        cache.incrgc()

        self.assertEqual(cache.cache_non_ghost_count, 49)

    def test_full_sweep(self):
        import gc
        from persistent.interfaces import UPTODATE
        cache = self._makeOne()
        oids = []
        for i in range(100):
            oid = 'oid_%04d' % i
            oids.append(oid)
            cache[oid] = self._makePersist(oid=oid, state=UPTODATE)
        self.assertEqual(cache.cache_non_ghost_count, 100)

        cache.full_sweep()
        gc.collect() # banish the ghosts who are no longer in the ring

        self.assertEqual(cache.cache_non_ghost_count, 0)
        self.failUnless(cache.ring.next is cache.ring)

        for oid in oids:
            self.failUnless(cache.get(oid) is None)

    def test_minimize(self):
        import gc
        from persistent.interfaces import UPTODATE
        cache = self._makeOne()
        oids = []
        for i in range(100):
            oid = 'oid_%04d' % i
            oids.append(oid)
            cache[oid] = self._makePersist(oid=oid, state=UPTODATE)
        self.assertEqual(cache.cache_non_ghost_count, 100)

        cache.minimize()
        gc.collect() # banish the ghosts who are no longer in the ring

        self.assertEqual(cache.cache_non_ghost_count, 0)

        for oid in oids:
            self.failUnless(cache.get(oid) is None)

    def test_new_ghost_non_persistent_object(self):
        cache = self._makeOne()
        self.assertRaises(AttributeError, cache.new_ghost, '123', object())

    def test_new_ghost_obj_already_has_oid(self):
        from persistent.interfaces import GHOST
        candidate = self._makePersist(oid='123', state=GHOST)
        cache = self._makeOne()
        self.assertRaises(ValueError, cache.new_ghost, '123', candidate)

    def test_new_ghost_obj_already_has_jar(self):
        class Dummy(object):
            _p_oid = None
            _p_jar = object()
        cache = self._makeOne()
        candidate = self._makePersist(oid=None, jar=object())
        self.assertRaises(ValueError, cache.new_ghost, '123', candidate)

    def test_new_ghost_obj_already_in_cache(self):
        cache = self._makeOne()
        candidate = self._makePersist(oid=None, jar=None)
        cache['123'] = candidate
        self.assertRaises(KeyError, cache.new_ghost, '123', candidate)

    def test_new_ghost_success_already_ghost(self):
        from persistent.interfaces import GHOST
        cache = self._makeOne()
        candidate = self._makePersist(oid=None, jar=None)
        cache.new_ghost('123', candidate)
        self.failUnless(cache.get('123') is candidate)
        self.assertEqual(candidate._p_oid, '123')
        self.assertEqual(candidate._p_jar, cache.jar)
        self.assertEqual(candidate._p_state, GHOST)

    def test_new_ghost_success_not_already_ghost(self):
        from persistent.interfaces import GHOST
        from persistent.interfaces import UPTODATE
        cache = self._makeOne()
        candidate = self._makePersist(oid=None, jar=None, state=UPTODATE)
        cache.new_ghost('123', candidate)
        self.failUnless(cache.get('123') is candidate)
        self.assertEqual(candidate._p_oid, '123')
        self.assertEqual(candidate._p_jar, cache.jar)
        self.assertEqual(candidate._p_state, GHOST)

    def test_new_ghost_w_pclass_non_ghost(self):
        class Pclass(object):
            _p_oid = None
            _p_jar = None
        cache = self._makeOne()
        cache.new_ghost('123', Pclass)
        self.failUnless(cache.get('123') is Pclass)
        self.failUnless(cache.persistent_classes['123'] is Pclass)
        self.assertEqual(Pclass._p_oid, '123')
        self.assertEqual(Pclass._p_jar, cache.jar)

    def test_new_ghost_w_pclass_ghost(self):
        class Pclass(object):
            _p_oid = None
            _p_jar = None
        cache = self._makeOne()
        cache.new_ghost('123', Pclass)
        self.failUnless(cache.get('123') is Pclass)
        self.failUnless(cache.persistent_classes['123'] is Pclass)
        self.assertEqual(Pclass._p_oid, '123')
        self.assertEqual(Pclass._p_jar, cache.jar)

    def test_reify_miss_single(self):
        cache = self._makeOne()
        self.assertRaises(KeyError, cache.reify, '123')

    def test_reify_miss_multiple(self):
        cache = self._makeOne()
        self.assertRaises(KeyError, cache.reify, ['123', '456'])

    def test_reify_hit_single_ghost(self):
        from persistent.interfaces import GHOST
        from persistent.interfaces import UPTODATE
        cache = self._makeOne()
        candidate = self._makePersist(oid='123', jar=cache.jar, state=GHOST)
        cache['123'] = candidate
        self.assertEqual(cache.ringlen(), 0)
        cache.reify('123')
        self.assertEqual(cache.ringlen(), 1)
        items = cache.lru_items()
        self.assertEqual(items[0][0], '123')
        self.failUnless(items[0][1] is candidate)
        self.assertEqual(candidate._p_state, UPTODATE)

    def test_reify_hit_single_non_ghost(self):
        from persistent.interfaces import UPTODATE
        cache = self._makeOne()
        candidate = self._makePersist(oid='123', jar=cache.jar, state=UPTODATE)
        cache['123'] = candidate
        self.assertEqual(cache.ringlen(), 1)
        cache.reify('123')
        self.assertEqual(cache.ringlen(), 1)
        self.assertEqual(candidate._p_state, UPTODATE)

    def test_reify_hit_multiple_mixed(self):
        from persistent.interfaces import GHOST
        from persistent.interfaces import UPTODATE
        cache = self._makeOne()
        c1 = self._makePersist(oid='123', jar=cache.jar, state=GHOST)
        cache['123'] = c1
        c2 = self._makePersist(oid='456', jar=cache.jar, state=UPTODATE)
        cache['456'] = c2
        self.assertEqual(cache.ringlen(), 1)
        cache.reify(['123', '456'])
        self.assertEqual(cache.ringlen(), 2)
        self.assertEqual(c1._p_state, UPTODATE)
        self.assertEqual(c2._p_state, UPTODATE)

    def test_invalidate_miss_single(self):
        cache = self._makeOne()
        cache.invalidate('123') # doesn't raise

    def test_invalidate_miss_multiple(self):
        cache = self._makeOne()
        cache.invalidate(['123', '456']) # doesn't raise

    def test_invalidate_hit_single_ghost(self):
        from persistent.interfaces import GHOST
        cache = self._makeOne()
        candidate = self._makePersist(oid='123', jar=cache.jar, state=GHOST)
        cache['123'] = candidate
        self.assertEqual(cache.ringlen(), 0)
        cache.invalidate('123')
        self.assertEqual(cache.ringlen(), 0)
        self.assertEqual(candidate._p_state, GHOST)

    def test_invalidate_hit_single_non_ghost(self):
        from persistent.interfaces import GHOST
        from persistent.interfaces import UPTODATE
        cache = self._makeOne()
        candidate = self._makePersist(oid='123', jar=cache.jar, state=UPTODATE)
        cache['123'] = candidate
        self.assertEqual(cache.ringlen(), 1)
        cache.invalidate('123')
        self.assertEqual(cache.ringlen(), 0)
        self.assertEqual(candidate._p_state, GHOST)

    def test_invalidate_hit_multiple_mixed(self):
        from persistent.interfaces import GHOST
        from persistent.interfaces import UPTODATE
        cache = self._makeOne()
        c1 = self._makePersist(oid='123', jar=cache.jar, state=GHOST)
        cache['123'] = c1
        c2 = self._makePersist(oid='456', jar=cache.jar, state=UPTODATE)
        cache['456'] = c2
        self.assertEqual(cache.ringlen(), 1)
        cache.invalidate(['123', '456'])
        self.assertEqual(cache.ringlen(), 0)
        self.assertEqual(c1._p_state, GHOST)
        self.assertEqual(c2._p_state, GHOST)

    def test_invalidate_hit_pclass(self):
        class Pclass(object):
            _p_oid = None
            _p_jar = None
        cache = self._makeOne()
        cache['123'] = Pclass
        self.failUnless(cache.persistent_classes['123'] is Pclass)
        cache.invalidate('123')
        self.failIf('123' in cache.persistent_classes)


class DummyPersistent(object):

    def _p_invalidate(self):
        from persistent.interfaces import GHOST
        self._p_state = GHOST

    def _p_activate(self):
        from persistent.interfaces import UPTODATE
        self._p_state = UPTODATE


class DummyConnection:
    pass


def test_suite():
    return unittest.TestSuite((
        unittest.makeSuite(PickleCacheTests),
        ))

if __name__ == '__main__':
    unittest.main()
