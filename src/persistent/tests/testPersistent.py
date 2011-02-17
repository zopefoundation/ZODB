#############################################################################
#
# Copyright (c) 2003 Zope Foundation and Contributors.
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

Picklable = None # avoid global import of Persistent;  updated later

class PersistenceTest(unittest.TestCase):

    def _makeOne(self):
        from persistent import Persistent

        class P(Persistent):
            pass

        return P()

    def _makeJar(self):
        from persistent.tests.utils import ResettingJar
        return ResettingJar()

    def test_oid_initial_value(self):
        obj = self._makeOne()
        self.assertEqual(obj._p_oid, None)

    def test_oid_mutable_and_deletable_when_no_jar(self):
        OID = '\x01' * 8
        obj = self._makeOne()
        obj._p_oid = OID
        self.assertEqual(obj._p_oid, OID)
        del obj._p_oid
        self.assertEqual(obj._p_oid, None)

    def test_oid_immutable_when_in_jar(self):
        OID = '\x01' * 8
        obj = self._makeOne()
        jar = self._makeJar()
        jar.add(obj)

        # Can't change oid of cache object.
        def deloid():
            del obj._p_oid
        self.assertRaises(ValueError, deloid)

        def setoid():
            obj._p_oid = OID
        self.assertRaises(ValueError, setoid)

    # The value returned for _p_changed can be one of:
    # 0 -- it is not changed
    # 1 -- it is changed
    # None -- it is a ghost

    def test_change_via_setattr(self):
        from persistent import CHANGED
        obj = self._makeOne()
        jar = self._makeJar()
        jar.add(obj)

        obj.x = 1

        self.assertEqual(obj._p_changed, 1)
        self.assertEqual(obj._p_state, CHANGED)
        self.assert_(obj in jar.registered)

    def test_setattr_then_mark_uptodate(self):
        from persistent import UPTODATE
        obj = self._makeOne()
        jar = self._makeJar()
        jar.add(obj)

        obj.x = 1
        obj._p_changed = 0

        self.assertEqual(obj._p_changed, 0)
        self.assertEqual(obj._p_state, UPTODATE)

    def test_set_changed_directly(self):
        from persistent import CHANGED
        obj = self._makeOne()
        jar = self._makeJar()
        jar.add(obj)

        obj._p_changed = 1

        self.assertEqual(obj._p_changed, 1)
        self.assertEqual(obj._p_state, CHANGED)
        self.assert_(obj in jar.registered)

    def test_cant_ghostify_if_changed(self):
        from persistent import CHANGED
        obj = self._makeOne()
        jar = self._makeJar()
        jar.add(obj)

        # setting obj._p_changed to None ghostifies if the
        # object is in the up-to-date state, but not otherwise.
        obj.x = 1
        obj._p_changed = None

        self.assertEqual(obj._p_changed, 1)
        self.assertEqual(obj._p_state, CHANGED)

    def test_can_ghostify_if_uptodate(self):
        from persistent import GHOST
        obj = self._makeOne()
        jar = self._makeJar()
        jar.add(obj)

        obj.x = 1
        obj._p_changed = 0
        obj._p_changed = None

        self.assertEqual(obj._p_changed, None)
        self.assertEqual(obj._p_state, GHOST)

    def test_can_ghostify_if_changed_but_del__p_changed(self):
        from persistent import GHOST
        obj = self._makeOne()
        jar = self._makeJar()
        jar.add(obj)

        # You can transition directly from modified to ghost if
        # you delete the _p_changed attribute.
        obj.x = 1
        del obj._p_changed

        self.assertEqual(obj._p_changed, None)
        self.assertEqual(obj._p_state, GHOST)

    def test__p_state_immutable(self):
        from persistent import CHANGED
        from persistent import GHOST
        from persistent import STICKY
        from persistent import UPTODATE
        # make sure we can't write to _p_state; we don't want yet
        # another way to change state!
        obj = self._makeOne()
        def setstate(value):
            obj._p_state = value

        self.assertRaises(Exception, setstate, GHOST)
        self.assertRaises(Exception, setstate, UPTODATE)
        self.assertRaises(Exception, setstate, CHANGED)
        self.assertRaises(Exception, setstate, STICKY)

    def test_invalidate(self):
        from persistent import GHOST
        from persistent import UPTODATE
        obj = self._makeOne()
        jar = self._makeJar()
        jar.add(obj)
        obj._p_activate()

        self.assertEqual(obj._p_changed, 0)
        self.assertEqual(obj._p_state, UPTODATE)

        obj._p_invalidate()

        self.assertEqual(obj._p_changed, None)
        self.assertEqual(obj._p_state, GHOST)

    def test_invalidate_activate_invalidate(self):
        from persistent import GHOST
        obj = self._makeOne()
        jar = self._makeJar()
        jar.add(obj)

        obj._p_invalidate()
        obj._p_activate()
        obj.x = 1
        obj._p_invalidate()

        self.assertEqual(obj._p_changed, None)
        self.assertEqual(obj._p_state, GHOST)

    def test_initial_serial(self):
        NOSERIAL = "\000" * 8
        obj = self._makeOne()
        self.assertEqual(obj._p_serial, NOSERIAL)

    def test_setting_serial_w_invalid_types_raises(self):
        # Serial must be an 8-digit string
        obj = self._makeOne()

        def set(val):
            obj._p_serial = val

        self.assertRaises(ValueError, set, 1)
        self.assertRaises(ValueError, set, "0123")
        self.assertRaises(ValueError, set, "012345678")
        self.assertRaises(ValueError, set, u"01234567")

    def test_del_serial_returns_to_initial(self):
        NOSERIAL = "\000" * 8
        obj = self._makeOne()
        obj._p_serial = "01234567"
        del obj._p_serial
        self.assertEqual(obj._p_serial, NOSERIAL)

    def test_initial_mtime(self):
        obj = self._makeOne()
        self.assertEqual(obj._p_mtime, None)

    def test_setting_serial_sets_mtime_to_now(self):
        from persistent.timestamp import TimeStamp

        obj = self._makeOne()
        ts = TimeStamp(2011, 2, 16, 14, 37, 22.0)

        obj._p_serial = ts.raw()

        self.assertEqual(obj._p_mtime, ts.timeTime())
        self.assert_(isinstance(obj._p_mtime, float))

    def test_pickle_unpickle(self):
        import pickle
        from persistent import Persistent

        # see above:  class must be at module scope to be pickled.
        global Picklable
        class Picklable(Persistent):
            pass

        obj = Picklable()
        obj.attr = "test"

        s = pickle.dumps(obj)
        obj2 = pickle.loads(s)

        self.assertEqual(obj.attr, obj2.attr)

    def test___getattr__(self):
        from persistent import CHANGED
        from persistent import Persistent

        class H1(Persistent):

            def __init__(self):
                self.n = 0

            def __getattr__(self, attr):
                self.n += 1
                return self.n

        obj = H1()
        self.assertEqual(obj.larry, 1)
        self.assertEqual(obj.curly, 2)
        self.assertEqual(obj.moe, 3)

        jar = self._makeJar()
        jar.add(obj)
        obj._p_invalidate()

        # The simple Jar used for testing re-initializes the object.
        self.assertEqual(obj.larry, 1)

        # The getattr hook modified the object, so it should now be
        # in the changed state.
        self.assertEqual(obj._p_changed, 1)
        self.assertEqual(obj._p_state, CHANGED)
        self.assertEqual(obj.curly, 2)
        self.assertEqual(obj.moe, 3)

    def test___getattribute__(self):
        from persistent import CHANGED
        from persistent import Persistent

        class H2(Persistent):

            def __init__(self):
                self.n = 0

            def __getattribute__(self, attr):
                supergetattr = super(H2, self).__getattribute__
                try:
                    return supergetattr(attr)
                except AttributeError:
                    n = supergetattr("n")
                    self.n = n + 1
                    return n + 1

        obj = H2()
        self.assertEqual(obj.larry, 1)
        self.assertEqual(obj.curly, 2)
        self.assertEqual(obj.moe, 3)

        jar = self._makeJar()
        jar.add(obj)
        obj._p_invalidate()

        # The simple Jar used for testing re-initializes the object.
        self.assertEqual(obj.larry, 1)

        # The getattr hook modified the object, so it should now be
        # in the changed state.
        self.assertEqual(obj._p_changed, 1)
        self.assertEqual(obj._p_state, CHANGED)
        self.assertEqual(obj.curly, 2)
        self.assertEqual(obj.moe, 3)

    # TODO:  Need to decide how __setattr__ and __delattr__ should work,
    # then write tests.


def test_suite():
    return unittest.makeSuite(PersistenceTest)
