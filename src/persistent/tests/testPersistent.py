#############################################################################
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
import unittest

Picklable = None # avoid global import of Persistent;  updated later

class PersistenceTest(unittest.TestCase):

    def setUp(self):
        from persistent.tests.utils import ResettingJar
        self.jar = ResettingJar()

    def tearDown(self):
        self.jar.close()

    def _make_P(self):
        from persistent import Persistent

        class P(Persistent):
            pass

        return P()

    def testOidAndJarAttrs(self):
        obj = self._make_P()
        self.assertEqual(obj._p_oid, None)
        obj._p_oid = 12
        self.assertEqual(obj._p_oid, 12)
        del obj._p_oid

        self.jar.add(obj)

        # Can't change oid of cache object.
        def deloid():
            del obj._p_oid
        self.assertRaises(ValueError, deloid)
        def setoid():
            obj._p_oid = 12
        self.assertRaises(ValueError, setoid)

        def deloid():
            del obj._p_jar
        self.assertRaises(ValueError, deloid)
        def setoid():
            obj._p_jar = 12
        self.assertRaises(ValueError, setoid)

    def testChangedAndState(self):
        from persistent import CHANGED
        from persistent import GHOST
        from persistent import UPTODATE
        obj = self._make_P()
        self.jar.add(obj)

        # The value returned for _p_changed can be one of:
        # 0 -- it is not changed
        # 1 -- it is changed
        # None -- it is a ghost

        obj.x = 1
        self.assertEqual(obj._p_changed, 1)
        self.assertEqual(obj._p_state, CHANGED)
        self.assert_(obj in self.jar.registered)

        obj._p_changed = 0
        self.assertEqual(obj._p_changed, 0)
        self.assertEqual(obj._p_state, UPTODATE)
        self.jar.registered.clear()

        obj._p_changed = 1
        self.assertEqual(obj._p_changed, 1)
        self.assertEqual(obj._p_state, CHANGED)
        self.assert_(obj in self.jar.registered)

        # setting obj._p_changed to None ghostifies if the
        # object is in the up-to-date state, but not otherwise.
        obj._p_changed = None
        self.assertEqual(obj._p_changed, 1)
        self.assertEqual(obj._p_state, CHANGED)
        obj._p_changed = 0
        # Now it's a ghost.
        obj._p_changed = None
        self.assertEqual(obj._p_changed, None)
        self.assertEqual(obj._p_state, GHOST)

        obj = self._make_P()
        self.jar.add(obj)
        obj._p_changed = 1
        # You can transition directly from modified to ghost if
        # you delete the _p_changed attribute.
        del obj._p_changed
        self.assertEqual(obj._p_changed, None)
        self.assertEqual(obj._p_state, GHOST)

    def testStateReadonly(self):
        from persistent import CHANGED
        from persistent import GHOST
        from persistent import STICKY
        from persistent import UPTODATE
        # make sure we can't write to _p_state; we don't want yet
        # another way to change state!
        obj = self._make_P()
        def setstate(value):
            obj._p_state = value
        self.assertRaises(Exception, setstate, GHOST)
        self.assertRaises(Exception, setstate, UPTODATE)
        self.assertRaises(Exception, setstate, CHANGED)
        self.assertRaises(Exception, setstate, STICKY)

    def testInvalidate(self):
        from persistent import GHOST
        from persistent import UPTODATE
        obj = self._make_P()
        self.jar.add(obj)

        self.assertEqual(obj._p_changed, 0)
        self.assertEqual(obj._p_state, UPTODATE)
        obj._p_invalidate()
        self.assertEqual(obj._p_changed, None)
        self.assertEqual(obj._p_state, GHOST)

        obj._p_activate()
        obj.x = 1
        obj._p_invalidate()
        self.assertEqual(obj._p_changed, None)
        self.assertEqual(obj._p_state, GHOST)

    def testSerial(self):
        noserial = "\000" * 8
        obj = self._make_P()
        self.assertEqual(obj._p_serial, noserial)

        def set(val):
            obj._p_serial = val
        self.assertRaises(ValueError, set, 1)
        self.assertRaises(ValueError, set, "0123")
        self.assertRaises(ValueError, set, "012345678")
        self.assertRaises(ValueError, set, u"01234567")

        obj._p_serial = "01234567"
        del obj._p_serial
        self.assertEqual(obj._p_serial, noserial)

    def testMTime(self):
        import time
        from persistent.TimeStamp import TimeStamp
        obj = self._make_P()
        self.assertEqual(obj._p_mtime, None)

        t = int(time.time())
        ts = TimeStamp(*time.gmtime(t)[:6])
        obj._p_serial = repr(ts)
        self.assertEqual(obj._p_mtime, t)
        self.assert_(isinstance(obj._p_mtime, float))

    def testPicklable(self):
        import pickle
        from persistent import Persistent

        global Picklable
        class Picklable(Persistent):
            pass

        obj = Picklable()
        obj.attr = "test"
        s = pickle.dumps(obj)
        obj2 = pickle.loads(s)
        self.assertEqual(obj.attr, obj2.attr)

    def testGetattr(self):
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

        self.jar.add(obj)
        obj._p_deactivate()

        # The simple Jar used for testing re-initializes the object.
        self.assertEqual(obj.larry, 1)
        # The getattr hook modified the object, so it should now be
        # in the changed state.
        self.assertEqual(obj._p_changed, 1)
        self.assertEqual(obj._p_state, CHANGED)
        self.assertEqual(obj.curly, 2)
        self.assertEqual(obj.moe, 3)

    def testGetattribute(self):
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

        self.jar.add(obj)
        obj._p_deactivate()

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
