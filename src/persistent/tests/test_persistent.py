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
import unittest

from persistent import Persistent
from persistent.interfaces import IPersistent

try:
    import zope.interface
except ImportError:
    interfaces = False
else:
    interfaces = True

class Test(unittest.TestCase):

    klass = None # override in subclass

    def testSaved(self):
        p = self.klass()
        p._p_oid = '\0\0\0\0\0\0hi'
        dm = DM()
        p._p_jar = dm
        self.assertEqual(p._p_changed, 0)
        self.assertEqual(dm.called, 0)
        p.inc()
        self.assertEqual(p._p_changed, 1)
        self.assertEqual(dm.called, 1)
        p.inc()
        self.assertEqual(p._p_changed, 1)
        self.assertEqual(dm.called, 1)
        p._p_deactivate()
        self.assertEqual(p._p_changed, 1)
        self.assertEqual(dm.called, 1)
        p._p_deactivate()
        self.assertEqual(p._p_changed, 1)
        self.assertEqual(dm.called, 1)
        del p._p_changed
        # XXX deal with current cPersistence implementation
        if p._p_changed != 3:
            self.assertEqual(p._p_changed, None)
        self.assertEqual(dm.called, 1)
        p.inc()
        self.assertEqual(p.x, 43)
        self.assertEqual(p._p_changed, 1)
        self.assertEqual(dm.called, 2)
        p._p_changed = 0
        self.assertEqual(p._p_changed, 0)
        self.assertEqual(dm.called, 2)
        self.assertEqual(p.x, 43)
        p.inc()
        self.assertEqual(p._p_changed, 1)
        self.assertEqual(dm.called, 3)

    def testUnsaved(self):
        p = self.klass()

        self.assertEqual(p.x, 0)
        self.assertEqual(p._p_changed, 0)
        self.assertEqual(p._p_jar, None)
        self.assertEqual(p._p_oid, None)
        p.inc()
        p.inc()
        self.assertEqual(p.x, 2)
        self.assertEqual(p._p_changed, 0)

        p._p_deactivate()
        self.assertEqual(p._p_changed, 0)
        p._p_changed = 1
        self.assertEqual(p._p_changed, 0)
        p._p_deactivate()
        self.assertEqual(p._p_changed, 0)
        del p._p_changed
        self.assertEqual(p._p_changed, 0)
        if self.has_dict:
            self.failUnless(p.__dict__)
        self.assertEqual(p.x, 2)

    def testState(self):
        p = self.klass()
        self.assertEqual(p.__getstate__(), {'x': 0})
        self.assertEqual(p._p_changed, 0)
        p.__setstate__({'x':5})
        self.assertEqual(p._p_changed, 0)
        if self.has_dict:
            p._v_foo = 2
        self.assertEqual(p.__getstate__(), {'x': 5})
        self.assertEqual(p._p_changed, 0)

    def testSetStateSerial(self):
        p = self.klass()
        p._p_serial = '00000012'
        p.__setstate__(p.__getstate__())
        self.assertEqual(p._p_serial, '00000012')

    def testDirectChanged(self):
        p = self.klass()
        p._p_oid = 1
        dm = DM()
        p._p_jar = dm
        self.assertEqual(p._p_changed, 0)
        self.assertEqual(dm.called, 0)
        p._p_changed = 1
        self.assertEqual(dm.called, 1)

    def testGhostChanged(self):
        # An object is a ghost, and it's _p_changed it set to True.
        # This assignment should have no effect.
        p = self.klass()
        p._p_oid = 1
        dm = DM()
        p._p_jar = dm
        p._p_deactivate()
        self.assertEqual(p._p_changed, None)
        p._p_changed = True
        self.assertEqual(p._p_changed, None)

    def testRegistrationFailure(self):
        p = self.klass()
        p._p_oid = 1
        dm = BrokenDM()
        p._p_jar = dm
        self.assertEqual(p._p_changed, 0)
        self.assertEqual(dm.called, 0)
        try:
            p._p_changed = 1
        except NotImplementedError:
            pass
        else:
            raise AssertionError("Exception not propagated")
        self.assertEqual(dm.called, 1)
        self.assertEqual(p._p_changed, 0)

    def testLoadFailure(self):
        p = self.klass()
        p._p_oid = 1
        dm = BrokenDM()
        p._p_jar = dm
        p._p_deactivate()  # make it a ghost

        try:
            p._p_activate()
        except NotImplementedError:
            pass
        else:
            raise AssertionError("Exception not propagated")
        self.assertEqual(p._p_changed, None)

    def testActivate(self):
        p = self.klass()
        dm = DM()
        p._p_oid = 1
        p._p_jar = dm
        p._p_changed = 0
        p._p_deactivate()
        # XXX does this really test the activate method?
        p._p_activate()
        self.assertEqual(p._p_changed, 0)
        self.assertEqual(p.x, 42)

    def testDeactivate(self):
        p = self.klass()
        dm = DM()
        p._p_oid = 1
        p._p_deactivate() # this deactive has no effect
        self.assertEqual(p._p_changed, 0)
        p._p_jar = dm
        p._p_changed = 0
        p._p_deactivate()
        self.assertEqual(p._p_changed, None)
        p._p_activate()
        self.assertEqual(p._p_changed, 0)
        self.assertEqual(p.x, 42)

    if interfaces:
        def testInterface(self):
            self.assert_(IPersistent.isImplementedByInstancesOf(Persistent),
                         "%s does not implement IPersistent" % Persistent)
            p = Persistent()
            self.assert_(IPersistent.isImplementedBy(p),
                         "%s does not implement IPersistent" % p)

            self.assert_(IPersistent.isImplementedByInstancesOf(P),
                         "%s does not implement IPersistent" % P)
            p = self.klass()
            self.assert_(IPersistent.isImplementedBy(p),
                         "%s does not implement IPersistent" % p)

    def testDataManagerAndAttributes(self):
        # Test to cover an odd bug where the instance __dict__ was
        # set at the same location as the data manager in the C type.
        p = P()
        p.inc()
        p.inc()
        self.assert_('x' in p.__dict__)
        self.assert_(p._p_jar is None)

    def testMultipleInheritance(self):
        # make sure it is possible to inherit from two different
        # subclasses of persistent.
        class A(Persistent):
            pass
        class B(Persistent):
            pass
        class C(A, B):
            pass
        class D(object):
            pass
        class E(D, B):
            pass

    def testMultipleMeta(self):
        # make sure it's possible to define persistent classes
        # with a base whose metaclass is different
        class alternateMeta(type):
            pass
        class alternate(object):
            __metaclass__ = alternateMeta
        class mixedMeta(alternateMeta, type):
            pass
        class mixed(alternate,Persistent):
            __metaclass__ = mixedMeta

    def testSlots(self):
        # Verify that Persistent classes behave the same way
        # as pure Python objects where '__slots__' and '__dict__'
        # are concerned.

        class noDict(object):
            __slots__ = ['foo']

        class shouldHaveDict(noDict):
            pass

        class p_noDict(Persistent):
            __slots__ = ['foo']

        class p_shouldHaveDict(p_noDict):
            pass

        self.assertEqual(noDict.__dictoffset__, 0)
        self.assertEqual(p_noDict.__dictoffset__, 0)

        self.assert_(shouldHaveDict.__dictoffset__ <> 0)
        self.assert_(p_shouldHaveDict.__dictoffset__ <> 0)

    def testBasicTypeStructure(self):
        # test that a persistent class has a sane C type structure
        # use P (defined below) as simplest example
        self.assertEqual(Persistent.__dictoffset__, 0)
        self.assertEqual(Persistent.__weakrefoffset__, 0)
        self.assert_(Persistent.__basicsize__ > object.__basicsize__)
        self.assert_(P.__dictoffset__)
        self.assert_(P.__weakrefoffset__)
        self.assert_(P.__dictoffset__ < P.__weakrefoffset__)
        self.assert_(P.__basicsize__ > Persistent.__basicsize__)

    def testDeactivateErrors(self):
        p = self.klass()
        p._p_oid = '\0\0\0\0\0\0hi'
        dm = DM()
        p._p_jar = dm

        def typeerr(*args, **kwargs):
            self.assertRaises(TypeError, p, *args, **kwargs)

        typeerr(1)
        typeerr(1, 2)
        typeerr(spam=1)
        typeerr(spam=1, force=1)

        p._p_changed = True
        class Err(object):
            def __nonzero__(self):
                raise RuntimeError

        typeerr(force=Err())

class P(Persistent):
    def __init__(self):
        self.x = 0
    def inc(self):
        self.x += 1

class P2(P):
    def __getstate__(self):
        return 42
    def __setstate__(self, v):
        self.v = v

class B(Persistent):

    __slots__ = ["x", "_p_serial"]

    def __init__(self):
        self.x = 0

    def inc(self):
        self.x += 1

    def __getstate__(self):
        return {'x': self.x}

    def __setstate__(self, state):
        self.x = state['x']

class DM:
    def __init__(self):
        self.called = 0
    def register(self, ob):
        self.called += 1
    def setstate(self, ob):
        ob.__setstate__({'x': 42})

class BrokenDM(DM):

    def register(self,ob):
        self.called += 1
        raise NotImplementedError

    def setstate(self,ob):
        raise NotImplementedError

class PersistentTest(Test):
    klass = P
    has_dict = 1

    def testPicklable(self):
        import pickle

        p = self.klass()
        p.inc()
        p2 = pickle.loads(pickle.dumps(p))
        self.assertEqual(p2.__class__, self.klass)

        # verify that the inc is reflected:
        self.assertEqual(p2.x, p.x)
        
        # This assertion would be invalid.  Interfaces
        # are compared by identity and copying doesn't
        # preserve identity. We would get false negatives due
        # to the differing identities of the original and copied
        # PersistentInterface:
        # self.assertEqual(p2.__dict__, p.__dict__)

    def testPicklableWCustomState(self):
        import pickle

        p = P2()
        p2 = pickle.loads(pickle.dumps(p))
        self.assertEqual(p2.__class__, P2);
        self.assertEqual(p2.__dict__, {'v': 42})

class BasePersistentTest(Test):
    klass = B
    has_dict = 0

def test_suite():
    s = unittest.TestSuite()
    for klass in PersistentTest, BasePersistentTest:
        s.addTest(unittest.makeSuite(klass))
    return s
