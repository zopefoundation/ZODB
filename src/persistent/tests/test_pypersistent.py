##############################################################################
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

class PersistentTests(unittest.TestCase):

    def _getTargetClass(self):
        from persistent.pypersistent import Persistent
        return Persistent

    def _makeOne(self, *args, **kw):
        return self._getTargetClass()(*args, **kw)

    def _makeJar(self):
        from zope.interface import implements
        from persistent.interfaces import IPersistentDataManager
        class _Jar(object):
            implements(IPersistentDataManager)
            def __init__(self):
                self._loaded = []
                self._registered = []
            def setstate(self, obj):
                self._loaded.append(obj._p_oid)
            def register(self, obj):
                self._registered.append(obj._p_oid)
        return _Jar()

    def _makeOneWithJar(self, *args, **kw):
        OID = '1' * 8
        inst = self._makeOne()
        jar = self._makeJar()
        inst._p_jar = jar
        inst._p_oid = OID
        return inst, jar, OID

    def test_class_conforms_to_IPersistent(self):
        from zope.interface.verify import verifyClass
        from persistent.interfaces import IPersistent
        verifyClass(IPersistent, self._getTargetClass())

    def test_instance_conforms_to_IPersistent(self):
        from zope.interface.verify import verifyObject
        from persistent.interfaces import IPersistent
        verifyObject(IPersistent, self._makeOne())

    def test_ctor(self):
        inst = self._makeOne()
        self.assertEqual(inst._p_jar, None)
        self.assertEqual(inst._p_oid, None)
        self.assertEqual(inst._p_serial, None)
        self.assertEqual(inst._p_changed, None)
        self.assertEqual(inst._p_sticky, False)

    def test_assign_p_jar_w_invalid_jar(self):
        inst = self._makeOne()
        def _test():
            inst._p_jar = object()
        self.assertRaises(ValueError, _test)

    def test_assign_p_jar_w_new_jar(self):
        inst = self._makeOne()
        inst._p_jar = self._makeJar()
        jar = self._makeJar()
        def _test():
            inst._p_jar = jar
        self.assertRaises(ValueError, _test)

    def test_assign_p_jar_w_valid_jar(self):
        jar = self._makeJar()
        inst = self._makeOne()
        inst._p_jar = jar
        self.failUnless(inst._p_jar is jar)
        inst._p_jar = jar # reassign only to same DM

    def test_assign_p_oid_w_invalid_oid(self):
        inst = self._makeOne()
        def _test():
            inst._p_oid = object()
        self.assertRaises(ValueError, _test)

    def test_assign_p_oid_w_valid_oid(self):
        OID = '1' * 8
        inst = self._makeOne()
        inst._p_oid = OID 
        self.assertEqual(inst._p_oid, OID)
        inst._p_oid = OID  # reassign only same OID

    def test_assign_p_oid_w_new_oid(self):
        OID1 = '1' * 8
        OID2 = '2' * 8
        inst = self._makeOne()
        inst._p_oid = OID1
        def _test():
            inst._p_oid = OID2
        self.assertRaises(ValueError, _test)

    def test_assign_p_serial_w_invalid_serial(self):
        inst = self._makeOne()
        def _test():
            inst._p_serial = object()
        self.assertRaises(ValueError, _test)

    def test_assign_p_serial_w_valid_serial(self):
        SERIAL = '1' * 8
        inst = self._makeOne()
        inst._p_serial = SERIAL 
        self.assertEqual(inst._p_serial, SERIAL)
        inst._p_serial = None
        self.assertEqual(inst._p_serial, None)

    def test_query_p_changed(self):
        inst = self._makeOne()
        self.assertEqual(inst._p_changed, None)
        inst._p_changed = True
        self.assertEqual(inst._p_changed, True)
        inst._p_changed = False
        self.assertEqual(inst._p_changed, False)

    def test_assign_p_changed_none_from_new(self):
        inst = self._makeOne()
        inst._p_changed = None
        self.assertEqual(inst._p_state, 'new')

    def test_assign_p_changed_true_from_new(self):
        inst = self._makeOne()
        inst._p_changed = True
        self.assertEqual(inst._p_state, 'unsaved')

    def test_assign_p_changed_false_from_new(self):
        inst = self._makeOne()
        inst._p_changed = False # activates
        self.assertEqual(inst._p_state, 'saved')

    def test_assign_p_changed_none_from_unsaved(self):
        inst = self._makeOne()
        inst._p_changed = True
        inst._p_changed = None
        # can't transition 'unsaved' -> 'new'
        self.assertEqual(inst._p_state, 'unsaved')

    def test_assign_p_changed_true_from_unsaved(self):
        inst = self._makeOne()
        inst._p_changed = True
        inst._p_changed = True
        self.assertEqual(inst._p_state, 'unsaved')

    def test_assign_p_changed_false_from_unsaved(self):
        inst = self._makeOne()
        inst._p_changed = True
        inst._p_changed = False
        self.assertEqual(inst._p_state, 'saved')

    def test_assign_p_changed_none_from_ghost(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = None
        self.assertEqual(inst._p_state, 'ghost')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test_assign_p_changed_true_from_ghost(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = True
        self.assertEqual(inst._p_state, 'changed')
        self.assertEqual(list(jar._loaded), [OID])
        self.assertEqual(list(jar._registered), [OID])

    def test_assign_p_changed_false_from_ghost(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = False
        self.assertEqual(inst._p_state, 'saved')
        self.assertEqual(list(jar._loaded), [OID])
        self.assertEqual(list(jar._registered), [])

    def test_assign_p_changed_none_from_saved(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_activate()
        jar._loaded = []
        inst._p_changed = None
        self.assertEqual(inst._p_state, 'ghost')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test_assign_p_changed_true_from_saved(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_activate()
        inst._p_changed = True
        self.assertEqual(inst._p_state, 'changed')
        self.assertEqual(list(jar._loaded), [OID])
        self.assertEqual(list(jar._registered), [OID])

    def test_assign_p_changed_false_from_saved(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_activate()
        jar._loaded = []
        inst._p_changed = False
        self.assertEqual(inst._p_state, 'saved')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test_assign_p_changed_none_from_changed(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_activate()
        inst._p_changed = True
        jar._loaded = []
        jar._registered = []
        inst._p_changed = None
        # assigning None is ignored when dirty
        self.assertEqual(inst._p_state, 'changed')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test_assign_p_changed_true_from_changed(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_activate()
        inst._p_changed = True
        jar._loaded = []
        jar._registered = []
        inst._p_changed = True
        self.assertEqual(inst._p_state, 'changed')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test_assign_p_changed_false_from_changed(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_activate()
        inst._p_changed = True
        jar._loaded = []
        jar._registered = []
        inst._p_changed = False
        self.assertEqual(inst._p_state, 'saved')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test_assign_p_changed_none_when_sticky(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = False
        inst._p_sticky = True
        def _test():
            inst._p_changed = None
        self.assertRaises(ValueError, _test)

    def test_delete_p_changed_from_new(self):
        inst = self._makeOne()
        del inst._p_changed
        self.assertEqual(inst._p_state, 'new')

    def test_delete_p_changed_from_unsaved(self):
        inst = self._makeOne()
        inst._p_changed = True
        del inst._p_changed
        # can't transition 'unsaved' -> 'new'
        self.assertEqual(inst._p_state, 'unsaved')

    def test_delete_p_changed_from_ghost(self):
        inst, jar, OID = self._makeOneWithJar()
        del inst._p_changed
        self.assertEqual(inst._p_state, 'ghost')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test_delete_p_changed_from_saved(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_activate()
        jar._loaded = []
        jar._registered = []
        del inst._p_changed
        self.assertEqual(inst._p_state, 'ghost')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test_delete_p_changed_from_changed(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_activate()
        inst._p_changed = True
        jar._loaded = []
        jar._registered = []
        del inst._p_changed
        # del is ignored when dirty
        self.assertEqual(inst._p_state, 'changed')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test_delete_p_changed_when_sticky(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = False
        inst._p_sticky = True
        def _test():
            del inst._p_changed
        self.assertRaises(ValueError, _test)

    def test_assign_p_sticky_true_when_ghost(self):
        inst = self._makeOne()
        def _test():
            inst._p_sticky = True
        self.assertRaises(ValueError, _test)

    def test_assign_p_sticky_false_when_ghost(self):
        inst = self._makeOne()
        def _test():
            inst._p_sticky = False
        self.assertRaises(ValueError, _test)

    def test_assign_p_sticky_true_non_ghost(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = False
        inst._p_sticky = True
        self.failUnless(inst._p_sticky)

    def test_assign_p_sticky_false_non_ghost(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = False
        inst._p_sticky = False
        self.failIf(inst._p_sticky)

    def test__p_state_new(self):
        inst = self._makeOne()
        self.assertEqual(inst._p_state, 'new')

    def test__p_state_unsaved(self):
        inst = self._makeOne()
        inst._p_changed = True
        self.assertEqual(inst._p_state, 'unsaved')

    def test__p_state_ghost(self):
        inst, jar, OID = self._makeOneWithJar()
        self.assertEqual(inst._p_state, 'ghost')

    def test__p_state_changed(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = True
        self.assertEqual(inst._p_state, 'changed')

    def test__p_state_changed_sticky(self):
        # 'sticky' is not a state, but a separate flag.
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = True
        inst._p_sticky = True
        self.assertEqual(inst._p_state, 'changed (sticky)')

    def test__p_state_saved(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = False
        self.assertEqual(inst._p_state, 'saved')

    def test__p_state_saved_sticky(self):
        # 'sticky' is not a state, but a separate flag.
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = False
        inst._p_sticky = True
        self.assertEqual(inst._p_state, 'saved (sticky)')

    def test___getstate__(self):
        inst = self._makeOne()
        self.assertEqual(inst.__getstate__(), {})

    def test___setstate___empty(self):
        inst = self._makeOne()
        inst.__setstate__({}) # doesn't raise, but doesn't change anything

    def test___setstate___nonempty(self):
        inst = self._makeOne()
        self.assertRaises(ValueError, inst.__setstate__, {'bogus': 1})
        self.assertEqual(inst._p_jar, None)
        self.assertEqual(inst._p_oid, None)
        self.assertEqual(inst._p_serial, None)
        self.assertEqual(inst._p_changed, None)
        self.assertEqual(inst._p_sticky, False)

    def test__p_activate_from_new(self):
        inst = self._makeOne()
        inst._p_activate()
        self.assertEqual(inst._p_state, 'saved')

    def test__p_activate_from_saved(self):
        inst = self._makeOne()
        inst._p_changed = False
        inst._p_activate() # noop from 'unsaved' state
        self.assertEqual(inst._p_state, 'saved')

    def test__p_activate_from_unsaved(self):
        inst = self._makeOne()
        inst._p_changed = True
        inst._p_activate() # noop from 'saved' state
        self.assertEqual(inst._p_state, 'unsaved')
