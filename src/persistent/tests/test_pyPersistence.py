##############################################################################
#
# Copyright (c) 2011 Zope Foundation and Contributors.
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
        from persistent.pyPersistence import Persistent
        return Persistent

    def _makeOne(self, *args, **kw):
        return self._getTargetClass()(*args, **kw)

    def _makeJar(self):
        from zope.interface import implements
        from persistent.interfaces import IPersistentDataManager

        class _Cache(object):
            def __init__(self):
                self._mru = []
            def mru(self, oid):
                self._mru.append(oid)

        class _Jar(object):
            implements(IPersistentDataManager)
            def __init__(self):
                self._loaded = []
                self._registered = []
                self._cache = _Cache()
            def setstate(self, obj):
                self._loaded.append(obj._p_oid)
            def register(self, obj):
                self._registered.append(obj._p_oid)

        return _Jar()

    def _makeOneWithJar(self, klass=None):
        from persistent.pyPersistence import _makeOctets
        OID = _makeOctets('\x01' * 8)
        if klass is not None:
            inst = klass()
        else:
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
        from persistent.pyPersistence import _INITIAL_SERIAL
        inst = self._makeOne()
        self.assertEqual(inst._p_jar, None)
        self.assertEqual(inst._p_oid, None)
        self.assertEqual(inst._p_serial, _INITIAL_SERIAL)
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
        from persistent.pyPersistence import _makeOctets
        OID = _makeOctets('\x01' * 8)
        inst = self._makeOne()
        inst._p_oid = OID 
        self.assertEqual(inst._p_oid, OID)
        inst._p_oid = OID  # reassign only same OID

    def test_assign_p_oid_w_new_oid_wo_jar(self):
        from persistent.pyPersistence import _makeOctets
        OID1 = _makeOctets('\x01' * 8)
        OID2 = _makeOctets('\x02' * 8)
        inst = self._makeOne()
        inst._p_oid = OID1
        inst._p_oid = OID2
        self.assertEqual(inst._p_oid, OID2)

    def test_assign_p_oid_w_new_oid_w_jar(self):
        from persistent.pyPersistence import _makeOctets
        OID1 = _makeOctets('\x01' * 8)
        OID2 = _makeOctets('\x02' * 8)
        inst = self._makeOne()
        inst._p_oid = OID1
        inst._p_jar = self._makeJar()
        def _test():
            inst._p_oid = OID2
        self.assertRaises(ValueError, _test)

    def test_delete_p_oid_wo_jar(self):
        from persistent.pyPersistence import _makeOctets
        OID = _makeOctets('\x01' * 8)
        inst = self._makeOne()
        inst._p_oid = OID
        del inst._p_oid
        self.assertEqual(inst._p_oid, None)

    def test_delete_p_oid_w_jar(self):
        from persistent.pyPersistence import _makeOctets
        OID = _makeOctets('\x01' * 8)
        inst = self._makeOne()
        inst._p_oid = OID
        inst._p_jar = self._makeJar()
        def _test():
            del inst._p_oid
        self.assertRaises(ValueError, _test)

    def test_assign_p_serial_w_invalid_type(self):
        inst = self._makeOne()
        def _test():
            inst._p_serial = object()
        self.assertRaises(ValueError, _test)

    def test_assign_p_serial_too_short(self):
        inst = self._makeOne()
        def _test():
            inst._p_serial = '\x01\x02\x03'
        self.assertRaises(ValueError, _test)

    def test_assign_p_serial_too_long(self):
        inst = self._makeOne()
        def _test():
            inst._p_serial = '\x01\x02\x03' * 3
        self.assertRaises(ValueError, _test)

    def test_assign_p_serial_w_valid_serial(self):
        from persistent.pyPersistence import _makeOctets
        from persistent.pyPersistence import _INITIAL_SERIAL
        SERIAL = _makeOctets('\x01' * 8)
        inst = self._makeOne()
        inst._p_serial = SERIAL 
        self.assertEqual(inst._p_serial, SERIAL)
        inst._p_serial = None
        self.assertEqual(inst._p_serial, _INITIAL_SERIAL)

    def test_delete_p_serial(self):
        from persistent.pyPersistence import _makeOctets
        from persistent.pyPersistence import _INITIAL_SERIAL
        SERIAL = _makeOctets('\x01' * 8)
        inst = self._makeOne()
        inst._p_serial = SERIAL 
        self.assertEqual(inst._p_serial, SERIAL)
        del(inst._p_serial)
        self.assertEqual(inst._p_serial, _INITIAL_SERIAL)

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
        self.assertEqual(inst._p_status, 'new')

    def test_assign_p_changed_true_from_new(self):
        inst = self._makeOne()
        inst._p_changed = True
        self.assertEqual(inst._p_status, 'unsaved')

    def test_assign_p_changed_false_from_new(self):
        inst = self._makeOne()
        inst._p_changed = False # activates
        self.assertEqual(inst._p_status, 'saved')

    def test_assign_p_changed_none_from_unsaved(self):
        inst = self._makeOne()
        inst._p_changed = True
        inst._p_changed = None
        # can't transition 'unsaved' -> 'new'
        self.assertEqual(inst._p_status, 'unsaved')

    def test_assign_p_changed_true_from_unsaved(self):
        inst = self._makeOne()
        inst._p_changed = True
        inst._p_changed = True
        self.assertEqual(inst._p_status, 'unsaved')

    def test_assign_p_changed_false_from_unsaved(self):
        inst = self._makeOne()
        inst._p_changed = True
        inst._p_changed = False
        self.assertEqual(inst._p_status, 'saved')

    def test_assign_p_changed_none_from_ghost(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = None
        self.assertEqual(inst._p_status, 'ghost')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test_assign_p_changed_true_from_ghost(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = True
        self.assertEqual(inst._p_status, 'changed')
        self.assertEqual(list(jar._loaded), [OID])
        self.assertEqual(list(jar._registered), [OID])

    def test_assign_p_changed_false_from_ghost(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = False
        self.assertEqual(inst._p_status, 'saved')
        self.assertEqual(list(jar._loaded), [OID])
        self.assertEqual(list(jar._registered), [])

    def test_assign_p_changed_none_from_saved(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_activate()
        jar._loaded = []
        inst._p_changed = None
        self.assertEqual(inst._p_status, 'ghost')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test_assign_p_changed_true_from_saved(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_activate()
        inst._p_changed = True
        self.assertEqual(inst._p_status, 'changed')
        self.assertEqual(list(jar._loaded), [OID])
        self.assertEqual(list(jar._registered), [OID])

    def test_assign_p_changed_false_from_saved(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_activate()
        jar._loaded = []
        inst._p_changed = False
        self.assertEqual(inst._p_status, 'saved')
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
        self.assertEqual(inst._p_status, 'changed')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test_assign_p_changed_true_from_changed(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_activate()
        inst._p_changed = True
        jar._loaded = []
        jar._registered = []
        inst._p_changed = True
        self.assertEqual(inst._p_status, 'changed')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test_assign_p_changed_false_from_changed(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_activate()
        inst._p_changed = True
        jar._loaded = []
        jar._registered = []
        inst._p_changed = False
        self.assertEqual(inst._p_status, 'saved')
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
        self.assertEqual(inst._p_status, 'new')

    def test_delete_p_changed_from_unsaved(self):
        inst = self._makeOne()
        inst._p_changed = True
        del inst._p_changed
        self.assertEqual(inst._p_status, 'new')

    def test_delete_p_changed_from_ghost(self):
        inst, jar, OID = self._makeOneWithJar()
        del inst._p_changed
        self.assertEqual(inst._p_status, 'ghost')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test_delete_p_changed_from_saved(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_activate()
        jar._loaded = []
        jar._registered = []
        del inst._p_changed
        self.assertEqual(inst._p_status, 'ghost')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test_delete_p_changed_from_changed(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_activate()
        inst._p_changed = True
        jar._loaded = []
        jar._registered = []
        del inst._p_changed
        self.assertEqual(inst._p_status, 'ghost')
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

    def test__p_status_new(self):
        inst = self._makeOne()
        self.assertEqual(inst._p_status, 'new')

    def test__p_status_unsaved(self):
        inst = self._makeOne()
        inst._p_changed = True
        self.assertEqual(inst._p_status, 'unsaved')

    def test__p_status_ghost(self):
        inst, jar, OID = self._makeOneWithJar()
        self.assertEqual(inst._p_status, 'ghost')

    def test__p_status_changed(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = True
        self.assertEqual(inst._p_status, 'changed')

    def test__p_status_changed_sticky(self):
        # 'sticky' is not a state, but a separate flag.
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = True
        inst._p_sticky = True
        self.assertEqual(inst._p_status, 'changed (sticky)')

    def test__p_status_saved(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = False
        self.assertEqual(inst._p_status, 'saved')

    def test__p_status_saved_sticky(self):
        # 'sticky' is not a state, but a separate flag.
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = False
        inst._p_sticky = True
        self.assertEqual(inst._p_status, 'saved (sticky)')

    def test__p_mtime_no_serial(self):
        inst = self._makeOne()
        self.assertEqual(inst._p_mtime, None)

    def test__p_mtime_w_serial(self):
        from persistent.timestamp import TimeStamp
        WHEN_TUPLE = (2011, 2, 15, 13, 33, 27.5)
        ts = TimeStamp(*WHEN_TUPLE)
        inst, jar, OID = self._makeOneWithJar()
        inst._p_serial = ts.raw()
        self.assertEqual(inst._p_mtime, ts.timeTime())

    def test__p_state_new(self):
        inst = self._makeOne()
        self.assertEqual(inst._p_state, 0)

    def test__p_state_unsaved(self):
        inst = self._makeOne()
        inst._p_changed = True
        self.assertEqual(inst._p_state, 0)

    def test__p_state_ghost(self):
        inst, jar, OID = self._makeOneWithJar()
        self.assertEqual(inst._p_state, -1)

    def test__p_state_changed(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = True
        self.assertEqual(inst._p_state, 1)

    def test__p_state_changed_sticky(self):
        # 'sticky' is not a state, but a separate flag.
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = True
        inst._p_sticky = True
        self.assertEqual(inst._p_state, 2)

    def test__p_state_saved(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = False
        self.assertEqual(inst._p_state, 0)

    def test__p_state_saved_sticky(self):
        # 'sticky' is not a state, but a separate flag.
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = False
        inst._p_sticky = True
        self.assertEqual(inst._p_state, 2)

    def test_query_p_estimated_size(self):
        inst = self._makeOne()
        self.assertEqual(inst._p_estimated_size, 0)

    def test_assign_p_estimated_size(self):
        # XXX at the moment, we don't store this value.
        inst = self._makeOne()
        inst._p_estimated_size = 123
        self.assertEqual(inst._p_estimated_size, 0)

    def test___getattribute___p__names(self):
        NAMES = ['_p_jar',
                 '_p_oid',
                 '_p_changed',
                 '_p_serial',
                 '_p_mtime',
                 '_p_state',
                 '_p_estimated_size',
                 '_p_sticky',
                 '_p_status',
                ]
        inst, jar, OID = self._makeOneWithJar()
        jar._cache._mru = []
        for name in NAMES:
            getattr(inst, name)
        self.assertEqual(jar._cache._mru, [])

    def test___getattribute__special_name(self):
        from persistent.pyPersistence import SPECIAL_NAMES
        inst, jar, OID = self._makeOneWithJar()
        jar._cache._mru = []
        for name in SPECIAL_NAMES:
            getattr(inst, name, None)
        self.assertEqual(jar._cache._mru, [])

    def test___getattribute__normal_name_from_new(self):
        class Derived(self._getTargetClass()):
            normal = 'value'
        inst = Derived()
        self.assertEqual(getattr(inst, 'normal', None), 'value')

    def test___getattribute__normal_name_from_unsaved(self):
        class Derived(self._getTargetClass()):
            normal = 'value'
        inst = Derived()
        inst._p_changed = True
        self.assertEqual(getattr(inst, 'normal', None), 'value')

    def test___getattribute__normal_name_from_ghost(self):
        class Derived(self._getTargetClass()):
            normal = 'value'
        inst, jar, OID = self._makeOneWithJar(Derived)
        jar._cache._mru = []
        self.assertEqual(getattr(inst, 'normal', None), 'value')
        self.assertEqual(jar._cache._mru, [OID])

    def test___getattribute__normal_name_from_saved(self):
        class Derived(self._getTargetClass()):
            normal = 'value'
        inst, jar, OID = self._makeOneWithJar(Derived)
        inst._p_changed = False
        jar._cache._mru = []
        self.assertEqual(getattr(inst, 'normal', None), 'value')
        self.assertEqual(jar._cache._mru, [OID])

    def test___getattribute__normal_name_from_changed(self):
        class Derived(self._getTargetClass()):
            normal = 'value'
        inst, jar, OID = self._makeOneWithJar(Derived)
        inst._p_changed = True
        jar._cache._mru = []
        self.assertEqual(getattr(inst, 'normal', None), 'value')
        self.assertEqual(jar._cache._mru, [OID])

    def test___setattr___p__names(self):
        from persistent.pyPersistence import _makeOctets
        SERIAL = _makeOctets('\x01' * 8)
        inst, jar, OID = self._makeOneWithJar()
        NAMES = [('_p_jar', jar),
                 ('_p_oid', OID),
                 ('_p_changed', False),
                 ('_p_serial', SERIAL),
                 ('_p_estimated_size', 0),
                 ('_p_sticky', False),
                ]
        jar._cache._mru = []
        for name, value in NAMES:
            setattr(inst, name, value)
        self.assertEqual(jar._cache._mru, [])

    def test___setattr__normal_name_from_new(self):
        class Derived(self._getTargetClass()):
            normal = 'before'
        inst = Derived()
        setattr(inst, 'normal', 'after')
        self.assertEqual(getattr(inst, 'normal', None), 'after')

    def test___setattr__normal_name_from_unsaved(self):
        class Derived(self._getTargetClass()):
            normal = 'before'
        inst = Derived()
        inst._p_changed = True
        setattr(inst, 'normal', 'after')
        self.assertEqual(getattr(inst, 'normal', None), 'after')

    def test___setattr__normal_name_from_ghost(self):
        class Derived(self._getTargetClass()):
            normal = 'before'
        inst, jar, OID = self._makeOneWithJar(Derived)
        jar._cache._mru = []
        setattr(inst, 'normal', 'after')
        self.assertEqual(jar._cache._mru, [OID])
        self.assertEqual(jar._registered, [OID])
        self.assertEqual(getattr(inst, 'normal', None), 'after')

    def test___setattr__normal_name_from_saved(self):
        class Derived(self._getTargetClass()):
            normal = 'before'
        inst, jar, OID = self._makeOneWithJar(Derived)
        inst._p_changed = False
        jar._cache._mru = []
        setattr(inst, 'normal', 'after')
        self.assertEqual(jar._cache._mru, [OID])
        self.assertEqual(jar._registered, [OID])
        self.assertEqual(getattr(inst, 'normal', None), 'after')

    def test___setattr__normal_name_from_changed(self):
        class Derived(self._getTargetClass()):
            normal = 'before'
        inst, jar, OID = self._makeOneWithJar(Derived)
        inst._p_changed = True
        jar._cache._mru = []
        jar._registered = []
        setattr(inst, 'normal', 'after')
        self.assertEqual(jar._cache._mru, [OID])
        self.assertEqual(jar._registered, [])
        self.assertEqual(getattr(inst, 'normal', None), 'after')

    def test___delattr___p__names(self):
        NAMES = ['_p_changed',
                 '_p_serial',
                ]
        inst, jar, OID = self._makeOneWithJar()
        jar._cache._mru = []
        jar._registered = []
        for name in NAMES:
            delattr(inst, name)
        self.assertEqual(jar._cache._mru, [])
        self.assertEqual(jar._registered, [])

    def test___delattr__normal_name_from_new(self):
        class Derived(self._getTargetClass()):
            normal = 'before'
            def __init__(self):
                self.__dict__['normal'] = 'after'
        inst = Derived()
        delattr(inst, 'normal')
        self.assertEqual(getattr(inst, 'normal', None), 'before')

    def test___delattr__normal_name_from_unsaved(self):
        class Derived(self._getTargetClass()):
            normal = 'before'
            def __init__(self):
                self.__dict__['normal'] = 'after'
        inst = Derived()
        inst._p_changed = True
        delattr(inst, 'normal')
        self.assertEqual(getattr(inst, 'normal', None), 'before')

    def test___delattr__normal_name_from_ghost(self):
        class Derived(self._getTargetClass()):
            normal = 'before'
            def __init__(self):
                self.__dict__['normal'] = 'after'
        inst, jar, OID = self._makeOneWithJar(Derived)
        jar._cache._mru = []
        jar._registered = []
        delattr(inst, 'normal')
        self.assertEqual(jar._cache._mru, [OID])
        self.assertEqual(jar._registered, [OID])
        self.assertEqual(getattr(inst, 'normal', None), 'before')

    def test___delattr__normal_name_from_saved(self):
        class Derived(self._getTargetClass()):
            normal = 'before'
            def __init__(self):
                self.__dict__['normal'] = 'after'
        inst, jar, OID = self._makeOneWithJar(Derived)
        inst._p_changed = False
        jar._cache._mru = []
        jar._registered = []
        delattr(inst, 'normal')
        self.assertEqual(jar._cache._mru, [OID])
        self.assertEqual(jar._registered, [OID])
        self.assertEqual(getattr(inst, 'normal', None), 'before')

    def test___delattr__normal_name_from_changed(self):
        class Derived(self._getTargetClass()):
            normal = 'before'
            def __init__(self):
                self.__dict__['normal'] = 'after'
        inst, jar, OID = self._makeOneWithJar(Derived)
        inst._p_changed = True
        jar._cache._mru = []
        jar._registered = []
        delattr(inst, 'normal')
        self.assertEqual(jar._cache._mru, [OID])
        self.assertEqual(jar._registered, [])
        self.assertEqual(getattr(inst, 'normal', None), 'before')

    def test___getstate__(self):
        inst = self._makeOne()
        self.assertEqual(inst.__getstate__(), ())

    def test___getstate___derived_w_dict(self):
        class Derived(self._getTargetClass()):
            pass
        inst = Derived()
        inst.foo = 'bar'
        inst._p_baz = 'bam'
        inst._v_qux = 'spam'
        self.assertEqual(inst.__getstate__(), {'foo': 'bar'})

    def test___setstate___empty(self):
        inst = self._makeOne()
        inst.__setstate__(()) # doesn't raise, but doesn't change anything

    def test___setstate___nonempty(self):
        from persistent.pyPersistence import _INITIAL_SERIAL
        inst = self._makeOne()
        self.assertRaises(ValueError, inst.__setstate__, {'bogus': 1})
        self.assertEqual(inst._p_jar, None)
        self.assertEqual(inst._p_oid, None)
        self.assertEqual(inst._p_serial, _INITIAL_SERIAL)
        self.assertEqual(inst._p_changed, None)
        self.assertEqual(inst._p_sticky, False)

    def test___setstate___nonempty_derived_w_dict(self):
        class Derived(self._getTargetClass()):
            pass
        inst = Derived()
        inst.foo = 'bar'
        inst.__setstate__({'baz': 'bam'})
        self.assertEqual(inst.__dict__, {'baz': 'bam'})

    def test___reduce__(self):
        from copy_reg import __newobj__
        inst = self._makeOne()
        first, second, third = inst.__reduce__()
        self.failUnless(first is __newobj__)
        self.assertEqual(second, (self._getTargetClass(),))
        self.assertEqual(third, ())

    def test___reduce__w_subclass_having_getstate(self):
        from copy_reg import __newobj__
        class Derived(self._getTargetClass()):
            def __getstate__(self):
                return {}
        inst = Derived()
        first, second, third = inst.__reduce__()
        self.failUnless(first is __newobj__)
        self.assertEqual(second, (Derived,))
        self.assertEqual(third, {})

    def test___reduce__w_subclass_having_gna_and_getstate(self):
        from copy_reg import __newobj__
        class Derived(self._getTargetClass()):
            def __getnewargs__(self):
                return ('a', 'b')
            def __getstate__(self):
                return {'foo': 'bar'}
        inst = Derived()
        first, second, third = inst.__reduce__()
        self.failUnless(first is __newobj__)
        self.assertEqual(second, (Derived, 'a', 'b'))
        self.assertEqual(third, {'foo': 'bar'})

    def test__p_activate_from_new(self):
        inst = self._makeOne()
        inst._p_activate()
        self.assertEqual(inst._p_status, 'saved')

    def test__p_activate_from_saved(self):
        inst = self._makeOne()
        inst._p_changed = False
        inst._p_activate() # noop from 'unsaved' state
        self.assertEqual(inst._p_status, 'saved')

    def test__p_activate_from_unsaved(self):
        inst = self._makeOne()
        inst._p_changed = True
        inst._p_activate() # noop from 'saved' state
        self.assertEqual(inst._p_status, 'unsaved')

    def test__p_deactivate_from_new(self):
        inst = self._makeOne()
        inst._p_deactivate()
        self.assertEqual(inst._p_status, 'new')

    def test__p_deactivate_from_new_w_dict(self):
        class Derived(self._getTargetClass()):
            normal = 'before'
            def __init__(self):
                self.__dict__['normal'] = 'after'
        inst = Derived()
        inst._p_deactivate()
        self.assertEqual(inst._p_status, 'new')
        self.assertEqual(inst.__dict__, {'normal': 'after'})

    def test__p_deactivate_from_unsaved(self):
        inst = self._makeOne()
        inst._p_changed = True
        inst._p_deactivate()
        # can't transition 'unsaved' -> 'new'
        self.assertEqual(inst._p_status, 'unsaved')

    def test__p_deactivate_from_unsaved_w_dict(self):
        class Derived(self._getTargetClass()):
            normal = 'before'
            def __init__(self):
                self.__dict__['normal'] = 'after'
        inst = Derived()
        inst._p_changed = True
        inst._p_deactivate()
        # can't transition 'unsaved' -> 'new'
        self.assertEqual(inst._p_status, 'unsaved')
        self.assertEqual(inst.__dict__, {'normal': 'after'})

    def test__p_deactivate_from_ghost(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_deactivate()
        self.assertEqual(inst._p_status, 'ghost')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test__p_deactivate_from_saved(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_activate()
        jar._loaded = []
        inst._p_deactivate()
        self.assertEqual(inst._p_status, 'ghost')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test__p_deactivate_from_saved_w_dict(self):
        class Derived(self._getTargetClass()):
            normal = 'before'
            def __init__(self):
                self.__dict__['normal'] = 'after'
        inst, jar, OID = self._makeOneWithJar(Derived)
        inst._p_activate()
        jar._loaded = []
        inst._p_deactivate()
        self.assertEqual(inst._p_status, 'ghost')
        self.assertEqual(inst.__dict__, {})
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test__p_deactivate_from_changed(self):
        class Derived(self._getTargetClass()):
            normal = 'before'
        inst, jar, OID = self._makeOneWithJar(Derived)
        inst.normal = 'after'
        jar._loaded = []
        jar._registered = []
        inst._p_deactivate()
        # assigning None is ignored when dirty
        self.assertEqual(inst._p_status, 'changed')
        self.assertEqual(inst.__dict__, {'normal': 'after'})
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test__p_deactivate_from_changed_w_dict(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_activate()
        inst._p_changed = True
        jar._loaded = []
        jar._registered = []
        inst._p_deactivate()
        # assigning None is ignored when dirty
        self.assertEqual(inst._p_status, 'changed')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test__p_deactivate_when_sticky(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = False
        inst._p_sticky = True
        self.assertRaises(ValueError, inst._p_deactivate)

    def test__p_invalidate_from_new(self):
        inst = self._makeOne()
        inst._p_invalidate()
        self.assertEqual(inst._p_status, 'new')

    def test__p_invalidate_from_new_w_dict(self):
        class Derived(self._getTargetClass()):
            normal = 'before'
            def __init__(self):
                self.__dict__['normal'] = 'after'
        inst = Derived()
        inst._p_invalidate()
        self.assertEqual(inst._p_status, 'new')
        self.assertEqual(inst.__dict__, {})

    def test__p_invalidate_from_unsaved(self):
        inst = self._makeOne()
        inst._p_changed = True
        inst._p_invalidate()
        self.assertEqual(inst._p_status, 'new')

    def test__p_invalidate_from_unsaved_w_dict(self):
        class Derived(self._getTargetClass()):
            normal = 'before'
            def __init__(self):
                self.__dict__['normal'] = 'after'
        inst = Derived()
        inst._p_changed = True
        inst._p_invalidate()
        self.assertEqual(inst._p_status, 'new')
        self.assertEqual(inst.__dict__, {})

    def test__p_invalidate_from_ghost(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_invalidate()
        self.assertEqual(inst._p_status, 'ghost')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test__p_invalidate_from_saved(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_activate()
        jar._loaded = []
        jar._registered = []
        inst._p_invalidate()
        self.assertEqual(inst._p_status, 'ghost')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test__p_invalidate_from_saved_w_dict(self):
        class Derived(self._getTargetClass()):
            normal = 'before'
            def __init__(self):
                self.__dict__['normal'] = 'after'
        inst, jar, OID = self._makeOneWithJar(Derived)
        inst._p_activate()
        jar._loaded = []
        jar._registered = []
        inst._p_invalidate()
        self.assertEqual(inst._p_status, 'ghost')
        self.assertEqual(inst.__dict__, {})
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test__p_invalidate_from_changed(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_activate()
        inst._p_changed = True
        jar._loaded = []
        jar._registered = []
        inst._p_invalidate()
        self.assertEqual(inst._p_status, 'ghost')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test__p_invalidate_from_changed_w_dict(self):
        class Derived(self._getTargetClass()):
            normal = 'before'
            def __init__(self):
                self.__dict__['normal'] = 'after'
        inst, jar, OID = self._makeOneWithJar(Derived)
        inst._p_activate()
        inst._p_changed = True
        jar._loaded = []
        jar._registered = []
        inst._p_invalidate()
        self.assertEqual(inst._p_status, 'ghost')
        self.assertEqual(inst.__dict__, {})
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._registered), [])

    def test__p_invalidate_when_sticky(self):
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = False
        inst._p_sticky = True
        self.assertRaises(ValueError, inst._p_invalidate)

    def test__p_getattr_w__p__names(self):
        NAMES = ['_p_jar',
                 '_p_oid',
                 '_p_changed',
                 '_p_serial',
                 '_p_mtime',
                 '_p_state',
                 '_p_estimated_size',
                 '_p_sticky',
                 '_p_status',
                ]
        inst, jar, OID = self._makeOneWithJar()
        for name in NAMES:
            self.failUnless(inst._p_getattr(name))
        self.assertEqual(inst._p_status, 'ghost')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._cache._mru), [])

    def test__p_getattr_w_special_names(self):
        from persistent.pyPersistence import SPECIAL_NAMES
        inst, jar, OID = self._makeOneWithJar()
        for name in SPECIAL_NAMES:
            self.failUnless(inst._p_getattr(name))
            self.assertEqual(inst._p_status, 'ghost')
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._cache._mru), [])

    def test__p_getattr_w_normal_name(self):
        inst, jar, OID = self._makeOneWithJar()
        self.failIf(inst._p_getattr('normal'))
        self.assertEqual(inst._p_status, 'saved')
        self.assertEqual(list(jar._loaded), [OID])
        self.assertEqual(list(jar._cache._mru), [OID])

    def test__p_setattr_w__p__name(self):
        from persistent.pyPersistence import _makeOctets
        SERIAL = _makeOctets('\x01' * 8)
        inst, jar, OID = self._makeOneWithJar()
        self.failUnless(inst._p_setattr('_p_serial', SERIAL))
        self.assertEqual(inst._p_status, 'ghost')
        self.assertEqual(inst._p_serial, SERIAL)
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._cache._mru), [])

    def test__p_setattr_w_normal_name(self):
        inst, jar, OID = self._makeOneWithJar()
        self.failIf(inst._p_setattr('normal', 'value'))
        # _p_setattr doesn't do the actual write for normal names
        self.assertEqual(inst._p_status, 'saved')
        self.assertEqual(list(jar._loaded), [OID])
        self.assertEqual(list(jar._cache._mru), [OID])

    def test__p_delattr_w__p__names(self):
        NAMES = ['_p_changed',
                 '_p_serial',
                ]
        inst, jar, OID = self._makeOneWithJar()
        inst._p_changed = True
        jar._loaded = []
        for name in NAMES:
            self.failUnless(inst._p_delattr(name))
        self.assertEqual(inst._p_status, 'ghost')
        self.assertEqual(inst._p_changed, None)
        self.assertEqual(list(jar._loaded), [])
        self.assertEqual(list(jar._cache._mru), [])

    def test__p_delattr_w_normal_name(self):
        class Derived(self._getTargetClass()):
            normal = 'before'
            def __init__(self):
                self.__dict__['normal'] = 'after'
        inst, jar, OID = self._makeOneWithJar(Derived)
        self.failIf(inst._p_delattr('normal'))
        # _p_delattr doesn't do the actual delete for normal names
        self.assertEqual(inst._p_status, 'saved')
        self.assertEqual(list(jar._loaded), [OID])
        self.assertEqual(list(jar._cache._mru), [OID])
