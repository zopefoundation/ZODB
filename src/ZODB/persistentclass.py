##############################################################################
#
# Copyright (c) 2004 Zope Foundation and Contributors.
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
"""Persistent Class Support

$Id$
"""


# Notes:
#
# Persistent classes are non-ghostable.  This has some interesting
# ramifications:
#
# - When an object is invalidated, it must reload its state
#
# - When an object is loaded from the database, its state must be
#   loaded.  Unfortunately, there isn't a clear signal when an object is
#   loaded from the database.  This should probably be fixed.
#
#   In the mean time, we need to infer.  This should be viewed as a
#   short term hack.
#
#   Here's the strategy we'll use:
#
#   - We'll have a need to be loaded flag that we'll set in
#     __new__, through an extra argument.
#
#   - When setting _p_oid and _p_jar, if both are set and we need to be
#     loaded, then we'll load out state.
#
#   - We'll use _p_changed is None to indicate that we're in this state.
#

class _p_DataDescr(object):
    # Descr used as base for _p_ data. Data are stored in
    # _p_class_dict.

    def __init__(self, name):
        self.__name__ = name

    def __get__(self, inst, cls):
        if inst is None:
            return self

        if '__global_persistent_class_not_stored_in_DB__' in inst.__dict__:
            raise AttributeError(self.__name__)
        return inst._p_class_dict.get(self.__name__)

    def __set__(self, inst, v):
        inst._p_class_dict[self.__name__] = v

    def __delete__(self, inst):
        raise AttributeError(self.__name__)


class _p_oid_or_jar_Descr(_p_DataDescr):
    # Special descr for _p_oid and _p_jar that loads
    # state when set if both are set and _p_changed is None
    #
    # See notes above

    def __set__(self, inst, v):
        get = inst._p_class_dict.get
        if v == get(self.__name__):
            return

        inst._p_class_dict[self.__name__] = v

        jar = get('_p_jar')
        if (jar is not None
                and get('_p_oid') is not None
                and get('_p_changed') is None):
            jar.setstate(inst)


class _p_ChangedDescr(object):
    # descriptor to handle special weird semantics of _p_changed

    def __get__(self, inst, cls):
        if inst is None:
            return self
        return inst._p_class_dict['_p_changed']

    def __set__(self, inst, v):
        if v is None:
            return
        inst._p_class_dict['_p_changed'] = bool(v)

    def __delete__(self, inst):
        inst._p_invalidate()


class _p_MethodDescr(object):
    """Provide unassignable class attributes
    """

    def __init__(self, func):
        self.func = func

    def __get__(self, inst, cls):
        if inst is None:
            return cls
        return self.func.__get__(inst, cls)

    def __set__(self, inst, v):
        raise AttributeError(self.__name__)

    def __delete__(self, inst):
        raise AttributeError(self.__name__)


special_class_descrs = '__dict__', '__weakref__'


class PersistentMetaClass(type):

    _p_jar = _p_oid_or_jar_Descr('_p_jar')
    _p_oid = _p_oid_or_jar_Descr('_p_oid')
    _p_changed = _p_ChangedDescr()
    _p_serial = _p_DataDescr('_p_serial')

    def __new__(self, name, bases, cdict, _p_changed=False):
        cdict = dict([(k, v) for (k, v) in cdict.items()
                      if not k.startswith('_p_')])
        cdict['_p_class_dict'] = {'_p_changed': _p_changed}
        return super(PersistentMetaClass, self).__new__(
            self, name, bases, cdict)

    def __getnewargs__(self):
        return self.__name__, self.__bases__, {}, None

    __getnewargs__ = _p_MethodDescr(__getnewargs__)

    def _p_maybeupdate(self, name):
        get = self._p_class_dict.get
        data_manager = get('_p_jar')

        if (
            (data_manager is not None)
            and
            (get('_p_oid') is not None)
            and
            (get('_p_changed') is False)
        ):

            self._p_changed = True
            data_manager.register(self)

    def __setattr__(self, name, v):
        if not ((name.startswith('_p_') or name.startswith('_v'))):
            self._p_maybeupdate(name)
        super(PersistentMetaClass, self).__setattr__(name, v)

    def __delattr__(self, name):
        if not ((name.startswith('_p_') or name.startswith('_v'))):
            self._p_maybeupdate(name)
        super(PersistentMetaClass, self).__delattr__(name)

    def _p_deactivate(self):
        # persistent classes can't be ghosts
        pass

    _p_deactivate = _p_MethodDescr(_p_deactivate)

    def _p_invalidate(self):
        # reset state
        self._p_class_dict['_p_changed'] = None
        self._p_jar.setstate(self)

    _p_invalidate = _p_MethodDescr(_p_invalidate)

    def __getstate__(self):
        return (self.__bases__,
                dict([(k, v) for (k, v) in self.__dict__.items()
                      if not (k.startswith('_p_')
                              or k.startswith('_v_')
                              or k in special_class_descrs
                              )
                      ]),
                )

    __getstate__ = _p_MethodDescr(__getstate__)

    def __setstate__(self, state):
        bases, cdict = state
        if self.__bases__ != bases:
            # __getnewargs__ should've taken care of that
            raise AssertionError(self.__bases__, '!=', bases)
        cdict = dict([(k, v) for (k, v) in cdict.items()
                      if not k.startswith('_p_')])

        _p_class_dict = self._p_class_dict
        self._p_class_dict = {}

        to_remove = [k for k in self.__dict__
                     if ((k not in cdict)
                         and
                         (k not in special_class_descrs)
                         and
                         (k != '_p_class_dict')
                         )]

        for k in to_remove:
            delattr(self, k)

        for k, v in cdict.items():
            setattr(self, k, v)

        self._p_class_dict = _p_class_dict

        self._p_changed = False

    __setstate__ = _p_MethodDescr(__setstate__)

    def _p_activate(self):
        self._p_jar.setstate(self)

    _p_activate = _p_MethodDescr(_p_activate)
