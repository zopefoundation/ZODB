##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################

"""Python implementation of persistent base types

$Id$"""

import persistent
import UserDict

class default(object):

    def __init__(self, func):
        self.func = func

    def __get__(self, inst, class_):
        if inst is None:
            return self
        return self.func(inst)


class PersistentMapping(UserDict.IterableUserDict, persistent.Persistent):
    """A persistent wrapper for mapping objects.

    This class allows wrapping of mapping objects so that object
    changes are registered.  As a side effect, mapping objects may be
    subclassed.

    A subclass of PersistentMapping or any code that adds new
    attributes should not create an attribute named _container.  This
    is reserved for backwards compatibility reasons.
    """

    # UserDict provides all of the mapping behavior.  The
    # PersistentMapping class is responsible marking the persistent
    # state as changed when a method actually changes the state.  At
    # the mapping API evolves, we may need to add more methods here.

    __super_delitem = UserDict.IterableUserDict.__delitem__
    __super_setitem = UserDict.IterableUserDict.__setitem__
    __super_clear = UserDict.IterableUserDict.clear
    __super_update = UserDict.IterableUserDict.update
    __super_setdefault = UserDict.IterableUserDict.setdefault
    __super_pop = UserDict.IterableUserDict.pop
    __super_popitem = UserDict.IterableUserDict.popitem

    def __delitem__(self, key):
        self.__super_delitem(key)
        self._p_changed = 1

    def __setitem__(self, key, v):
        self.__super_setitem(key, v)
        self._p_changed = 1

    def clear(self):
        self.__super_clear()
        self._p_changed = 1

    def update(self, b):
        self.__super_update(b)
        self._p_changed = 1

    def setdefault(self, key, failobj=None):
        # We could inline all of UserDict's implementation into the
        # method here, but I'd rather not depend at all on the
        # implementation in UserDict (simple as it is).
        if not self.has_key(key):
            self._p_changed = 1
        return self.__super_setdefault(key, failobj)

    def pop(self, key, *args):
        self._p_changed = 1
        return self.__super_pop(key, *args)

    def popitem(self):
        self._p_changed = 1
        return self.__super_popitem()

    # Old implementations used _container rather than data.
    # Use a descriptor to provide data when we have _container instead

    @default
    def data(self):
        # We don't want to cause a write on read, so wer're careful not to
        # do anything that would cause us to become marked as changed, however,
        # if we're modified, then the saved record will have data, not
        # _container.
        data = self.__dict__.pop('_container')
        self.__dict__['data'] = data

        return data
