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
"""Python implementation of persistent container type

$Id: dict.py,v 1.2 2004/02/19 02:59:30 jeremy Exp $
"""

import persistent
from UserDict import IterableUserDict

__metaclass__ = type

class PersistentDict(persistent.Persistent, IterableUserDict):
    """A persistent wrapper for mapping objects.

    This class allows wrapping of mapping objects so that object
    changes are registered.  As a side effect, mapping objects may be
    subclassed.
    """

    # IterableUserDict provides all of the mapping behavior.  The
    # PersistentDict class is responsible marking the persistent
    # state as changed when a method actually changes the state.  At
    # the mapping API evolves, we may need to add more methods here.

    __super_delitem = IterableUserDict.__delitem__
    __super_setitem = IterableUserDict.__setitem__
    __super_clear = IterableUserDict.clear
    __super_update = IterableUserDict.update
    __super_setdefault = IterableUserDict.setdefault
    __super_popitem = IterableUserDict.popitem

    __super_p_init = persistent.Persistent.__init__
    __super_init = IterableUserDict.__init__

    def __init__(self, dict=None):
        self.__super_init(dict)
        self.__super_p_init()

    def __delitem__(self, key):
        self.__super_delitem(key)
        self._p_changed = True

    def __setitem__(self, key, v):
        self.__super_setitem(key, v)
        self._p_changed = True

    def clear(self):
        self.__super_clear()
        self._p_changed = True

    def update(self, b):
        self.__super_update(b)
        self._p_changed = True

    def setdefault(self, key, failobj=None):
        # We could inline all of UserDict's implementation into the
        # method here, but I'd rather not depend at all on the
        # implementation in UserDict (simple as it is).
        if not self.has_key(key):
            self._p_changed = True
        return self.__super_setdefault(key, failobj)

    def popitem(self):
        self._p_changed = True
        return self.__super_popitem()
