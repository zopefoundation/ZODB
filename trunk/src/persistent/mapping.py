# Copyright (c) 2001 Zope Corporation and Contributors.  All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 1.1 (ZPL).  A copy of the ZPL should accompany this
# distribution.  THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL
# EXPRESS OR IMPLIED WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST
# INFRINGEMENT, AND FITNESS FOR A PARTICULAR PURPOSE.

"""Python implementation of persistent base types

$Id: mapping.py,v 1.16 2001/11/28 23:59:02 jeremy Exp $"""

__version__='$Revision: 1.16 $'[11:-2]

import Persistence
from UserDict import UserDict

class PersistentMapping(UserDict, Persistence.Persistent):
    """A persistent wrapper for mapping objects.

    This class allows wrapping of mapping objects so that object
    changes are registered.  As a side effect, mapping objects may be
    subclassed.
    """

    __super_delitem = UserDict.__delitem__
    __super_setitem = UserDict.__setitem__
    __super_clear = UserDict.clear
    __super_update = UserDict.update
    __super_setdefault = UserDict.setdefault
    __super_popitem = UserDict.popitem

    def __setstate__(self, state):
        # The old PersistentMapping used _container to hold the data.
        # We need to make the current code work with objects pickled
        # using the old code.  Unfortunately, this forces us to expose
        # the rep of UserDict, because __init__() won't be called when
        # a pickled object is being loaded.
        if state.has_key('_container'):
            assert not state.has_key('data'), \
                   ("object state has _container and data attributes: %s"
                    % repr(state))
            self.data = state['_container']
            del state['_container']
        for k, v in state.items():
            self.__dict__[k] = v

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

    def popitem(self):
        self._p_changed = 1
        return self.__super_popitem()
