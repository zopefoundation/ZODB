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
# FOR A PARTICULAR PURPOSE
#
##############################################################################

"""Python implementation of persistent list.

$Id: list.py,v 1.6 2004/02/19 02:59:30 jeremy Exp $"""

__version__='$Revision: 1.6 $'[11:-2]

import persistent
from UserList import UserList

class PersistentList(UserList, persistent.Persistent):
    __super_setitem = UserList.__setitem__
    __super_delitem = UserList.__delitem__
    __super_setslice = UserList.__setslice__
    __super_delslice = UserList.__delslice__
    __super_iadd = UserList.__iadd__
    __super_imul = UserList.__imul__
    __super_append = UserList.append
    __super_insert = UserList.insert
    __super_pop = UserList.pop
    __super_remove = UserList.remove
    __super_reverse = UserList.reverse
    __super_sort = UserList.sort
    __super_extend = UserList.extend

    def __setitem__(self, i, item):
        self.__super_setitem(i, item)
        self._p_changed = 1

    def __delitem__(self, i):
        self.__super_delitem(i)
        self._p_changed = 1

    def __setslice__(self, i, j, other):
        self.__super_setslice(i, j, other)
        self._p_changed = 1

    def __delslice__(self, i, j):
        self.__super_delslice(i, j)
        self._p_changed = 1

    def __iadd__(self, other):
        L = self.__super_iadd(other)
        self._p_changed = 1
        return L

    def __imul__(self, n):
        L = self.__super_imul(n)
        self._p_changed = 1
        return L

    def append(self, item):
        self.__super_append(item)
        self._p_changed = 1

    def insert(self, i, item):
        self.__super_insert(i, item)
        self._p_changed = 1

    def pop(self, i=-1):
        rtn = self.__super_pop(i)
        self._p_changed = 1
        return rtn

    def remove(self, item):
        self.__super_remove(item)
        self._p_changed = 1

    def reverse(self):
        self.__super_reverse()
        self._p_changed = 1

    def sort(self, *args):
        self.__super_sort(*args)
        self._p_changed = 1

    def extend(self, other):
        self.__super_extend(other)
        self._p_changed = 1

    # This works around a bug in Python 2.1.x (up to 2.1.2 at least) where the
    # __cmp__ bogusly raises a RuntimeError, and because this is an extension
    # class, none of the rich comparison stuff works anyway.
    def __cmp__(self, other):
        return cmp(self.data, self._UserList__cast(other))
