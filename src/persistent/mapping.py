##############################################################################
# 
# Copyright (c) 2001 Zope Corporation and Contributors. All Rights Reserved.
# 
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
# 
##############################################################################
__doc__='''Python implementation of persistent base types


$Id: mapping.py,v 1.15 2001/11/28 22:20:11 chrism Exp $'''
__version__='$Revision: 1.15 $'[11:-2]

import Persistence
import types

_marker=[]
class PersistentMapping(Persistence.Persistent):
    """A persistent wrapper for mapping objects.

    This class allows wrapping of mapping objects so that
    object changes are registered.  As a side effect,
    mapping objects may be subclassed.
    """

    def __init__(self,container=None):
        if container is None: container={}
        self._container=container

    def __delitem__(self, key):
        del self._container[key]
        try: del self._v_keys
        except: pass
        self.__changed__(1)

    def __getitem__(self, key):
        return self._container[key]

    def __len__(self):     return len(self._container)

    def __setitem__(self, key, v):
        self._container[key]=v
        try: del self._v_keys
        except: pass
        self.__changed__(1)

    def clear(self):
        self._container.clear()
        self._p_changed=1
        if hasattr(self,'_v_keys'): del self._v_keys

    def copy(self): return self.__class__(self._container.copy())

    def get(self, key, default=_marker):
        if default is _marker:
            return self._container.get(key)
        else:
            return self._container.get(key, default)

    def has_key(self,key): return self._container.has_key(key)

    def items(self):
        return map(lambda k, d=self: (k,d[k]), self.keys())

    def keys(self):
        try: return list(self._v_keys) # return a copy (Collector 2283)
        except: pass
        keys=self._v_keys=filter(
            lambda k: not isinstance(k,types.StringType) or k[:1]!='_',
            self._container.keys())
        keys.sort()
        return list(keys)

    def update(self, b):
        a=self._container
        for k, v in b.items(): a[k] = v
        try: del self._v_keys
        except: pass
        self._p_changed=1

    def values(self):
        return map(lambda k, d=self: d[k], self.keys())

    def __cmp__(self,other):
        return cmp(self._container, other._container)

