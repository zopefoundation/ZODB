##############################################################################
#
# Copyright (c) 1996-1998, Digital Creations, Fredericksburg, VA, USA.
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
#   o Redistributions of source code must retain the above copyright
#     notice, this list of conditions, and the disclaimer that follows.
# 
#   o Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions, and the following disclaimer in
#     the documentation and/or other materials provided with the
#     distribution.
# 
#   o Neither the name of Digital Creations nor the names of its
#     contributors may be used to endorse or promote products derived
#     from this software without specific prior written permission.
# 
# 
# THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS AND CONTRIBUTORS *AS IS*
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
# TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
# PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL
# CREATIONS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
# OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
# TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
# USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
# DAMAGE.
#
# 
# If you have questions regarding this software, contact:
#
#   Digital Creations, L.C.
#   910 Princess Ann Street
#   Fredericksburge, Virginia  22401
#
#   info@digicool.com
#
#   (540) 371-6909
#
##############################################################################
__doc__='''Python implementation of persistent base types


$Id: PersistentMapping.py,v 1.3 1998/11/11 02:00:56 jim Exp $'''
__version__='$Revision: 1.3 $'[11:-2]

import Persistence

class PM(Persistence.Persistent):
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

    def get(self, key, default): return self._container.get(key, default)

    def has_key(self,key): return self._container.has_key(key)

    def items(self):
        return map(lambda k, d=self: (k,d[k]), self.keys())

    def keys(self):
        try: return self._v_keys
        except: pass
        keys=self._v_keys=filter(
            lambda k: k[:1]!='_',
            self._container.keys())
        keys.sort()
        return keys

    def update(self, b):
        a=self._container
        for k, v in b.items(): a[k] = v
        self._p_changed=1

    def values(self):
        return map(lambda k, d=self: d[k], self.keys())

PersistentMapping=PM
