##############################################################################
# 
# Zope Public License (ZPL) Version 1.0
# -------------------------------------
# 
# Copyright (c) Digital Creations.  All rights reserved.
# 
# This license has been certified as Open Source(tm).
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
# 1. Redistributions in source code must retain the above copyright
#    notice, this list of conditions, and the following disclaimer.
# 
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions, and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
# 
# 3. Digital Creations requests that attribution be given to Zope
#    in any manner possible. Zope includes a "Powered by Zope"
#    button that is installed by default. While it is not a license
#    violation to remove this button, it is requested that the
#    attribution remain. A significant investment has been put
#    into Zope, and this effort will continue if the Zope community
#    continues to grow. This is one way to assure that growth.
# 
# 4. All advertising materials and documentation mentioning
#    features derived from or use of this software must display
#    the following acknowledgement:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    In the event that the product being advertised includes an
#    intact Zope distribution (with copyright and license included)
#    then this clause is waived.
# 
# 5. Names associated with Zope or Digital Creations must not be used to
#    endorse or promote products derived from this software without
#    prior written permission from Digital Creations.
# 
# 6. Modified redistributions of any form whatsoever must retain
#    the following acknowledgment:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    Intact (re-)distributions of any official Zope release do not
#    require an external acknowledgement.
# 
# 7. Modifications are encouraged but must be packaged separately as
#    patches to official Zope releases.  Distributions that do not
#    clearly separate the patches from the original work must be clearly
#    labeled as unofficial distributions.  Modifications which do not
#    carry the name Zope may be packaged in any form, as long as they
#    conform to all of the clauses above.
# 
# 
# Disclaimer
# 
#   THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS ``AS IS'' AND ANY
#   EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
#   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL CREATIONS OR ITS
#   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
#   USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#   ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#   OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
#   OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
#   SUCH DAMAGE.
# 
# 
# This software consists of contributions made by Digital Creations and
# many individuals on behalf of Digital Creations.  Specific
# attributions are listed in the accompanying credits file.
# 
##############################################################################
__doc__='''Python implementation of persistent base types


$Id: PersistentMapping.py,v 1.9 2001/06/05 18:45:33 chrism Exp $'''
__version__='$Revision: 1.9 $'[11:-2]

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
        return keys

    def update(self, b):
        a=self._container
        for k, v in b.items(): a[k] = v
        try: del self._v_keys
        except: pass
        self._p_changed=1

    def values(self):
        return map(lambda k, d=self: d[k], self.keys())

