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
'''Python implementation of a persistent base types

$Id: Persistence.py,v 1.17 1998/11/11 02:00:56 jim Exp $'''
__version__='$Revision: 1.17 $'[11:-2]

_marker=[]
class Persistent:
    """\
    Persistent object support mix-in class

    When a persistent object is loaded from a database, the object's
    data is not immediately loaded.  Loading of the objects data is
    defered until an attempt is made to access an attribute of the
    object. 

    The object also tries to keep track of whether it has changed.  It
    is easy for this to be done incorrectly.  For this reason, methods
    of subclasses that change state other than by setting attributes
    should: 'self.__changed__(1)' to flag instances as changed.

    You must not override the object's '__getattr__' and '__setattr__'
    methods.  If you override the objects '__getstate__' method, then
    you must be careful not to include any attributes with names
    starting with '_p_' or '_v_' in the state.

    """ 
    _p_oid=None        # A Persistent object-id, unique within a jar
    _p_changed=0       # The object state: None=ghost, 0=normal, 1=changed
    _p_jar=None        # The last jar that this object was stored in.

    def _p_deactivate(self):
        d=self.__dict__
        oid=d['_p_oid']
        jar=d['_p_jar']
        d.clear()
        d['_p_oid']=oid
        d['_p_jar']=jar
        d['_p_changed']=None

    def __getattr__(self,key):
        'Get an item'
        if self._p_changed is None and key[:3] != '_p_':
            self._p_jar.setstate(self)
            if self.__dict__.has_key(key): return self.__dict__[key]

        raise AttributeError, key

    def __setattr__(self,key,value):
        ' '
        changed=self._p_changed
        if changed:
            self.__dict__[key]=value
            return

        k=key[:3]
        if k=='_p_' or k=='_v_':
            if key=='_p_changed':
                if changed == value: return
                if value:
                    if changed: return
                    try:
                        if self._p_jar and self._p_oid:
                            get_transaction().register(self)
                    except: pass
                elif value is None:
                    if self._p_jar and self._p_oid:
                        return self._p_deactivate()
                    return
                value=not not value
            self.__dict__[key]=value
            return

        jar=self._p_jar
        if jar is None:
            self.__dict__[key]=value
            return

        d=self.__dict__
        if changed is None:
            d['_p_changed']=1
            jar.setstate(self)

        d[key]=value
        try:
            get_transaction().register(self)
            d['_p_changed']=1
        except: pass

    def __changed__(self,v=_marker):
        if v is _marker: return not not self._p_changed
        self._p_changed = not not v

    def __getstate__(self):

        # First, update my state, if necessary:
        if self._p_changed is None:	self._p_jar.setstate(self)

        state={}
        d=self.__dict__
        for k,v in d.items():
            if k[:3] != '_p_' and k[:3] != '_v_': state[k]=v
        return state

    def __setstate__(self,state):
        self.__dict__.update(state)

    def __repr__(self):
        ' '
        return '<%s instance at %s>' % (self.__class__.__name__,
 					    hex(id(self)))

