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
"""Transaction management

$Id: Transaction.py,v 1.3 1998/11/11 02:00:56 jim Exp $"""
__version__='$Revision: 1.3 $'[11:-2]

import time, sys, struct
from struct import pack
from string import split, strip, join

ConflictError=""

class Transaction:
    'Simple transaction objects for single-threaded applications.'
    user=''
    description=''
    _connections=None

    def __init__(self,
                 time=time.time, pack=struct.pack, gmtime=time.gmtime):
        self._objects=[]
        self._append=self._objects.append
        self.time=now=time()
        y,mo,d,h,m=gmtime(now)[:5]
        s=int((now%60)*1000000)
        self.id=pack("<II", (((y*12+mo)*31+d)*24+h)*60+m, s)
        self._note=self._user=self._description=''
        if self._connections:
            for c in self._connections.values(): c.close()
            del self._connections
        
    def __str__(self): return "%.3f\t%s" % (self.time,self._note)

    def abort(self, freeme=1):
        'Abort the transaction.'
        t=v=tb=None
        try:
            for o in self._objects:
                try:
                    if hasattr(o,'_p_jar'): o=o._p_jar
                    if hasattr(o,'tpc_abort'): o.tpc_abort(self)
                except: t,v,tb=sys.exc_info()
            if t is not None: raise t,v,tb
        finally:
            tb=None
            if freeme: free_transaction()

    def begin(self, info=None):
        '''Begin a new transaction.

        This aborts any transaction in progres.
        '''
        if self._objects: self._abort(0)
        self.__init__()
        if info:
            info=split(info,'\t')
            self.user=strip(info[0])
            self.description=strip(join(info,'\t'))

    def commit(self):
        'Finalize the transaction'
        
        t=v=tb=None
        try:
            try:
                for o in self._objects:
                    if hasattr(o,'_p_jar'):
                        j=o._p_jar
                        j.tpc_begin(self)
                        j.commit(o,self)
                    elif hasattr(o,'tpc_begin'):
                        o.tpc_begin(self)
            except:
                t,v,tb=sys.exc_info()
                self.abort()
                raise t,v,tb

            for o in self._objects:
                try:
                    if hasattr(o,'_p_jar'): o=o._p_jar
                    if hasattr(o,'tpc_finish'): o.tpc_finish(self)
                except: t,v,tb=sys.exc_info()
            if t is not None: raise t,v,tb

        finally:
            tb=None
            free_transaction()

    def register(self,object):
        'Register the given object for transaction control.'
        self._append(object)

    def remark(self, text):
        if self.description:
            self.description = "%s\n\n%s" % (self.description, strip(text))
        else: 
            self.description = strip(text)
        
    def setUser(self, user_name, path='/'):
        self.user="%s %s" % (path, user_name)
        


############################################################################
# install get_transaction:

try:
    import thread
    _t={}
    def get_transaction(_id=thread.get_ident, _t=_t):
        id=_id()
        try: t=_t[id]
        except KeyError: _t[id]=t=Transaction()
        return t

    def free_transaction(_id=thread.get_ident, _t=_t):
        id=_id()
        try: del _t[id]
        except KeyError: pass

    del thread

except:
    _t=Transaction()
    def get_transaction(_t=_t): return _t
    def free_transaction(_t=_t): _t.__init__()

del _t

import __main__ 
__main__.__builtins__.get_transaction=get_transaction

def time2id(now, gmtime=time.gmtime, pack=struct.pack):
    y,m,d,h,m=gmtime(now)[:5]
    s=int((now%60)*1000000)
    return pack("<II", ((y*12+m)*31+d)*24, s)
    
