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
"""Transaction management

$Id: Transaction.py,v 1.7 1999/05/18 15:55:10 jim Exp $"""
__version__='$Revision: 1.7 $'[11:-2]

import time, sys, struct
from struct import pack
from string import split, strip, join

from POSException import ConflictError

class Transaction:
    'Simple transaction objects for single-threaded applications.'
    user=''
    description=''
    _connections=None
    _extension=None

    def __init__(self, id=None):
        self._id=id
        self._objects=[]
        self._append=self._objects.append

    def _init(self):
        self._objects=[]
        self._append=self._objects.append
        self.user=self.description=''
        if self._connections:
            for c in self._connections.values(): c.close()
            del self._connections

    def sub(self):
        r=self.__class__()
        r.user=self.user
        r.description=self.description
        r._extention=self._extension
        return r
        
    def __str__(self): return "%.3f\t%s" % (self._id, self.user)

    def abort(self, freeme=1):
        '''Abort the transaction.

        This is called from the application.  This means that we haven\'t
        entered two-phase commit yet, so no tpc_ messages are sent.
        '''
        t=v=tb=None
        try:
            # Abort the objects
            for o in self._objects:
                try:
                    j=getattr(o, '_p_jar', o)
                    j.abort(o, self)
                except:
                    if t is None:
                        t,v,tb=sys.exc_info()
        
            if t is not None: raise t,v,tb

        finally:
            tb=None
            if self._id is not None and freeme: free_transaction()

    def begin(self, info=None):
        '''Begin a new transaction.

        This aborts any transaction in progres.
        '''
        if self._objects: self.abort(0)
        self._init()
        if info:
            info=split(info,'\t')
            self.user=strip(info[0])
            self.description=strip(join(info[1:],'\t'))

    def commit(self):
        'Finalize the transaction'
        
        t=v=tb=None
        jars={}
        objects=self._objects
        try:
            try:
                while objects:
                    o=objects[-1]
                    j=getattr(o, '_p_jar', o)
                    i=id(j)
                    if not jars.has_key(i):
                        jars[i]=j
                        j.tpc_begin(self)
                    j.commit(o,self)
                    del objects[-1]
            except:
                t,v,tb=sys.exc_info()

                # Ugh, we got an got an error during commit, so we
                # have to clean up.

                # First, we have to abort any uncommitted objects.
                for o in objects:
                    try:
                        j=getattr(o, '_p_jar', o)
                        j.abort(o, self)
                    except: pass

                # Then, we unwind TPC for the jars that began it.
                for j in jars.values():
                    try: j.tpc_abort(self) # This should never fail
                    except: pass

                raise t,v,tb

            for j in jars.values():
                try:
                    j.tpc_finish(self) # This should never fail
                except:
                    if t is None:
                        t,v,tb=sys.exc_info()
                        
            if t is not None: raise t,v,tb

        finally:
            tb=None
            if self._id is not None: free_transaction()

    def register(self,object):
        'Register the given object for transaction control.'
        self._append(object)

    def note(self, text):
        if self.description:
            self.description = "%s\n\n%s" % (self.description, strip(text))
        else: 
            self.description = strip(text)
    
    def setUser(self, user_name, path='/'):
        self.user="%s %s" % (path, user_name)

    def setExtendedInfo(self, name, value):
        ext=self._extension
        if ext is None:
            ext=self._extension={}
        ext[name]=value


############################################################################
# install get_transaction:

try:
    import thread

except:
    _t=Transaction(None)
    def get_transaction(_t=_t): return _t
    def free_transaction(_t=_t): _t.__init__()

else:
    _t={}
    def get_transaction(_id=thread.get_ident, _t=_t):
        id=_id()
        try: t=_t[id]
        except KeyError: _t[id]=t=Transaction(id)
        return t

    def free_transaction(_id=thread.get_ident, _t=_t):
        id=_id()
        try: del _t[id]
        except KeyError: pass

    del thread

del _t

import __main__ 
__main__.__builtins__.get_transaction=get_transaction
    
