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

$Id: Transaction.py,v 1.31 2001/10/22 20:08:33 jeremy Exp $"""
__version__='$Revision: 1.31 $'[11:-2]

import time, sys, struct, POSException
from struct import pack
from string import split, strip, join
from zLOG import LOG, ERROR, PANIC
from POSException import ConflictError

# Flag indicating whether certain errors have occurred.
hosed=0

class Transaction:
    'Simple transaction objects for single-threaded applications.'
    user=''
    description=''
    _connections=None
    _extension=None
    _sub=None # This is a subtrasaction flag

    # The _non_st_objects variable is either None or a list
    # of jars that do not support subtransactions. This is used to
    # manage non-subtransaction-supporting jars during subtransaction
    # commits and aborts to ensure that they are correctly committed
    # or aborted in the "outside" transaction.
    _non_st_objects=None
    
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
        # Create a manually managed subtransaction for internal use
        r=self.__class__()
        r.user=self.user
        r.description=self.description
        r._extension=self._extension
        return r
        
    def __str__(self): return "%.3f\t%s" % (self._id or 0, self.user)

    def __del__(self):
        if self._objects: self.abort(freeme=0)

    def abort(self, subtransaction=0, freeme=1):
        '''Abort the transaction.

        This is called from the application.  This means that we haven\'t
        entered two-phase commit yet, so no tpc_ messages are sent.
        '''

        if subtransaction and (self._non_st_objects is not None):
            raise POSException.TransactionError, (
                """Attempted to abort a sub-transaction, but a participating
                data manager doesn't support partial abort.
                """)

        t = None
        subj = self._sub
        subjars = ()

        if not subtransaction:

            # Must add in any non-subtransaction supporting objects that
            # may have been stowed away from previous subtransaction
            # commits.
            if self._non_st_objects is not None:
                self._objects.extend(self._non_st_objects)
                self._non_st_objects = None

            if subj is not None:
                # Abort of top-level transaction after commiting
                # subtransactions.
                subjars = subj.values()
                self._sub = None

        try:
            # Abort the objects
            for o in self._objects:
                try:
                    j = getattr(o, '_p_jar', o)
                    if j is not None:
                        j.abort(o, self)
                except:
                    if t is None:
                        t, v, tb = sys.exc_info()

            # Ugh, we need to abort work done in sub-transactions.
            while subjars:
                j = subjars.pop()
                j.abort_sub(self) # This should never fail
        
            if t is not None:
                raise t, v, tb

        finally:
            if t is not None:
                del tb # don't keep traceback in local variable
            del self._objects[:] # Clear registered
            if not subtransaction and freeme:
                if self._id is not None:
                    free_transaction()
            else:
                self._init()

    def begin(self, info=None, subtransaction=None):
        '''Begin a new transaction.

        This aborts any transaction in progres.
        '''
        if self._objects: self.abort(subtransaction, 0)
        if info:
            info=split(info,'\t')
            self.user=strip(info[0])
            self.description=strip(join(info[1:],'\t'))

    def commit(self, subtransaction=None):
        'Finalize the transaction'

        global hosed
        
        objects=self._objects
        jars={}
        jarsv = None
        subj=self._sub
        subjars=()

        if subtransaction:
            if subj is None: self._sub=subj={}
        else:
            if subj is not None:
                if objects:
                    # Do an implicit sub-transaction commit:
                    self.commit(1)
                    objects=[]
                subjars=subj.values()
                self._sub=None

        # If not a subtransaction, then we need to add any non-
        # subtransaction-supporting objects that may have been
        # stowed away during subtransaction commits to _objects.
        if (subtransaction is None) and (self._non_st_objects is not None):
            append=objects.append
            for object in self._non_st_objects:
                append(object)
            self._non_st_objects = None

        t=v=tb=None

        if (objects or subjars) and hosed:
            # Something really bad happened and we don't
            # trust the system state.
            raise POSException.TransactionError, (
                
                """A serious error, which was probably a system error,
                occurred in a previous database transaction.  This
                application may be in an invalid state and must be
                restarted before database updates can be allowed.

                Beware though that if the error was due to a serious
                system problem, such as a disk full condition, then
                the application may not come up until you deal with
                the system problem.  See your application log for
                information on the error that lead to this problem.
                """)

        try:

            # It's important that:
            #
            # - Every object in self._objects is either committed
            #   or aborted.
            #
            # - For each object that is committed
            #   we call tpc_begin on it's jar at least once
            #
            # - For every jar for which we've called tpc_begin on,
            #   we either call tpc_abort or tpc_finish. It is OK
            #   to call these multiple times, as the storage is
            #   required to ignore these calls if tpc_begin has not
            #   been called.
            
            ncommitted=0
            try:
                for o in objects:
                    j=getattr(o, '_p_jar', o)
                    if j is not None:
                        i=id(j)
                        if not jars.has_key(i):
                            jars[i]=j
                            if subtransaction:

                                # If a jar does not support subtransactions,
                                # we need to save it away to be committed in
                                # the outer transaction.
                                try: j.tpc_begin(self, subtransaction)
                                except TypeError:
                                    j.tpc_begin(self)

                                if hasattr(j, 'commit_sub'):
                                    subj[i]=j
                                else:
                                    if self._non_st_objects is None:
                                        self._non_st_objects = []
                                    self._non_st_objects.append(o)
                                    continue

                            else:
                                j.tpc_begin(self)
                        j.commit(o,self)
                    ncommitted=ncommitted+1

                # Commit work done in subtransactions
                while subjars:
                    j=subjars.pop()
                    i=id(j)
                    if not jars.has_key(i):
                        jars[i]=j
                    
                    j.commit_sub(self)

                jarsv = jars.values()
                for jar in jarsv:
                    if not subtransaction:
                        try: jar=jar.tpc_vote
                        except: pass
                        else: jar(self) # last chance to bail

                try:
                    # Try to finish one jar, since we may be able to
                    # recover if the first one fails.
                    if jarsv:
                        jarsv[-1].tpc_finish(self) # This should never fail
                        jarsv.pop() # It didn't, so it's taken care of.
                except:
                    # Bug if it does, we need to keep track of it
                    LOG('ZODB', ERROR,
                        "A storage error occurred in the last phase of a "
                        "two-phase commit.  This shouldn\'t happen. ",
                        error=sys.exc_info())
                    raise

                try:
                    while jarsv:
                        jarsv[-1].tpc_finish(self) # This should never fail
                        jarsv.pop() # It didn't, so it's taken care of.
                except:                        
                    # Bug if it does, we need to yell FIRE!
                    # Someone finished, so don't allow any more
                    # work without at least a restart!
                    hosed=1
                    LOG('ZODB', PANIC,
                        "A storage error occurred in the last phase of a "
                        "two-phase commit.  This shouldn\'t happen. "
                        "The application may be in a hosed state, so "
                        "transactions will not be allowed to commit "
                        "until the site/storage is reset by a restart. ",
                        error=sys.exc_info())
                    raise
                
            except:
                t,v,tb=sys.exc_info()

                # Ugh, we got an got an error during commit, so we
                # have to clean up.

                # First, we have to abort any uncommitted objects.
                for o in objects[ncommitted:]:
                    try:
                        j=getattr(o, '_p_jar', o)
                        if j is not None: j.abort(o, self)
                    except: pass

                # Then, we unwind TPC for the jars that began it.
                if jarsv is None:
                    jarsv = jars.values()
                for j in jarsv:
                    try: j.tpc_abort(self) # This should never fail
                    except:     
                        LOG('ZODB', ERROR,
                            "A storage error occured during object abort "
                            "This shouldn\'t happen. ",
                            error=sys.exc_info())
                
                # Ugh, we need to abort work done in sub-transactions.
                while subjars:
                    j=subjars.pop()
                    j.abort_sub(self) # This should never fail

                raise t,v,tb

        finally:
            tb=None
            del objects[:] # clear registered
            if not subtransaction and self._id is not None: free_transaction()

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
    def get_transaction(_id=thread.get_ident, _t=_t, get=_t.get, None=None):
        id=_id()
        t=get(id, None)
        if t is None: _t[id]=t=Transaction(id)
        return t

    def free_transaction(_id=thread.get_ident, _t=_t):
        id=_id()
        try: del _t[id]
        except KeyError: pass

    del thread

del _t

import __main__ 
__main__.__builtins__.get_transaction=get_transaction
    
