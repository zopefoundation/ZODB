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
"""Database connection support

$Id: Connection.py,v 1.1 1998/11/11 02:00:55 jim Exp $"""
__version__='$Revision: 1.1 $'[11:-2]

from PickleCache import PickleCache
from bpthread import allocate_lock
from POSException import ConflictError
from cStringIO import StringIO
from cPickle import Unpickler, Pickler

class HelperClass: pass
ClassType=type(HelperClass)

class Connection:
    """Object managers for individual object space.

    An object space is a version of collection of objects.  In a
    multi-threaded application, each thread get's it's own object
    space.

    The Connection manages movement of objects in and out of object storage.
    """

    def __init__(self, storage, version='', cache_size=400,
                 cache_deactivate_after=60):
        """Create a new Connection"""
        self._storage=storage
        self.new_oid=storage.new_oid
        self._version=version
        self._cache=cache=PickleCache(cache_size, cache_deactivate_after)
        self._incrgc=cache.incrgc
        self._invalidated={}
        lock=allocate_lock()
        self._a=lock.acquire
        self._r=lock.release

    def _breakcr(self):
        try: del self._cache
        except: pass
        try: del self._incrgc
        except: pass

    def __getitem__(self, oid,
                    tt=type(()), ct=type(HelperClass)):
        cache=self._cache
        if cache.has_key(oid): return cache[oid]

        __traceback_info__=oid
        p=self._storage.load(oid, self._version)
        file=StringIO(p)
        unpickler=Unpickler(file)
        unpickler.persistent_load=self._persistent_load

        object = unpickler.load()

        if type(object) is tt:
            klass, args = object
            if (args is None or
                not args and not hasattr(klass,'__getinitargs__')):
                if type(klass) is ct:
                    object=HelperClass()
                    object.__class__=klass
                else: object=klass.__basicnew__()
            else:
                object=apply(klass,args)
                object.__dict__.clear()
        else:
            object.__dict__.clear()
            klass=object.__class__


        if type(klass) is ct:
            d=object.__dict__
            d['_p_oid']=oid
            d['_p_jar']=self
            d['_p_changed']=None
        else:
            object._p_oid=oid
            object._p_jar=self
            object._p_changed=None

        cache[oid]=object
        return object

    def _persistent_load(self,oid,
                        d={'__builtins__':{}},
                        tt=type(()), st=type(''), ct=type(HelperClass)):

        __traceback_info__=oid

        cache=self._cache

        if type(oid) is tt:
            # Quick instance reference.  We know all we need to know
            # to create the instance wo hitting the db, so go for it!
            oid, klass = oid
            if cache.has_key(oid): return cache[oid]
            if type(klass) is ct:
                object=HelperClass()
                object.__class__=klass
                d=object.__dict__
                d['_p_oid']=oid
                d['_p_jar']=self
                d['_p_changed']=None
            else:
                object=klass.__basicnew__()
                object._p_oid=oid
                object._p_jar=self
                object._p_changed=None
            
            cache[oid]=object
            return object
                
        if type(oid) is st: oid=atoi(oid)

        if cache.has_key(oid): return cache[oid]
        object=cache[oid]=self[oid]
        return object

    def _planToStore(self,object,stackp):
        oid=object._p_oid
        if oid is None or object._p_jar is not self:
            oid = self.new_oid()
            object._p_jar=self
            object._p_oid=oid
            stackp(object)
        elif object._p_changed:
            stackp(object)
        return oid

    def _setDB(self, odb=None):
        """Begin a new transaction.

        Any objects modified since the last transaction are invalidated.
        """     
        self._db=odb
        cache=self._cache
        for oid in self._invalidated.keys():
            if cache.has_key(oid):
                cache[oid]._p_deactivate()
        self._invalidated.clear()

        return self

    def close(self):
        self._incrgc()
        self._db._closeConnection(self)
        del self._db

    def commit(self, object, transaction):
        oid=object._p_oid
        if self._invalidated.has_key(oid): raise ConflictError, oid
        self._invalidating.append(oid)
        plan=self._planToStore
        stack=[]
        stackup=stack.append
        topoid=plan(object,stackup)
        version=self._version
        if stack:
            # Create a special persistent_id that passes T and the subobject
            # stack along:
            def persistent_id(object,self=self,stackup=stackup):
                if (not hasattr(object, '_p_oid') or
                    type(object) is ClassType): return None

                oid=object._p_oid

                if oid is None or object._p_jar is not self:
                    oid = self.new_oid()
                    object._p_jar=self
                    object._p_oid=oid
                    stackup(object)

                if hasattr(object.__class__, '__getinitargs__'): return oid
                return oid, object.__class__

            file=StringIO()
            seek=file.seek
            pickler=Pickler(file,1)
            pickler.persistent_id=persistent_id
            dbstore=self._storage.store
            file=file.getvalue
            cache=self._cache
            dump=pickler.dump
            clear_memo=pickler.clear_memo

            while stack:
                object=stack[-1]
                del stack[-1]
                oid=object._p_oid
                if self._invalidated.has_key(oid): raise ConflictError, oid
                cls = object.__class__
                if hasattr(cls, '__getinitargs__'):
                    args = object.__getinitargs__()
                    len(args) # XXX Assert it's a sequence
                else:
                    args = None # New no-constructor protocol!
                seek(0)
                clear_memo()
                dump((cls,args))
                state=object.__getstate__()
                dump(state)
                p=file()
                dbstore(oid,p,version,transaction)
                object._p_changed=0
                cache[oid]=object

        return topoid

    def commitVersion(self, destination=''):
        raise 'Not Implemented Yet!'

    def db(self): return self._db

    def getVersion(self): return self._version
        
    def invalidate(self, oid):
        """Invalidate a particular oid

        This marks the oid as invalid, but doesn't actually invalidate
        it.  The object data will be actually invalidated at certain
        transaction boundaries.
        """
        self._a()
        self._invalidated[oid]=1
        self._r()

    def modifiedInVersion(self, o):
        return self._db.modifiedInVersion(o._p_oid)

    def root(self): return self['\0\0\0\0\0\0\0\0']

    def setstate(self,object):
        # Note, we no longer mess with the object's state
        # flag, _p_changed.  This is the object's job.
        oid=object._p_oid
        self._a()
        if self._invalidated.has_key(oid):
            self._r()
            raise ConflictError, oid
        self._r()
        p=self._storage.load(oid, self._version)
        file=StringIO(p)
        unpickler=Unpickler(file)
        unpickler.persistent_load=self._persistent_load
        unpickler.load()
        state = unpickler.load()
        if hasattr(object, '__setstate__'):
            object.__setstate__(state)
        else:
            d=object.__dict__
            for k,v in state.items(): d[k]=v

    def tpc_abort(self, transaction):
        self._storage.tpc_abort(transaction)
        cache=self._cache
        invalidated=self._invalidated
        for oid in invalidated.keys():
            if cache.has_key(oid):
                cache[oid]._p_deactivate()
        invalidated.clear()
        for oid in self._invalidating:
            cache[oid]._p_deactivate()

    def tpc_begin(self, transaction):
        self._invalidating=[]
        self._storage.tpc_begin(transaction)

    def tpc_finish(self, transaction):
        self._storage.tpc_finish(transaction, self.tpc_finish_)
        cache=self._cache
        invalidated=self._invalidated
        for oid in invalidated.keys():
            if cache.has_key(oid):
                cache[oid]._p_deactivate()
        invalidated.clear()

    def tpc_finish_(self):
        invalidate=self._db.invalidate
        for oid in self._invalidating: invalidate(oid, self)

class tConnection(Connection):

    def close(self):
        self._breakcr()
