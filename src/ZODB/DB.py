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
"""Database objects

$Id: DB.py,v 1.2 1999/04/28 11:10:48 jim Exp $"""
__version__='$Revision: 1.2 $'[11:-2]

import cPickle, cStringIO, sys
from Connection import Connection
from bpthread import allocate_lock
from Transaction import Transaction

class DB:
    """The Object Database

    The Object database coordinates access to and interaction of one
    or more connections, which manage object spaces.  Most of the actual work
    of managing objects is done by the connections.
    """

    def __init__(self, storage,
                 pool_size=7,
                 cache_size=400,
                 cache_deactivate_after=60,
                 version_pool_size=3,
                 version_cache_size=100,
                 version_cache_deactivate_after=10,
                 ):
        """Create an object database.

        The storage for the object database must be passed in.
        Optional arguments are:

        pool_size -- The size of the pool of object spaces.

        """
        self._storage=storage
        try: storage.load('\0\0\0\0\0\0\0\0','')
        except:
            import PersistentMapping
            file=cStringIO.StringIO()
            p=cPickle.Pickler(file,1)
            p.dump((PersistentMapping.PersistentMapping,None))
            p.dump({'_container': {}})
            t=Transaction()
            t.description='initial database creation'
            storage.tpc_begin(t)
            storage.store('\0\0\0\0\0\0\0\0', file.getvalue(), '', t)
            storage.tpc_finish(t)

        # Allocate locks:
        l=allocate_lock()
        self._a=l.acquire
        self._r=l.release

        self._pools={},[]
        self._temps=[]

        self._pool_size=pool_size
        self._cache_size=cache_size
        self._cache_deactivate_after=cache_deactivate_after
        self._version_pool_size=version_pool_size
        self._version_cache_size=version_cache_size
        self._version_cache_deactivate_after=version_cache_deactivate_after

        # Pass through methods:
        for m in ('history', 'modifiedInVersion',
                  'supportsUndo', 'supportsVersions',
                  'undo', 'undoLog', 'versionEmpty'):
            setattr(self, m, getattr(storage, m))
        

    def _cacheMean(self, attr):
        m=[0,0]
        def f(con, m=m):
            t=getattr(con._cache,attr)
            m[0]=m[0]+t
            m[1]=m[1]+1

        self._connectionMap(f)
        if m[1]: m=m[0]/m[1]
        else: m=None
        return m
            
    def _closeConnection(self, connection):
        """Return a connection to the pool"""
        self._a()
        try:
            version=connection._version
            pools,pooll=self._pools
            pool, allocated, pool_lock = pools[version]
            pool.append(connection)
            if len(pool)==1:
                # Pool now usable again, unlock it.
                pool_lock.release()
        finally: self._r()
        
    def _connectionMap(f):
        self._a()
        try:
            pools,pooll=self._pools
            for pool, allocated in pooll:
                for cc in allocated: f(cc)

            temps=self._temps
            if temps:
                t=[]
                for cc in temps:
                    if rc(cc) > 3: f(cc)
                self._temps=t
        finally: self._r()

    def abortVersion(self, version):
        raise 'Not Yet Implemented'

    def cacheDetail(self):
        """Return information on objects in the various caches

        Organized by class."""

        detail={}
        def f(con,detail=detail,have_detail=detail.has_key):
            for oid, ob in con._cache.items():
                c="%s.%s" % (ob.__class__.__module__, ob.__class__.__name__)
                if have_detail(c): detail[c]=detail[c]+1
                else: detail[c]=1
        
        self._connectionMap(f)
        detail=detail.items()
        detail.sort()
        return detail

    def cacheExtremeDetail(self):
        detail=[]
        def f(con, detail=detail, rc=sys.getrefcount):
            for oid, ob in con._cache.items():
                id=oid
                if hasattr(ob,'__dict__'):
                    d=ob.__dict__
                    if d.has_key('id'):
                        id="%s (%s)" % (oid, d['id'])
                    elif d.has_key('__name__'):
                        id="%s (%s)" % (oid, d['__name__'])
    
                detail.append({
                    'oid': id,
                    'klass': "%s.%s" % (ob.__class__.__module__,
                                        ob.__class__.__name__),
                    'rc': rc(ob)-4,
                    'references': con.references(oid),
                    })

        self._connectionMap(f)
        return detail

    def cacheFullSweep(self, value):
        self._connectionMap(lambda c, v=value: c._cache.full_sweep(v))

    def cacheLastGCTime(self):
        m=[0]
        def f(con, m=m):
            t=con._cache.cache_last_gc_time
            if t > m[0]: m[0]=t

        self._connectionMap(f)
        return m[0]

    def cacheMinimize(self, value):
        self._connectionMap(lambda c, v=value: c._cache.minimize(v))

    def cacheMeanAge(self): return self._cacheMean('cache_mean_age')
    def cacheMeanDeac(self): return self._cacheMean('cache_mean_deac')
    def cacheMeanDeal(self): return self._cacheMean('cache_mean_deal')

    def cacheSize(self):
        m=[0]
        def f(con, m=m):
            m[0]=m[0]+len(con._cache)

        self._connectionMap(f)
        return m[0]

    def close(self): self._storage.close()

    def commitVersion(self, source, destination=''):
        raise 'Not yet implemented'

    def exportFile(self, oid, file=None):
        raise 'Not yet implemented'

    def getName(self): return self._storage.getName()

    def getSize(self): return self._storage.getSize()

    def importFile(self, file):
        raise 'Not yet implemented'

    def invalidate(self, oid, connection=None, rc=sys.getrefcount):
        """Invalidate references to a given oid.

        This is used to indicate that one of the connections has committed a
        change to the object.  The connection commiting the change should be
        passed in to prevent useless (but harmless) messages to the
        connection.
        """
        if connection is not None: version=connection._version
        else: version=''
        self._a()
        try:
            pools,pooll=self._pools
            for pool, allocated in pooll:
                for cc in allocated:
                    if (cc is not connection and
                        (not version or cc._version==version)):
                        if rc(cc) <= 3:
                            cc.close()
                        cc.invalidate(oid)

            temps=self._temps
            if temps:
                t=[]
                for cc in temps:
                    if rc(cc) > 3:
                        if cc is not connection and cc._version==version:
                            cc.invalidate(oid)
                        t.append(cc)
                    else: cc.close()
                self._temps=t
        finally: self._r()

    def objectCount(self): return len(self._storage)
        
    def open(self, version='', transaction=None, temporary=0, force=None,
             waitflag=1):
        """Return a object space (AKA connection) to work in

        The optional version argument can be used to specify that a
        version connection is desired.

        The optional transaction argument can be provided to cause the
        connection to be automatically closed when a transaction is
        terminated.  In addition, connections per transaction are
        reused, if possible.

        Note that the connection pool is managed as a stack, to increate the
        likelihood that the connection's stack will include useful objects.
        """
        self._a()
        try:

            if transaction is not None:
                connections=transaction._connections
                if connections:
                    if connection.has_key(version) and not temporary:
                        return connections[version]
                else:
                    transaction._connections=connections={}
                transaction=transaction._connections
                    

            if temporary:
                # This is a temporary connection.
                # We won't bother with the pools.  This will be
                # a one-use connection.
                c=Connection(
                    storage=self._storage,
                    version=version,
                    cache_size=self._version_cache_size,
                    cache_deactivate_after=
                    self._version_cache_deactivate_after)
                c._setDB(self)
                self._temps.append(c)
                if transaction is not None: transaction[id(c)]=c
                return c


            pools,pooll=self._pools
            if pools.has_key(version):
                pool, allocated, pool_lock = pools[version]
            else:
                pool, allocated, pool_lock = pools[version] = (
                    [], [], allocate_lock())
                pooll.append((pool, allocated))
                pool_lock.acquire()


            if not pool:
                c=None
                if version:
                    if self._version_pool_size < len(allocated) or force:
                        c=Connection(
                            storage=self._storage,
                            version=version,
                            cache_size=self._version_cache_size,
                            cache_deactivate_after=
                            self._version_cache_deactivate_after)
                        allocated.append(c)
                        pool.append(c)
                elif self._pool_size > len(allocated) or force:
                    c=Connection(
                        storage=self._storage,
                        version=version,
                        cache_size=self._cache_size,
                        cache_deactivate_after=
                        self._cache_deactivate_after)
                    allocated.append(c)
                    pool.append(c)
                    
                if c is None:
                    if waitflag:
                        self._r()
                        pool_lock.acquire()
                        self._a()
                    else: return

            elif len(pool)==1:
                # Taking last one, lock the pool
                # We know that the pool lock is not set.
                pool_lock.acquire()

            c=pool[-1]
            del pool[-1]
            c._setDB(self)
            for pool, allocated in pooll:
                for cc in pool:
                    cc._incrgc()

            if transaction is not None: transaction[version]=c
            return c

        finally: self._r()
        
    def pack(self, t):
        self._storage.pack(t,referencesf,-1)
                           
    def setCacheDeactivateAfter(self, v): self._cache_deactivate_after=v
    def setCacheSize(self, v): self._cache_size=v
    def setPoolSize(self, v): self._pool_size=v
    def setVersionCacheDeactivateAfter(self, v):
        self._version_cache_deactivate_after=v
    def setVersionCacheSize(self, v): self._version_cache_size=v
    def setVersionPoolSize(self, v): self._version_pool_size=v
                           
    def getCacheDeactivateAfter(self): return self._cache_deactivate_after
    def getCacheSize(self): return self._cache_size
    def getPoolSize(self): return self._pool_size
    def getVersionCacheDeactivateAfter(self): return
        self._version_cache_deactivate_after
    def getVersionCacheSize(self): return self._version_cache_size
    def getVersionPoolSize(self): return self._version_pool_size

    def cacheStatistics(self): return () # :(

    def versionEmpty(self, version):
        return self._storage.versionEmpty(version)

def referencesf(p,rootl,
                Unpickler=cPickle.Unpickler,
                StringIO=cStringIO.StringIO):
    u=Unpickler(StringIO(p))
    u.persistent_load=rootl
    u.noload()
    try: u.noload()
    except:
        # Hm.  We failed to do second load.  Maybe there wasn't a
        # second pickle.  Let's check:
        f=StringIO(p)
        u=Unpickler(f)
        u.persistent_load=[]
        u.noload()
        if len(p) > f.tell(): raise ValueError, 'Error unpickling, %s' % p
    
    
