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
"""Database objects

$Id: DB.py,v 1.12 1999/07/30 14:34:33 jim Exp $"""
__version__='$Revision: 1.12 $'[11:-2]

import cPickle, cStringIO, sys, POSException
from Connection import Connection
from bpthread import allocate_lock
from Transaction import Transaction
from referencesf import referencesf

StringType=type('')

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
        storage.registerDB(self, None)
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
            storage.store('\0\0\0\0\0\0\0\0', None, file.getvalue(), '', t)
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

        self._miv_cache={}

        # Pass through methods:
        for m in ('history',
                  'supportsUndo', 'supportsVersions', 'undoLog',
                  'versionEmpty', 'versions'):
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

    def _classFactory(self, connection, location, name,
                      _silly=('__doc__',), _globals={}):
        return getattr(__import__(location, _globals, _globals, _silly),
                       name)
            
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
        
    def _connectionMap(self, f):
        self._a()
        try:
            pools,pooll=self._pools
            for pool, allocated in pooll:
                for cc in allocated: f(cc)

            temps=self._temps
            if temps:
                t=[]
                rc=sys.getrefcount
                for cc in temps:
                    if rc(cc) > 3: f(cc)
                self._temps=t
        finally: self._r()

    def abortVersion(self, version):
        AbortVersion(self, version)

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
        CommitVersion(self, source, destination)

    def exportFile(self, oid, file=None):
        raise 'Not yet implemented'
                           
    def getCacheDeactivateAfter(self): return self._cache_deactivate_after
    def getCacheSize(self): return self._cache_size

    def getName(self): return self._storage.getName()

    def getPoolSize(self): return self._pool_size

    def getSize(self): return self._storage.getSize()

    def getVersionCacheDeactivateAfter(self):
        return self._version_cache_deactivate_after
    def getVersionCacheSize(self): return self._version_cache_size

    def getVersionPoolSize(self): return self._version_pool_size

    def importFile(self, file):
        raise 'Not yet implemented'

    def invalidate(self, oid, connection=None, version='',
                   rc=sys.getrefcount):
        """Invalidate references to a given oid.

        This is used to indicate that one of the connections has committed a
        change to the object.  The connection commiting the change should be
        passed in to prevent useless (but harmless) messages to the
        connection.
        """
        if connection is not None: version=connection._version
        self._a()
        try:

            # Update modified in version cache
            h=hash(oid)%131
            cache=self._miv_cache
            o=cache.get(h, None)
            if o and o[0]==oid: del cache[h]

            # Notify connections
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

    def invalidateMany(self, oids=None, version=''):
        if oids is None: self.invalidate(None, version=version)
        else:
            for oid in oids: self.invalidate(oid, version=version)

    def modifiedInVersion(self, oid):
        h=hash(oid)%131
        cache=self._miv_cache
        o=cache.get(h, None)
        if o and o[0]==oid:
            return o[1]
        v=self._storage.modifiedInVersion(oid)
        cache[h]=oid, v
        return v

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
        if type(version) is not StringType:
            raise POSException.Unimplemented, 'temporary versions'
        
        self._a()
        try:

            if transaction is not None:
                connections=transaction._connections
                if connections:
                    if connections.has_key(version) and not temporary:
                        return connections[version]
                else:
                    transaction._connections=connections={}
                transaction=transaction._connections
                    

            if temporary:
                # This is a temporary connection.
                # We won't bother with the pools.  This will be
                # a one-use connection.
                c=Connection(
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
                    if self._version_pool_size > len(allocated) or force:
                        c=Connection(
                            version=version,
                            cache_size=self._version_cache_size,
                            cache_deactivate_after=
                            self._version_cache_deactivate_after)
                        allocated.append(c)
                        pool.append(c)
                elif self._pool_size > len(allocated) or force:
                    c=Connection(
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
        self._storage.pack(t,referencesf)
                           
    def setCacheDeactivateAfter(self, v): self._cache_deactivate_after=v
    def setCacheSize(self, v): self._cache_size=v

    def setClassFactory(self, factory):
        self._classFactory=factory

    def setPoolSize(self, v): self._pool_size=v
    def setVersionCacheDeactivateAfter(self, v):
        self._version_cache_deactivate_after=v
    def setVersionCacheSize(self, v): self._version_cache_size=v
    def setVersionPoolSize(self, v): self._version_pool_size=v

    def cacheStatistics(self): return () # :(

    def undo(self, id):
        for oid in self._storage.undo(id):
            self.invalidate(oid)

    def versionEmpty(self, version):
        return self._storage.versionEmpty(version)

class CommitVersion:
    """An object that will see to version commit

    in cooperation with a transaction manager.
    """
    def __init__(self, db, version, dest=''):
        self._db=db
        s=db._storage
        self._version=version
        self._dest=dest
        self.tpc_abort=s.tpc_abort
        self.tpc_begin=s.tpc_begin
        self.tpc_finish=s.tpc_finish
        get_transaction().register(self)

    def abort(self, reallyme, t): pass

    def commit(self, reallyme, t):
        db=self._db
        dest=self._dest
        for oid in db._storage.commitVersion(self._version, dest, t):
            db.invalidate(oid, version=dest)
    
class AbortVersion(CommitVersion):
    """An object that will see to version abortion

    in cooperation with a transaction manager.
    """

    def commit(self, reallyme, t):
        db=self._db
        version=self._version
        for oid in db._storage.abortVersion(version, t):
            db.invalidate(oid, version=version)
