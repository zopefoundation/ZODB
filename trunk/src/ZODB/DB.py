##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Database objects

$Id: DB.py,v 1.53 2003/06/24 21:50:18 jeremy Exp $"""
__version__='$Revision: 1.53 $'[11:-2]

import cPickle, cStringIO, sys, POSException, UndoLogCompatible
from Connection import Connection
from bpthread import allocate_lock
from Transaction import Transaction, get_transaction
from referencesf import referencesf
from time import time, ctime
from zLOG import LOG, ERROR

from types import StringType

def list2dict(L):
    d = {}
    for elt in L:
        d[elt] = 1
    return d

class DB(UndoLogCompatible.UndoLogCompatible):
    """The Object Database

    The Object database coordinates access to and interaction of one
    or more connections, which manage object spaces.  Most of the actual work
    of managing objects is done by the connections.
    """
    klass = Connection  # Class to use for connections
    _activity_monitor = None

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

        # Allocate locks:
        l=allocate_lock()
        self._a=l.acquire
        self._r=l.release

        # Setup connection pools and cache info
        self._pools={},[]
        self._temps=[]
        self._pool_size=pool_size
        self._cache_size=cache_size
        self._cache_deactivate_after = cache_deactivate_after
        self._version_pool_size=version_pool_size
        self._version_cache_size=version_cache_size
        self._version_cache_deactivate_after = version_cache_deactivate_after

        self._miv_cache={}

        # Setup storage
        self._storage=storage
        storage.registerDB(self, None)
        if not hasattr(storage,'tpc_vote'): storage.tpc_vote=lambda *args: None
        try:
            storage.load('\0\0\0\0\0\0\0\0','')
        except KeyError:
            # Create the database's root in the storage if it doesn't exist
            import PersistentMapping
            root = PersistentMapping.PersistentMapping()
            # Manually create a pickle for the root to put in the storage.
            # The pickle must be in the special ZODB format.
            file = cStringIO.StringIO()
            p = cPickle.Pickler(file, 1)
            p.dump((root.__class__, None))
            p.dump(root.__getstate__())
            t = Transaction()
            t.description = 'initial database creation'
            storage.tpc_begin(t)
            storage.store('\0\0\0\0\0\0\0\0', None, file.getvalue(), '', t)
            storage.tpc_vote(t)
            storage.tpc_finish(t)

        # Pass through methods:
        for m in ('history',
                  'supportsUndo', 'supportsVersions', 'undoLog',
                  'versionEmpty', 'versions'):
            setattr(self, m, getattr(storage, m))

        if hasattr(storage, 'undoInfo'):
            self.undoInfo=storage.undoInfo


    def _cacheMean(self, attr):
        # XXX this method doesn't work
        m=[0,0]
        def f(con, m=m, attr=attr):
            t=getattr(con._cache, attr)
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
            am = self._activity_monitor
            if am is not None:
                am.closedConnection(connection)
            version=connection._version
            pools,pooll=self._pools
            try:
                pool, allocated, pool_lock = pools[version]
            except KeyError:
                # No such version. We must have deleted the pool.
                # Just let the connection go.

                # We need to break circular refs to make it really go.
                # XXX What objects are involved in the cycle?
                connection.__dict__.clear()
                
                return
                
            pool.append(connection)
            if len(pool)==1:
                # Pool now usable again, unlock it.
                pool_lock.release()
        finally:
            self._r()

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

    def abortVersion(self, version, transaction=None):
        if transaction is None:
            transaction = get_transaction()
        transaction.register(AbortVersion(self, version))

    def cacheDetail(self):
        """Return information on objects in the various caches

        Organized by class."""

        detail = {}
        def f(con, detail=detail, have_detail=detail.has_key):
            for oid, ob in con._cache.items():
                module = getattr(ob.__class__, '__module__', '')
                module = module and '%s.' % module or ''
                c = "%s%s" % (module, ob.__class__.__name__)
                if have_detail(c):
                    detail[c] = detail[c] + 1
                else:
                    detail[c] = 1

        self._connectionMap(f)
        detail = detail.items()
        detail.sort()
        return detail

    def cacheExtremeDetail(self):
        detail=[]
        conn_no = [0]  # A mutable reference to a counter
        def f(con, detail=detail, rc=sys.getrefcount, conn_no=conn_no):
            conn_no[0] = conn_no[0] + 1
            cn = conn_no[0]
            for oid, ob in con._cache_items():
                id=''
                if hasattr(ob,'__dict__'):
                    d=ob.__dict__
                    if d.has_key('id'):
                        id=d['id']
                    elif d.has_key('__name__'):
                        id=d['__name__']

                module = getattr(ob.__class__, '__module__', '')
                module = module and '%s.' % module or ''

                detail.append({
                    'conn_no': cn,
                    'oid': oid,
                    'id': id,
                    'klass': "%s%s" % (module, ob.__class__.__name__),
                    'rc': rc(ob)-4,
                    'state': ob._p_changed,
                    #'references': con.references(oid),
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
            m[0] = m[0] + con._cache.cache_non_ghost_count

        self._connectionMap(f)
        return m[0]

    def cacheDetailSize(self):
        m=[]
        def f(con, m=m):
            m.append({'connection':repr(con),
                      'ngsize':con._cache.cache_non_ghost_count,
                      'size':len(con._cache)})
        self._connectionMap(f)
        m.sort()
        return m

    def close(self):
        self._storage.close()
        for x, allocated in self._pools[1]:
            for c in allocated:
                c._breakcr()

    def commitVersion(self, source, destination='', transaction=None):
        if transaction is None:
            transaction = get_transaction()
        transaction.register(CommitVersion(self, source, destination))

    def exportFile(self, oid, file=None):
        raise 'Not yet implemented'

    def getCacheDeactivateAfter(self):
        return self._cache_deactivate_after

    def getCacheSize(self):
        return self._cache_size

    def getName(self): return self._storage.getName()

    def getPoolSize(self): return self._pool_size

    def getSize(self): return self._storage.getSize()

    def getVersionCacheDeactivateAfter(self):
        return self._version_cache_deactivate_after

    def getVersionCacheSize(self):
        return self._version_cache_size

    def getVersionPoolSize(self):
        return self._version_pool_size

    def importFile(self, file):
        raise 'Not yet implemented'

    def invalidate(self, oids, connection=None, version='',
                   rc=sys.getrefcount):
        """Invalidate references to a given oid.

        This is used to indicate that one of the connections has committed a
        change to the object.  The connection commiting the change should be
        passed in to prevent useless (but harmless) messages to the
        connection.
        """
        if connection is not None:
            version=connection._version
        # Update modified in version cache
        # XXX must make this work with list or dict to backport to 2.6
        for oid in oids.keys():
            h=hash(oid)%131
            o=self._miv_cache.get(h, None)
            if o is not None and o[0]==oid: del self._miv_cache[h]

        # Notify connections
        for pool, allocated in self._pools[1]:
            for cc in allocated:
                if (cc is not connection and
                    (not version or cc._version==version)):
                    if rc(cc) <= 3:
                        cc.close()
                    cc.invalidate(oids)

        temps=self._temps
        if temps:
            t=[]
            for cc in temps:
                if rc(cc) > 3:
                    if (cc is not connection and
                        (not version or cc._version==version)):
                        cc.invalidate(oids)
                    t.append(cc)
                else: cc.close()
            self._temps=t

    def modifiedInVersion(self, oid):
        h=hash(oid)%131
        cache=self._miv_cache
        o=cache.get(h, None)
        if o and o[0]==oid:
            return o[1]
        v=self._storage.modifiedInVersion(oid)
        cache[h]=oid, v
        return v

    def objectCount(self):
        return len(self._storage)

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
                    if connections.has_key(version) and not temporary:
                        return connections[version]
                else:
                    transaction._connections=connections={}
                transaction=transaction._connections


            if temporary:
                # This is a temporary connection.
                # We won't bother with the pools.  This will be
                # a one-use connection.
                c=self.klass(
                    version=version,
                    cache_size=self._version_cache_size)
                c._setDB(self)
                self._temps.append(c)
                if transaction is not None: transaction[id(c)]=c
                return c


            pools, pooll = self._pools

            # pools is a mapping object:
            #
            #   {version -> (pool, allocated, lock)
            #
            # where:
            #
            #   pool is the connection pool for the version,
            #   allocated is a list of all of the allocated
            #     connections, and
            #   lock is a lock that is used to block when a pool is
            #     empty and no more connections can be allocated.
            #
            # pooll is a list of all of the pools and allocated for
            # use in cases where we need to iterate over all
            # connections or all inactive connections.

            # Pool locks are tricky.  Basically, the lock needs to be
            # set whenever the pool becomes empty so that threads are
            # forced to wait until the pool gets a connection in it.
            # The lock is acquired when the (empty) pool is
            # created. The The lock is acquired just prior to removing
            # the last connection from the pool and just after adding
            # a connection to an empty pool.


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
                        c=self.klass(
                            version=version,
                            cache_size=self._version_cache_size)
                        allocated.append(c)
                        pool.append(c)
                elif self._pool_size > len(allocated) or force:
                    c=self.klass(
                        version=version,
                        cache_size=self._cache_size)
                    allocated.append(c)
                    pool.append(c)

                if c is None:
                    if waitflag:
                        self._r()
                        pool_lock.acquire()
                        self._a()
                        if len(pool) > 1:
                            # Note that the pool size will normally be 1 here,
                            # but it could be higher due to a race condition.
                            pool_lock.release()
                    else: return

            elif len(pool)==1:
                # Taking last one, lock the pool
                # Note that another thread might grab the lock
                # before us, so we might actually block, however,
                # when we get the lock back, there *will* be a
                # connection in the pool.
                self._r()
                pool_lock.acquire()
                self._a()
                if len(pool) > 1:
                    # Note that the pool size will normally be 1 here,
                    # but it could be higher due to a race condition.
                    pool_lock.release()

            c=pool[-1]
            del pool[-1]
            c._setDB(self)
            for pool, allocated in pooll:
                for cc in pool:
                    cc._incrgc()

            if transaction is not None: transaction[version]=c
            return c

        finally: self._r()

    def removeVersionPool(self, version):
        pools, pooll = self._pools
        info = pools.get(version)
        if info:
            del pools[version]
            pool, allocated, pool_lock = info
            pooll.remove((pool, allocated))
            try:
                pool_lock.release()
            except: # XXX Do we actually expect this to fail?
                pass
            del pool[:]
            del allocated[:]

    def connectionDebugInfo(self):
        r=[]
        pools,pooll=self._pools
        t=time()
        for version, (pool, allocated, lock) in pools.items():
            for c in allocated:
                o=c._opened
                d=c._debug_info
                if d:
                    if len(d)==1: d=d[0]
                else: d=''
                d="%s (%s)" % (d, len(c._cache))

                r.append({
                    'opened': o and ("%s (%.2fs)" % (ctime(o), t-o)),
                    'info': d,
                    'version': version,
                    })
        return r

    def getActivityMonitor(self):
        return self._activity_monitor

    def pack(self, t=None, days=0):
        if t is None: t=time()
        t=t-(days*86400)
        try: self._storage.pack(t,referencesf)
        except:
            LOG("ZODB", ERROR, "packing", error=sys.exc_info())
            raise

    def setCacheDeactivateAfter(self, v):
        self._cache_deactivate_after = v
        d = self._pools[0]
        pool_info = d.get('')
        if pool_info is not None:
            for c in pool_info[1]:
                c._cache.cache_age = v

    def setCacheSize(self, v):
        self._cache_size = v
        d = self._pools[0]
        pool_info = d.get('')
        if pool_info is not None:
            for c in pool_info[1]:
                c._cache.cache_size = v

    def setClassFactory(self, factory):
        self._classFactory = factory

    def setPoolSize(self, v):
        self._pool_size=v

    def setActivityMonitor(self, am):
        self._activity_monitor = am

    def setVersionCacheDeactivateAfter(self, v):
        self._version_cache_deactivate_after=v
        for ver in self._pools[0].keys():
            if ver:
                for c in self._pools[0][ver][1]:
                    c._cache.cache_age=v

    def setVersionCacheSize(self, v):
        self._version_cache_size=v
        for ver in self._pools[0].keys():
            if ver:
                for c in self._pools[0][ver][1]:
                    c._cache.cache_size=v

    def setVersionPoolSize(self, v): self._version_pool_size=v

    def cacheStatistics(self): return () # :(

    def undo(self, id, transaction=None):
        storage=self._storage
        try: supportsTransactionalUndo = storage.supportsTransactionalUndo
        except AttributeError:
            supportsTransactionalUndo=0
        else:
            supportsTransactionalUndo=supportsTransactionalUndo()

        if supportsTransactionalUndo:
            # new style undo
            if transaction is None:
                transaction = get_transaction()
            transaction.register(TransactionalUndo(self, id))
        else:
            # fall back to old undo
            d = {}
            for oid in storage.undo(id):
                d[oid] = 1
            self.invalidate(d)

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
        self.tpc_vote=s.tpc_vote
        self.tpc_finish=s.tpc_finish
        self._sortKey=s.sortKey

    def sortKey(self):
        return "%s:%s" % (self._sortKey(), id(self))

    def abort(self, reallyme, t): pass

    def commit(self, reallyme, t):
        dest=self._dest
        oids = self._db._storage.commitVersion(self._version, dest, t)
        oids = list2dict(oids)
        self._db.invalidate(oids, version=dest)
        if dest:
            # the code above just invalidated the dest version.
            # now we need to invalidate the source!
            self._db.invalidate(oids, version=self._version)

class AbortVersion(CommitVersion):
    """An object that will see to version abortion

    in cooperation with a transaction manager.
    """

    def commit(self, reallyme, t):
        version=self._version
        oids = self._db._storage.abortVersion(version, t)
        self._db.invalidate(list2dict(oids), version=version)


class TransactionalUndo(CommitVersion):
    """An object that will see to transactional undo

    in cooperation with a transaction manager.
    """

    # I (Jim) am lazy.  I'm reusing __init__ and abort and reusing the
    # version attr for the transaction id.  There's such a strong
    # similarity of rhythm that I think it's justified.

    def commit(self, reallyme, t):
        oids = self._db._storage.transactionalUndo(self._version, t)
        self._db.invalidate(list2dict(oids))
