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

$Id: DB.py,v 1.67 2004/03/01 15:45:03 jeremy Exp $"""

import cPickle, cStringIO, sys
from thread import allocate_lock
from time import time, ctime
import warnings

from ZODB.broken import find_global
from ZODB.Connection import Connection
from ZODB.serialize import referencesf
from ZODB.Transaction import Transaction, get_transaction
from zLOG import LOG, ERROR

class DB(object):
    """The Object Database

    The C{DB} class coordinates the activities of multiple database
    L{Connection} instances.  Most of the work is done by the
    C{Connections} created via the L{open} method.

    The C{DB} instance manages a pool of connections.  If a connection
    is closed, it is returned to the pool and its object cache is
    preserved.  A subsequent call to open() will reuse the connection.
    There is a limit to the pool size; if all its connections are in
    use, calls to open() will block until one of the open connections
    is closed.

    @cvar klass: Class used by L{open} to create database connections
    @type klass: L{Connection} or a subclass

    @group User Methods: __init__, open, close, undo, pack, setClassFactory
    @group Inspection Methods: getName, getSize, objectCount,
        getActivityMonitor, setActivityMonitor
    @group Connection Pool Methods: getPoolSize, getVersionPoolSize,
        removeVersionPool, setPoolSize, setVersionPoolSize
    @group Transaction Methods: invalidate
    @group Other Methods: lastTransaction, connectionDebugInfo
    @group Version Methods: modifiedInVersion, abortVersion, commitVersion,
        versionEmpty
    @group Cache Inspection Methods: cacheDetail, cacheExtremeDetail,
        cacheFullSweep, cacheLastGCTime, cacheMinimize, cacheMeanAge,
        cacheMeanDeac, cacheMeanDeal, cacheSize, cacheDetailSize,
        getCacheSize, getVersionCacheSize, setCacheSize, setVersionCacheSize,
        cacheStatistics
    @group Deprecated Methods: getCacheDeactivateAfter,
        setCacheDeactivateAfter,
        getVersionCacheDeactivateAfter, setVersionCacheDeactivateAfter
    """
    klass = Connection  # Class to use for connections
    _activity_monitor = None

    def __init__(self, storage,
                 pool_size=7,
                 cache_size=400,
                 cache_deactivate_after=None,
                 version_pool_size=3,
                 version_cache_size=100,
                 version_cache_deactivate_after=None,
                 ):
        """Create an object database.

        @param storage: storage for the database, e.g. C{FileStorage}
        @param pool_size: maximum number of open connections
        @type pool_size: C{int}
        @param cache_size: target size of L{Connection} object cache
        @type cache_size: C{int}
        @param cache_deactivate_after: ignored
        @param version_pool_size: maximum number of connections (per version)
        @type version_pool_size: C{int}
        @param version_cache_size: target size of L{Connection} object
            cache for version connectios
        @type version_cache_size: C{int}
        @param version_cache_deactivate_after: ignored
        """
        # Allocate locks:
        l=allocate_lock()
        self._a=l.acquire
        self._r=l.release

        # Setup connection pools and cache info
        self._pools = {},[]
        self._temps = []
        self._pool_size = pool_size
        self._cache_size = cache_size
        self._version_pool_size = version_pool_size
        self._version_cache_size = version_cache_size

        # warn about use of deprecated arguments
        if (cache_deactivate_after is not None or
            version_cache_deactivate_after is not None):
            warnings.warn("cache_deactivate_after has no effect",
                          DeprecationWarning)

        self._miv_cache = {}

        # Setup storage
        self._storage=storage
        storage.registerDB(self, None)
        if not hasattr(storage,'tpc_vote'): storage.tpc_vote=lambda *args: None
        try:
            storage.load('\0\0\0\0\0\0\0\0','')
        except KeyError:
            # Create the database's root in the storage if it doesn't exist
            from persistent.mapping import PersistentMapping
            root = PersistentMapping()
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

    def _classFactory(self, connection, modulename, globalname):
        return find_global(modulename, globalname)

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

    def cacheFullSweep(self):
        self._connectionMap(lambda c: c._cache.full_sweep())

    def cacheLastGCTime(self):
        m=[0]
        def f(con, m=m):
            t=con._cache.cache_last_gc_time
            if t > m[0]: m[0]=t

        self._connectionMap(f)
        return m[0]

    def cacheMinimize(self):
        self._connectionMap(lambda c: c._cache.minimize())

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
        """Close the database and its underlying storage.

        It is important to close the database, because the storage may
        flush in-memory data structures to disk when it is closed.
        Leaving the storage open with the process exits can cause the
        next open to be slow.

        What effect does closing the database have on existing
        connections?  Technically, they remain open, but their storage
        is closed, so they stop behaving usefully.  Perhaps close()
        should also close all the Connections.
        """
        self._storage.close()

    def commitVersion(self, source, destination='', transaction=None):
        if transaction is None:
            transaction = get_transaction()
        transaction.register(CommitVersion(self, source, destination))

    def getCacheSize(self):
        return self._cache_size

    def lastTransaction(self):
        return self._storage.lastTransaction()

    def getName(self): return self._storage.getName()

    def getPoolSize(self): return self._pool_size

    def getSize(self): return self._storage.getSize()

    def getVersionCacheSize(self):
        return self._version_cache_size

    def getVersionPoolSize(self):
        return self._version_pool_size

    def invalidate(self, tid, oids, connection=None, version=''):
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
                    if sys.getrefcount(cc) <= 3:
                        cc.close()
                    cc.invalidate(tid, oids)

        if self._temps:
            t=[]
            for cc in self._temps:
                if sys.getrefcount(cc) > 3:
                    if (cc is not connection and
                        (not version or cc._version == version)):
                        cc.invalidate(tid, oids)
                    t.append(cc)
                else:
                    cc.close()
            self._temps = t

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
             waitflag=1, mvcc=True):
        """Return a database L{Connection}

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
                connections = transaction._connections
                if connections:
                    if connections.has_key(version) and not temporary:
                        return connections[version]
                else:
                    transaction._connections = connections = {}
                transaction = transaction._connections

            if temporary:
                # This is a temporary connection.
                # We won't bother with the pools.  This will be
                # a one-use connection.
                c = self.klass(version=version,
                               cache_size=self._version_cache_size,
                               mvcc=mvcc)
                c._setDB(self)
                self._temps.append(c)
                if transaction is not None:
                    transaction[id(c)] = c
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
                c = None
                if version:
                    if self._version_pool_size > len(allocated) or force:
                        c = self.klass(version=version,
                                       cache_size=self._version_cache_size,
                                       mvcc=mvcc)
                        allocated.append(c)
                        pool.append(c)
                elif self._pool_size > len(allocated) or force:
                    c = self.klass(version=version,
                                   cache_size=self._cache_size,
                                   mvcc=mvcc)
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

            elif len(pool) == 1:
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

            c = pool[-1]
            del pool[-1]
            c._setDB(self)
            for pool, allocated in pooll:
                for cc in pool:
                    cc._incrgc()

            if transaction is not None:
                transaction[version] = c
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
        """Pack the storage, deleting unused object revisions.

        A pack is always performed relative to a particular time, by
        default the current time.  All object revisions that are not
        reachable as of the pack time are deleted from the storage.

        The cost of this operation varies by storage, but it is
        usually an expensive operation.

        @param t: pack time in seconds since the epoch
        @type t: C{float}
        @param days: days to subtract from C{t} to compute pack time
        @type days: C{int}
        """
        if t is None: t=time()
        t=t-(days*86400)
        try: self._storage.pack(t,referencesf)
        except:
            LOG("ZODB", ERROR, "packing", error=sys.exc_info())
            raise

    def setCacheSize(self, v):
        self._cache_size = v
        d = self._pools[0]
        pool_info = d.get('')
        if pool_info is not None:
            for c in pool_info[1]:
                c._cache.cache_size = v

    def setClassFactory(self, factory):
        """Override default mechanism for loading object classes.

        The database stores objects, but uses Python's standard import
        to load the code for those objects.  The class factory is used
        by the database serialization layer to find the classes.  It
        uses L{ZODB.broken.find_global<find_global>} by default.

        This method can be used to override the default class loading
        code.  See L{ZODB.broken.find_global<find_global>} for details
        about the contract of C{factory}.
        
        @param factory: A class factory for unpickling
        @type factory: C{function}
        """
        self._classFactory = factory

    def setPoolSize(self, v):
        self._pool_size=v

    def setActivityMonitor(self, am):
        self._activity_monitor = am

    def setVersionCacheSize(self, v):
        self._version_cache_size=v
        for ver in self._pools[0].keys():
            if ver:
                for c in self._pools[0][ver][1]:
                    c._cache.cache_size=v

    def setVersionPoolSize(self, v): self._version_pool_size=v

    def cacheStatistics(self): return () # :(

    def undo(self, id, transaction=None):
        """Undo a transaction identified by C{id}.

        A transaction can be undone if all of the objects involved in
        the transaction were not modified subsequently, if any
        modifications can be resolved by conflict resolution, or if
        subsequent changes resulted in the same object state.

        The value of C{id} should be generated by calling C{undoLog}
        or C{undoInfo}.  The value of C{id} is not the same as a
        transaction id used by other methods; it is unique to C{undo}.

        @param id: a storage-specific transaction identifier
        @type id: C{string}
        @param transaction: a transaction context to use
        @type transaction: C{Transaction}        
        """
        if transaction is None:
            transaction = get_transaction()
        transaction.register(TransactionalUndo(self, id))

    def versionEmpty(self, version):
        return self._storage.versionEmpty(version)

    # The following methods are deprecated and have no effect

    def getCacheDeactivateAfter(self):
        """Deprecated"""
        warnings.warn("cache_deactivate_after has no effect",
                      DeprecationWarning)

    def getVersionCacheDeactivateAfter(self):
        """Deprecated"""
        warnings.warn("cache_deactivate_after has no effect",
                      DeprecationWarning)

    def setCacheDeactivateAfter(self, v):
        """Deprecated"""
        warnings.warn("cache_deactivate_after has no effect",
                      DeprecationWarning)

    def setVersionCacheDeactivateAfter(self, v):
        """Deprecated"""
        warnings.warn("cache_deactivate_after has no effect",
                      DeprecationWarning)

class ResourceManager(object):
    """Transaction participation for a version or undo resource."""

    def __init__(self, db):
        self._db = db
        # Delegate the actual 2PC methods to the storage
        self.tpc_begin = self._db._storage.tpc_begin
        self.tpc_vote = self._db._storage.tpc_vote
        self.tpc_finish = self._db._storage.tpc_finish
        self.tpc_abort = self._db._storage.tpc_abort

    def sortKey(self):
        return "%s:%s" % (self._db._storage.sortKey(), id(self))

    # The object registers itself with the txn manager, so the ob
    # argument to the methods below is self.

    def abort(self, ob, t):
        pass

    def commit(self, ob, t):
        pass

class CommitVersion(ResourceManager):

    def __init__(self, db, version, dest=''):
        super(CommitVersion, self).__init__(db)
        self._version = version
        self._dest = dest

    def commit(self, ob, t):
        dest=self._dest
        tid, oids = self._db._storage.commitVersion(self._version, self._dest,
                                                    t)
        oids = dict.fromkeys(oids, 1)
        self._db.invalidate(tid, oids, version=self._dest)
        if self._dest:
            # the code above just invalidated the dest version.
            # now we need to invalidate the source!
            self._db.invalidate(tid, oids, version=self._version)

class AbortVersion(ResourceManager):

    def __init__(self, db, version):
        super(AbortVersion, self).__init__(db)
        self._version = version
        
    def commit(self, ob, t):
        tid, oids = self._db._storage.abortVersion(self._version, t)
        self._db.invalidate(tid, dict.fromkeys(oids, 1), version=self._version)

class TransactionalUndo(ResourceManager):

    def __init__(self, db, tid):
        super(TransactionalUndo, self).__init__(db)
        self._tid = tid

    def commit(self, ob, t):
        tid, oids = self._db._storage.undo(self._tid, t)
        self._db.invalidate(tid, dict.fromkeys(oids, 1))
