##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Database objects

$Id$"""

import cPickle, cStringIO, sys
import threading
from time import time, ctime
import logging

from ZODB.broken import find_global
from ZODB.utils import z64
from ZODB.Connection import Connection
from ZODB.serialize import referencesf
from ZODB.utils import WeakSet
from ZODB.utils import DEPRECATED_ARGUMENT, deprecated36

from zope.interface import implements
from ZODB.interfaces import IDatabase

import transaction

logger = logging.getLogger('ZODB.DB')

class _ConnectionPool(object):
    """Manage a pool of connections.

    CAUTION:  Methods should be called under the protection of a lock.
    This class does no locking of its own.

    There's no limit on the number of connections this can keep track of,
    but a warning is logged if there are more than pool_size active
    connections, and a critical problem if more than twice pool_size.

    New connections are registered via push().  This will log a message if
    "too many" connections are active.

    When a connection is explicitly closed, tell the pool via repush().
    That adds the connection to a stack of connections available for
    reuse, and throws away the oldest stack entries if the stack is too large.
    pop() pops this stack.

    When a connection is obtained via pop(), the pool holds only a weak
    reference to it thereafter.  It's not necessary to inform the pool
    if the connection goes away.  A connection handed out by pop() counts
    against pool_size only so long as it exists, and provided it isn't
    repush()'ed.  A weak reference is retained so that DB methods like
    connectionDebugInfo() can still gather statistics.
    """

    def __init__(self, pool_size):
        # The largest # of connections we expect to see alive simultaneously.
        self.pool_size = pool_size

        # A weak set of all connections we've seen.  A connection vanishes
        # from this set if pop() hands it out, it's not reregistered via
        # repush(), and it becomes unreachable.
        self.all = WeakSet()

        # A stack of connections available to hand out.  This is a subset
        # of self.all.  push() and repush() add to this, and may remove
        # the oldest available connections if the pool is too large.
        # pop() pops this stack.  There are never more than pool_size entries
        # in this stack.
        # In Python 2.4, a collections.deque would make more sense than
        # a list (we push only "on the right", but may pop from both ends).
        self.available = []

    # Change our belief about the expected maximum # of live connections.
    # If the pool_size is smaller than the current value, this may discard
    # the oldest available connections.
    def set_pool_size(self, pool_size):
        self.pool_size = pool_size
        self._reduce_size()

    # Register a new available connection.  We must not know about c already.
    # c will be pushed onto the available stack even if we're over the
    # pool size limit.
    def push(self, c):
        assert c not in self.all
        assert c not in self.available
        self._reduce_size(strictly_less=True)
        self.all.add(c)
        self.available.append(c)
        n, limit = len(self.all), self.pool_size
        if n > limit:
            reporter = logger.warn
            if n > 2 * limit:
                reporter = logger.critical
            reporter("DB.open() has %s open connections with a pool_size "
                     "of %s", n, limit)

    # Reregister an available connection formerly obtained via pop().  This
    # pushes it on the stack of available connections, and may discard
    # older available connections.
    def repush(self, c):
        assert c in self.all
        assert c not in self.available
        self._reduce_size(strictly_less=True)
        self.available.append(c)

    # Throw away the oldest available connections until we're under our
    # target size (strictly_less=False) or no more than that (strictly_less=
    # True, the default).
    def _reduce_size(self, strictly_less=False):
        target = self.pool_size - bool(strictly_less)
        while len(self.available) > target:
            c = self.available.pop(0)
            self.all.remove(c)

    # Pop an available connection and return it, or return None if none are
    # available.  In the latter case, the caller should create a new
    # connection, register it via push(), and call pop() again.  The
    # caller is responsible for serializing this sequence.
    def pop(self):
        result = None
        if self.available:
            result = self.available.pop()
            # Leave it in self.all, so we can still get at it for statistics
            # while it's alive.
            assert result in self.all
        return result

    # For every live connection c, invoke f(c).
    def map(self, f):
        self.all.map(f)

class DB(object):
    """The Object Database
    -------------------

    The DB class coordinates the activities of multiple database
    Connection instances.  Most of the work is done by the
    Connections created via the open method.

    The DB instance manages a pool of connections.  If a connection is
    closed, it is returned to the pool and its object cache is
    preserved.  A subsequent call to open() will reuse the connection.
    There is no hard limit on the pool size.  If more than `pool_size`
    connections are opened, a warning is logged, and if more than twice
    that many, a critical problem is logged.

    The class variable 'klass' is used by open() to create database
    connections.  It is set to Connection, but a subclass could override
    it to provide a different connection implementation.

    The database provides a few methods intended for application code
    -- open, close, undo, and pack -- and a large collection of
    methods for inspecting the database and its connections' caches.

    :Cvariables:
      - `klass`: Class used by L{open} to create database connections

    :Groups:
      - `User Methods`: __init__, open, close, undo, pack, classFactory
      - `Inspection Methods`: getName, getSize, objectCount,
        getActivityMonitor, setActivityMonitor
      - `Connection Pool Methods`: getPoolSize, getVersionPoolSize,
        removeVersionPool, setPoolSize, setVersionPoolSize
      - `Transaction Methods`: invalidate
      - `Other Methods`: lastTransaction, connectionDebugInfo
      - `Version Methods`: modifiedInVersion, abortVersion, commitVersion,
        versionEmpty
      - `Cache Inspection Methods`: cacheDetail, cacheExtremeDetail,
        cacheFullSweep, cacheLastGCTime, cacheMinimize, cacheSize,
        cacheDetailSize, getCacheSize, getVersionCacheSize, setCacheSize,
        setVersionCacheSize
      - `Deprecated Methods`: getCacheDeactivateAfter,
        setCacheDeactivateAfter,
        getVersionCacheDeactivateAfter, setVersionCacheDeactivateAfter
    """
    implements(IDatabase)

    klass = Connection  # Class to use for connections
    _activity_monitor = None

    def __init__(self, storage,
                 pool_size=7,
                 cache_size=400,
                 cache_deactivate_after=DEPRECATED_ARGUMENT,
                 version_pool_size=3,
                 version_cache_size=100,
                 database_name='unnamed',
                 databases=None,
                 version_cache_deactivate_after=DEPRECATED_ARGUMENT,
                 ):
        """Create an object database.

        :Parameters:
          - `storage`: the storage used by the database, e.g. FileStorage
          - `pool_size`: expected maximum number of open connections
          - `cache_size`: target size of Connection object cache
          - `version_pool_size`: expected maximum number of connections (per
            version)
          - `version_cache_size`: target size of Connection object cache for
            version connections
          - `cache_deactivate_after`: ignored
          - `version_cache_deactivate_after`: ignored
        """
        # Allocate lock.
        x = threading.RLock()
        self._a = x.acquire
        self._r = x.release

        # Setup connection pools and cache info
        # _pools maps a version string to a _ConnectionPool object.
        self._pools = {}
        self._pool_size = pool_size
        self._cache_size = cache_size
        self._version_pool_size = version_pool_size
        self._version_cache_size = version_cache_size

        # warn about use of deprecated arguments
        if cache_deactivate_after is not DEPRECATED_ARGUMENT:
            deprecated36("cache_deactivate_after has no effect")
        if version_cache_deactivate_after is not DEPRECATED_ARGUMENT:
            deprecated36("version_cache_deactivate_after has no effect")

        self._miv_cache = {}

        # Setup storage
        self._storage=storage
        storage.registerDB(self, None)
        if not hasattr(storage,'tpc_vote'):
            storage.tpc_vote = lambda *args: None
        try:
            storage.load(z64,'')
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
            t = transaction.Transaction()
            t.description = 'initial database creation'
            storage.tpc_begin(t)
            storage.store(z64, None, file.getvalue(), '', t)
            storage.tpc_vote(t)
            storage.tpc_finish(t)

        # Multi-database setup.
        if databases is None:
            databases = {}
        self.databases = databases
        self.database_name = database_name
        if database_name in databases:
            raise ValueError("database_name %r already in databases" %
                             database_name)
        databases[database_name] = self

        # Pass through methods:
        for m in ['history', 'supportsUndo', 'supportsVersions', 'undoLog',
                  'versionEmpty', 'versions']:
            setattr(self, m, getattr(storage, m))

        if hasattr(storage, 'undoInfo'):
            self.undoInfo = storage.undoInfo

    # This is called by Connection.close().
    def _closeConnection(self, connection):
        """Return a connection to the pool.

        connection._db must be self on entry.
        """

        self._a()
        try:
            assert connection._db is self
            connection._db = None

            am = self._activity_monitor
            if am is not None:
                am.closedConnection(connection)

            version = connection._version
            try:
                pool = self._pools[version]
            except KeyError:
                # No such version. We must have deleted the pool.
                # Just let the connection go.

                # We need to break circular refs to make it really go.
                # TODO:  Figure out exactly which objects are involved in the
                # cycle.
                connection.__dict__.clear()
                return
            pool.repush(connection)

        finally:
            self._r()

    # Call f(c) for all connections c in all pools in all versions.
    def _connectionMap(self, f):
        self._a()
        try:
            for pool in self._pools.values():
                pool.map(f)
        finally:
            self._r()

    def abortVersion(self, version, txn=None):
        if txn is None:
            txn = transaction.get()
        txn.register(AbortVersion(self, version))

    def cacheDetail(self):
        """Return information on objects in the various caches

        Organized by class.
        """

        detail = {}
        def f(con, detail=detail):
            for oid, ob in con._cache.items():
                module = getattr(ob.__class__, '__module__', '')
                module = module and '%s.' % module or ''
                c = "%s%s" % (module, ob.__class__.__name__)
                if c in detail:
                    detail[c] += 1
                else:
                    detail[c] = 1

        self._connectionMap(f)
        detail = detail.items()
        detail.sort()
        return detail

    def cacheExtremeDetail(self):
        detail = []
        conn_no = [0]  # A mutable reference to a counter
        def f(con, detail=detail, rc=sys.getrefcount, conn_no=conn_no):
            conn_no[0] += 1
            cn = conn_no[0]
            for oid, ob in con._cache_items():
                id = ''
                if hasattr(ob, '__dict__'):
                    d = ob.__dict__
                    if d.has_key('id'):
                        id = d['id']
                    elif d.has_key('__name__'):
                        id = d['__name__']

                module = getattr(ob.__class__, '__module__', '')
                module = module and ('%s.' % module) or ''

                # What refcount ('rc') should we return?  The intent is
                # that we return the true Python refcount, but as if the
                # cache didn't exist.  This routine adds 3 to the true
                # refcount:  1 for binding to name 'ob', another because
                # ob lives in the con._cache_items() list we're iterating
                # over, and calling sys.getrefcount(ob) boosts ob's
                # count by 1 too.  So the true refcount is 3 less than
                # sys.getrefcount(ob) returns.  But, in addition to that,
                # the cache holds an extra reference on non-ghost objects,
                # and we also want to pretend that doesn't exist.
                detail.append({
                    'conn_no': cn,
                    'oid': oid,
                    'id': id,
                    'klass': "%s%s" % (module, ob.__class__.__name__),
                    'rc': rc(ob) - 3 - (ob._p_changed is not None),
                    'state': ob._p_changed,
                    #'references': con.references(oid),
                    })

        self._connectionMap(f)
        return detail

    def cacheFullSweep(self):
        self._connectionMap(lambda c: c._cache.full_sweep())

    def cacheLastGCTime(self):
        m = [0]
        def f(con, m=m):
            t = con._cache.cache_last_gc_time
            if t > m[0]:
                m[0] = t

        self._connectionMap(f)
        return m[0]

    def cacheMinimize(self):
        self._connectionMap(lambda c: c._cache.minimize())

    def cacheSize(self):
        m = [0]
        def f(con, m=m):
            m[0] += con._cache.cache_non_ghost_count

        self._connectionMap(f)
        return m[0]

    def cacheDetailSize(self):
        m = []
        def f(con, m=m):
            m.append({'connection': repr(con),
                      'ngsize': con._cache.cache_non_ghost_count,
                      'size': len(con._cache)})
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

    def commitVersion(self, source, destination='', txn=None):
        if txn is None:
            txn = transaction.get()
        txn.register(CommitVersion(self, source, destination))

    def getCacheSize(self):
        return self._cache_size

    def lastTransaction(self):
        return self._storage.lastTransaction()

    def getName(self):
        return self._storage.getName()

    def getPoolSize(self):
        return self._pool_size

    def getSize(self):
        return self._storage.getSize()

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
            version = connection._version
        # Update modified in version cache
        for oid in oids.keys():
            h = hash(oid) % 131
            o = self._miv_cache.get(h, None)
            if o is not None and o[0]==oid:
                del self._miv_cache[h]

        # Notify connections.
        def inval(c):
            if (c is not connection and
                  (not version or c._version == version)):
                c.invalidate(tid, oids)
        self._connectionMap(inval)

    def modifiedInVersion(self, oid):
        h = hash(oid) % 131
        cache = self._miv_cache
        o = cache.get(h, None)
        if o and o[0] == oid:
            return o[1]
        v = self._storage.modifiedInVersion(oid)
        cache[h] = oid, v
        return v

    def objectCount(self):
        return len(self._storage)

    def open(self, version='',
             transaction=DEPRECATED_ARGUMENT, temporary=DEPRECATED_ARGUMENT,
             force=DEPRECATED_ARGUMENT, waitflag=DEPRECATED_ARGUMENT,
             mvcc=True, txn_mgr=None, synch=True):
        """Return a database Connection for use by application code.

        The optional `version` argument can be used to specify that a
        version connection is desired.

        Note that the connection pool is managed as a stack, to
        increase the likelihood that the connection's stack will
        include useful objects.

        :Parameters:
          - `version`: the "version" that all changes will be made
             in, defaults to no version.
          - `mvcc`: boolean indicating whether MVCC is enabled
          - `txn_mgr`: transaction manager to use.  None means
             used the default transaction manager.
          - `synch`: boolean indicating whether Connection should
             register for afterCompletion() calls.
        """

        if temporary is not DEPRECATED_ARGUMENT:
            deprecated36("DB.open() temporary= ignored. "
                         "open() no longer blocks.")

        if force is not DEPRECATED_ARGUMENT:
            deprecated36("DB.open() force= ignored. "
                         "open() no longer blocks.")

        if waitflag is not DEPRECATED_ARGUMENT:
            deprecated36("DB.open() waitflag= ignored. "
                         "open() no longer blocks.")

        if transaction is not DEPRECATED_ARGUMENT:
            deprecated36("DB.open() transaction= ignored.")

        self._a()
        try:
            # pool <- the _ConnectionPool for this version
            pool = self._pools.get(version)
            if pool is None:
                if version:
                    size = self._version_pool_size
                else:
                    size = self._pool_size
                self._pools[version] = pool = _ConnectionPool(size)
            assert pool is not None

            # result <- a connection
            result = pool.pop()
            if result is None:
                if version:
                    size = self._version_cache_size
                else:
                    size = self._cache_size
                c = self.klass(version=version, cache_size=size,
                               mvcc=mvcc, txn_mgr=txn_mgr)
                pool.push(c)
                result = pool.pop()
            assert result is not None

            # Tell the connection it belongs to self.
            result._setDB(self, mvcc=mvcc, txn_mgr=txn_mgr, synch=synch)

            # A good time to do some cache cleanup.
            self._connectionMap(lambda c: c.cacheGC())

            return result

        finally:
            self._r()

    def removeVersionPool(self, version):
        try:
            del self._pools[version]
        except KeyError:
            pass

    def connectionDebugInfo(self):
        result = []
        t = time()

        def get_info(c):
            # `result`, `time` and `version` are lexically inherited.
            o = c._opened
            d = c.getDebugInfo()
            if d:
                if len(d) == 1:
                    d = d[0]
            else:
                d = ''
            d = "%s (%s)" % (d, len(c._cache))

            result.append({
                'opened': o and ("%s (%.2fs)" % (ctime(o), t-o)),
                'info': d,
                'version': version,
                })

        for version, pool in self._pools.items():
            pool.map(get_info)
        return result

    def getActivityMonitor(self):
        return self._activity_monitor

    def pack(self, t=None, days=0):
        """Pack the storage, deleting unused object revisions.

        A pack is always performed relative to a particular time, by
        default the current time.  All object revisions that are not
        reachable as of the pack time are deleted from the storage.

        The cost of this operation varies by storage, but it is
        usually an expensive operation.

        There are two optional arguments that can be used to set the
        pack time: t, pack time in seconds since the epcoh, and days,
        the number of days to subtract from t or from the current
        time if t is not specified.
        """
        if t is None:
            t = time()
        t -= days * 86400
        try:
            self._storage.pack(t, referencesf)
        except:
            logger.error("packing", exc_info=True)
            raise

    def setActivityMonitor(self, am):
        self._activity_monitor = am

    def classFactory(self, connection, modulename, globalname):
        # Zope will rebind this method to arbitrary user code at runtime.
        return find_global(modulename, globalname)

    def setCacheSize(self, size):
        self._a()
        try:
            self._cache_size = size
            pool = self._pools.get('')
            if pool is not None:
                def setsize(c):
                    c._cache.cache_size = size
                pool.map(setsize)
        finally:
            self._r()

    def setVersionCacheSize(self, size):
        self._a()
        try:
            self._version_cache_size = size
            def setsize(c):
                c._cache.cache_size = size
            for version, pool in self._pools.items():
                if version:
                    pool.map(setsize)
        finally:
            self._r()

    def setPoolSize(self, size):
        self._pool_size = size
        self._reset_pool_sizes(size, for_versions=False)

    def setVersionPoolSize(self, size):
        self._version_pool_size = size
        self._reset_pool_sizes(size, for_versions=True)

    def _reset_pool_sizes(self, size, for_versions=False):
        self._a()
        try:
            for version, pool in self._pools.items():
                if (version != '') == for_versions:
                    pool.set_pool_size(size)
        finally:
            self._r()

    def undo(self, id, txn=None):
        """Undo a transaction identified by id.

        A transaction can be undone if all of the objects involved in
        the transaction were not modified subsequently, if any
        modifications can be resolved by conflict resolution, or if
        subsequent changes resulted in the same object state.

        The value of id should be generated by calling undoLog()
        or undoInfo().  The value of id is not the same as a
        transaction id used by other methods; it is unique to undo().

        :Parameters:
          - `id`: a storage-specific transaction identifier
          - `txn`: transaction context to use for undo().
            By default, uses the current transaction.
        """
        if txn is None:
            txn = transaction.get()
        txn.register(TransactionalUndo(self, id))

    def versionEmpty(self, version):
        return self._storage.versionEmpty(version)

    # The following methods are deprecated and have no effect

    def getCacheDeactivateAfter(self):
        """Deprecated"""
        deprecated36("getCacheDeactivateAfter has no effect")

    def getVersionCacheDeactivateAfter(self):
        """Deprecated"""
        deprecated36("getVersionCacheDeactivateAfter has no effect")

    def setCacheDeactivateAfter(self, v):
        """Deprecated"""
        deprecated36("setCacheDeactivateAfter has no effect")

    def setVersionCacheDeactivateAfter(self, v):
        """Deprecated"""
        deprecated36("setVersionCacheDeactivateAfter has no effect")

class ResourceManager(object):
    """Transaction participation for a version or undo resource."""

    def __init__(self, db):
        self._db = db
        # Delegate the actual 2PC methods to the storage
        self.tpc_vote = self._db._storage.tpc_vote
        self.tpc_finish = self._db._storage.tpc_finish
        self.tpc_abort = self._db._storage.tpc_abort

    def sortKey(self):
        return "%s:%s" % (self._db._storage.sortKey(), id(self))

    def tpc_begin(self, txn, sub=False):
        if sub:
            raise ValueError("doesn't support sub-transactions")
        self._db._storage.tpc_begin(txn)

    # The object registers itself with the txn manager, so the ob
    # argument to the methods below is self.

    def abort(self, obj, txn):
        pass

    def commit(self, obj, txn):
        pass

class CommitVersion(ResourceManager):

    def __init__(self, db, version, dest=''):
        super(CommitVersion, self).__init__(db)
        self._version = version
        self._dest = dest

    def commit(self, ob, t):
        dest = self._dest
        tid, oids = self._db._storage.commitVersion(self._version,
                                                    self._dest,
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
        self._db.invalidate(tid,
                            dict.fromkeys(oids, 1),
                            version=self._version)

class TransactionalUndo(ResourceManager):

    def __init__(self, db, tid):
        super(TransactionalUndo, self).__init__(db)
        self._tid = tid

    def commit(self, ob, t):
        tid, oids = self._db._storage.undo(self._tid, t)
        self._db.invalidate(tid, dict.fromkeys(oids, 1))
