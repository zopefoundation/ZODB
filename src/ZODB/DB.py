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

import warnings

import cPickle
import cStringIO
import sys
import threading
import logging
import datetime
import calendar
import time

from ZODB.broken import find_global
from ZODB.utils import z64
from ZODB.Connection import Connection
import ZODB.serialize

import transaction.weakset

from zope.interface import implements
from ZODB.interfaces import IDatabase

import BTrees.OOBTree
import transaction

from persistent.TimeStamp import TimeStamp


logger = logging.getLogger('ZODB.DB')

class AbstractConnectionPool(object):
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

    def __init__(self, size, timeout=None):
        # The largest # of connections we expect to see alive simultaneously.
        self._size = size

        # The minimum number of seconds that an available connection should
        # be kept, or None.
        self._timeout = timeout

        # A weak set of all connections we've seen.  A connection vanishes
        # from this set if pop() hands it out, it's not reregistered via
        # repush(), and it becomes unreachable.
        self.all = transaction.weakset.WeakSet()

    def setSize(self, size):
        """Change our belief about the expected maximum # of live connections.

        If the pool_size is smaller than the current value, this may discard
        the oldest available connections.
        """
        self._size = size
        self._reduce_size()

    def setTimeout(self, timeout):
        old = self._timeout
        self._timeout = timeout
        if timeout is not None and old != timeout and (
            old is None or old > timeout):
            self._reduce_size()

    def getSize(self):
        return self._size

    def getTimeout(self):
        return self._timeout

    timeout = property(getTimeout, lambda self, v: self.setTimeout(v))

    size = property(getSize, lambda self, v: self.setSize(v))

class ConnectionPool(AbstractConnectionPool):

    # XXX WTF, passing time.time() as a default?
    def __init__(self, size, timeout=time.time()):
        super(ConnectionPool, self).__init__(size, timeout)

        # A stack of connections available to hand out.  This is a subset
        # of self.all.  push() and repush() add to this, and may remove
        # the oldest available connections if the pool is too large.
        # pop() pops this stack.  There are never more than size entries
        # in this stack.
        self.available = []

    def push(self, c):
        """Register a new available connection.

        We must not know about c already. c will be pushed onto the available
        stack even if we're over the pool size limit.
        """
        assert c not in self.all
        assert c not in self.available
        self._reduce_size(strictly_less=True)
        self.all.add(c)
        self.available.append((time.time(), c))
        n = len(self.all)
        limit = self.size
        if n > limit:
            reporter = logger.warn
            if n > 2 * limit:
                reporter = logger.critical
            reporter("DB.open() has %s open connections with a pool_size "
                     "of %s", n, limit)

    def repush(self, c):
        """Reregister an available connection formerly obtained via pop().

        This pushes it on the stack of available connections, and may discard
        older available connections.
        """
        assert c in self.all
        assert c not in self.available
        self._reduce_size(strictly_less=True)
        self.available.append((time.time(), c))

    def _reduce_size(self, strictly_less=False):
        """Throw away the oldest available connections until we're under our
        target size (strictly_less=False, the default) or no more than that
        (strictly_less=True).
        """
        threshhold = time.time() - self.timeout
        target = self.size
        if strictly_less:
            target -= 1

        available = self.available
        while (
            (len(available) > target)
            or
            (available and available[0][0] < threshhold)
            ):
            t, c = available.pop(0)
            self.all.remove(c)
            # While application code may still hold a reference to `c`,
            # there's little useful that can be done with this Connection
            # anymore. Its cache may be holding on to limited resources,
            # and we replace the cache with an empty one now so that we
            # don't have to wait for gc to reclaim it. Note that it's not
            # possible for DB.open() to return `c` again: `c` can never be
            # in an open state again.
            # TODO: Perhaps it would be better to break the reference
            # cycles between `c` and `c._cache`, so that refcounting
            # reclaims both right now. But if user code _does_ have a
            # strong reference to `c` now, breaking the cycle would not
            # reclaim `c` now, and `c` would be left in a user-visible
            # crazy state.
            c._resetCache()

    def reduce_size(self):
        self._reduce_size()

    def pop(self):
        """Pop an available connection and return it.

        Return None if none are available - in this case, the caller should
        create a new connection, register it via push(), and call pop() again.
        The caller is responsible for serializing this sequence.
        """
        result = None
        if self.available:
            _, result = self.available.pop()
            # Leave it in self.all, so we can still get at it for statistics
            # while it's alive.
            assert result in self.all
        return result

    def map(self, f):
        """For every live connection c, invoke f(c)."""
        self.all.map(f)

    def availableGC(self):
        """Perform garbage collection on available connections.
        
        If a connection is no longer viable because it has timed out, it is
        garbage collected."""
        threshhold = time.time() - self.timeout
        for t, c in list(self.available):
            if t < threshhold:
                del self.available[t]
                self.all.remove(c)
                c._resetCache()
            else:
                c.cacheGC()

class KeyedConnectionPool(AbstractConnectionPool):
    # this pool keeps track of keyed connections all together.  It makes
    # it possible to make assertions about total numbers of keyed connections.
    # The keys in this case are "before" TIDs, but this is used by other
    # packages as well.

    # see the comments in ConnectionPool for method descriptions.

    def __init__(self, size, timeout=time.time()):
        super(KeyedConnectionPool, self).__init__(size, timeout)
        self.pools = {}

    def setSize(self, v):
        self._size = v
        for pool in self.pools.values():
            pool.setSize(v)

    def setTimeout(self, v):
        self._timeout = v
        for pool in self.pools.values():
            pool.setTimeout(v)

    def push(self, c, key):
        pool = self.pools.get(key)
        if pool is None:
            pool = self.pools[key] = ConnectionPool(self.size, self.timeout)
        pool.push(c)

    def repush(self, c, key):
        self.pools[key].repush(c)

    def _reduce_size(self, strictly_less=False):
        for key, pool in list(self.pools.items()):
            pool._reduce_size(strictly_less)
            if not pool.all:
                del self.pools[key]

    def reduce_size(self):
        self._reduce_size()

    def pop(self, key):
        pool = self.pools.get(key)
        if pool is not None:
            return pool.pop()

    def map(self, f):
        for pool in self.pools.itervalues():
            pool.map(f)

    def availableGC(self):
        for key, pool in self.pools.items():
            pool.availableGC()
            if not pool.all:
                del self.pools[key]

    @property
    def test_all(self):
        result = set()
        for pool in self.pools.itervalues():
            result.update(pool.all)
        return frozenset(result)

    @property
    def test_available(self):
        result = []
        for pool in self.pools.itervalues():
            result.extend(pool.available)
        return tuple(result)
        
        

def toTimeStamp(dt):
    utc_struct = dt.utctimetuple()
    # if this is a leapsecond, this will probably fail.  That may be a good
    # thing: leapseconds are not really accounted for with serials.
    args = utc_struct[:5]+(utc_struct[5] + dt.microsecond/1000000.0,)
    return TimeStamp(*args)

def getTID(at, before):
    if at is not None:
        if before is not None:
            raise ValueError('can only pass zero or one of `at` and `before`')
        if isinstance(at, datetime.datetime):
            at = toTimeStamp(at)
        else:
            at = TimeStamp(at)
        before = repr(at.laterThan(at))
    elif before is not None:
        if isinstance(before, datetime.datetime):
            before = repr(toTimeStamp(before))
        else:
            before = repr(TimeStamp(before))
    return before


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
      - `Connection Pool Methods`: getPoolSize, getHistoricalPoolSize,
        setPoolSize, setHistoricalPoolSize, getHistoricalTimeout,
        setHistoricalTimeout
      - `Transaction Methods`: invalidate
      - `Other Methods`: lastTransaction, connectionDebugInfo
      - `Cache Inspection Methods`: cacheDetail, cacheExtremeDetail,
        cacheFullSweep, cacheLastGCTime, cacheMinimize, cacheSize,
        cacheDetailSize, getCacheSize, getHistoricalCacheSize, setCacheSize,
        setHistoricalCacheSize
    """
    implements(IDatabase)

    klass = Connection  # Class to use for connections
    _activity_monitor = next = previous = None

    def __init__(self, storage,
                 pool_size=7,
                 cache_size=400,
                 cache_size_bytes=0,
                 historical_pool_size=3,
                 historical_cache_size=1000,
                 historical_cache_size_bytes=0,
                 historical_timeout=300,
                 database_name='unnamed',
                 databases=None,
                 ):
        """Create an object database.

        :Parameters:
          - `storage`: the storage used by the database, e.g. FileStorage
          - `pool_size`: expected maximum number of open connections
          - `cache_size`: target size of Connection object cache
          - `cache_size_bytes`: target size measured in total estimated size
               of objects in the Connection object cache.
               "0" means unlimited.
          - `historical_pool_size`: expected maximum number of total
            historical connections
          - `historical_cache_size`: target size of Connection object cache for
            historical (`at` or `before`) connections
          - `historical_cache_size_bytes` -- similar to `cache_size_bytes` for
            the historical connection.
          - `historical_timeout`: minimum number of seconds that
            an unused historical connection will be kept, or None.
        """
        if isinstance(storage, basestring):
            from ZODB import FileStorage
            storage = ZODB.FileStorage.FileStorage(storage)

        # Allocate lock.
        x = threading.RLock()
        self._a = x.acquire
        self._r = x.release

        # pools and cache sizes
        self.pool = ConnectionPool(pool_size)
        self.historical_pool = KeyedConnectionPool(historical_pool_size,
                                                   historical_timeout)
        self._cache_size = cache_size
        self._cache_size_bytes = cache_size_bytes
        self._historical_cache_size = historical_cache_size
        self._historical_cache_size_bytes = historical_cache_size_bytes

        # Setup storage
        self.storage = storage
        self.references = ZODB.serialize.referencesf
        try:
            storage.registerDB(self)
        except TypeError:
            storage.registerDB(self, None) # Backward compat

        if (not hasattr(storage, 'tpc_vote')) and not storage.isReadOnly():
            warnings.warn(
                "Storage doesn't have a tpc_vote and this violates "
                "the storage API. Violently monkeypatching in a do-nothing "
                "tpc_vote.",
                DeprecationWarning, 2)
            storage.tpc_vote = lambda *args: None

        try:
            storage.load(z64, '')
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

        self._setupUndoMethods()
        self.history = storage.history

    def _setupUndoMethods(self):
        storage = self.storage
        try:
            self.supportsUndo = storage.supportsUndo
        except AttributeError:
            self.supportsUndo = lambda : False

        if self.supportsUndo():
            self.undoLog = storage.undoLog
            if hasattr(storage, 'undoInfo'):
                self.undoInfo = storage.undoInfo
        else:
            self.undoLog = self.undoInfo = lambda *a,**k: ()
            def undo(*a, **k):
                raise NotImplementedError
            self.undo = undo

    @property
    def _storage(self):      # Backward compatibility
        return self.storage

    # This is called by Connection.close().
    def _returnToPool(self, connection):
        """Return a connection to the pool.

        connection._db must be self on entry.
        """

        self._a()
        try:
            assert connection._db is self
            connection._opened = None

            am = self._activity_monitor
            if am is not None:
                am.closedConnection(connection)

            if connection.before:
                self.historical_pool.repush(connection, connection.before)
            else:
                self.pool.repush(connection)
        finally:
            self._r()

    def _connectionMap(self, f):
        """Call f(c) for all connections c in all pools, live and historical.
        """
        self._a()
        try:
            self.pool.map(f)
            self.historical_pool.map(f)
        finally:
            self._r()

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
        self.storage.close()

    def getCacheSize(self):
        return self._cache_size

    def getCacheSizeBytes(self):
        return self._cache_size_bytes

    def lastTransaction(self):
        return self.storage.lastTransaction()

    def getName(self):
        return self.storage.getName()

    def getPoolSize(self):
        return self.pool.size

    def getSize(self):
        return self.storage.getSize()

    def getHistoricalCacheSize(self):
        return self._historical_cache_size

    def getHistoricalCacheSizeBytes(self):
        return self._historical_cache_size_bytes

    def getHistoricalPoolSize(self):
        return self.historical_pool.size

    def getHistoricalTimeout(self):
        return self.historical_pool.timeout

    def invalidate(self, tid, oids, connection=None, version=''):
        """Invalidate references to a given oid.

        This is used to indicate that one of the connections has committed a
        change to the object.  The connection commiting the change should be
        passed in to prevent useless (but harmless) messages to the
        connection.
        """
        # Storages, esp. ZEO tests, need the version argument still. :-/
        assert version==''
        # Notify connections.
        def inval(c):
            if c is not connection:
                c.invalidate(tid, oids)
        self._connectionMap(inval)

    def invalidateCache(self):
        """Invalidate each of the connection caches
        """
        self._connectionMap(lambda c: c.invalidateCache())

    def objectCount(self):
        return len(self.storage)

    def open(self, transaction_manager=None, at=None, before=None):
        """Return a database Connection for use by application code.

        Note that the connection pool is managed as a stack, to
        increase the likelihood that the connection's stack will
        include useful objects.

        :Parameters:
          - `transaction_manager`: transaction manager to use.  None means
            use the default transaction manager.
          - `at`: a datetime.datetime or 8 character transaction id of the
            time to open the database with a read-only connection.  Passing
            both `at` and `before` raises a ValueError, and passing neither
            opens a standard writable transaction of the newest state.
            A timezone-naive datetime.datetime is treated as a UTC value.
          - `before`: like `at`, but opens the readonly state before the
            tid or datetime.
        """
        # `at` is normalized to `before`, since we use storage.loadBefore
        # as the underlying implementation of both.
        before = getTID(at, before)
        if (before is not None and
            before > self.lastTransaction() and
            before > getTID(self.lastTransaction(), None)):
            raise ValueError(
                'cannot open an historical connection in the future.')

        self._a()
        try:
            # result <- a connection
            if before is not None:
                result = self.historical_pool.pop(before)
                if result is None:
                    c = self.klass(self,
                                   self._historical_cache_size,
                                   before,
                                   self._historical_cache_size_bytes,
                                   )
                    self.historical_pool.push(c, before)
                    result = self.historical_pool.pop(before)
            else:
                result = self.pool.pop()
                if result is None:
                    c = self.klass(self,
                                   self._cache_size,
                                   None,
                                   self._cache_size_bytes,
                                   )
                    self.pool.push(c)
                    result = self.pool.pop()
            assert result is not None

            # open the connection.
            result.open(transaction_manager)

            # A good time to do some cache cleanup.
            # (note we already have the lock)
            self.pool.availableGC()
            self.historical_pool.availableGC()

            return result

        finally:
            self._r()

    def connectionDebugInfo(self):
        result = []
        t = time.time()

        def get_info(c):
            # `result`, `time` and `before` are lexically inherited.
            o = c._opened
            d = c.getDebugInfo()
            if d:
                if len(d) == 1:
                    d = d[0]
            else:
                d = ''
            d = "%s (%s)" % (d, len(c._cache))

            # output UTC time with the standard Z time zone indicator
            result.append({
                'opened': o and ("%s (%.2fs)" % (
                    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(o)),
                    t-o)),
                'info': d,
                'before': c.before,
                })

        self._connectionMap(get_info)
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
            t = time.time()
        t -= days * 86400
        try:
            self.storage.pack(t, self.references)
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
            def setsize(c):
                c._cache.cache_size = size
            self.pool.map(setsize)
        finally:
            self._r()

    def setCacheSizeBytes(self, size):
        self._a()
        try:
            self._cache_size_bytes = size
            def setsize(c):
                c._cache.cache_size_bytes = size
            self.pool.map(setsize)
        finally:
            self._r()

    def setHistoricalCacheSize(self, size):       
        self._a()
        try:
            self._historical_cache_size = size
            def setsize(c):
                c._cache.cache_size = size
            self.historical_pool.map(setsize)
        finally:
            self._r()

    def setHistoricalCacheSizeBytes(self, size):       
        self._a()
        try:
            self._historical_cache_size_bytes = size
            def setsize(c):
                c._cache.cache_size_bytes = size
            self.historical_pool.map(setsize)
        finally:
            self._r()

    def setPoolSize(self, size):
        self._a()
        try:
            self.pool.size = size
        finally:
            self._r()

    def setHistoricalPoolSize(self, size):
        self._a()
        try:
            self.historical_pool.size = size
        finally:
            self._r()

    def setHistoricalTimeout(self, timeout):
        self._a()
        try:
            self.historical_pool.timeout = timeout
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


resource_counter_lock = threading.Lock()
resource_counter = 0

class ResourceManager(object):
    """Transaction participation for an undo resource."""

    # XXX This implementation is broken.  Subclasses invalidate oids
    # in their commit calls. Invalidations should not be sent until
    # tpc_finish is called.  In fact, invalidations should be sent to
    # the db *while* tpc_finish is being called on the storage.

    def __init__(self, db):
        self._db = db
        # Delegate the actual 2PC methods to the storage
        self.tpc_vote = self._db.storage.tpc_vote
        self.tpc_finish = self._db.storage.tpc_finish
        self.tpc_abort = self._db.storage.tpc_abort

        # Get a number from a simple thread-safe counter, then
        # increment it, for the purpose of sorting ResourceManagers by
        # creation order.  This ensures that multiple ResourceManagers
        # within a transaction commit in a predictable sequence.
        resource_counter_lock.acquire()
        try:
            global resource_counter
            self._count = resource_counter
            resource_counter += 1
        finally:
            resource_counter_lock.release()

    def sortKey(self):
        return "%s:%016x" % (self._db.storage.sortKey(), self._count)

    def tpc_begin(self, txn, sub=False):
        if sub:
            raise ValueError("doesn't support sub-transactions")
        self._db.storage.tpc_begin(txn)

    # The object registers itself with the txn manager, so the ob
    # argument to the methods below is self.

    def abort(self, obj, txn):
        raise NotImplementedError

    def commit(self, obj, txn):
        raise NotImplementedError

class TransactionalUndo(ResourceManager):

    def __init__(self, db, tid):
        super(TransactionalUndo, self).__init__(db)
        self._tid = tid

    def commit(self, ob, t):
        # XXX see XXX in ResourceManager
        tid, oids = self._db.storage.undo(self._tid, t)
        self._db.invalidate(tid, dict.fromkeys(oids, 1))
