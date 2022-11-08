##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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
"""
from __future__ import print_function

import datetime
import logging
import sys
import time
import warnings
import weakref
from itertools import chain

import six

import transaction
from persistent.TimeStamp import TimeStamp
from zope.interface import implementer

import ZODB.serialize
from ZODB import utils
from ZODB import valuedoc
from ZODB.broken import find_global
from ZODB.Connection import Connection
from ZODB.Connection import TransactionMetaData
from ZODB.Connection import noop
from ZODB.interfaces import IDatabase
from ZODB.interfaces import IMVCCStorage
from ZODB.utils import z64


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

    def __init__(self, size, timeout):
        # The largest # of connections we expect to see alive simultaneously.
        self._size = size

        # The minimum number of seconds that an available connection should
        # be kept, or None.
        self._timeout = timeout

        # A weak set of all connections we've seen.  A connection vanishes
        # from this set if pop() hands it out, it's not reregistered via
        # repush(), and it becomes unreachable.
        self.all = weakref.WeakSet()

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
        if timeout < old:
            self._reduce_size()

    def getSize(self):
        return self._size

    def getTimeout(self):
        return self._timeout

    timeout = property(getTimeout, lambda self, v: self.setTimeout(v))

    size = property(getSize, lambda self, v: self.setSize(v))

    def clear(self):
        pass


class ConnectionPool(AbstractConnectionPool):

    def __init__(self, size, timeout=1 << 31):
        super(ConnectionPool, self).__init__(size, timeout)

        # A stack of connections available to hand out.  This is a subset
        # of self.all.  push() and repush() add to this, and may remove
        # the oldest available connections if the pool is too large.
        # pop() pops this stack.  There are never more than size entries
        # in this stack.
        self.available = []

    def __iter__(self):
        return iter(self.all)

    def _append(self, c):
        available = self.available
        cactive = c._cache.cache_non_ghost_count
        if (available
                and (available[-1][1]._cache.cache_non_ghost_count > cactive)):
            i = len(available) - 1
            while (i and
                   (available[i-1][1]._cache.cache_non_ghost_count > cactive)
                   ):
                i -= 1
            available.insert(i, (time.time(), c))
        else:
            available.append((time.time(), c))

    def push(self, c):
        """Register a new available connection.

        We must not know about c already. c will be pushed onto the available
        stack even if we're over the pool size limit.
        """
        assert c not in self.all
        assert c not in self.available
        self._reduce_size(strictly_less=True)
        self.all.add(c)
        self._append(c)
        n = len(self.all)
        limit = self.size
        if n > limit:
            reporter = logger.warning
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
        self._append(c)

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
            assert not c.opened
            self.all.remove(c)
            c._release_resources()

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

    def availableGC(self):
        """Perform garbage collection on available connections.

        If a connection is no longer viable because it has timed out, it is
        garbage collected.
        """
        threshhold = time.time() - self.timeout

        to_remove = ()
        for (t, c) in self.available:
            assert not c.opened
            if t < threshhold:
                to_remove += (c,)
                self.all.remove(c)
                c._release_resources()
            else:
                c.cacheGC()

        if to_remove:
            self.available[:] = [i for i in self.available
                                 if i[1] not in to_remove]

    def clear(self):
        while self.pop():
            pass


class KeyedConnectionPool(AbstractConnectionPool):
    # this pool keeps track of keyed connections all together.  It makes
    # it possible to make assertions about total numbers of keyed connections.
    # The keys in this case are "before" TIDs, but this is used by other
    # packages as well.

    # see the comments in ConnectionPool for method descriptions.

    def __init__(self, size, timeout=1 << 31):
        super(KeyedConnectionPool, self).__init__(size, timeout)
        self.pools = {}

    def __iter__(self):
        return chain(*self.pools.values())

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

    def availableGC(self):
        for key, pool in list(self.pools.items()):
            pool.availableGC()
            if not pool.all:
                del self.pools[key]

    def clear(self):
        for pool in self.pools.values():
            pool.clear()
        self.pools.clear()


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
        before = at.laterThan(at).raw()
    elif before is not None:
        if isinstance(before, datetime.datetime):
            before = toTimeStamp(before).raw()
        else:
            before = TimeStamp(before).raw()
    return before


@implementer(IDatabase)
class DB(object):
    """The Object Database

    The DB class coordinates the activities of multiple database
    Connection instances.  Most of the work is done by the
    Connections created via the open method.

    The DB instance manages a pool of connections.  If a connection is
    closed, it is returned to the pool and its object cache is
    preserved.  A subsequent call to open() will reuse the connection.
    There is no hard limit on the pool size.  If more than `pool_size`
    connections are opened, a warning is logged, and if more than twice
    that many, a critical problem is logged.

    The database provides a few methods intended for application code
    -- open, close, undo, and pack -- and a large collection of
    methods for inspecting the database and its connections' caches.
    """

    klass = Connection  # Class to use for connections
    _activity_monitor = next = previous = None

    #: Database storage, implementing :interface:`~ZODB.interfaces.IStorage`
    storage = valuedoc.ValueDoc('storage object')

    def __init__(self,
                 storage,
                 pool_size=7,
                 pool_timeout=1 << 31,
                 cache_size=400,
                 cache_size_bytes=0,
                 historical_pool_size=3,
                 historical_cache_size=1000,
                 historical_cache_size_bytes=0,
                 historical_timeout=300,
                 database_name='unnamed',
                 databases=None,
                 xrefs=True,
                 large_record_size=1 << 24,
                 **storage_args):
        """Create an object database.

        :param storage: the storage used by the database, such as a
             :class:`~ZODB.FileStorage.FileStorage.FileStorage`.
             This can be a string path name to use a constructed
             :class:`~ZODB.FileStorage.FileStorage.FileStorage`
             storage or ``None`` to use a constructed
             :class:`~ZODB.MappingStorage.MappingStorage`.
        :param int pool_size: expected maximum number of open connections.
             Warnings are logged when this is exceeded and critical
             messages are logged if twice the pool size is exceeded.
        :param seconds pool_timeout: Maximum age of inactive connections
             When a connection has remained unused in a connection
             pool for more than pool_timeout seconds, it will be
             discarded and it's resources released.
        :param objects cache_size: target maximum number of non-ghost
             objects in each connection object cache.
        :param int cache_size_bytes: target total memory usage of non-ghost
             objects in each connection object cache.
        :param int historical_pool_size: expected maximum number of total
            historical connections
        :param objects historical_cache_size: target maximum number
             of non-ghost objects in each historical connection object
             cache.
        :param int historical_cache_size_bytes: target total memory
             usage of non-ghost objects in each historical connection
             object cache.
        :param seconds historical_timeout: Maximum age of inactive
             historical connections.  When a connection has remained
             unused in a historical connection pool for more than pool_timeout
             seconds, it will be discarded and it's resources
             released.
        :param str database_name: The name of this database in a
             multi-database configuration.  The name is used when
             constructing cross-database references ans when accessing
             database connections fron other databases.
        :param dict databases: dictionary of database name to
             databases in a multi-database configuration. The new
             database will add itself to this dictionary. The
             dictionary is used when getting connections in other databases.
        :param boolean xrefs: Flag indicating whether cross-database
            references are allowed from this database to other
            databases in a multi-database configuration.
        :param int large_record_size: When object records are saved
             that are larger than this, a warning is issued,
             suggesting that blobs should be used instead.
        :param storage_args: Extra keywork arguments passed to a
             storage constructor if a path name or None is passed as
             the storage argument.
        """

        # Allocate lock.
        self._lock = utils.RLock()

        # pools and cache sizes
        self.pool = ConnectionPool(pool_size, pool_timeout)
        self.historical_pool = KeyedConnectionPool(historical_pool_size,
                                                   historical_timeout)
        self._cache_size = cache_size
        self._cache_size_bytes = cache_size_bytes
        self._historical_cache_size = historical_cache_size
        self._historical_cache_size_bytes = historical_cache_size_bytes

        # Setup storage
        if isinstance(storage, six.string_types):
            from ZODB import FileStorage  # noqa: F401 import unused
            storage = ZODB.FileStorage.FileStorage(storage, **storage_args)
        elif storage is None:
            from ZODB import MappingStorage  # noqa: F401 import unused
            storage = ZODB.MappingStorage.MappingStorage(**storage_args)
        else:
            assert not storage_args

        self.storage = storage

        if IMVCCStorage.providedBy(storage):
            self._mvcc_storage = storage
        else:
            from .mvccadapter import MVCCAdapter
            self._mvcc_storage = MVCCAdapter(storage)

        self.references = ZODB.serialize.referencesf

        if (not hasattr(storage, 'tpc_vote')) and not storage.isReadOnly():
            warnings.warn(
                "Storage doesn't have a tpc_vote and this violates "
                "the storage API. Violently monkeypatching in a do-nothing "
                "tpc_vote.",
                DeprecationWarning, 2)
            storage.tpc_vote = lambda *args: None

        # Multi-database setup.
        if databases is None:
            databases = {}
        self.databases = databases
        self.database_name = database_name
        if database_name in databases:
            raise ValueError("database_name %r already in databases" %
                             database_name)
        databases[database_name] = self
        self.xrefs = xrefs

        self.large_record_size = large_record_size

        # Make sure we have a root:
        with self.transaction(u'initial database creation') as conn:
            try:
                conn.get(z64)
            except KeyError:
                from persistent.mapping import PersistentMapping
                root = PersistentMapping()
                conn._add(root, z64)

    @property
    def _storage(self):      # Backward compatibility
        return self.storage

    # This is called by Connection.close().
    def _returnToPool(self, connection):
        """Return a connection to the pool.

        connection._db must be self on entry.
        """

        with self._lock:
            assert connection._db is self
            connection.opened = None

            if connection.before:
                self.historical_pool.repush(connection, connection.before)
            else:
                self.pool.repush(connection)

    def _connectionMap(self, f):
        """Call f(c) for all connections c in all pools, live and historical.
        """
        with self._lock:
            for c in self.pool:
                f(c)
            for c in self.historical_pool:
                f(c)

    def cacheDetail(self):
        """Return object counts by class accross all connections.
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
        return sorted(detail.items())

    def cacheExtremeDetail(self):
        """Return information about all of the objects in the object caches.

        Information includes a connection number, class, object id,
        reference count and state.  The reference count returned
        excludes references help by ZODB itself.
        """
        detail = []
        conn_no = [0]  # A mutable reference to a counter
        # sys.getrefcount is a CPython implementation detail
        # not required to exist on, e.g., PyPy.
        rc = getattr(sys, 'getrefcount', None)

        def f(con, detail=detail, rc=rc, conn_no=conn_no):
            conn_no[0] += 1
            cn = conn_no[0]
            for oid, ob in con._cache_items():
                id = ''
                if hasattr(ob, '__dict__'):
                    d = ob.__dict__
                    if 'id' in d:
                        id = d['id']
                    elif '__name__' in d:
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
                # If we have no way to get a refcount, we return False
                # to symbolize that. As opposed to None, this has the
                # advantage of being usable as a number (0) in case
                # clients depended on that.
                detail.append({
                    'conn_no': cn,
                    'oid': oid,
                    'id': id,
                    'klass': "%s%s" % (module, ob.__class__.__name__),
                    'rc': (rc(ob) - 3 - (ob._p_changed is not None)
                           if rc else False),
                    'state': ob._p_changed,
                    # 'references': con.references(oid),
                })

        self._connectionMap(f)
        return detail

    def cacheFullSweep(self):  # XXX this is the same as cacheMinimize
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
        """Minimize cache sizes for all connections
        """
        self._connectionMap(lambda c: c._cache.minimize())

    def cacheSize(self):
        """Return the total count of non-ghost objects in all object caches
        """
        m = [0]

        def f(con, m=m):
            m[0] += con._cache.cache_non_ghost_count

        self._connectionMap(f)
        return m[0]

    def cacheDetailSize(self):
        """Return non-ghost counts sizes for all connections.
        """
        m = []

        def f(con, m=m):
            m.append({'connection': repr(con),
                      'ngsize': con._cache.cache_non_ghost_count,
                      'size': len(con._cache)})
        self._connectionMap(f)
        # Py3: Simulate Python 2 m.sort() functionality.
        return sorted(
            m, key=lambda x: (x['connection'], x['ngsize'], x['size']))

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
        self.close = noop

        @self._connectionMap
        def _(conn):
            if conn.transaction_manager is not None:
                for c in six.itervalues(conn.connections):
                    # Prevent connections from implicitly starting new
                    # transactions.
                    c.explicit_transactions = True
                conn.transaction_manager.abort()
            conn._release_resources()

        self._mvcc_storage.close()
        del self.storage
        del self._mvcc_storage
        # clean up references to other DBs
        self.databases = {}
        # clean up the connection pool
        self.pool.clear()
        self.historical_pool.clear()

    def getCacheSize(self):
        """Get the configured cache size (objects).
        """
        return self._cache_size

    def getCacheSizeBytes(self):
        """Get the configured cache size in bytes.
        """
        return self._cache_size_bytes

    def lastTransaction(self):
        """Get the storage last transaction id.
        """
        return self.storage.lastTransaction()

    def getName(self):
        """Get the storage name
        """
        return self.storage.getName()

    def getPoolSize(self):
        """Get the configured pool size
        """
        return self.pool.size

    def getSize(self):
        """Get the approximate database size, in bytes
        """
        return self.storage.getSize()

    def getHistoricalCacheSize(self):
        """Get the configured historical cache size (objects).
        """
        return self._historical_cache_size

    def getHistoricalCacheSizeBytes(self):
        """Get the configured historical cache size in bytes.
        """
        return self._historical_cache_size_bytes

    def getHistoricalPoolSize(self):
        """Get the configured historical pool size
        """
        return self.historical_pool.size

    def getHistoricalTimeout(self):
        """Get the configured historical pool timeout
        """
        return self.historical_pool.timeout

    transform_record_data = untransform_record_data = lambda self, data: data

    def objectCount(self):
        """Get the approximate object count
        """
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

        if isinstance(transaction_manager, six.string_types):
            if transaction_manager:
                raise TypeError("Versions aren't supported.")
            warnings.warn(
                "A version string was passed to open.\n"
                "The first argument is a transaction manager.",
                DeprecationWarning, 2)
            transaction_manager = None

        with self._lock:
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

            # A good time to do some cache cleanup.
            # (note we already have the lock)
            self.pool.availableGC()
            self.historical_pool.availableGC()

        result.open(transaction_manager)
        return result

    def connectionDebugInfo(self):
        """Get debugging information about connections

        This is especially useful to debug connections that seem to be
        leaking or open too long.  Information includes connection
        info, the connection before setting, and, if a connection is
        open, the time it was opened.  The info is the result of
        calling :meth:`~ZODB.Connection.Connection.getDebugInfo` on
        the connection, and the connection's cache size.
        """
        result = []
        t = time.time()

        def get_info(c):
            # `result`, `time` and `before` are lexically inherited.
            o = c.opened
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
        except:  # noqa: E722 do not use bare 'except'
            logger.exception("packing")
            raise

    def setActivityMonitor(self, am):
        self._activity_monitor = am

    def classFactory(self, connection, modulename, globalname):
        # Zope will rebind this method to arbitrary user code at runtime.
        return find_global(modulename, globalname)

    def setCacheSize(self, size):
        """Reconfigure the cache size (non-ghost object count)
        """
        with self._lock:
            self._cache_size = size
            for c in self.pool:
                c._cache.cache_size = size

    def setCacheSizeBytes(self, size):
        """Reconfigure the cache total size in bytes
        """
        with self._lock:
            self._cache_size_bytes = size
            for c in self.pool:
                c._cache.cache_size_bytes = size

    def setHistoricalCacheSize(self, size):
        """Reconfigure the historical cache size (non-ghost object count)
        """
        with self._lock:
            self._historical_cache_size = size
            for c in self.historical_pool:
                c._cache.cache_size = size

    def setHistoricalCacheSizeBytes(self, size):
        """Reconfigure the historical cache total size in bytes
        """
        with self._lock:
            self._historical_cache_size_bytes = size
            for c in self.historical_pool:
                c._cache.cache_size_bytes = size

    def setPoolSize(self, size):
        """Reconfigure the connection pool size
        """
        with self._lock:
            self.pool.size = size

    def setHistoricalPoolSize(self, size):
        """Reconfigure the connection historical pool size
        """
        with self._lock:
            self.historical_pool.size = size

    def setHistoricalTimeout(self, timeout):
        """Reconfigure the connection historical pool timeout
        """
        with self._lock:
            self.historical_pool.timeout = timeout

    def history(self, oid, size=1):
        """Get revision history information for an object.

        See :meth:`ZODB.interfaces.IStorage.history`.
        """
        return _text_transaction_info(self.storage.history(oid, size))

    def supportsUndo(self):
        """Return whether the database supports undo.
        """
        try:
            f = self.storage.supportsUndo
        except AttributeError:
            return False
        return f()

    def undoLog(self, *args, **kw):
        """Return a sequence of descriptions for transactions.

        See :meth:`ZODB.interfaces.IStorageUndoable.undoLog`.
        """

        if not self.supportsUndo():
            return ()
        return _text_transaction_info(self.storage.undoLog(*args, **kw))

    def undoInfo(self, *args, **kw):
        """Return a sequence of descriptions for transactions.

        See :meth:`ZODB.interfaces.IStorageUndoable.undoInfo`.
        """
        if not self.supportsUndo():
            return ()
        return _text_transaction_info(self.storage.undoInfo(*args, **kw))

    def undoMultiple(self, ids, txn=None):
        """Undo multiple transactions identified by ids.

        A transaction can be undone if all of the objects involved in
        the transaction were not modified subsequently, if any
        modifications can be resolved by conflict resolution, or if
        subsequent changes resulted in the same object state.

        The values in ids should be generated by calling undoLog()
        or undoInfo().  The value of ids are not the same as a
        transaction ids used by other methods; they are unique to undo().

        :Parameters:
          - `ids`: a sequence of storage-specific transaction identifiers
          - `txn`: transaction context to use for undo().
            By default, uses the current transaction.
        """
        if not self.supportsUndo():
            raise NotImplementedError
        if txn is None:
            txn = transaction.get()
        if isinstance(ids, six.string_types):
            ids = [ids]
        txn.join(TransactionalUndo(self, ids))

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
          - `id`: a transaction identifier
          - `txn`: transaction context to use for undo().
            By default, uses the current transaction.
        """
        self.undoMultiple([id], txn)

    def transaction(self, note=None):
        """Execute a block of code as a transaction.

        If a note is given, it will be added to the transaction's
        description.

        The ``transaction`` method returns a context manager that can
        be used with the ``with`` statement.
        """
        return ContextManager(self, note)

    def new_oid(self):
        """
        Return a new oid from the storage.

        Kept for backwards compatibility only. New oids should be
        allocated in a transaction using an open Connection.
        """
        return self.storage.new_oid()  # pragma: no cover

    def open_then_close_db_when_connection_closes(self):
        """Create and return a connection.

        When the connection closes, the database will close too.
        """
        conn = self.open()
        conn.onCloseCallback(self.close)
        return conn


class ContextManager(object):
    """PEP 343 context manager
    """

    def __init__(self, db, note=None):
        self.db = db
        self.note = note

    def __enter__(self):
        self.tm = tm = transaction.TransactionManager()
        self.conn = self.db.open(self.tm)
        t = tm.begin()
        if self.note:
            t.note(self.note)
        return self.conn

    def __exit__(self, t, v, tb):
        if t is None:
            self.tm.commit()
        else:
            self.tm.abort()
        self.conn.close()


resource_counter_lock = utils.Lock()
resource_counter = 0


class TransactionalUndo(object):

    def __init__(self, db, tids):
        self._db = db
        self._tids = tids
        self._storage = None

    def abort(self, transaction):
        pass

    def close(self):
        if self._storage is not None:
            # We actually want to release the storage we've created,
            # not close it. releasing it frees external resources
            # dedicated to this instance, closing might make permanent
            # changes that affect other instances.
            self._storage.release()
            self._storage = None

    def tpc_begin(self, transaction):
        assert self._storage is None, "Already in an active transaction"

        tdata = TransactionMetaData(
            transaction.user,
            transaction.description,
            transaction.extension)
        transaction.set_data(self, tdata)
        # `undo_instance` is not part of any IStorage interface;
        # it is defined in our MVCCAdapter. Regardless, we're opening
        # a new storage instance, and so we must close it to be sure
        # to reclaim resources in a timely manner.
        #
        # Once the tpc_begin method has been called, the transaction manager
        # will guarantee to call either `tpc_finish` or `tpc_abort`, so those
        # are the only methods we need to be concerned about calling close()
        # from.
        db_mvcc_storage = self._db._mvcc_storage
        self._storage = getattr(
            db_mvcc_storage,
            'undo_instance',
            db_mvcc_storage.new_instance)()

        self._storage.tpc_begin(tdata)

    def commit(self, transaction):
        transaction = transaction.data(self)
        for tid in self._tids:
            self._storage.undo(tid, transaction)

    def tpc_vote(self, transaction):
        transaction = transaction.data(self)
        self._storage.tpc_vote(transaction)

    def tpc_finish(self, transaction):
        try:
            transaction = transaction.data(self)
            self._storage.tpc_finish(transaction)
        finally:
            self.close()

    def tpc_abort(self, transaction):
        try:
            transaction = transaction.data(self)
            self._storage.tpc_abort(transaction)
        finally:
            self.close()

    def sortKey(self):
        # The transaction sorts data managers first before it calls
        # `tpc_begin`, so we can't use our own storage because it's
        # not open yet. Fortunately new_instances of a storage are
        # supposed to return the same sort key as the original storage
        # did.
        return "%s:%s" % (self._db._mvcc_storage.sortKey(), id(self))


def connection(*args, **kw):
    """Create a database :class:`connection <ZODB.Connection.Connection>`.

    A database is created using the given arguments and opened to
    create the returned connection. The database will be closed when
    the connection is closed.  This is a convenience function to avoid
    managing a separate database object.
    """
    return DB(*args, **kw).open_then_close_db_when_connection_closes()


_transaction_meta_data_text_variables = 'user_name', 'description'


def _text_transaction_info(info):
    for d in info:
        for name in _transaction_meta_data_text_variables:
            if name in d:
                d[name] = d[name].decode('utf-8')

    return info
