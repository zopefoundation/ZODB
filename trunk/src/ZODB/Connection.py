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
"""Database connection support

$Id: Connection.py,v 1.103 2003/12/08 21:12:25 jeremy Exp $"""

import sys
import threading
from time import time

from persistent import PickleCache
from zLOG import LOG, ERROR, BLATHER, WARNING

from ZODB.ConflictResolution import ResolvedSerial
from ZODB.coptimizations import new_persistent_id
from ZODB.ExportImport import ExportImport
from ZODB.POSException \
     import ConflictError, ReadConflictError, TransactionError
from ZODB.TmpStore import TmpStore
from ZODB.Transaction import Transaction, get_transaction
from ZODB.utils import oid_repr, z64
from ZODB.serialize \
     import ObjectWriter, getClassMetadata, ConnectionObjectReader

global_code_timestamp = 0

def updateCodeTimestamp():
    """Called after changes are made to persistence-based classes.

    Causes all connection caches to be re-created as the connections are
    reopened.
    """
    global global_code_timestamp
    global_code_timestamp = time()

class Connection(ExportImport, object):
    """Object managers for individual object space.

    An object space is a version of collection of objects.  In a
    multi-threaded application, each thread gets its own object space.

    The Connection manages movement of objects in and out of object storage.
    """
    _tmp=None
    _debug_info=()
    _opened=None
    _code_timestamp = 0
    _transaction = None

    # Experimental. Other connections can register to be closed
    # when we close by putting something here.

    def __init__(self, version='', cache_size=400,
                 cache_deactivate_after=60):
        """Create a new Connection"""
        self._version=version
        self._cache = cache = PickleCache(self, cache_size)
        if version:
            # Caches for versions end up empty if the version
            # is not used for a while. Non-version caches
            # keep their content indefinitely.

            # XXX Why do we want version caches to behave this way?

            self._cache.cache_drain_resistance = 100
        self._incrgc = self.cacheGC = cache.incrgc
        self._committed = []
        self._code_timestamp = global_code_timestamp
        self._load_count = 0   # Number of objects unghosted
        self._store_count = 0  # Number of objects stored

        # _invalidated queues invalidate messages delivered from the DB
        # _inv_lock prevents one thread from modifying the set while
        # another is processing invalidations.  All the invalidations
        # from a single transaction should be applied atomically, so
        # the lock must be held when reading _invalidated.

        # XXX It sucks that we have to hold the lock to read
        # _invalidated.  Normally, _invalidated is written by call
        # dict.update, which will execute atomically by virtue of the
        # GIL.  But some storage might generate oids where hash or
        # compare invokes Python code.  In that case, the GIL can't
        # save us.
        self._inv_lock = threading.Lock()
        self._invalidated = d = {}
        self._invalid = d.has_key
        self._conflicts = {}

    def getTransaction(self):
        t = self._transaction
        if t is None:
            # Fall back to thread-bound transactions
            t = get_transaction()
        return t

    def setLocalTransaction(self):
        """Use a transaction bound to the connection rather than the thread"""
        if self._transaction is None:
            self._transaction = Transaction()
        return self._transaction

    def _cache_items(self):
        # find all items on the lru list
        items = self._cache.lru_items()
        # fine everything. some on the lru list, some not
        everything = self._cache.cache_data
        # remove those items that are on the lru list
        for k,v in items:
            del everything[k]
        # return a list of [ghosts....not recently used.....recently used]
        return everything.items() + items

    def __repr__(self):
        if self._version:
            ver = ' (in version %s)' % `self._version`
        else:
            ver = ''
        return '<Connection at %08x%s>' % (id(self), ver)

    def __getitem__(self, oid):
        obj = self._cache.get(oid, None)
        if obj is not None:
            return obj

        p, serial = self._storage.load(oid, self._version)
        obj = self._reader.getGhost(p)

        obj._p_oid = oid
        obj._p_jar = self
        obj._p_changed = None
        obj._p_serial = serial

        self._cache[oid] = obj
        return obj

    def sortKey(self):
        # XXX will raise an exception if the DB hasn't been set
        storage_key = self._sortKey()
        # If two connections use the same storage, give them a
        # consistent order using id().  This is unique for the
        # lifetime of a connection, which is good enough.
        return "%s:%s" % (storage_key, id(self))

    def _setDB(self, odb):
        """Begin a new transaction.

        Any objects modified since the last transaction are invalidated.
        """
        self._db = odb
        self._storage = odb._storage
        self._sortKey = odb._storage.sortKey
        self.new_oid = odb._storage.new_oid
        if self._code_timestamp != global_code_timestamp:
            # New code is in place.  Start a new cache.
            self._resetCache()
        else:
            self._flush_invalidations()
        self._reader = ConnectionObjectReader(self, self._cache,
                                              self._db._classFactory)
        self._opened = time()

        return self

    def _resetCache(self):
        """Creates a new cache, discarding the old."""
        self._code_timestamp = global_code_timestamp
        self._invalidated.clear()
        cache_size = self._cache.cache_size
        self._cache = cache = PickleCache(self, cache_size)
        self._incrgc = self.cacheGC = cache.incrgc

    def abort(self, object, transaction):
        """Abort the object in the transaction.

        This just deactivates the thing.
        """
        if object is self:
            self._flush_invalidations()
        else:
            assert object._p_oid is not None
            self._cache.invalidate(object._p_oid)

    def cacheFullSweep(self, dt=0):
        self._cache.full_sweep(dt)

    def cacheMinimize(self, dt=0):
        # dt is ignored
        self._cache.minimize()

    __onCloseCallbacks = None

    def onCloseCallback(self, f):
        if self.__onCloseCallbacks is None:
            self.__onCloseCallbacks = []
        self.__onCloseCallbacks.append(f)

    def close(self):
        if self._incrgc is not None:
            self._incrgc() # This is a good time to do some GC

        # Call the close callbacks.
        if self.__onCloseCallbacks is not None:
            for f in self.__onCloseCallbacks:
                try: f()
                except:
                    f=getattr(f, 'im_self', f)
                    LOG('ZODB',ERROR, 'Close callback failed for %s' % f,
                        error=sys.exc_info())
            self.__onCloseCallbacks = None
        self._storage = self._tmp = self.new_oid = self._opened = None
        self._debug_info = ()
        # Return the connection to the pool.
        self._db._closeConnection(self)

    __onCommitActions = None

    def onCommitAction(self, method_name, *args, **kw):
        if self.__onCommitActions is None:
            self.__onCommitActions = []
        self.__onCommitActions.append((method_name, args, kw))
        self.getTransaction().register(self)

    def commit(self, object, transaction):
        if object is self:
            # We registered ourself.  Execute a commit action, if any.
            if self.__onCommitActions is not None:
                method_name, args, kw = self.__onCommitActions.pop(0)
                getattr(self, method_name)(transaction, *args, **kw)
            return

        oid = object._p_oid
        if self._conflicts.has_key(oid):
            self.getTransaction().register(object)
            raise ReadConflictError(object=object)

        invalid = self._invalid
        if oid is None or object._p_jar is not self:
            # new object
            oid = self.new_oid()
            object._p_jar = self
            object._p_oid = oid
            self._creating.append(oid)

        elif object._p_changed:
            if invalid(oid):
                resolve = getattr(object, "_p_resolveConflict", None)
                if resolve is None:
                    raise ConflictError(object=object)
            self._modified.append(oid)

        else:
            # Nothing to do
            return

        w = ObjectWriter(object)
        for obj in w:
            oid = obj._p_oid
            serial = getattr(obj, '_p_serial', z64)
            if serial == z64:
                # new object
                self._creating.append(oid)
            else:
                #XXX We should never get here
                if invalid(oid) and not hasattr(object, '_p_resolveConflict'):
                    raise ConflictError(object=obj)
                self._modified.append(oid)

            p = w.serialize(obj)
            s = self._storage.store(oid, serial, p, self._version, transaction)
            self._store_count = self._store_count + 1
            # Put the object in the cache before handling the
            # response, just in case the response contains the
            # serial number for a newly created object
            try:
                self._cache[oid] = obj
            except:
                # Dang, I bet its wrapped:
                if hasattr(obj, 'aq_base'):
                    self._cache[oid] = obj.aq_base
                else:
                    raise

            self._handle_serial(s, oid)

    def commit_sub(self, t):
        """Commit all work done in subtransactions"""
        tmp=self._tmp
        if tmp is None: return
        src=self._storage

        LOG('ZODB', BLATHER,
            'Commiting subtransaction of size %s' % src.getSize())

        self._storage=tmp
        self._tmp=None

        tmp.tpc_begin(t)

        load=src.load
        store=tmp.store
        dest=self._version
        oids=src._index.keys()

        # Copy invalidating and creating info from temporary storage:
        modified = self._modified
        modified[len(modified):] = oids
        creating = self._creating
        creating[len(creating):]=src._creating

        for oid in oids:
            data, serial = load(oid, src)
            s=store(oid, serial, data, dest, t)
            self._handle_serial(s, oid, change=0)

    def abort_sub(self, t):
        """Abort work done in subtransactions"""
        tmp=self._tmp
        if tmp is None: return
        src=self._storage
        self._tmp=None
        self._storage=tmp

        self._cache.invalidate(src._index.keys())
        self._invalidate_creating(src._creating)

    def _invalidate_creating(self, creating=None):
        """Dissown any objects newly saved in an uncommitted transaction.
        """
        if creating is None:
            creating=self._creating
            self._creating=[]

        cache=self._cache
        cache_get=cache.get
        for oid in creating:
            o=cache_get(oid, None)
            if o is not None:
                del cache[oid]
                del o._p_jar
                del o._p_oid

    def db(self):
        return self._db

    def getVersion(self):
        return self._version

    def isReadOnly(self):
        return self._storage.isReadOnly()

    def invalidate(self, oids):
        """Invalidate a set of oids.

        This marks the oid as invalid, but doesn't actually invalidate
        it.  The object data will be actually invalidated at certain
        transaction boundaries.
        """
        self._inv_lock.acquire()
        try:
            self._invalidated.update(oids)
        finally:
            self._inv_lock.release()

    def _flush_invalidations(self):
        self._inv_lock.acquire()
        try:
            self._cache.invalidate(self._invalidated)
            self._invalidated.clear()
        finally:
            self._inv_lock.release()
        # Now is a good time to collect some garbage
        self._cache.incrgc()

    def modifiedInVersion(self, oid):
        try: return self._db.modifiedInVersion(oid)
        except KeyError:
            return self._version

    def register(self, object):
        """Register an object with the appropriate transaction manager.

        A subclass could override this method to customize the default
        policy of one transaction manager for each thread.
        """
        assert object._p_jar is self
        # XXX Figure out why this assert causes test failures
        # assert object._p_oid is not None
        self.getTransaction().register(object)

    def root(self):
        return self[z64]

    def setstate(self, obj):
        oid = obj._p_oid

        if self._storage is None:
            msg = ("Shouldn't load state for %s "
                   "when the connection is closed" % oid_repr(oid))
            LOG('ZODB', ERROR, msg)
            raise RuntimeError(msg)

        try:
            # Avoid reading data from a transaction that committed
            # after the current transaction started, as that might
            # lead to mixing of cached data from earlier transactions
            # and new inconsistent data.
            #
            # Wait for check until after data is loaded from storage
            # to avoid time-of-check to time-of-use race.
            p, serial = self._storage.load(oid, self._version)
            self._load_count = self._load_count + 1
            invalid = self._is_invalidated(obj)
            self._reader.setGhostState(obj, p)
            obj._p_serial = serial
            if invalid:
                self._handle_independent(obj)
        except ConflictError:
            raise
        except:
            LOG('ZODB', ERROR,
                "Couldn't load state for %s" % oid_repr(oid),
                error=sys.exc_info())
            raise

    def _is_invalidated(self, obj):
        # Helper method for setstate() covers three cases:
        # returns false if obj is valid
        # returns true if obj was invalidation, but is independent
        # otherwise, raises ConflictError for invalidated objects
        self._inv_lock.acquire()
        try:
            if self._invalidated.has_key(obj._p_oid):
                # Defer _p_independent() call until state is loaded.
                ind = getattr(obj, "_p_independent", None)
                if ind is not None:
                    # Defer _p_independent() call until state is loaded.
                    return 1
                else:
                    self.getTransaction().register(obj)
                    self._conflicts[obj._p_oid] = 1
                    raise ReadConflictError(object=obj)
            else:
                return 0
        finally:
            self._inv_lock.release()

    def _handle_independent(self, obj):
        # Helper method for setstate() handles possibly independent objects
        # Call _p_independent(), if it returns True, setstate() wins.
        # Otherwise, raise a ConflictError.

        if obj._p_independent():
            self._inv_lock.acquire()
            try:
                try:
                    del self._invalidated[obj._p_oid]
                except KeyError:
                    pass
            finally:
                self._inv_lock.release()
        else:
            self.getTransaction().register(obj)
            raise ReadConflictError(object=obj)

    def oldstate(self, obj, serial):
        p = self._storage.loadSerial(obj._p_oid, serial)
        return self._reader.getState(p)

    def setklassstate(self, obj):
        # Special case code to handle ZClasses, I think.
        # Called the cache when an object of type type is invalidated.
        try:
            oid = obj._p_oid
            p, serial = self._storage.load(oid, self._version)

            # We call getGhost(), but we actually get a non-ghost back.
            # The object is a class, which can't actually be ghosted.
            copy = self._reader.getGhost(p)
            obj.__dict__.clear()
            obj.__dict__.update(copy.__dict__)

            obj._p_oid = oid
            obj._p_jar = self
            obj._p_changed = 0
            obj._p_serial = serial
        except:
            LOG('ZODB',ERROR, 'setklassstate failed', error=sys.exc_info())
            raise

    def tpc_abort(self, transaction):
        if self.__onCommitActions is not None:
            del self.__onCommitActions
        self._storage.tpc_abort(transaction)
        self._cache.invalidate(self._modified)
        self._flush_invalidations()
        self._conflicts.clear()
        self._invalidate_creating()

    def tpc_begin(self, transaction, sub=None):
        self._modified = []
        self._creating = []
        if sub:
            # Sub-transaction!
            if self._tmp is None:
                _tmp = TmpStore(self._version)
                self._tmp = self._storage
                self._storage = _tmp
                _tmp.registerDB(self._db, 0)

        self._storage.tpc_begin(transaction)

    def tpc_vote(self, transaction):
        if self.__onCommitActions is not None:
            del self.__onCommitActions
        try:
            vote = self._storage.tpc_vote
        except AttributeError:
            return
        s = vote(transaction)
        self._handle_serial(s)

    def _handle_serial(self, store_return, oid=None, change=1):
        """Handle the returns from store() and tpc_vote() calls."""

        # These calls can return different types depending on whether
        # ZEO is used.  ZEO uses asynchronous returns that may be
        # returned in batches by the ClientStorage.  ZEO1 can also
        # return an exception object and expect that the Connection
        # will raise the exception.

        # When commit_sub() exceutes a store, there is no need to
        # update the _p_changed flag, because the subtransaction
        # tpc_vote() calls already did this.  The change=1 argument
        # exists to allow commit_sub() to avoid setting the flag
        # again.

        # When conflict resolution occurs, the object state held by
        # the connection does not match what is written to the
        # database.  Invalidate the object here to guarantee that
        # the new state is read the next time the object is used.

        if not store_return:
            return
        if isinstance(store_return, str):
            assert oid is not None
            self._handle_one_serial(oid, store_return, change)
        else:
            for oid, serial in store_return:
                self._handle_one_serial(oid, serial, change)

    def _handle_one_serial(self, oid, serial, change):
        if not isinstance(serial, str):
            raise serial
        obj = self._cache.get(oid, None)
        if obj is None:
            return
        if serial == ResolvedSerial:
            del obj._p_changed # transition from changed to ghost
        else:
            if change:
                obj._p_changed = 0 # trans. from changed to uptodate
            obj._p_serial = serial

    def tpc_finish(self, transaction):
        # It's important that the storage call the function we pass
        # while it still has it's lock.  We don't want another thread
        # to be able to read any updated data until we've had a chance
        # to send an invalidation message to all of the other
        # connections!

        if self._tmp is not None:
            # Commiting a subtransaction!
            # There is no need to invalidate anything.
            self._storage.tpc_finish(transaction)
            self._storage._creating[:0]=self._creating
            del self._creating[:]
        else:
            def callback():
                d = {}
                for oid in self._modified:
                    d[oid] = 1
                self._db.invalidate(d, self)
            self._storage.tpc_finish(transaction, callback)

        self._conflicts.clear()
        self._flush_invalidations()

    def sync(self):
        self.getTransaction().abort()
        sync=getattr(self._storage, 'sync', 0)
        if sync != 0: sync()
        self._flush_invalidations()

    def getDebugInfo(self):
        return self._debug_info

    def setDebugInfo(self, *args):
        self._debug_info = self._debug_info + args

    def getTransferCounts(self, clear=0):
        """Returns the number of objects loaded and stored.

        Set the clear argument to reset the counters.
        """
        res = self._load_count, self._store_count
        if clear:
            self._load_count = 0
            self._store_count = 0
        return res


    ######################################################################
    # Just plain weird. Don't try this at home kids.
    def exchange(self, old, new):
        oid=old._p_oid
        new._p_oid=oid
        new._p_jar=self
        new._p_changed=1
        self.getTransaction().register(new)
        self._cache[oid]=new
