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
"""Database connection support

$Id$"""

import logging
import sys
import threading
import warnings
from time import time

from persistent import PickleCache

# interfaces
from persistent.interfaces import IPersistentDataManager 
from ZODB.interfaces import IConnection 
from transaction.interfaces import IDataManager
from zope.interface import implements

import transaction

from ZODB.ConflictResolution import ResolvedSerial
from ZODB.ExportImport import ExportImport
from ZODB.POSException \
     import ConflictError, ReadConflictError, InvalidObjectReference, \
            ConnectionStateError
from ZODB.TmpStore import TmpStore
from ZODB.serialize import ObjectWriter, ObjectReader, myhasattr
from ZODB.utils import u64, oid_repr, z64, positive_id, \
        DEPRECATED_ARGUMENT, deprecated36

global_reset_counter = 0

def resetCaches():
    """Causes all connection caches to be reset as connections are reopened.

    Zope's refresh feature uses this.  When you reload Python modules,
    instances of classes continue to use the old class definitions.
    To use the new code immediately, the refresh feature asks ZODB to
    clear caches by calling resetCaches().  When the instances are
    loaded by subsequent connections, they will use the new class
    definitions.
    """
    global global_reset_counter
    global_reset_counter += 1

class Connection(ExportImport, object):
    """Connection to ZODB for loading and storing objects."""

    implements(IConnection, IDataManager, IPersistentDataManager)

    _tmp = None
    _code_timestamp = 0

    # ZODB.IConnection

    def __init__(self, version='', cache_size=400,
                 cache_deactivate_after=None, mvcc=True, txn_mgr=None,
                 synch=True):
        """Create a new Connection."""
        self._log = logging.getLogger("ZODB.Connection")
        self._storage = None
        self._debug_info = ()
        self._opened = None # time.time() when DB.open() opened us

        self._version = version
        self._cache = cache = PickleCache(self, cache_size)
        if version:
            # Caches for versions end up empty if the version
            # is not used for a while. Non-version caches
            # keep their content indefinitely.
            # Unclear:  Why do we want version caches to behave this way?

            self._cache.cache_drain_resistance = 100
        self._committed = []
        self._added = {}
        self._added_during_commit = None
        self._reset_counter = global_reset_counter
        self._load_count = 0   # Number of objects unghosted
        self._store_count = 0  # Number of objects stored

        # List of oids of modified objects (to be invalidated on an abort).
        self._modified = []

        # List of all objects (not oids) registered as modified by the
        # persistence machinery.
        self._registered_objects = []

        # Do we need to join a txn manager?
        self._needs_to_join = True

        # If a transaction manager is passed to the constructor, use
        # it instead of the global transaction manager.  The instance
        # variable will hold a TM instance.
        self._txn_mgr = txn_mgr or transaction.manager
        # _synch is a boolean; if True, the Connection will register
        # with the TM to receive afterCompletion() calls.
        self._synch = synch

        # _invalidated queues invalidate messages delivered from the DB
        # _inv_lock prevents one thread from modifying the set while
        # another is processing invalidations.  All the invalidations
        # from a single transaction should be applied atomically, so
        # the lock must be held when reading _invalidated.

        # It sucks that we have to hold the lock to read _invalidated.
        # Normally, _invalidated is written by calling dict.update, which
        # will execute atomically by virtue of the GIL.  But some storage
        # might generate oids where hash or compare invokes Python code.  In
        # that case, the GIL can't save us.
        self._inv_lock = threading.Lock()
        self._invalidated = d = {}
        self._invalid = d.has_key

        # We intend to prevent committing a transaction in which
        # ReadConflictError occurs.  _conflicts is the set of oids that
        # experienced ReadConflictError.  Any time we raise ReadConflictError,
        # the oid should be added to this set, and we should be sure that the
        # object is registered.  Because it's registered, Connection.commit()
        # will raise ReadConflictError again (because the oid is in
        # _conflicts).
        self._conflicts = {}

        # If MVCC is enabled, then _mvcc is True and _txn_time stores
        # the upper bound on transactions visible to this connection.
        # That is, all object revisions must be written before _txn_time.
        # If it is None, then the current revisions are acceptable.
        # If the connection is in a version, mvcc will be disabled, because
        # loadBefore() only returns non-version data.
        self._mvcc = mvcc and not version
        self._txn_time = None

        # To support importFile(), implemented in the ExportImport base
        # class, we need to run _importDuringCommit() from our commit()
        # method.  If _import is not None, it is a two-tuple of arguments
        # to pass to _importDuringCommit().
        self._import = None

        self.connections = None

    def get_connection(self, database_name):
        """Return a Connection for the named database."""
        connection = self.connections.get(database_name)
        if connection is None:
            new_con = self._db.databases[database_name].open()
            self.connections.update(new_con.connections)
            new_con.connections = self.connections
            connection = new_con
        return connection

    def get(self, oid):
        """Return the persistent object with oid 'oid'."""
        if self._storage is None:
            raise ConnectionStateError("The database connection is closed")

        obj = self._cache.get(oid, None)
        if obj is not None:
            return obj
        obj = self._added.get(oid, None)
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

    def add(self, obj):
        """Add a new object 'obj' to the database and assign it an oid."""
        if self._storage is None:
            raise ConnectionStateError("The database connection is closed")

        marker = object()
        oid = getattr(obj, "_p_oid", marker)
        if oid is marker:
            raise TypeError("Only first-class persistent objects may be"
                            " added to a Connection.", obj)
        elif obj._p_jar is None:
            assert obj._p_oid is None
            oid = obj._p_oid = self._storage.new_oid()
            obj._p_jar = self
            if self._added_during_commit is not None:
                self._added_during_commit.append(obj)
            self._register(obj)
            # Add to _added after calling register(), so that _added
            # can be used as a test for whether the object has been
            # registered with the transaction.
            self._added[oid] = obj
        elif obj._p_jar is not self:
            raise InvalidObjectReference(obj, obj._p_jar)

    def sortKey(self):
        """Return a consistent sort key for this connection."""
        return "%s:%s" % (self._storage.sortKey(), id(self))

    def abort(self, transaction):
        """Abort a transaction and forget all changes."""
        for obj in self._registered_objects:
            oid = obj._p_oid
            assert oid is not None
            if oid in self._added:
                del self._added[oid]
                del obj._p_jar
                del obj._p_oid
            else:
                self._cache.invalidate(oid)

        self._tpc_cleanup()

    # TODO: we should test what happens when cacheGC is called mid-transaction.

    def cacheGC(self):
        """Reduce cache size to target size."""
        self._cache.incrgc()

    __onCloseCallbacks = None

    def onCloseCallback(self, f):
        """Register a callable, f, to be called by close()."""
        if self.__onCloseCallbacks is None:
            self.__onCloseCallbacks = []
        self.__onCloseCallbacks.append(f)

    def close(self):
        """Close the Connection."""
        if not self._needs_to_join:
            # We're currently joined to a transaction.
            raise ConnectionStateError("Cannot close a connection joined to "
                                       "a transaction")

        if self._tmp is not None:
            # There are no direct modifications pending, but a subtransaction
            # is pending.
            raise ConnectionStateError("Cannot close a connection with a "
                                       "pending subtransaction")

        if self._cache is not None:
            self._cache.incrgc() # This is a good time to do some GC

        # Call the close callbacks.
        if self.__onCloseCallbacks is not None:
            for f in self.__onCloseCallbacks:
                try:
                    f()
                except: # except what?
                    f = getattr(f, 'im_self', f)
                    self._log.error("Close callback failed for %s", f,
                                    exc_info=sys.exc_info())
            self.__onCloseCallbacks = None
        self._storage = self._tmp = self.new_oid = None
        self._debug_info = ()
        self._opened = None
        # Return the connection to the pool.
        if self._db is not None:
            if self._synch:
                self._txn_mgr.unregisterSynch(self)
            self._db._closeConnection(self)
            # _closeConnection() set self._db to None.  However, we can't
            # assert that here, because self may have been reused (by
            # another thread) by the time we get back here.

    # transaction.interfaces.IDataManager

    def commit(self, transaction):
        """Commit changes to an object"""
        if self._import:
            # TODO:  This code seems important for Zope, but needs docs
            # to explain why.
            self._importDuringCommit(transaction, *self._import)
            self._import = None

        # Just in case an object is added as a side-effect of storing
        # a modified object.  If, for example, a __getstate__() method
        # calls add(), the newly added objects will show up in
        # _added_during_commit.  This sounds insane, but has actually
        # happened.

        self._added_during_commit = []

        for obj in self._registered_objects:
            oid = obj._p_oid
            assert oid
            if oid in self._conflicts:
                raise ReadConflictError(object=obj)

            if obj._p_jar is not self:
                raise InvalidObjectReference(obj, obj._p_jar)
            elif oid in self._added:
                assert obj._p_serial == z64
            elif obj._p_changed:
                if oid in self._invalidated:
                    resolve = getattr(obj, "_p_resolveConflict", None)
                    if resolve is None:
                        raise ConflictError(object=obj)
                self._modified.append(oid)
            else:
                # Nothing to do.  It's been said that it's legal, e.g., for
                # an object to set _p_changed to false after it's been
                # changed and registered.
                continue

            self._store_objects(ObjectWriter(obj), transaction)

        for obj in self._added_during_commit:
            self._store_objects(ObjectWriter(obj), transaction)
        self._added_during_commit = None

    def _store_objects(self, writer, transaction):
        for obj in writer:
            oid = obj._p_oid
            serial = getattr(obj, "_p_serial", z64)

            if serial == z64:
                # obj is a new object
                self._creating.append(oid)
                # Because obj was added, it is now in _creating, so it can
                # be removed from _added.
                self._added.pop(oid, None)
            else:
                if (oid in self._invalidated
                    and not hasattr(obj, '_p_resolveConflict')):
                    raise ConflictError(object=obj)
                self._modified.append(oid)
            p = writer.serialize(obj)  # This calls __getstate__ of obj
            s = self._storage.store(oid, serial, p, self._version, transaction)
            self._store_count += 1
            # Put the object in the cache before handling the
            # response, just in case the response contains the
            # serial number for a newly created object
            try:
                self._cache[oid] = obj
            except:
                # Dang, I bet it's wrapped:
                # TODO:  Deprecate, then remove, this.
                if hasattr(obj, 'aq_base'):
                    self._cache[oid] = obj.aq_base
                else:
                    raise

            self._handle_serial(s, oid)

    def commit_sub(self, t):
        """Commit all changes made in subtransactions and begin 2-phase commit
        """
        if self._tmp is None:
            return
        src = self._storage
        self._storage = self._tmp
        self._tmp = None

        self._log.debug("Commiting subtransaction of size %s", src.getSize())
        oids = src._index.keys()
        self._storage.tpc_begin(t)

        # Copy invalidating and creating info from temporary storage:
        self._modified.extend(oids)
        self._creating.extend(src._creating)

        for oid in oids:
            data, serial = src.load(oid, src)
            s = self._storage.store(oid, serial, data, self._version, t)
            self._handle_serial(s, oid, change=False)

    def abort_sub(self, t):
        """Discard all subtransaction data."""
        if self._tmp is None:
            return
        src = self._storage
        self._storage = self._tmp
        self._tmp = None

        self._cache.invalidate(src._index.keys())
        self._invalidate_creating(src._creating)

    def _invalidate_creating(self, creating=None):
        """Disown any objects newly saved in an uncommitted transaction."""
        if creating is None:
            creating = self._creating
            self._creating = []

        for oid in creating:
            o = self._cache.get(oid)
            if o is not None:
                del self._cache[oid]
                del o._p_jar
                del o._p_oid

    # The next two methods are callbacks for transaction synchronization.

    def beforeCompletion(self, txn):
        # We don't do anything before a commit starts.
        pass

    def afterCompletion(self, txn):
        self._flush_invalidations()

    def _flush_invalidations(self):
        self._inv_lock.acquire()
        try:
            self._cache.invalidate(self._invalidated)
            self._invalidated.clear()
            self._txn_time = None
        finally:
            self._inv_lock.release()
        # Now is a good time to collect some garbage
        self._cache.incrgc()

    def root(self):
        """Return the database root object."""
        return self.get(z64)

    def db(self):
        """Returns a handle to the database this connection belongs to."""
        return self._db

    def isReadOnly(self):
        """Returns True if the storage for this connection is read only."""
        if self._storage is None:
            raise ConnectionStateError("The database connection is closed")
        return self._storage.isReadOnly()

    def invalidate(self, tid, oids):
        """Notify the Connection that transaction 'tid' invalidated oids."""
        self._inv_lock.acquire()
        try:
            if self._txn_time is None:
                self._txn_time = tid
            self._invalidated.update(oids)
        finally:
            self._inv_lock.release()

    # IDataManager

    def tpc_begin(self, transaction, sub=False):
        """Begin commit of a transaction, starting the two-phase commit."""
        self._modified = []

        # _creating is a list of oids of new objects, which is used to
        # remove them from the cache if a transaction aborts.
        self._creating = []
        if sub and self._tmp is None:
            # Sub-transaction!
            self._tmp = self._storage
            self._storage = TmpStore(self._version, self._storage)

        self._storage.tpc_begin(transaction)

    def tpc_vote(self, transaction):
        """Verify that a data manager can commit the transaction."""
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
                obj._p_changed = 0 # transition from changed to up-to-date
            obj._p_serial = serial

    def tpc_finish(self, transaction):
        """Indicate confirmation that the transaction is done."""
        if self._tmp is not None:
            # Commiting a subtransaction!
            # There is no need to invalidate anything.
            self._storage.tpc_finish(transaction)
            self._storage._creating[:0]=self._creating
            del self._creating[:]
        else:
            def callback(tid):
                d = {}
                for oid in self._modified:
                    d[oid] = 1
                self._db.invalidate(tid, d, self)
            self._storage.tpc_finish(transaction, callback)
        self._tpc_cleanup()

    def tpc_abort(self, transaction):
        """Abort a transaction."""
        if self._import:
            self._import = None
        self._storage.tpc_abort(transaction)
        self._cache.invalidate(self._modified)
        self._invalidate_creating()
        while self._added:
            oid, obj = self._added.popitem()
            del obj._p_oid
            del obj._p_jar
        self._tpc_cleanup()

    def _tpc_cleanup(self):
        """Performs cleanup operations to support tpc_finish and tpc_abort."""
        self._conflicts.clear()
        if not self._synch:
            self._flush_invalidations()
        self._needs_to_join = True
        self._registered_objects = []

    def sync(self):
        """Manually update the view on the database."""
        self._txn_mgr.get().abort()
        sync = getattr(self._storage, 'sync', 0)
        if sync:
            sync()
        self._flush_invalidations()

    def getDebugInfo(self):
        """Returns a tuple with different items for debugging the
        connection.
        """ 
        return self._debug_info

    def setDebugInfo(self, *args):
        """Add the given items to the debug information of this connection."""
        self._debug_info = self._debug_info + args

    def getTransferCounts(self, clear=False):
        """Returns the number of objects loaded and stored."""
        res = self._load_count, self._store_count
        if clear:
            self._load_count = 0
            self._store_count = 0
        return res

    ##############################################
    # persistent.interfaces.IPersistentDatamanager

    def oldstate(self, obj, tid):
        """Return copy of 'obj' that was written by transaction 'tid'."""
        assert obj._p_jar is self
        p = self._storage.loadSerial(obj._p_oid, tid)
        return self._reader.getState(p)

    def setstate(self, obj):
        """Turns the ghost 'obj' into a real object by loading it's from the
        database."""
        oid = obj._p_oid

        if self._storage is None:
            msg = ("Shouldn't load state for %s "
                   "when the connection is closed" % oid_repr(oid))
            self._log.error(msg)
            raise ConnectionStateError(msg)

        try:
            self._setstate(obj)
        except ConflictError:
            raise
        except:
            self._log.error("Couldn't load state for %s", oid_repr(oid),
                            exc_info=sys.exc_info())
            raise

    def _setstate(self, obj):
        # Helper for setstate(), which provides logging of failures.

        # The control flow is complicated here to avoid loading an
        # object revision that we are sure we aren't going to use.  As
        # a result, invalidation tests occur before and after the
        # load.  We can only be sure about invalidations after the
        # load.

        # If an object has been invalidated, there are several cases
        # to consider:
        # 1. Check _p_independent()
        # 2. Try MVCC
        # 3. Raise ConflictError.

        # Does anything actually use _p_independent()?  It would simplify
        # the code if we could drop support for it.

        # There is a harmless data race with self._invalidated.  A
        # dict update could go on in another thread, but we don't care
        # because we have to check again after the load anyway.

        if (obj._p_oid in self._invalidated
            and not myhasattr(obj, "_p_independent")):
            # If the object has _p_independent(), we will handle it below.
            self._load_before_or_conflict(obj)
            return

        p, serial = self._storage.load(obj._p_oid, self._version)
        self._load_count += 1

        self._inv_lock.acquire()
        try:
            invalid = obj._p_oid in self._invalidated
        finally:
            self._inv_lock.release()

        if invalid:
            if myhasattr(obj, "_p_independent"):
                # This call will raise a ReadConflictError if something
                # goes wrong
                self._handle_independent(obj)
            else:
                self._load_before_or_conflict(obj)
                return

        self._reader.setGhostState(obj, p)
        obj._p_serial = serial

    def _load_before_or_conflict(self, obj):
        """Load non-current state for obj or raise ReadConflictError."""
        if not (self._mvcc and self._setstate_noncurrent(obj)):
            self._register(obj)
            self._conflicts[obj._p_oid] = True
            raise ReadConflictError(object=obj)

    def _setstate_noncurrent(self, obj):
        """Set state using non-current data.

        Return True if state was available, False if not.
        """
        try:
            # Load data that was current before the commit at txn_time.
            t = self._storage.loadBefore(obj._p_oid, self._txn_time)
        except KeyError:
            return False
        if t is None:
            return False
        data, start, end = t
        # The non-current transaction must have been written before
        # txn_time.  It must be current at txn_time, but could have
        # been modified at txn_time.

        assert start < self._txn_time, (u64(start), u64(self._txn_time))
        assert end is not None
        assert self._txn_time <= end, (u64(self._txn_time), u64(end))
        self._reader.setGhostState(obj, data)
        obj._p_serial = start
        return True

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
            self._conflicts[obj._p_oid] = 1
            self._register(obj)
            raise ReadConflictError(object=obj)

    def register(self, obj):
        """Register obj with the current transaction manager.

        A subclass could override this method to customize the default
        policy of one transaction manager for each thread.

        obj must be an object loaded from this Connection.
        """
        assert obj._p_jar is self
        if obj._p_oid is None:
            # There is some old Zope code that assigns _p_jar
            # directly.  That is no longer allowed, but we need to
            # provide support for old code that still does it.

            # The actual complaint here is that an object without
            # an oid is being registered.  I can't think of any way to
            # achieve that without assignment to _p_jar.  If there is
            # a way, this will be a very confusing warning.
            deprecated36("Assigning to _p_jar is deprecated, and will be "
                         "changed to raise an exception.")
        elif obj._p_oid in self._added:
            # It was registered before it was added to _added.
            return
        self._register(obj)

    def _register(self, obj=None):
        if obj is not None:
            self._registered_objects.append(obj)
        if self._needs_to_join:
            self._txn_mgr.get().join(self)
            self._needs_to_join = False

    # PROTECTED stuff (used by e.g. ZODB.DB.DB)

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

    def _setDB(self, odb, mvcc=None, txn_mgr=None, synch=None):
        """Register odb, the DB that this Connection uses.

        This method is called by the DB every time a Connection
        is opened.  Any invalidations received while the Connection
        was closed will be processed.

        If the global module function resetCaches() was called, the
        cache will be cleared.

        Parameters:
        odb: database that owns the Connection
        mvcc: boolean indicating whether MVCC is enabled
        txn_mgr: transaction manager to use.  None means
            used the default transaction manager.
        synch: boolean indicating whether Connection should
        register for afterCompletion() calls.
        """

        # TODO:  Why do we go to all the trouble of setting _db and
        # other attributes on open and clearing them on close?
        # A Connection is only ever associated with a single DB
        # and Storage.

        self._db = odb
        self._storage = odb._storage
        self.new_oid = odb._storage.new_oid
        self._opened = time()
        if synch is not None:
            self._synch = synch
        if mvcc is not None:
            self._mvcc = mvcc
        self._txn_mgr = txn_mgr or transaction.manager
        if self._reset_counter != global_reset_counter:
            # New code is in place.  Start a new cache.
            self._resetCache()
        else:
            self._flush_invalidations()
        if self._synch:
            self._txn_mgr.registerSynch(self)
        self._reader = ObjectReader(self, self._cache, self._db.classFactory)

        # Multi-database support
        self.connections = {self._db.database_name: self}

    def _resetCache(self):
        """Creates a new cache, discarding the old one.

        See the docstring for the resetCaches() function.
        """
        self._reset_counter = global_reset_counter
        self._invalidated.clear()
        cache_size = self._cache.cache_size
        self._cache = cache = PickleCache(self, cache_size)

    # Python protocol

    def __repr__(self):
        if self._version:
            ver = ' (in version %s)' % `self._version`
        else:
            ver = ''
        return '<Connection at %08x%s>' % (positive_id(self), ver)

    # DEPRECATION candidates

    __getitem__ = get

    def modifiedInVersion(self, oid):
        """Returns the version the object with the given oid was modified in.

        If it wasn't modified in a version, the current version of this 
        connection is returned.
        """
        try:
            return self._db.modifiedInVersion(oid)
        except KeyError:
            import pdb; pdb.set_trace() 
            return self.getVersion()

    def getVersion(self):
        """Returns the version this connection is attached to."""
        if self._storage is None:
            raise ConnectionStateError("The database connection is closed")
        return self._version

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
            self._log.error("setklassstate failed", exc_info=sys.exc_info())
            raise

    def exchange(self, old, new):
        # called by a ZClasses method that isn't executed by the test suite
        oid = old._p_oid
        new._p_oid = oid
        new._p_jar = self
        new._p_changed = 1
        self._register(new)
        self._cache[oid] = new

    # DEPRECATED methods

    def getTransaction(self):
        """Get the current transaction for this connection.

        :deprecated:

        The transaction manager's get method works the same as this
        method.  You can pass a transaction manager (TM) to DB.open()
        to control which TM the Connection uses.
        """
        deprecated36("getTransaction() is deprecated. "
                     "Use the txn_mgr argument to DB.open() instead.")
        return self._txn_mgr.get()

    def setLocalTransaction(self):
        """Use a transaction bound to the connection rather than the thread.

        :deprecated:

        Returns the transaction manager used by the connection.  You
        can pass a transaction manager (TM) to DB.open() to control
        which TM the Connection uses.
        """
        deprecated36("setLocalTransaction() is deprecated. "
                     "Use the txn_mgr argument to DB.open() instead.")
        if self._txn_mgr is transaction.manager:
            if self._synch:
                self._txn_mgr.unregisterSynch(self)
            self._txn_mgr = transaction.TransactionManager()
            if self._synch:
                self._txn_mgr.registerSynch(self)
        return self._txn_mgr

    def cacheFullSweep(self, dt=None):
        deprecated36("cacheFullSweep is deprecated. "
                     "Use cacheMinimize instead.")
        if dt is None:
            self._cache.full_sweep()
        else:
            self._cache.full_sweep(dt)

    def cacheMinimize(self, dt=DEPRECATED_ARGUMENT):
        """Deactivate all unmodified objects in the cache."""
        if dt is not DEPRECATED_ARGUMENT:
            deprecated36("cacheMinimize() dt= is ignored.")
        self._cache.minimize()

