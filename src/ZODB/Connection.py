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

$Id: Connection.py,v 1.155 2004/04/23 17:26:37 gintautasm Exp $"""

import logging
import sys
import threading
import warnings
from time import time
from utils import u64

from persistent import PickleCache
from persistent.interfaces import IPersistent

import transaction

from ZODB.ConflictResolution import ResolvedSerial
from ZODB.ExportImport import ExportImport
from ZODB.POSException \
     import ConflictError, ReadConflictError, InvalidObjectReference, \
            ConnectionStateError
from ZODB.TmpStore import TmpStore
from ZODB.utils import oid_repr, z64, positive_id
from ZODB.serialize import ObjectWriter, ConnectionObjectReader, myhasattr

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
    """Connection to ZODB for loading and storing objects.

    The Connection object serves as a data manager.  The root() method
    on a Connection returns the root object for the database.  This
    object and all objects reachable from it are associated with the
    Connection that loaded them.  When a transaction commits, it uses
    the Connection to store modified objects.

    Typical use of ZODB is for each thread to have its own
    Connection and that no thread should have more than one Connection
    to the same database.  A thread is associated with a Connection by
    loading objects from that Connection.  Objects loaded by one
    thread should not be used by another thread.

    A Connection can be associated with a single version when it is
    created.  By default, a Connection is not associated with a
    version; it uses non-version data.

    Each Connection provides an isolated, consistent view of the
    database, by managing independent copies of objects in the
    database.  At transaction boundaries, these copies are updated to
    reflect the current state of the database.

    You should not instantiate this class directly; instead call the
    open() method of a DB instance.

    In many applications, root() is the only method of the Connection
    that you will need to use.

    Synchronization
    ---------------

    A Connection instance is not thread-safe.  It is designed to
    support a thread model where each thread has its own transaction.
    If an application has more than one thread that uses the
    connection or the transaction the connection is registered with,
    the application should provide locking.

    The Connection manages movement of objects in and out of object
    storage.

    XXX We should document an intended API for using a Connection via
    multiple threads.

    XXX We should explain that the Connection has a cache and that
    multiple calls to get() will return a reference to the same
    object, provided that one of the earlier objects is still
    referenced.  Object identity is preserved within a connection, but
    not across connections.

    XXX Mention the database pool.

    A database connection always presents a consistent view of the
    objects in the database, although it may not always present the
    most current revision of any particular object.  Modifications
    made by concurrent transactions are not visible until the next
    transaction boundary (abort or commit).

    Two options affect consistency.  By default, the mvcc and synch
    options are enabled by default.

    If you pass mvcc=True to db.open(), the Connection will never read
    non-current revisions of an object.  Instead it will raise a
    ReadConflictError to indicate that the current revision is
    unavailable because it was written after the current transaction
    began.

    The logic for handling modifications assumes that the thread that
    opened a Connection (called db.open()) is the thread that will use
    the Connection.  If this is not true, you should pass synch=False
    to db.open().  When the synch option is disabled, some transaction
    boundaries will be missed by the Connection; in particular, if a
    transaction does not involve any modifications to objects loaded
    from the Connection and synch is disabled, the Connection will
    miss the transaction boundary.  Two examples of this behavior are
    db.undo() and read-only transactions.


    :Groups:

      - `User Methods`: root, get, add, close, db, sync, isReadOnly,
        cacheGC, cacheFullSweep, cacheMinimize, getVersion,
        modifiedInVersion
      - `Experimental Methods`: setLocalTransaction, getTransaction,
        onCloseCallbacks
      - `Transaction Data Manager Methods`: tpc_begin, tpc_vote,
        tpc_finish, tpc_abort, sortKey, abort, commit, commit_sub,
        abort_sub
      - `Database Invalidation Methods`: invalidate, _setDB
      - `IPersistentDataManager Methods`: setstate, register,
        setklassstate
      - `Other Methods`: oldstate, exchange, getDebugInfo, setDebugInfo,
        getTransferCounts

    """

    _tmp = None
    _code_timestamp = 0

    def __init__(self, version='', cache_size=400,
                 cache_deactivate_after=None, mvcc=True, txn_mgr=None,
                 synch=True):
        """Create a new Connection.

        A Connection instance should by instantiated by the DB
        instance that it is connected to.

        :Parameters:
          - `version`: the "version" that all changes will be made
             in, defaults to no version.
          - `cache_size`: the target size of the in-memory object
             cache, measured in objects.
          - `cache_deactivate_after`: deprecated, ignored
          - `mvcc`: boolean indicating whether MVCC is enabled
          - `txn_mgr`: transaction manager to use.  None means
             used the default transaction manager.
          - `synch`: boolean indicating whether Connection should
             register for afterCompletion() calls.
        """

        self._log = logging.getLogger("ZODB.Connection")
        self._storage = None
        self._debug_info = ()

        self._version = version
        self._cache = cache = PickleCache(self, cache_size)
        if version:
            # Caches for versions end up empty if the version
            # is not used for a while. Non-version caches
            # keep their content indefinitely.

            # XXX Why do we want version caches to behave this way?

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

        # XXX It sucks that we have to hold the lock to read
        # _invalidated.  Normally, _invalidated is written by calling
        # dict.update, which will execute atomically by virtue of the
        # GIL.  But some storage might generate oids where hash or
        # compare invokes Python code.  In that case, the GIL can't
        # save us.
        self._inv_lock = threading.Lock()
        self._invalidated = d = {}
        self._invalid = d.has_key
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

    def getTransaction(self):
        """Get the current transaction for this connection.

        :deprecated:

        The transaction manager's get method works the same as this
        method.  You can pass a transaction manager (TM) to DB.open()
        to control which TM the Connection uses.
        """
        warnings.warn("getTransaction() is deprecated. "
                      "Use the txn_mgr argument to DB.open() instead.",
                      DeprecationWarning)
        return self._txn_mgr.get()

    def setLocalTransaction(self):
        """Use a transaction bound to the connection rather than the thread.

        :deprecated:

        Returns the transaction manager used by the connection.  You
        can pass a transaction manager (TM) to DB.open() to control
        which TM the Connection uses.
        """
        warnings.warn("setLocalTransaction() is deprecated. "
                      "Use the txn_mgr argument to DB.open() instead.",
                      DeprecationWarning)
        if self._txn_mgr is transaction.manager:
            if self._synch:
                self._txn_mgr.unregisterSynch(self)
            self._txn_mgr = transaction.TransactionManager()
            if self._synch:
                self._txn_mgr.registerSynch(self)
        return self._txn_mgr

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
        return '<Connection at %08x%s>' % (positive_id(self), ver)

    def get(self, oid):
        """Return the persistent object with oid 'oid'.

        If the object was not in the cache and the object's class is
        ghostable, then a ghost will be returned.  If the object is
        already in the cache, a reference to the cached object will be
        returned.

        Applications seldom need to call this method, because objects
        are loaded transparently during attribute lookup.

        :return: persistent object corresponding to `oid`

        :Parameters:
          - `oid`: an object id

        :Exceptions:
          - `KeyError`: if oid does not exist.  It is possible that an
            object does not exist as of the current transaction, but
            existed in the past.  It may even exist again in the
            future, if the transaction that removed it is undone.
          - `ConnectionStateError`:  if the connection is closed.
        """
        if self._storage is None:
            # XXX Should this be a ZODB-specific exception?
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

    # deprecate this method?
    __getitem__ = get

    def add(self, obj):
        """Add a new object 'obj' to the database and assign it an oid.

        A persistent object is normally added to the database and
        assigned an oid when it becomes reachable to an object already in
        the database.  In some cases, it is useful to create a new
        object and use its oid (_p_oid) in a single transaction.

        This method assigns a new oid regardless of whether the object
        is reachable.

        The object is added when the transaction commits.  The object
        must implement the IPersistent interface and must not
        already be associated with a Connection.

        :Parameters:
          - `obj`: a Persistent object

        :Exceptions:
          - `TypeError`: if obj is not a persistent object.
          - `InvalidObjectReference`: if obj is already associated
            with another connection.
          - `ConnectionStateError`: if the connection is closed.
        """
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
        # If two connections use the same storage, give them a
        # consistent order using id().  This is unique for the
        # lifetime of a connection, which is good enough.
        return "%s:%s" % (self._sortKey(), id(self))

    def _setDB(self, odb, mvcc=None, txn_mgr=None, synch=None):
        """Register odb, the DB that this Connection uses.

        This method is called by the DB every time a Connection
        is opened.  Any invalidations received while the Connection
        was closed will be processed.

        If the global module function resetCaches() was called, the
        cache will be cleared.

        :Parameters:
          - `odb`: database that owns the Connection
          - `mvcc`: boolean indicating whether MVCC is enabled
          - `txn_mgr`: transaction manager to use.  None means
             used the default transaction manager.
          - `synch`: boolean indicating whether Connection should
             register for afterCompletion() calls.
        """

        # XXX Why do we go to all the trouble of setting _db and
        # other attributes on open and clearing them on close?
        # A Connection is only ever associated with a single DB
        # and Storage.

        self._db = odb
        self._storage = odb._storage
        self._sortKey = odb._storage.sortKey
        self.new_oid = odb._storage.new_oid
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
        self._reader = ConnectionObjectReader(self, self._cache,
                                              self._db.classFactory)

    def _resetCache(self):
        """Creates a new cache, discarding the old one.

        See the docstring for the resetCaches() function.
        """
        self._reset_counter = global_reset_counter
        self._invalidated.clear()
        cache_size = self._cache.cache_size
        self._cache = cache = PickleCache(self, cache_size)

    def abort(self, transaction):
        """Abort the object in the transaction.

        This just deactivates the thing.
        """

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

    # XXX should there be a way to call incrgc directly?
    # perhaps "full sweep" should do that?

    # XXX we should test what happens when these methods are called
    # mid-transaction.

    def cacheFullSweep(self, dt=None):
        # XXX needs doc string
        warnings.warn("cacheFullSweep is deprecated. "
                      "Use cacheMinimize instead.", DeprecationWarning)
        if dt is None:
            self._cache.full_sweep()
        else:
            self._cache.full_sweep(dt)

    def cacheMinimize(self, dt=None):
        """Deactivate all unmodified objects in the cache.

        Call _p_deactivate() on each cached object, attempting to turn
        it into a ghost.  It is possible for individual objects to
        remain active.

        :Parameters:
          - `dt`: ignored.  It is provided only for backwards compatibility.
        """
        if dt is not None:
            warnings.warn("The dt argument to cacheMinimize is ignored.",
                          DeprecationWarning)
        self._cache.minimize()

    def cacheGC(self):
        """Reduce cache size to target size.

        Call _p_deactivate() on cached objects until the cache size
        falls under the target size.
        """
        self._cache.incrgc()

    __onCloseCallbacks = None

    def onCloseCallback(self, f):
        """Register a callable, f, to be called by close().

        The callable, f, will be called at most once, the next time
        the Connection is closed.

        :Parameters:
          - `f`: object that will be called on `close`
        """
        if self.__onCloseCallbacks is None:
            self.__onCloseCallbacks = []
        self.__onCloseCallbacks.append(f)

    def close(self):
        """Close the Connection.

        A closed Connection should not be used by client code.  It
        can't load or store objects.  Objects in the cache are not
        freed, because Connections are re-used and the cache are
        expected to be useful to the next client.

        When the Connection is closed, all callbacks registered by
        onCloseCallback() are invoked and the cache is scanned for
        old objects.
        """

        if not self._needs_to_join:
            # We're currently joined to a transaction.
            raise ConnectionStateError("Cannot close a connection joined to "
                                       "a transaction")

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
        # Return the connection to the pool.
        if self._db is not None:
            if self._synch:
                self._txn_mgr.unregisterSynch(self)
            self._db._closeConnection(self)
            # _closeConnection() set self._db to None.  However, we can't
            # assert that here, because self may have been reused (by
            # another thread) by the time we get back here.

    def commit(self, transaction):
        if self._import:
            # XXX eh?
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
                if hasattr(obj, 'aq_base'):
                    self._cache[oid] = obj.aq_base
                else:
                    raise

            self._handle_serial(s, oid)

    def commit_sub(self, t):
        """Commit all work done in all subtransactions for this transaction."""
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
        """Abort work done in all subtransactions for this transaction."""
        if self._tmp is None:
            return
        src = self._storage
        self._storage = self._tmp
        self._tmp = None

        self._cache.invalidate(src._index.keys())
        self._invalidate_creating(src._creating)

    def _invalidate_creating(self, creating=None):
        """Dissown any objects newly saved in an uncommitted transaction."""
        if creating is None:
            creating = self._creating
            self._creating = []

        for oid in creating:
            o = self._cache.get(oid)
            if o is not None:
                del self._cache[oid]
                del o._p_jar
                del o._p_oid

    def db(self):
        return self._db

    def getVersion(self):
        if self._storage is None:
            raise ConnectionStateError("The database connection is closed")
        return self._version

    def isReadOnly(self):
        if self._storage is None:
            raise ConnectionStateError("The database connection is closed")
        return self._storage.isReadOnly()

    def invalidate(self, tid, oids):
        """Notify the Connection that transaction 'tid' invalidated oids.

        When the next transaction boundary is reached, objects will be
        invalidated.  If any of the invalidated objects is accessed by
        the current transaction, the revision written before C{tid}
        will be used.

        The DB calls this method, even when the Connection is closed.

        :Parameters:
          - `tid`: the storage-level id of the transaction that committed
          - `oids`: oids is a set of oids, represented as a dict with oids
            as keys.
        """
        self._inv_lock.acquire()
        try:
            if self._txn_time is None:
                self._txn_time = tid
            self._invalidated.update(oids)
        finally:
            self._inv_lock.release()

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

    def modifiedInVersion(self, oid):
        try:
            return self._db.modifiedInVersion(oid)
        except KeyError:
            return self._version

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

            # XXX The actual complaint here is that an object without
            # an oid is being registered.  I can't think of any way to
            # achieve that without assignment to _p_jar.  If there is
            # a way, this will be a very confusing warning.
            warnings.warn("Assigning to _p_jar is deprecated",
                          DeprecationWarning)
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

    def root(self):
        """Return the database root object.

        The root is a PersistentDict.
        """
        return self.get(z64)

    def setstate(self, obj):
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
            self._register(obj)
            raise ReadConflictError(object=obj)

    def oldstate(self, obj, tid):
        """Return copy of obj that was written by tid.

        XXX The returned object does not have the typical metadata
        (_p_jar, _p_oid, _p_serial) set.  I'm not sure how references
        to other peristent objects are handled.

        :return: a persistent object

        :Parameters:
          - `obj`: a persistent object from this Connection.
          - `tid`: id of a transaction that wrote an earlier revision.

        :Exceptions:
          - `KeyError`: if tid does not exist or if tid deleted a revision
            of obj.
        """
        assert obj._p_jar is self
        p = self._storage.loadSerial(obj._p_oid, tid)
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
            self._log.error("setklassstate failed", exc_info=sys.exc_info())
            raise

    def tpc_begin(self, transaction, sub=False):
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
        # It's important that the storage calls the function we pass
        # while it still has its lock.  We don't want another thread
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
            def callback(tid):
                d = {}
                for oid in self._modified:
                    d[oid] = 1
                self._db.invalidate(tid, d, self)
            self._storage.tpc_finish(transaction, callback)
        self._tpc_cleanup()

    def tpc_abort(self, transaction):
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

    # Common cleanup actions after tpc_finish/tpc_abort.
    def _tpc_cleanup(self):
        self._conflicts.clear()
        if not self._synch:
            self._flush_invalidations()
        self._needs_to_join = True
        self._registered_objects = []


    def sync(self):
        self._txn_mgr.get().abort()
        sync = getattr(self._storage, 'sync', 0)
        if sync:
            sync()
        self._flush_invalidations()

    def getDebugInfo(self):
        return self._debug_info

    def setDebugInfo(self, *args):
        self._debug_info = self._debug_info + args

    def getTransferCounts(self, clear=False):
        """Returns the number of objects loaded and stored.

        If clear is True, reset the counters.
        """
        res = self._load_count, self._store_count
        if clear:
            self._load_count = 0
            self._store_count = 0
        return res

    def exchange(self, old, new):
        # called by a ZClasses method that isn't executed by the test suite
        oid = old._p_oid
        new._p_oid = oid
        new._p_jar = self
        new._p_changed = 1
        self._register(new)
        self._cache[oid] = new
