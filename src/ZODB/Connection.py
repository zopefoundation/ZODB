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
import tempfile
import threading
import warnings
import os
import shutil
from time import time

from persistent import PickleCache

# interfaces
from persistent.interfaces import IPersistentDataManager
from ZODB.interfaces import IConnection
from ZODB.interfaces import IBlobStorage
from ZODB.blob import Blob, rename_or_copy_blob
from transaction.interfaces import ISavepointDataManager
from transaction.interfaces import IDataManagerSavepoint
from transaction.interfaces import ISynchronizer
from zope.interface import implements

import transaction

from ZODB.blob import SAVEPOINT_SUFFIX
from ZODB.ConflictResolution import ResolvedSerial
from ZODB.ExportImport import ExportImport
from ZODB import POSException
from ZODB.POSException import InvalidObjectReference, ConnectionStateError
from ZODB.POSException import ConflictError, ReadConflictError
from ZODB.POSException import Unsupported, ReadOnlyHistoryError
from ZODB.POSException import POSKeyError
from ZODB.serialize import ObjectWriter, ObjectReader, myhasattr
from ZODB.utils import p64, u64, z64, oid_repr, positive_id
from ZODB import utils

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

    implements(IConnection,
               ISavepointDataManager,
               IPersistentDataManager,
               ISynchronizer)


    _code_timestamp = 0

    ##########################################################################
    # Connection methods, ZODB.IConnection

    def __init__(self, db, cache_size=400, before=None, cache_size_bytes=0):
        """Create a new Connection."""

        self._log = logging.getLogger('ZODB.Connection')
        self._debug_info = ()

        self._db = db
        
        # historical connection
        self.before = before
        
        # Multi-database support
        self.connections = {self._db.database_name: self}

        self._normal_storage = self._storage = db._storage
        self.new_oid = db._storage.new_oid
        self._savepoint_storage = None

        # Do we need to join a txn manager?
        self._needs_to_join = True
        self.transaction_manager = None
        self._opened = None # time.time() when DB.open() opened us

        self._reset_counter = global_reset_counter
        self._load_count = 0   # Number of objects unghosted
        self._store_count = 0  # Number of objects stored

        # Cache which can ghostify (forget the state of) objects not
        # recently used. Its API is roughly that of a dict, with
        # additional gc-related and invalidation-related methods.
        self._cache = PickleCache(self, cache_size, cache_size_bytes)

        # The pre-cache is used by get to avoid infinite loops when
        # objects immediately load their state whern they get their
        # persistent data set.
        self._pre_cache = {}

        # List of all objects (not oids) registered as modified by the
        # persistence machinery, or by add(), or whose access caused a
        # ReadConflictError (just to be able to clean them up from the
        # cache on abort with the other modified objects). All objects
        # of this list are either in _cache or in _added.
        self._registered_objects = []

        # Dict of oid->obj added explicitly through add(). Used as a
        # preliminary cache until commit time when objects are all moved
        # to the real _cache. The objects are moved to _creating at
        # commit time.
        self._added = {}

        # During commit this is turned into a list, which receives
        # objects added as a side-effect of storing a modified object.
        self._added_during_commit = None

        # During commit, all objects go to either _modified or _creating:

        # Dict of oid->flag of new objects (without serial), either
        # added by add() or implicitly added (discovered by the
        # serializer during commit). The flag is True for implicit
        # adding. Used during abort to remove created objects from the
        # _cache, and by persistent_id to check that a new object isn't
        # reachable from multiple databases.
        self._creating = {}

        # List of oids of modified objects, which have to be invalidated
        # in the cache on abort and in other connections on finish.
        self._modified = []


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
        # Note:  since that was written, it was officially declared that the
        # type of an oid is str.  TODO:  remove the related now-unnecessary
        # critical sections (if any -- this needs careful thought).

        self._inv_lock = threading.Lock()
        self._invalidated = set()

        # Flag indicating whether the cache has been invalidated:
        self._invalidatedCache = False

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
        self._txn_time = None

        # To support importFile(), implemented in the ExportImport base
        # class, we need to run _importDuringCommit() from our commit()
        # method.  If _import is not None, it is a two-tuple of arguments
        # to pass to _importDuringCommit().
        self._import = None

        self._reader = ObjectReader(self, self._cache, self._db.classFactory)


    def add(self, obj):
        """Add a new object 'obj' to the database and assign it an oid."""
        if self._opened is None:
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

    def get(self, oid):
        """Return the persistent object with oid 'oid'."""
        if self._opened is None:
            raise ConnectionStateError("The database connection is closed")

        obj = self._cache.get(oid, None)
        if obj is not None:
            return obj
        obj = self._added.get(oid, None)
        if obj is not None:
            return obj        
        obj = self._pre_cache.get(oid, None)
        if obj is not None:
            return obj

        # This appears to be an MVCC violation because we are loading
        # the must recent data when perhaps we shouldnt. The key is
        # that we are only creating a ghost!
        # A disadvantage to this optimization is that _p_serial cannot be
        # trusted until the object has been loaded, which affects both MVCC
        # and historical connections.
        p, serial = self._storage.load(oid, '')
        obj = self._reader.getGhost(p)

        # Avoid infiniate loop if obj tries to load its state before
        # it is added to the cache and it's state refers to it.
        self._pre_cache[oid] = obj
        obj._p_oid = oid
        obj._p_jar = self
        obj._p_changed = None
        obj._p_serial = serial
        self._pre_cache.pop(oid)
        self._cache[oid] = obj
        return obj

    def cacheMinimize(self):
        """Deactivate all unmodified objects in the cache."""
        self._cache.minimize()

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

    def close(self, primary=True):
        """Close the Connection."""
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

        self._debug_info = ()

        if self._opened:
            self.transaction_manager.unregisterSynch(self)

        if primary:
            for connection in self.connections.values():
                if connection is not self:
                    connection.close(False)

            # Return the connection to the pool.
            if self._opened is not None:
                self._db._returnToPool(self)

                # _returnToPool() set self._opened to None.
                # However, we can't assert that here, because self may
                # have been reused (by another thread) by the time we
                # get back here.
        else:
            self._opened = None

    def db(self):
        """Returns a handle to the database this connection belongs to."""
        return self._db

    def isReadOnly(self):
        """Returns True if this connection is read only."""
        if self._opened is None:
            raise ConnectionStateError("The database connection is closed")
        return self.before is not None or self._storage.isReadOnly()

    def invalidate(self, tid, oids):
        """Notify the Connection that transaction 'tid' invalidated oids."""
        if self.before is not None:
            # this is an historical connection.  Invalidations are irrelevant.
            return
        self._inv_lock.acquire()
        try:
            if self._txn_time is None:
                self._txn_time = tid
            self._invalidated.update(oids)
        finally:
            self._inv_lock.release()

    def invalidateCache(self):
        self._inv_lock.acquire()
        try:
            self._invalidatedCache = True
        finally:
            self._inv_lock.release()

    def root(self):
        """Return the database root object."""
        return self.get(z64)

    def get_connection(self, database_name):
        """Return a Connection for the named database."""
        connection = self.connections.get(database_name)
        if connection is None:
            new_con = self._db.databases[database_name].open(
                transaction_manager=self.transaction_manager,
                before=self.before,
                )
            self.connections.update(new_con.connections)
            new_con.connections = self.connections
            connection = new_con
        return connection

    def _implicitlyAdding(self, oid):
        """Are we implicitly adding an object within the current transaction

        This is used in a check to avoid implicitly adding an object
        to a database in a multi-database situation.
        See serialize.ObjectWriter.persistent_id.

        """
        return (self._creating.get(oid, 0)
                or
                ((self._savepoint_storage is not None)
                 and
                 self._savepoint_storage.creating.get(oid, 0)
                 )
                )

    def sync(self):
        """Manually update the view on the database."""
        self.transaction_manager.abort()
        self._storage_sync()

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

    # Connection methods
    ##########################################################################

    ##########################################################################
    # Data manager (ISavepointDataManager) methods

    def abort(self, transaction):
        """Abort a transaction and forget all changes."""

        # The order is important here.  We want to abort registered
        # objects before we process the cache.  Otherwise, we may un-add
        # objects added in savepoints.  If they've been modified since
        # the savepoint, then they won't have _p_oid or _p_jar after
        # they've been unadded. This will make the code in _abort
        # confused.

        self._abort()

        if self._savepoint_storage is not None:
            self._abort_savepoint()

        self._tpc_cleanup()

    def _abort(self):
        """Abort a transaction and forget all changes."""

        for obj in self._registered_objects:
            oid = obj._p_oid
            assert oid is not None
            if oid in self._added:
                del self._added[oid]
                del obj._p_jar
                del obj._p_oid
            else:

                # Note: If we invalidate a non-ghostifiable object
                # (i.e. a persistent class), the object will
                # immediately reread its state.  That means that the
                # following call could result in a call to
                # self.setstate, which, of course, must succeed.
                # In general, it would be better if the read could be
                # delayed until the start of the next transaction.  If
                # we read at the end of a transaction and if the
                # object was invalidated during this transaction, then
                # we'll read non-current data, which we'll discard
                # later in transaction finalization.  Unfortnately, we
                # can only delay the read if this abort corresponds to
                # a top-level-transaction abort.  We can't tell if
                # this is a top-level-transaction abort, so we have to
                # go ahead and invalidate now.  Fortunately, it's
                # pretty unlikely that the object we are invalidating
                # was invalidated by another thread, so the risk of a
                # reread is pretty low.

                self._cache.invalidate(oid)

    def _tpc_cleanup(self):
        """Performs cleanup operations to support tpc_finish and tpc_abort."""
        self._conflicts.clear()
        self._needs_to_join = True
        self._registered_objects = []
        self._creating.clear()

    # Process pending invalidations.
    def _flush_invalidations(self):
        self._inv_lock.acquire()
        try:
            # Non-ghostifiable objects may need to read when they are
            # invalidated, so we'll quickly just replace the
            # invalidating dict with a new one.  We'll then process
            # the invalidations after freeing the lock *and* after
            # resetting the time.  This means that invalidations will
            # happen after the start of the transactions.  They are
            # subject to conflict errors and to reading old data.

            # TODO: There is a potential problem lurking for persistent
            # classes.  Suppose we have an invalidation of a persistent
            # class and of an instance.  If the instance is
            # invalidated first and if the invalidation logic uses
            # data read from the class, then the invalidation could
            # be performed with stale data.  Or, suppose that there
            # are instances of the class that are freed as a result of
            # invalidating some object.  Perhaps code in their __del__
            # uses class data.  Really, the only way to properly fix
            # this is to, in fact, make classes ghostifiable.  Then
            # we'd have to reimplement attribute lookup to check the
            # class state and, if necessary, activate the class.  It's
            # much worse than that though, because we'd also need to
            # deal with slots.  When a class is ghostified, we'd need
            # to replace all of the slot operations with versions that
            # reloaded the object when called. It's hard to say which
            # is better or worse.  For now, it seems the risk of
            # using a class while objects are being invalidated seems
            # small enough to be acceptable.

            invalidated = dict.fromkeys(self._invalidated)
            self._invalidated = set()
            self._txn_time = None
            if self._invalidatedCache:
                self._invalidatedCache = False
                invalidated = self._cache.cache_data.copy()
        finally:
            self._inv_lock.release()

        self._cache.invalidate(invalidated)

        # Now is a good time to collect some garbage.
        self._cache.incrgc()

    def tpc_begin(self, transaction):
        """Begin commit of a transaction, starting the two-phase commit."""
        self._modified = []

        # _creating is a list of oids of new objects, which is used to
        # remove them from the cache if a transaction aborts.
        self._creating.clear()
        self._normal_storage.tpc_begin(transaction)

    def commit(self, transaction):
        """Commit changes to an object"""

        if self._savepoint_storage is not None:

            # We first checkpoint the current changes to the savepoint
            self.savepoint()

            # then commit all of the savepoint changes at once
            self._commit_savepoint(transaction)

            # No need to call _commit since savepoint did.

        else:
            self._commit(transaction)

    def _commit(self, transaction):
        """Commit changes to an object"""
        
        if self.before is not None:
            raise ReadOnlyHistoryError()

        if self._import:
            # We are importing an export file. We alsways do this
            # while making a savepoint so we can copy export data
            # directly to our storage, typically a TmpStore.
            self._importDuringCommit(transaction, *self._import)
            self._import = None

        # Just in case an object is added as a side-effect of storing
        # a modified object.  If, for example, a __getstate__() method
        # calls add(), the newly added objects will show up in
        # _added_during_commit.  This sounds insane, but has actually
        # happened.

        self._added_during_commit = []

        if self._invalidatedCache:
            raise ConflictError()            

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

            if ((serial == z64)
                and
                ((self._savepoint_storage is None)
                 or (oid not in self._savepoint_storage.creating)
                 or self._savepoint_storage.creating[oid]
                 )
                ):
                
                # obj is a new object

                # Because obj was added, it is now in _creating, so it
                # can be removed from _added.  If oid wasn't in
                # adding, then we are adding it implicitly.

                implicitly_adding = self._added.pop(oid, None) is None

                self._creating[oid] = implicitly_adding

            else:
                if (oid in self._invalidated
                    and not hasattr(obj, '_p_resolveConflict')):
                    raise ConflictError(object=obj)
                self._modified.append(oid)
            p = writer.serialize(obj)  # This calls __getstate__ of obj

            if isinstance(obj, Blob):
                if not IBlobStorage.providedBy(self._storage):
                    raise Unsupported(
                        "Storing Blobs in %s is not supported." % 
                        repr(self._storage))
                if obj.opened():
                    raise ValueError("Can't commit with opened blobs.")
                s = self._storage.storeBlob(oid, serial, p,
                                            obj._uncommitted(),
                                            '', transaction)
                # we invalidate the object here in order to ensure
                # that that the next attribute access of its name
                # unghostify it, which will cause its blob data
                # to be reattached "cleanly"
                obj._p_invalidate()
            else:
                s = self._storage.store(oid, serial, p, '', transaction)
            self._cache.update_object_size_estimation(oid,
                                                   len(p)
                                                   )
            obj._p_estimated_size = len(p)
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

    def _handle_serial(self, store_return, oid=None, change=1):
        """Handle the returns from store() and tpc_vote() calls."""

        # These calls can return different types depending on whether
        # ZEO is used.  ZEO uses asynchronous returns that may be
        # returned in batches by the ClientStorage.  ZEO1 can also
        # return an exception object and expect that the Connection
        # will raise the exception.

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

    def tpc_abort(self, transaction):
        if self._import:
            self._import = None

        if self._savepoint_storage is not None:
            self._abort_savepoint()

        self._storage.tpc_abort(transaction)

        # Note: If we invalidate a non-ghostifiable object (i.e. a
        # persistent class), the object will immediately reread its
        # state.  That means that the following call could result in a
        # call to self.setstate, which, of course, must succeed.  In
        # general, it would be better if the read could be delayed
        # until the start of the next transaction.  If we read at the
        # end of a transaction and if the object was invalidated
        # during this transaction, then we'll read non-current data,
        # which we'll discard later in transaction finalization.  We
        # could, theoretically queue this invalidation by calling
        # self.invalidate.  Unfortunately, attempts to make that
        # change resulted in mysterious test failures.  It's pretty
        # unlikely that the object we are invalidating was invalidated
        # by another thread, so the risk of a reread is pretty low.
        # It's really not worth the effort to pursue this.

        self._cache.invalidate(self._modified)
        self._invalidate_creating()
        while self._added:
            oid, obj = self._added.popitem()
            del obj._p_oid
            del obj._p_jar
        self._tpc_cleanup()

    def _invalidate_creating(self, creating=None):
        """Disown any objects newly saved in an uncommitted transaction."""
        if creating is None:
            creating = self._creating
            self._creating = {}

        for oid in creating:
            o = self._cache.get(oid)
            if o is not None:
                del self._cache[oid]
                del o._p_jar
                del o._p_oid

    def tpc_vote(self, transaction):
        """Verify that a data manager can commit the transaction."""
        try:
            vote = self._storage.tpc_vote
        except AttributeError:
            return
        s = vote(transaction)
        self._handle_serial(s)

    def tpc_finish(self, transaction):
        """Indicate confirmation that the transaction is done."""

        def callback(tid):
            d = dict.fromkeys(self._modified)
            self._db.invalidate(tid, d, self)
#       It's important that the storage calls the passed function
#       while it still has its lock.  We don't want another thread
#       to be able to read any updated data until we've had a chance
#       to send an invalidation message to all of the other
#       connections!
        self._storage.tpc_finish(transaction, callback)
        self._tpc_cleanup()

    def sortKey(self):
        """Return a consistent sort key for this connection."""
        return "%s:%s" % (self._storage.sortKey(), id(self))

    # Data manager (ISavepointDataManager) methods
    ##########################################################################

    ##########################################################################
    # Transaction-manager synchronization -- ISynchronizer

    def beforeCompletion(self, txn):
        # We don't do anything before a commit starts.
        pass

    # Call the underlying storage's sync() method (if any), and process
    # pending invalidations regardless.  Of course this should only be
    # called at transaction boundaries.
    def _storage_sync(self, *ignored):
        sync = getattr(self._storage, 'sync', 0)
        if sync:
            sync()
        self._flush_invalidations()

    afterCompletion =  _storage_sync
    newTransaction = _storage_sync

     # Transaction-manager synchronization -- ISynchronizer
    ##########################################################################

    ##########################################################################
    # persistent.interfaces.IPersistentDatamanager

    def oldstate(self, obj, tid):
        """Return copy of 'obj' that was written by transaction 'tid'."""
        assert obj._p_jar is self
        p = self._storage.loadSerial(obj._p_oid, tid)
        return self._reader.getState(p)

    def setstate(self, obj):
        """Turns the ghost 'obj' into a real object by loading its state from
        the database."""
        oid = obj._p_oid

        if self._opened is None:
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
        # (BTrees.Length does.)


        if self.before is not None:
            # Load data that was current before the time we have.
            before = self.before
            t = self._storage.loadBefore(obj._p_oid, before)
            if t is None:
                raise POSKeyError() # historical connection!
            p, serial, end = t
        
        else:
            # There is a harmless data race with self._invalidated.  A
            # dict update could go on in another thread, but we don't care
            # because we have to check again after the load anyway.

            if self._invalidatedCache:
                raise ReadConflictError()
    
            if (obj._p_oid in self._invalidated and
                    not myhasattr(obj, "_p_independent")):
                # If the object has _p_independent(), we will handle it below.
                self._load_before_or_conflict(obj)
                return
    
            p, serial = self._storage.load(obj._p_oid, '')
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
        self._cache.update_object_size_estimation(obj._p_oid,
                                               len(p)
                                               )
        obj._p_estimated_size = len(p)

        # Blob support
        if isinstance(obj, Blob):
            obj._p_blob_uncommitted = None
            obj._p_blob_committed = self._storage.loadBlob(obj._p_oid, serial)

    def _load_before_or_conflict(self, obj):
        """Load non-current state for obj or raise ReadConflictError."""
        if not self._setstate_noncurrent(obj):
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

        # MVCC Blob support
        if isinstance(obj, Blob):
            obj._p_blob_uncommitted = None
            obj._p_blob_committed = self._storage.loadBlob(obj._p_oid, start)

        return True

    def _handle_independent(self, obj):
        # Helper method for setstate() handles possibly independent objects
        # Call _p_independent(), if it returns True, setstate() wins.
        # Otherwise, raise a ConflictError.

        if obj._p_independent():
            self._inv_lock.acquire()
            try:
                try:
                    self._invalidated.remove(obj._p_oid)
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
            # The actual complaint here is that an object without
            # an oid is being registered.  I can't think of any way to
            # achieve that without assignment to _p_jar.  If there is
            # a way, this will be a very confusing exception.
            raise ValueError("assigning to _p_jar is not supported")
        elif obj._p_oid in self._added:
            # It was registered before it was added to _added.
            return
        self._register(obj)

    def _register(self, obj=None):

        # The order here is important.  We need to join before
        # registering the object, because joining may take a
        # savepoint, and the savepoint should not reflect the change
        # to the object.

        if self._needs_to_join:
            self.transaction_manager.get().join(self)
            self._needs_to_join = False

        if obj is not None:
            self._registered_objects.append(obj)


    # persistent.interfaces.IPersistentDatamanager
    ##########################################################################

    ##########################################################################
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

    def open(self, transaction_manager=None, delegate=True):
        """Register odb, the DB that this Connection uses.

        This method is called by the DB every time a Connection
        is opened.  Any invalidations received while the Connection
        was closed will be processed.

        If the global module function resetCaches() was called, the
        cache will be cleared.

        Parameters:
        odb: database that owns the Connection
        transaction_manager: transaction manager to use.  None means
            use the default transaction manager.
        register for afterCompletion() calls.
        """

        self._opened = time()

        if transaction_manager is None:
            transaction_manager = transaction.manager

        self.transaction_manager = transaction_manager

        if self._reset_counter != global_reset_counter:
            # New code is in place.  Start a new cache.
            self._resetCache()
        else:
            self._flush_invalidations()

        transaction_manager.registerSynch(self)

        if self._cache is not None:
            self._cache.incrgc() # This is a good time to do some GC

        if delegate:
            # delegate open to secondary connections
            for connection in self.connections.values():
                if connection is not self:
                    connection.open(transaction_manager, False)

    def _resetCache(self):
        """Creates a new cache, discarding the old one.

        See the docstring for the resetCaches() function.
        """
        self._reset_counter = global_reset_counter
        self._invalidated.clear()
        self._invalidatedCache = False
        cache_size = self._cache.cache_size
        cache_size_bytes = self._cache.cache_size_bytes
        self._cache = cache = PickleCache(self, cache_size, cache_size_bytes)
        if getattr(self, '_reader', None) is not None:
            self._reader._cache = cache

    ##########################################################################
    # Python protocol

    def __repr__(self):
        return '<Connection at %08x>' % (positive_id(self),)

    # Python protocol
    ##########################################################################

    ##########################################################################
    # DEPRECATION candidates

    __getitem__ = get

    def exchange(self, old, new):
        # called by a ZClasses method that isn't executed by the test suite
        oid = old._p_oid
        new._p_oid = oid
        new._p_jar = self
        new._p_changed = 1
        self._register(new)
        self._cache[oid] = new

    # DEPRECATION candidates
    ##########################################################################

    ##########################################################################
    # DEPRECATED methods

    # None at present.

    # DEPRECATED methods
    ##########################################################################

    #####################################################################
    # Savepoint support

    def savepoint(self):
        if self._savepoint_storage is None:
            tmpstore = TmpStore(self._normal_storage)
            self._savepoint_storage = tmpstore
            self._storage = self._savepoint_storage

        self._creating.clear()
        self._commit(None)
        self._storage.creating.update(self._creating)
        self._creating.clear()
        self._registered_objects = []

        state = self._storage.position, self._storage.index.copy()
        result = Savepoint(self, state)
        # While the interface doesn't guarantee this, savepoints are
        # sometimes used just to "break up" very long transactions, and as
        # a pragmatic matter this is a good time to reduce the cache
        # memory burden.
        self.cacheGC()
        return result

    def _rollback(self, state):
        self._abort()
        self._registered_objects = []
        src = self._storage
        self._cache.invalidate(src.index)
        src.reset(*state)

    def _commit_savepoint(self, transaction):
        """Commit all changes made in savepoints and begin 2-phase commit
        """
        src = self._savepoint_storage
        self._storage = self._normal_storage
        self._savepoint_storage = None

        self._log.debug("Committing savepoints of size %s", src.getSize())
        oids = src.index.keys()

        # Copy invalidating and creating info from temporary storage:
        self._modified.extend(oids)
        self._creating.update(src.creating)

        for oid in oids:
            data, serial = src.load(oid, src)
            obj = self._cache.get(oid, None)
            if obj is not None:
                self._cache.update_object_size_estimation(obj._p_oid,
                                                       len(data)
                                                       )
                obj._p_estimated_size = len(data)
            if isinstance(self._reader.getGhost(data), Blob):
                blobfilename = src.loadBlob(oid, serial)
                s = self._storage.storeBlob(oid, serial, data, blobfilename,
                                            '', transaction)
                # we invalidate the object here in order to ensure
                # that that the next attribute access of its name
                # unghostify it, which will cause its blob data
                # to be reattached "cleanly"
                self.invalidate(s, {oid:True})
            else:
                s = self._storage.store(oid, serial, data,
                                        '', transaction)

            self._handle_serial(s, oid, change=False)
        src.close()

    def _abort_savepoint(self):
        """Discard all savepoint data."""
        src = self._savepoint_storage
        self._storage = self._normal_storage
        self._savepoint_storage = None

        # Note: If we invalidate a non-ghostifiable object (i.e. a
        # persistent class), the object will immediately reread it's
        # state.  That means that the following call could result in a
        # call to self.setstate, which, of course, must succeed.  In
        # general, it would be better if the read could be delayed
        # until the start of the next transaction.  If we read at the
        # end of a transaction and if the object was invalidated
        # during this transaction, then we'll read non-current data,
        # which we'll discard later in transaction finalization.  We
        # could, theoretically queue this invalidation by calling
        # self.invalidate.  Unfortunately, attempts to make that
        # change resulted in mysterious test failures.  It's pretty
        # unlikely that the object we are invalidating was invalidated
        # by another thread, so the risk of a reread is pretty low.
        # It's really not worth the effort to pursue this.

        self._cache.invalidate(src.index)
        self._invalidate_creating(src.creating)
        src.close()

    # Savepoint support
    #####################################################################

class Savepoint:

    implements(IDataManagerSavepoint)

    def __init__(self, datamanager, state):
        self.datamanager = datamanager
        self.state = state

    def rollback(self):
        self.datamanager._rollback(self.state)

class TmpStore:
    """A storage-like thing to support savepoints."""

    implements(IBlobStorage)

    def __init__(self, storage):
        self._storage = storage
        for method in (
            'getName', 'new_oid', 'getSize', 'sortKey', 'loadBefore',
            'isReadOnly'
            ):
            setattr(self, method, getattr(storage, method))

        self._file = tempfile.TemporaryFile()
        # position: current file position
        # _tpos: file position at last commit point
        self.position = 0L
        # index: map oid to pos of last committed version
        self.index = {}
        self.creating = {}

    def __len__(self):
        return len(self.index)

    def close(self):
        self._file.close()

    def load(self, oid, version):
        pos = self.index.get(oid)
        if pos is None:
            return self._storage.load(oid, '')
        self._file.seek(pos)
        h = self._file.read(8)
        oidlen = u64(h)
        read_oid = self._file.read(oidlen)
        if read_oid != oid:
            raise POSException.StorageSystemError('Bad temporary storage')
        h = self._file.read(16)
        size = u64(h[8:])
        serial = h[:8]
        return self._file.read(size), serial

    def store(self, oid, serial, data, version, transaction):
        # we have this funny signature so we can reuse the normal non-commit
        # commit logic
        assert version == ''
        self._file.seek(self.position)
        l = len(data)
        if serial is None:
            serial = z64
        header = p64(len(oid)) + oid + serial + p64(l)
        self._file.write(header)
        self._file.write(data)
        self.index[oid] = self.position
        self.position += l + len(header)
        return serial

    def storeBlob(self, oid, serial, data, blobfilename, version,
                  transaction):
        assert version == ''
        serial = self.store(oid, serial, data, '', transaction)

        targetpath = self._getBlobPath()
        if not os.path.exists(targetpath):
            os.makedirs(targetpath, 0700)

        targetname = self._getCleanFilename(oid, serial)
        rename_or_copy_blob(blobfilename, targetname, chmod=False)

    def loadBlob(self, oid, serial):
        """Return the filename where the blob file can be found.
        """
        if not IBlobStorage.providedBy(self._storage):
            raise Unsupported(
                "Blobs are not supported by the underlying storage %r." %
                self._storage)
        filename = self._getCleanFilename(oid, serial)
        if not os.path.exists(filename):
            return self._storage.loadBlob(oid, serial)
        return filename

    def _getBlobPath(self):
        return os.path.join(self.temporaryDirectory(), 'savepoints')

    def _getCleanFilename(self, oid, tid):
        return os.path.join(self._getBlobPath(),
                            "%s-%s%s" % (utils.oid_repr(oid), utils.tid_repr(tid), SAVEPOINT_SUFFIX,)
                            )

    def temporaryDirectory(self):
        return self._storage.temporaryDirectory()

    def reset(self, position, index):
        self._file.truncate(position)
        self.position = position
        # Caution:  We're typically called as part of a savepoint rollback.
        # Other machinery remembers the index to restore, and passes it to
        # us.  If we simply bind self.index to `index`, then if the caller
        # didn't pass a copy of the index, the caller's index will mutate
        # when self.index mutates.  This can be a disaster if the caller is a
        # savepoint to which the user rolls back again later (the savepoint
        # loses the original index it passed).  Therefore, to be safe, we make
        # a copy of the index here.  An alternative would be to ensure that
        # all callers pass copies.  As is, our callers do not make copies.
        self.index = index.copy()
