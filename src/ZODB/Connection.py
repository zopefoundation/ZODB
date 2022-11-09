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
"""Database connection support
"""
from __future__ import print_function

import logging
import os
import tempfile
import time
import warnings

import six

import transaction
from persistent import PickleCache
# interfaces
from persistent.interfaces import IPersistentDataManager
from transaction.interfaces import IDataManagerSavepoint
from transaction.interfaces import ISavepointDataManager
from transaction.interfaces import ISynchronizer
from zope.interface import implementer

import ZODB
from ZODB import POSException
from ZODB import utils
from ZODB.blob import SAVEPOINT_SUFFIX
from ZODB.blob import Blob
from ZODB.blob import remove_committed_dir
from ZODB.blob import rename_or_copy_blob
from ZODB.ExportImport import ExportImport
from ZODB.interfaces import IBlobStorage
from ZODB.interfaces import IConnection
from ZODB.interfaces import IStorageTransactionMetaData
from ZODB.POSException import ConflictError
from ZODB.POSException import ConnectionStateError
from ZODB.POSException import InvalidObjectReference
from ZODB.POSException import ReadConflictError
from ZODB.POSException import ReadOnlyHistoryError
from ZODB.POSException import Unsupported
from ZODB.serialize import ObjectReader
from ZODB.serialize import ObjectWriter
from ZODB.utils import oid_repr
from ZODB.utils import p64
from ZODB.utils import positive_id
from ZODB.utils import u64
from ZODB.utils import z64

from . import valuedoc
from ._compat import _protocol
from ._compat import dumps
from ._compat import loads
from .mvccadapter import HistoricalStorageAdapter


global_reset_counter = 0


def noop():
    return None


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


def className(obj):
    cls = type(obj)
    return "%s.%s" % (cls.__module__, cls.__name__)


@implementer(IConnection,
             ISavepointDataManager,
             IPersistentDataManager,
             ISynchronizer)
class Connection(ExportImport, object):
    """Connection to ZODB for loading and storing objects.

    Connections manage object state in collaboration with transaction
    managers.  They're created by calling the
    :meth:`~ZODB.DB.open` method on :py:class:`database
    <ZODB.DB>` objects.
    """

    _code_timestamp = 0

    #: Transaction manager associated with the connection when it was opened.
    transaction_manager = valuedoc.ValueDoc('current transaction manager')

    ##########################################################################
    # Connection methods, ZODB.IConnection

    def __init__(self, db, cache_size=400, before=None, cache_size_bytes=0):
        """Create a new Connection."""

        self._log = logging.getLogger('ZODB.Connection')
        self._debug_info = ()

        self._db = db
        self.large_record_size = db.large_record_size

        # historical connection
        self.before = before

        # Multi-database support
        self.connections = {self._db.database_name: self}

        storage = db._mvcc_storage
        if before:
            try:
                before_instance = storage.before_instance
            except AttributeError:
                def before_instance(before):
                    return HistoricalStorageAdapter(
                        storage.new_instance(), before)
            storage = before_instance(before)
        else:
            storage = storage.new_instance()

        self._normal_storage = self._storage = storage
        self._savepoint_storage = None

        # Do we need to join a txn manager?
        self._needs_to_join = True
        self.transaction_manager = None
        self.opened = None  # time.time() when DB.open() opened us

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
        # persistence machinery.
        # All objects of this list are either in _cache or in _added.
        self._registered_objects = []  # [object]

        # ids and serials of objects for which readCurrent was called
        # in a transaction.
        self._readCurrent = {}  # {oid ->serial}

        # Dict of oid->obj added explicitly through add(). Used as a
        # preliminary cache until commit time when objects are all moved
        # to the real _cache. The objects are moved to _creating at
        # commit time.
        self._added = {}  # {oid -> object}

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
        self._creating = {}  # {oid -> implicitly_added_flag}

        # List of oids of modified objects, which have to be invalidated
        # in the cache on abort and in other connections on finish.
        self._modified = []  # [oid]

        # To support importFile(), implemented in the ExportImport base
        # class, we need to run _importDuringCommit() from our commit()
        # method.  If _import is not None, it is a two-tuple of arguments
        # to pass to _importDuringCommit().
        self._import = None

        self._reader = ObjectReader(self, self._cache, self._db.classFactory)

    def new_oid(self):
        return self._storage.new_oid()

    def add(self, obj):
        """Add a new object 'obj' to the database and assign it an oid."""
        if self.opened is None:
            raise ConnectionStateError("The database connection is closed")

        marker = object()
        oid = getattr(obj, "_p_oid", marker)
        if oid is marker:
            raise TypeError("Only first-class persistent objects may be"
                            " added to a Connection.", obj)
        elif obj._p_jar is None:
            self._add(obj, self.new_oid())
        elif obj._p_jar is not self:
            raise InvalidObjectReference(obj, obj._p_jar)

    def _add(self, obj, oid):
        assert obj._p_oid is None
        oid = obj._p_oid = oid
        obj._p_jar = self
        if self._added_during_commit is not None:
            self._added_during_commit.append(obj)
        self._register(obj)
        # Add to _added after calling register(), so that _added
        # can be used as a test for whether the object has been
        # registered with the transaction.
        self._added[oid] = obj

    def get(self, oid):
        """Return the persistent object with oid 'oid'."""
        if self.opened is None:
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

        p, _ = self._storage.load(oid)
        obj = self._reader.getGhost(p)

        # Avoid infiniate loop if obj tries to load its state before
        # it is added to the cache and it's state refers to it.
        # (This will typically be the case for non-ghostifyable objects,
        # like persistent caches.)
        self._pre_cache[oid] = obj
        self._cache.new_ghost(oid, obj)
        self._pre_cache.pop(oid)
        return obj

    def cacheMinimize(self):
        """Deactivate all unmodified objects in the cache.
        """
        for connection in six.itervalues(self.connections):
            connection._cache.minimize()

    # TODO: we should test what happens when cacheGC is called mid-transaction.
    def cacheGC(self):
        """Reduce cache size to target size.
        """
        for connection in six.itervalues(self.connections):
            connection._cache.incrgc()

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

        self._cache.incrgc()  # This is a good time to do some GC

        # Call the close callbacks.
        if self.__onCloseCallbacks is not None:
            callbacks = self.__onCloseCallbacks
            self.__onCloseCallbacks = None
            for f in callbacks:
                try:
                    f()
                except:  # noqa: E722 do not use bare 'except'
                    f = getattr(f, 'im_self', f)
                    self._log.exception("Close callback failed for %s", f)

        self._debug_info = ()

        if self.opened and self.transaction_manager is not None:
            # transaction_manager could be None if one of the
            # __onCloseCallbacks closed the DB already, .e.g, ZODB.connection()
            # does this.
            self.transaction_manager.unregisterSynch(self)

        am = self._db._activity_monitor
        if am is not None:
            am.closedConnection(self)

        # Drop transaction manager to release resources and help prevent errors
        self.transaction_manager = None

        if hasattr(self._storage, 'afterCompletion'):
            self._storage.afterCompletion()

        if primary:
            for connection in self.connections.values():
                if connection is not self:
                    connection.close(False)

            # Return the connection to the pool.
            if self.opened is not None:
                self._db._returnToPool(self)

                # _returnToPool() set self.opened to None.
                # However, we can't assert that here, because self may
                # have been reused (by another thread) by the time we
                # get back here.
        else:
            self.opened = None

        # We may have been reused by another thread at this point so
        # we can't manipulate or check the state of `self` any more.

    def db(self):
        """Returns a handle to the database this connection belongs to."""
        return self._db

    def isReadOnly(self):
        """Returns True if this connection is read only."""
        if self.opened is None:
            raise ConnectionStateError("The database connection is closed")
        return self._storage.isReadOnly()

    @property
    def root(self):
        """Return the database root object."""
        return RootConvenience(self.get(z64))

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
        self.transaction_manager.begin()

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

        self._invalidate_creating()
        self._tpc_cleanup()

    def _abort(self):
        """Abort a transaction and forget all changes."""

        for obj in self._registered_objects:
            oid = obj._p_oid
            assert oid is not None
            if oid in self._added:
                del self._added[oid]
                if self._cache.get(oid) is not None:
                    del self._cache[oid]
                del obj._p_jar
                del obj._p_oid
                if obj._p_changed:
                    obj._p_changed = False
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
        self._needs_to_join = True
        self._registered_objects = []
        self._creating.clear()

    def tpc_begin(self, transaction):
        """Begin commit of a transaction, starting the two-phase commit."""
        self._modified = []
        meta_data = TransactionMetaData(
            transaction.user,
            transaction.description,
            transaction.extension)
        transaction.set_data(self, meta_data)

        # _creating is a list of oids of new objects, which is used to
        # remove them from the cache if a transaction aborts.
        self._creating.clear()
        self._normal_storage.tpc_begin(meta_data)

    def commit(self, transaction):
        """Commit changes to an object"""
        transaction = transaction.data(self)

        if self._savepoint_storage is not None:

            # We first checkpoint the current changes to the savepoint
            self.savepoint()

            # then commit all of the savepoint changes at once
            self._commit_savepoint(transaction)

            # No need to call _commit since savepoint did.

        else:
            self._commit(transaction)

        for oid, serial in six.iteritems(self._readCurrent):
            try:
                self._storage.checkCurrentSerialInTransaction(
                    oid, serial, transaction)
            except ConflictError:
                self._cache.invalidate(oid)
                raise

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

        for obj in self._registered_objects:
            oid = obj._p_oid
            assert oid

            if obj._p_jar is not self:
                raise InvalidObjectReference(obj, obj._p_jar)
            elif oid in self._added:
                assert obj._p_serial == z64
            elif oid in self._creating or not obj._p_changed:
                # Nothing to do.  It's been said that it's legal, e.g., for
                # an object to set _p_changed to false after it's been
                # changed and registered.
                # And new objects that are registered after any referrer are
                # already processed.
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
                     )):

                # obj is a new object

                # Because obj was added, it is now in _creating, so it
                # can be removed from _added.  If oid wasn't in
                # adding, then we are adding it implicitly.

                implicitly_adding = self._added.pop(oid, None) is None

                self._creating[oid] = implicitly_adding

            else:
                self._modified.append(oid)

            p = writer.serialize(obj)  # This calls __getstate__ of obj
            if len(p) >= self.large_record_size:
                warnings.warn(large_object_message % (obj.__class__, len(p)))

            if isinstance(obj, Blob):
                if not IBlobStorage.providedBy(self._storage):
                    raise Unsupported(
                        "Storing Blobs in %s is not supported." %
                        repr(self._storage))
                if obj.opened():
                    raise ValueError("Can't commit with opened blobs.")
                blobfilename = obj._uncommitted()
                if blobfilename is None:
                    assert serial is not None  # See _uncommitted
                    self._modified.pop()  # not modified
                    continue
                s = self._storage.storeBlob(oid, serial, p, blobfilename,
                                            '', transaction)
                # we invalidate the object here in order to ensure
                # that that the next attribute access of its name
                # unghostify it, which will cause its blob data
                # to be reattached "cleanly"
                obj._p_invalidate()
            else:
                s = self._storage.store(oid, serial, p, '', transaction)

            self._store_count += 1
            # Put the object in the cache before handling the
            # response, just in case the response contains the
            # serial number for a newly created object
            try:
                self._cache[oid] = obj
            except:  # noqa: E722 do not use bare 'except'
                # Dang, I bet it's wrapped:
                # TODO:  Deprecate, then remove, this.
                if hasattr(obj, 'aq_base'):
                    self._cache[oid] = obj.aq_base
                else:
                    raise

            self._cache.update_object_size_estimation(oid, len(p))
            obj._p_estimated_size = len(p)

            # if we write an object, we don't want to check if it was read
            # while current.  This is a convenient choke point to do this.
            self._readCurrent.pop(oid, None)
            if s:
                # savepoint
                obj._p_changed = 0  # transition from changed to up-to-date
                obj._p_serial = s

    def tpc_abort(self, transaction):
        transaction = transaction.data(self)

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
            if obj._p_changed:
                obj._p_changed = False
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
                if o._p_changed:
                    o._p_changed = False
                del o._p_jar
                del o._p_oid

    def tpc_vote(self, transaction):
        """Verify that a data manager can commit the transaction."""
        try:
            vote = self._storage.tpc_vote
        except AttributeError:
            return

        transaction = transaction.data(self)

        try:
            s = vote(transaction)
        except ReadConflictError as v:
            if v.oid:
                self._cache.invalidate(v.oid)
            raise
        if s:
            # Resolved conflicts.
            for oid in s:
                obj = self._cache.get(oid)
                if obj is not None:
                    del obj._p_changed  # transition from changed to ghost

    def tpc_finish(self, transaction):
        """Indicate confirmation that the transaction is done.
        """
        transaction = transaction.data(self)

        serial = self._storage.tpc_finish(transaction)
        assert type(serial) is bytes, repr(serial)
        for oid_iterator in self._modified, self._creating:
            for oid in oid_iterator:
                obj = self._cache.get(oid)
                # Ignore missing objects and don't update ghosts.
                if obj is not None and obj._p_changed is not None:
                    obj._p_changed = 0
                    obj._p_serial = serial
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

    def newTransaction(self, transaction, sync=True):
        self._readCurrent.clear()
        self._storage.sync(sync)
        invalidated = self._storage.poll_invalidations()
        if invalidated is None:
            # special value: the transaction is so old that
            # we need to flush the whole cache.
            invalidated = self._cache.cache_data.copy()
        self._cache.invalidate(invalidated)

    def afterCompletion(self, transaction):
        # Note that we we call newTransaction here for 2 reasons:
        # a) Applying invalidations early frees up resources
        #    early. This is especially useful if the connection isn't
        #    going to be used in a while.
        # b) Non-hygienic applications might start new transactions by
        #    finalizing previous ones without calling begin.  We pass
        #    False to avoid possiblyt expensive sync calls to not
        #    penalize well-behaved applications that call begin.
        if hasattr(self._storage, 'afterCompletion'):
            self._storage.afterCompletion()

        if not self.explicit_transactions:
            self.newTransaction(transaction, False)

        # Now is a good time to collect some garbage.
        self._cache.incrgc()

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
        """Load the state for an (ghost) object
        """

        oid = obj._p_oid

        if self.opened is None:
            msg = ("Shouldn't load state for %s %s "
                   "when the connection is closed"
                   % (className(obj), oid_repr(oid)))
            try:
                raise ConnectionStateError(msg)
            except:  # noqa: E722 do not use bare 'except'
                self._log.exception(msg)
                raise

        try:
            p, serial = self._storage.load(oid)

            self._load_count += 1

            self._reader.setGhostState(obj, p)
            obj._p_serial = serial
            self._cache.update_object_size_estimation(oid, len(p))
            obj._p_estimated_size = len(p)

            # Blob support
            if isinstance(obj, Blob):
                obj._p_blob_uncommitted = None
                obj._p_blob_committed = self._storage.loadBlob(oid, serial)

        except ConflictError:
            raise
        except:  # noqa: E722 do not use bare 'except'
            self._log.exception("Couldn't load state for %s %s",
                                className(obj), oid_repr(oid))
            raise

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

    def readCurrent(self, ob):
        assert ob._p_jar is self
        assert ob._p_oid is not None and ob._p_serial is not None
        if ob._p_serial != z64:
            self._readCurrent[ob._p_oid] = ob._p_serial

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
        for k, v in items:
            del everything[k]
        # return a list of [ghosts....not recently used.....recently used]
        return list(everything.items()) + items

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

        if transaction_manager is None:
            # The .manager bit below unwraps the threaded
            # manager so we can call unregisterSynch in close
            # when close is called from another thread.
            transaction_manager = transaction.manager.manager

        self.transaction_manager = transaction_manager

        self.explicit_transactions = getattr(transaction_manager,
                                             'explicit', False)

        self.opened = time.time()

        if self._reset_counter != global_reset_counter:
            # New code is in place.  Start a new cache.
            self._resetCache()

        if not self.explicit_transactions:
            # This newTransaction is to deal with some pathalogical cases:
            #
            # a) Someone opens a connection when a transaction isn't
            #    active and proceeeds without calling begin on a
            #    transaction manager. We initialize the transaction for
            #    the connection, but we don't do a storage sync, since
            #    this will be done if a well-nehaved application calls
            #    begin, and we don't want to penalize well-behaved
            #    transactions by syncing twice, as storage syncs might be
            #    expensive.
            # b) Lots of tests assume that connection transaction
            #    information is set on open.
            #
            # Fortunately, this is a cheap operation.  It doesn't
            # really cost much, if anything.  Well, except for
            # RelStorage, in which case it adds a server round
            # trip.
            self.newTransaction(None, False)

        transaction_manager.registerSynch(self)

        self._cache.incrgc()  # This is a good time to do some GC

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
        cache_size = self._cache.cache_size
        cache_size_bytes = self._cache.cache_size_bytes
        self._cache = cache = PickleCache(self, cache_size, cache_size_bytes)
        if getattr(self, '_reader', None) is not None:
            self._reader._cache = cache

    def _release_resources(self):
        for c in six.itervalues(self.connections):
            if c._storage is not None:
                c._storage.release()
            c._storage = c._normal_storage = None
            c._cache = PickleCache(self, 0, 0)
            c.close(False)

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

        state = (self._storage.position,
                 self._storage.index.copy(),
                 self._storage.creating.copy(),
                 )
        result = Savepoint(self, state)
        # While the interface doesn't guarantee this, savepoints are
        # sometimes used just to "break up" very long transactions, and as
        # a pragmatic matter this is a good time to reduce the cache
        # memory burden.
        self.cacheGC()
        return result

    def _rollback_savepoint(self, state):
        self._abort()
        self._registered_objects = []
        src = self._storage

        # Invalidate objects created *after* the savepoint.
        self._invalidate_creating((oid for oid in src.creating
                                   if oid not in state[2]))
        index = src.index
        src.reset(*state)
        self._cache.invalidate(index)

    def _commit_savepoint(self, transaction):
        """Commit all changes made in savepoints and begin 2-phase commit
        """
        src = self._savepoint_storage
        self._storage = self._normal_storage
        self._savepoint_storage = None
        try:
            self._log.debug("Committing savepoints of size %s", src.getSize())
            oids = sorted(src.index.keys())

            # Copy invalidating and creating info from temporary storage:
            self._modified.extend(oids)
            self._creating.update(src.creating)

            for oid in oids:
                data, serial = src.load(oid)
                obj = self._cache.get(oid, None)
                if obj is not None:
                    self._cache.update_object_size_estimation(
                        obj._p_oid, len(data))
                    obj._p_estimated_size = len(data)
                if isinstance(self._reader.getGhost(data), Blob):
                    blobfilename = src.loadBlob(oid, serial)
                    self._storage.storeBlob(
                        oid, serial, data, blobfilename,
                        '', transaction)
                    # we invalidate the object here in order to ensure
                    # that that the next attribute access of its name
                    # unghostify it, which will cause its blob data
                    # to be reattached "cleanly"
                    self._cache.invalidate(oid)
                else:
                    self._storage.store(oid, serial, data, '', transaction)

                self._readCurrent.pop(oid, None)  # same as in _store_objects()
        finally:
            src.close()

    def _abort_savepoint(self):
        """Discard all savepoint data."""
        src = self._savepoint_storage
        self._invalidate_creating(src.creating)
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

        # Note that we do this *after* resetting the storage so that, if
        # data are read, we read it from the reset storage!

        self._cache.invalidate(src.index)

        src.close()

    # Savepoint support
    #####################################################################

    def prefetch(self, *args):
        try:
            self._storage.prefetch(self._prefetch_flatten(args))
        except AttributeError:
            if not hasattr(self._storage, 'prefetch'):
                self.prefetch = lambda *a: None
            else:
                raise

    def _prefetch_flatten(self, args):
        for arg in args:
            if isinstance(arg, bytes):
                yield arg
            elif hasattr(arg, '_p_oid'):
                yield arg._p_oid
            else:
                for ob in arg:
                    if isinstance(ob, bytes):
                        yield ob
                    else:
                        yield ob._p_oid


@implementer(IDataManagerSavepoint)
class Savepoint(object):

    def __init__(self, datamanager, state):
        self.datamanager = datamanager
        self.state = state

    def rollback(self):
        self.datamanager._rollback_savepoint(self.state)


@implementer(IBlobStorage)
class TmpStore(object):
    """A storage-like thing to support savepoints."""

    def __init__(self, storage):
        self._storage = storage
        for method in (
            'getName', 'new_oid', 'sortKey',
            'isReadOnly'
        ):
            setattr(self, method, getattr(storage, method))

        self._file = tempfile.TemporaryFile(prefix='TmpStore')
        # position: current file position. If objects are only stored
        # once, this is approximately the byte size of object data stored.
        self.position = 0
        # index: map oid to pos of last committed version
        self.index = {}
        self.creating = {}
        self._blob_dir = None

    def getSize(self):
        return self.position

    def __len__(self):
        return len(self.index)

    def close(self):
        self._file.close()
        if self._blob_dir is not None:
            remove_committed_dir(self._blob_dir)
            self._blob_dir = None

    def load(self, oid, version=''):
        pos = self.index.get(oid)
        if pos is None:
            return self._storage.load(oid)
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
        lenght = len(data)
        if serial is None:
            serial = z64
        header = p64(len(oid)) + oid + serial + p64(lenght)
        self._file.write(header)
        self._file.write(data)
        self.index[oid] = self.position
        self.position += lenght + len(header)
        return serial

    def storeBlob(self, oid, serial, data, blobfilename, version,
                  transaction):
        assert version == ''
        serial = self.store(oid, serial, data, '', transaction)

        targetpath = self._getBlobPath()
        if not os.path.exists(targetpath):
            os.makedirs(targetpath)

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

    def openCommittedBlobFile(self, oid, serial, blob=None):
        blob_filename = self.loadBlob(oid, serial)
        if blob is None:
            return open(blob_filename, 'rb')
        else:
            return ZODB.blob.BlobFile(blob_filename, 'r', blob)

    def _getBlobPath(self):
        blob_dir = self._blob_dir
        if blob_dir is None:
            blob_dir = tempfile.mkdtemp(dir=self.temporaryDirectory(),
                                        prefix='savepoints')
            self._blob_dir = blob_dir
        return blob_dir

    def _getCleanFilename(self, oid, tid):
        return os.path.join(
            self._getBlobPath(),
            "%s-%s%s" % (utils.oid_repr(oid), utils.tid_repr(tid),
                         SAVEPOINT_SUFFIX,)
        )

    def temporaryDirectory(self):
        return self._storage.temporaryDirectory()

    def reset(self, position, index, creating):
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
        self.creating = creating


class RootConvenience(object):

    def __init__(self, root):
        self.__dict__['_root'] = root

    def __getattr__(self, name):
        try:
            return self._root[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, v):
        self._root[name] = v

    def __delattr__(self, name):
        try:
            del self._root[name]
        except KeyError:
            raise AttributeError(name)

    def __call__(self):
        return self._root

    def __repr__(self):
        names = " ".join(sorted(self._root))
        if len(names) > 60:
            names = names[:57].rsplit(' ', 1)[0] + ' ...'
        return "<root: %s>" % names


large_object_message = """The %s
object you're saving is large. (%s bytes.)

Perhaps you're storing media which should be stored in blobs.

Perhaps you're using a non-scalable data structure, such as a
PersistentMapping or PersistentList.

Perhaps you're storing data in objects that aren't persistent at
all. In cases like that, the data is stored in the record of the
containing persistent object.

In any case, storing records this big is probably a bad idea.

If you insist and want to get rid of this warning, use the
large_record_size option of the ZODB.DB constructor (or the
large-record-size option in a configuration file) to specify a larger
size.
"""


class overridable_property(object):
    """
    Same as property() with only a getter, except that setting a
    value overrides the property rather than raising AttributeError.
    """

    def __init__(self, func):
        self.__doc__ = func.__doc__
        self.func = func

    def __get__(self, obj, cls):
        return self if obj is None else self.func(obj)


@implementer(IStorageTransactionMetaData)
class TransactionMetaData(object):

    def __init__(self, user=u'', description=u'', extension=None):
        if not isinstance(user, bytes):
            user = user.encode('utf-8')
        self.user = user

        if not isinstance(description, bytes):
            description = description.encode('utf-8')
        self.description = description

        if isinstance(extension, bytes):
            self.extension_bytes = extension
        else:
            self.extension = {} if extension is None else extension

    @overridable_property
    def extension(self):
        extension_bytes = self.extension_bytes
        return loads(extension_bytes) if extension_bytes else {}

    @overridable_property
    def extension_bytes(self):
        extension = self.extension
        return dumps(extension, _protocol) if extension else b''

    def note(self, text):  # for tests
        text = text.strip()
        if not isinstance(text, bytes):
            text = text.encode('utf-8')
        if self.description:
            self.description = self.description.strip() + b' ' + text
        else:
            self.description = text

    @property
    def _extension(self):
        warnings.warn("_extension is deprecated, use extension",
                      DeprecationWarning, stacklevel=2)
        return self.extension

    @_extension.setter
    def _extension(self, v):
        self.extension = v

    def data(self, ob):
        try:
            return self._data[id(ob)]
        except (AttributeError, KeyError):
            raise KeyError(ob)

    def set_data(self, ob, ob_data):
        try:
            data = self._data
        except AttributeError:
            data = self._data = {}

        data[id(ob)] = ob_data
