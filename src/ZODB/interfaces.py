##############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Interfaces for ZODB.

$Id$
"""

from zope.interface import Interface, Attribute

class IConnection(Interface):
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

    TODO:  We should document an intended API for using a Connection via
    multiple threads.

    TODO:  We should explain that the Connection has a cache and that
    multiple calls to get() will return a reference to the same
    object, provided that one of the earlier objects is still
    referenced.  Object identity is preserved within a connection, but
    not across connections.

    TODO:  Mention the database pool.

    A database connection always presents a consistent view of the
    objects in the database, although it may not always present the
    most current revision of any particular object.  Modifications
    made by concurrent transactions are not visible until the next
    transaction boundary (abort or commit).

    Two options affect consistency.  By default, the mvcc and synch
    options are enabled by default.

    If you pass mvcc=False to db.open(), the Connection will never read
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

    Groups of methods:

        User Methods:
            root, get, add, close, db, sync, isReadOnly, cacheGC, cacheFullSweep,
            cacheMinimize, getVersion, modifiedInVersion

        Experimental Methods:
            onCloseCallbacks

        Database Invalidation Methods:
            invalidate

        Other Methods: exchange, getDebugInfo, setDebugInfo,
            getTransferCounts
    """

    def add(ob):
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

        Parameters:
        obj: a Persistent object

        Raises TypeError if obj is not a persistent object.

        Raises InvalidObjectReference if obj is already associated with another
        connection.

        Raises ConnectionStateError if the connection is closed.
        """

    def get(oid):
        """Return the persistent object with oid 'oid'.

        If the object was not in the cache and the object's class is
        ghostable, then a ghost will be returned.  If the object is
        already in the cache, a reference to the cached object will be
        returned.

        Applications seldom need to call this method, because objects
        are loaded transparently during attribute lookup.

        Parameters:
        oid: an object id

        Raises KeyError if oid does not exist.

            It is possible that an object does not exist as of the current
            transaction, but existed in the past.  It may even exist again in
            the future, if the transaction that removed it is undone.

        Raises ConnectionStateError if the connection is closed.
        """

    def cacheMinimize():
        """Deactivate all unmodified objects in the cache.

        Call _p_deactivate() on each cached object, attempting to turn
        it into a ghost.  It is possible for individual objects to
        remain active.
        """

    def cacheGC():
        """Reduce cache size to target size.

        Call _p_deactivate() on cached objects until the cache size
        falls under the target size.
        """

    def onCloseCallback(f):
        """Register a callable, f, to be called by close().

        f will be called with no arguments before the Connection is closed.

        Parameters:
        f: method that will be called on `close`
        """

    def close():
        """Close the Connection.

        When the Connection is closed, all callbacks registered by
        onCloseCallback() are invoked and the cache is garbage collected.

        A closed Connection should not be used by client code.  It can't load
        or store objects.  Objects in the cache are not freed, because
        Connections are re-used and the cache is expected to be useful to the
        next client.
        """

    def db():
        """Returns a handle to the database this connection belongs to."""

    def isReadOnly():
        """Returns True if the storage for this connection is read only."""

    def invalidate(tid, oids):
        """Notify the Connection that transaction 'tid' invalidated oids.

        When the next transaction boundary is reached, objects will be
        invalidated.  If any of the invalidated objects are accessed by the
        current transaction, the revision written before Connection.tid will be
        used.

        The DB calls this method, even when the Connection is closed.

        Parameters:
        tid: the storage-level id of the transaction that committed
        oids: oids is a set of oids, represented as a dict with oids as keys.
        """

    def root():
        """Return the database root object.

        The root is a persistent.mapping.PersistentMapping.
        """

    def getVersion():
        """Returns the version this connection is attached to."""

    # Multi-database support.

    connections = Attribute("""\
        A mapping from database name to a Connection to that database.

        In multi-database use, the Connections of all members of a database
        collection share the same .connections object.

        In single-database use, of course this mapping contains a single
        entry.
        """)

    # TODO:  should this accept all the arguments one may pass to DB.open()?
    def get_connection(database_name):
        """Return a Connection for the named database.

        This is intended to be called from an open Connection associated with
        a multi-database.  In that case, database_name must be the name of a
        database within the database collection (probably the name of a
        different database than is associated with the calling Connection
        instance, but it's fine to use the name of the calling Connection
        object's database).  A Connection for the named database is
        returned.  If no connection to that database is already open, a new
        Connection is opened.  So long as the multi-database remains open,
        passing the same name to get_connection() multiple times returns the
        same Connection object each time.
        """

    def sync():
        """Manually update the view on the database.

        This includes aborting the current transaction, getting a fresh and
        consistent view of the data (synchronizing with the storage if
        possible) and calling cacheGC() for this connection.

        This method was especially useful in ZODB 3.2 to better support
        read-only connections that were affected by a couple of problems.
        """

    # Debug information

    def getDebugInfo():
        """Returns a tuple with different items for debugging the connection.

        Debug information can be added to a connection by using setDebugInfo.
        """

    def setDebugInfo(*items):
        """Add the given items to the debug information of this connection."""

    def getTransferCounts(clear=False):
        """Returns the number of objects loaded and stored.

        If clear is True, reset the counters.
        """

class IDatabase(Interface):
    """ZODB DB.

    TODO: This interface is incomplete.
    """

    def __init__(storage,
                 pool_size=7,
                 cache_size=400,
                 version_pool_size=3,
                 version_cache_size=100,
                 database_name='unnamed',
                 databases=None,
                 ):
        """Create an object database.

        storage: the storage used by the database, e.g. FileStorage
        pool_size: expected maximum number of open connections
        cache_size: target size of Connection object cache, in number of
            objects
        version_pool_size: expected maximum number of connections (per
            version)
        version_cache_size: target size of Connection object cache for
             version connections, in number of objects
        database_name: when using a multi-database, the name of this DB
            within the database group.  It's a (detected) error if databases
            is specified too and database_name is already a key in it.
            This becomes the value of the DB's database_name attribute.
        databases: when using a multi-database, a mapping to use as the
            binding of this DB's .databases attribute.  It's intended
            that the second and following DB's added to a multi-database
            pass the .databases attribute set on the first DB added to the
            collection.
        """

    databases = Attribute("""\
        A mapping from database name to DB (database) object.

        In multi-database use, all DB members of a database collection share
        the same .databases object.

        In single-database use, of course this mapping contains a single
        entry.
        """)

class IStorage(Interface):
    """A storage is responsible for storing and retrieving data of objects.
    """

## What follows is the union of methods found across various storage
## implementations.  Exactly what "the storage API" is and means has
## become fuzzy over time.  Methods should be uncommented here, or
## even deleted, as the storage API regains a crisp definition.

##    def load(oid, version):
##        """TODO"""
##
##    def close():
##        """TODO"""
##
##    def cleanup():
##        """TODO"""
##
##    def lastSerial():
##        """TODO"""
##
##    def lastTransaction():
##        """TODO"""
##
##    def lastTid(oid):
##        """Return last serialno committed for object oid."""
##
##    def loadSerial(oid, serial):
##        """TODO"""
##
##    def loadBefore(oid, tid):
##        """TODO"""
##
##    def iterator(start=None, stop=None):
##        """TODO"""
##
##    def sortKey():
##        """TODO"""
##
##    def getName():
##        """TODO"""
##
##    def getSize():
##        """TODO"""
##
##    def history(oid, version, length=1, filter=None):
##        """TODO"""
##
##    def new_oid():
##        """TODO"""
##
##    def set_max_oid(possible_new_max_oid):
##        """TODO"""
##
##    def registerDB(db, limit):
##        """TODO"""
##
##    def isReadOnly():
##        """TODO"""
##
##    def supportsUndo():
##        """TODO"""
##
##    def supportsVersions():
##        """TODO"""
##
##    def tpc_abort(transaction):
##        """TODO"""
##
##    def tpc_begin(transaction):
##        """TODO"""
##
##    def tpc_vote(transaction):
##        """TODO"""
##
##    def tpc_finish(transaction, f=None):
##        """TODO"""
##
##    def getSerial(oid):
##        """TODO"""
##
##    def loadSerial(oid, serial):
##        """TODO"""
##
##    def loadBefore(oid, tid):
##        """TODO"""
##
##    def getExtensionMethods():
##        """TODO"""
##
##    def copyTransactionsFrom():
##        """TODO"""
##
##    def store(oid, oldserial, data, version, transaction):
##        """
##
##        may return the new serial or not
##        """

class IStorageUndoable(IStorage):
    """A storage supporting transactional undo.
    """

    def undo(transaction_id, txn):
        """TODO"""

    def undoLog(first, last, filter=(lambda desc: True)):
        """Return a sequence of descriptions for undoable transactions.

        Application code should call undoLog() on a DB instance instead of on
        the storage directly.

        A transaction description is a mapping with at least these keys:

            "time":  The time, as float seconds since the epoch, when
                     the transaction committed.
            "user_name":  The value of the `.user` attribute on that
                          transaction.
            "description":  The value of the `.description` attribute on
                            that transaction.
            "id`"  A string uniquely identifying the transaction to the
                   storage.  If it's desired to undo this transaction,
                   this is the `transaction_id` to pass to `undo()`.

        In addition, if any name+value pairs were added to the transaction
        by `setExtendedInfo()`, those may be added to the transaction
        description mapping too (for example, FileStorage's `undoLog()` does
        this).

        `filter` is a callable, taking one argument.  A transaction
        description mapping is passed to `filter` for each potentially
        undoable transaction.  The sequence returned by `undoLog()` excludes
        descriptions for which `filter` returns a false value.  By default,
        `filter` always returns a true value.

        ZEO note:  Arbitrary callables cannot be passed from a ZEO client
        to a ZEO server, and a ZEO client's implementation of `undoLog()`
        ignores any `filter` argument that may be passed.  ZEO clients
        should use the related `undoInfo()` method instead (if they want
        to do filtering).

        Now picture a list containing descriptions of all undoable
        transactions that pass the filter, most recent transaction first (at
        index 0).  The `first` and `last` arguments specify the slice of this
        (conceptual) list to be returned:

            `first`:  This is the index of the first transaction description
                      in the slice.  It must be >= 0.
            `last`:  If >= 0, first:last acts like a Python slice, selecting
                     the descriptions at indices `first`, first+1, ..., up to
                     but not including index `last`.  At most last-first
                     descriptions are in the slice, and `last` should be at
                     least as large as `first` in this case.  If `last` is
                     less than 0, then abs(last) is taken to be the maximum
                     number of descriptions in the slice (which still begins
                     at index `first`).  When `last` < 0, the same effect
                     could be gotten by passing the positive first-last for
                     `last` instead.
        """

    def undoInfo(first, last, specification=None):
        """Return a sequence of descriptions for undoable transactions.

        This is like `undoLog()`, except for the `specification` argument.
        If given, `specification` is a dictionary, and `undoInfo()`
        synthesizes a `filter` function `f` for `undoLog()` such that
        `f(desc)` returns true for a transaction description mapping
        `desc` if and only if `desc` maps each key in `specification` to
        the same value `specification` maps that key to.  In other words,
        only extensions (or supersets) of `specification` match.

        ZEO note:  `undoInfo()` passes the `specification` argument from a
        ZEO client to its ZEO server (while a ZEO client ignores any `filter`
        argument passed to `undoLog()`).
        """

    def pack(t, referencesf):
        """TODO"""

class IStorageVersioning(IStorage):
    """A storage supporting versions.
    """

## What follows is the union of methods found across various version storage
## implementations.  Exactly what "the storage API" is and means has
## become fuzzy over time.  Methods should be uncommented here, or
## even deleted, as the storage API regains a crisp definition.

##    def abortVersion(src, transaction):
##        """TODO"""
##
##    def commitVersion(src, dest, transaction):
##        """TODO"""
##
##    def modifiedInVersion(oid):
##        """TODO"""
##
##    def versionEmpty(version):
##        """TODO"""
##
##    def versions(max=None):
##        """TODO"""
