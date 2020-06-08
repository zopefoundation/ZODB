# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) Zope Corporation and Contributors.
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

    A Connection can be frozen to a serial--a transaction id, a single point in
    history-- when it is created. By default, a Connection is not associated
    with a serial; it uses current data. A Connection frozen to a serial is
    read-only.

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
            root, get, add, close, db, sync, isReadOnly, cacheGC,
            cacheFullSweep, cacheMinimize

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
        oids: oids is an iterable of oids.
        """

    def root():
        """Return the database root object.

        The root is a persistent.mapping.PersistentMapping.
        """

    # Multi-database support.

    connections = Attribute(
        """A mapping from database name to a Connection to that database.

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

    def invalidateCache():
        """Invalidate the connection cache

        This invalidates *all* objects in the cache. If the connection
        is open, subsequent reads will fail until a new transaction
        begins or until the connection os reopned.
        """

    def readCurrent(obj):
        """Make sure an object being read is current

        This is used when applications want to ensure a higher level
        of consistency for some operations. This should be called when
        an object is read and the information read is used to write a
        separate object.
        """

class IStorageWrapper(Interface):
    """Storage wrapper interface

    This interface provides 3 facilities:

    - Out-of-band invalidation support

      A storage can notify it's wrapper of object invalidations that
      don't occur due to direct operations on the storage.  Currently
      this is only used by ZEO client storages to pass invalidation
      messages sent from a server.

    - Record-reference extraction

      The references method can be used to extract referenced object
      IDs from a database record.  This can be used by storages to
      provide more advanced garbage collection.  A wrapper storage
      that transforms data will provide a references method that
      untransforms data passed to it and then pass the data to the
      layer above it.

    - Record transformation

      A storage wrapper may transform data, for example for
      compression or encryption.  Methods are provided to transform or
      untransform data.

    This interface may be implemented by storage adapters or other
    intermediaries.  For example, a storage adapter that provides
    encryption and/or compresssion will apply record transformations
    in it's references method.
    """

    def invalidateCache():
        """Discard all cached data

        This can be necessary if there have been major changes to
        stored data and it is either impractical to enumerate them or
        there would be so many that it would be inefficient to do so.
        """

    def invalidate(transaction_id, oids, version=''):
        """Invalidate object ids committed by the given transaction

        The oids argument is an iterable of object identifiers.

        The version argument is provided for backward
        compatibility. If passed, it must be an empty string.

        """

    def references(record, oids=None):
        """Scan the given record for object ids

        A list of object ids is returned.  If a list is passed in,
        then it will be used and augmented. Otherwise, a new list will
        be created and returned.
        """

    def transform_record_data(data):
        """Return transformed data
        """

    def untransform_record_data(data):
        """Return untransformed data
        """

IStorageDB = IStorageWrapper # for backward compatibility


class IDatabase(IStorageDB):
    """ZODB DB.
    """

    # TODO: This interface is incomplete.
    # XXX how is it incomplete?

    databases = Attribute(
        """A mapping from database name to DB (database) object.

        In multi-database use, all DB members of a database collection share
        the same .databases object.

        In single-database use, of course this mapping contains a single
        entry.
        """)

    storage = Attribute(
        """The object that provides storage for the database

        This attribute is useful primarily for tests.  Normal
        application code should rarely, if ever, have a need to use
        this attribute.
        """)


    def open(transaction_manager=None, serial=''):
        """Return an IConnection object for use by application code.

        transaction_manager: transaction manager to use.  None means
            use the default transaction manager.
        serial: the serial (transaction id) of the database to open.
            An empty string (the default) means to open it to the newest
            serial. Specifying a serial results in a read-only historical
            connection.

        Note that the connection pool is managed as a stack, to
        increase the likelihood that the connection's stack will
        include useful objects.
        """

    # TODO: Should this method be moved into some subinterface?
    def pack(t=None, days=0):
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

    # TODO: Should this method be moved into some subinterface?
    def undo(id, txn=None):
        """Undo a transaction identified by id.

        A transaction can be undone if all of the objects involved in
        the transaction were not modified subsequently, if any
        modifications can be resolved by conflict resolution, or if
        subsequent changes resulted in the same object state.

        The value of id should be generated by calling undoLog()
        or undoInfo().  The value of id is not the same as a
        transaction id used by other methods; it is unique to undo().

        id: a storage-specific transaction identifier
        txn: transaction context to use for undo().
            By default, uses the current transaction.
        """

    def close():
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


class IStorage(Interface):
    """A storage is responsible for storing and retrieving data of objects.

    Consistency and locking
    -----------------------

    When transactions are committed, a storage assigns monotonically
    increasing transaction identifiers (tids) to the transactions and
    to the object versions written by the transactions.  ZODB relies
    on this to decide if data in object caches are up to date and to
    implement multi-version concurrency control.

    There are methods in IStorage and in derived interfaces that
    provide information about the current revisions (tids) for objects
    or for the database as a whole.  It is critical for the proper
    working of ZODB that the resulting tids are increasing with
    respect to the object identifier given or to the databases.  That
    is, if there are 2 results for an object or for the database, R1
    and R2, such that R1 is returned before R2, then the tid returned
    by R2 must be greater than or equal to the tid returned by R1.
    (When thinking about results for the database, think of these as
    results for all objects in the database.)

    This implies some sort of locking strategy.  The key method is
    tcp_finish, which causes new tids to be generated and also,
    through the callback passed to it, returns new current tids for
    the objects stored in a transaction and for the database as a whole.

    The IStorage methods affected are lastTransaction, load, store,
    and tpc_finish.  Derived interfaces may introduce additional
    methods.

    """

    def close():
        """Close the storage.

        Finalize the storage, releasing any external resources.  The
        storage should not be used after this method is called.
        """

    def getName():
        """The name of the storage

        The format and interpretation of this name is storage
        dependent. It could be a file name, a database name, etc..

        This is used soley for informational purposes.
        """

    def getSize():
        """An approximate size of the database, in bytes.

        This is used soley for informational purposes.
        """

    def history(oid, size=1):
        """Return a sequence of history information dictionaries.

        Up to size objects (including no objects) may be returned.

        The information provides a log of the changes made to the
        object. Data are reported in reverse chronological order.

        Each dictionary has the following keys:

        time
            UTC seconds since the epoch (as in time.time) that the
            object revision was committed.

        tid
            The transaction identifier of the transaction that
            committed the version.

        serial
            An alias for tid, which expected by older clients.

        user_name
            The user identifier, if any (or an empty string) of the
            user on whos behalf the revision was committed.

        description
            The transaction description for the transaction that
            committed the revision.

        size
            The size of the revision data record.

        If the transaction had extension items, then these items are
        also included if they don't conflict with the keys above.

        """

    def isReadOnly():
        """Test whether a storage allows committing new transactions

        For a given storage instance, this method always returns the
        same value.  Read-only-ness is a static property of a storage.
        """

        # XXX Note that this method doesn't really buy us much,
        # especially since we have to account for the fact that a
        # ostensibly non-read-only storage may be read-only
        # transiently.  It would be better to just have read-only errors.

    def lastTransaction():
        """Return the id of the last committed transaction.

        Returned tid is ID of last committed transaction as observed from
        some time _before_ lastTransaction call returns. In particular for
        client-sever case, lastTransaction can return cached view of storage
        that was learned some time ago.

        It is guaranteed that for all IStorageWrappers, that wrap the storage,
        invalidation notifications have been completed for transactions
        with ID ≤ returned tid.

        It is guaranteed that after lastTransaction returns, "current" view of
        the storage as observed by load() is ≥ returned tid.

        If no transactions have been committed, return a string of 8
        null (0) characters.
        """

    def __len__():
        """The approximate number of objects in the storage

        This is used soley for informational purposes.
        """

    def load(oid, version):
        """Load data for an object id

        The version argumement should always be an empty string. It
        exists soley for backward compatibility with older storage
        implementations.

        A data record and serial are returned.  The serial is a
        transaction identifier of the transaction that wrote the data
        record.

        A POSKeyError is raised if there is no record for the object id.
        """

    def loadBefore(oid, tid):
        """Load the object data written before a transaction id

        If there isn't data before the object before the given
        transaction, then None is returned, otherwise three values are
        returned:

        - The data record

        - The transaction id of the data record

        - The transaction id of the following revision, if any, or None.

        If the object id isn't in the storage, then POSKeyError is raised.
        """

    def loadSerial(oid, serial):
        """Load the object record for the give transaction id

        If a matching data record can be found, it is returned,
        otherwise, POSKeyError is raised.
        """

#     The following two methods are effectively part of the interface,
#     as they are generally needed when one storage wraps
#     another. This deserves some thought, at probably debate, before
#     adding them.
#
#     def _lock_acquire():
#         """Acquire the storage lock
#         """

#     def _lock_release():
#         """Release the storage lock
#         """

    def new_oid():
        """Allocate a new object id.

        The object id returned is reserved at least as long as the
        storage is opened.

        The return value is a string.
        """

    def pack(pack_time, referencesf):
        """Pack the storage

        It is up to the storage to interpret this call, however, the
        general idea is that the storage free space by:

        - discarding object revisions that were old and not current as of the
          given pack time.

        - garbage collecting objects that aren't reachable from the
          root object via revisions remaining after discarding
          revisions that were not current as of the pack time.

        The pack time is given as a UTC time in seconds since the
        epoch.

        The second argument is a function that should be used to
        extract object references from database records.  This is
        needed to determine which objects are referenced from object
        revisions.
        """

    def registerDB(wrapper):
        """Register a storage wrapper IStorageWrapper.

        The passed object is a wrapper object that provides an upcall
        interface to support composition.

        Note that, for historical reasons, an implementation may
        require a second argument, however, if required, the None will
        be passed as the second argument.

        Also, for historical reasons, this is called registerDB rather
        than register_wrapper.
        """

    def sortKey():
        """Sort key used to order distributed transactions

        When a transaction involved multiple storages, 2-phase commit
        operations are applied in sort-key order.  This must be unique
        among storages used in a transaction. Obviously, the storage
        can't assure this, but it should construct the sort key so it
        has a reasonable chance of being unique.

        The result must be a string.
        """

    def store(oid, serial, data, version, transaction):
        """Store data for the object id, oid.

        Arguments:

        oid
            The object identifier.  This is either a string
            consisting of 8 nulls or a string previously returned by
            new_oid.

        serial
            The serial of the data that was read when the object was
            loaded from the database.  If the object was created in
            the current transaction this will be a string consisting
            of 8 nulls.

        data
            The data record. This is opaque to the storage.

        version
            This must be an empty string. It exists for backward compatibility.

        transaction
            A transaction object.  This should match the current
            transaction for the storage, set by tpc_begin.

        The new serial for the object is returned, but not necessarily
        immediately.  It may be returned directly, or on a subsequent
        store or tpc_vote call.

        The return value may be:

        - None, or

        - A new serial (string) for the object

        If None is returned, then a new serial (or other special
        values) must ve returned in tpc_vote results.

        A serial, returned as a string, may be the special value
        ZODB.ConflictResolution.ResolvedSerial to indicate that a
        conflict occured and that the object should be invalidated.

        Several different exceptions may be raised when an error occurs.

        ConflictError
          is raised when serial does not match the most recent serial
          number for object oid and the conflict was not resolved by
          the storage.

        StorageTransactionError
          is raised when transaction does not match the current
          transaction.

        StorageError or, more often, a subclass of it
          is raised when an internal error occurs while the storage is
          handling the store() call.

        """

    def tpc_abort(transaction):
        """Abort the transaction.

        Any changes made by the transaction are discarded.

        This call is ignored is the storage is not participating in
        two-phase commit or if the given transaction is not the same
        as the transaction the storage is commiting.
        """

    def tpc_begin(transaction):
        """Begin the two-phase commit process.

        If storage is already participating in a two-phase commit
        using the same transaction, a StorageTransactionError is raised.

        If the storage is already participating in a two-phase commit
        using a different transaction, the call blocks until the
        current transaction ends (commits or aborts).
        """

    def tpc_finish(transaction, func = lambda tid: None):
        """Finish the transaction, making any transaction changes permanent.

        Changes must be made permanent at this point.

        This call raises a StorageTransactionError if the storage
        isn't participating in two-phase commit or if it is committing
        a different transaction.  Failure of this method is extremely
        serious.

        The second argument is a call-back function that must be
        called while the storage transaction lock is held.  It takes
        the new transaction id generated by the transaction.

        The return value may be None or the transaction id of the
        committed transaction, as described in IMultiCommitStorage.
        """

    def tpc_vote(transaction):
        """Provide a storage with an opportunity to veto a transaction

        This call raises a StorageTransactionError if the storage
        isn't participating in two-phase commit or if it is commiting
        a different transaction.

        If a transaction can be committed by a storage, then the
        method should return.  If a transaction cannot be committed,
        then an exception should be raised.  If this method returns
        without an error, then there must not be an error if
        tpc_finish or tpc_abort is called subsequently.

        The return value can be None or a sequence of object-id
        and serial pairs giving new serials for objects whose ids were
        passed to previous store calls in the same transaction. The serial
        can be the special value ZODB.ConflictResolution.ResolvedSerial to
        indicate that a conflict occurred and that the object should be
        invalidated.

        The return value can also be a sequence of object ids, as
        described in IMultiCommitStorage.tpc_vote.

        After the tpc_vote call, all solved conflicts must have been notified,
        either from tpc_vote or store for objects passed to store.
        """

class IMultiCommitStorage(IStorage):
    """A multi-commit storage can commit multiple transactions at once.

    It's likely that future versions of ZODB will require all storages
    to provide this interface.
    """

    def store(oid, serial, data, version, transaction):
        """Store data for the object id, oid.

        See IStorage.store. For objects implementing this interface,
        the return value is always None.
        """

    def tpc_finish(transaction, func = lambda tid: None):
        """Finish the transaction, making any transaction changes permanent.

        See IStorage.store. For objects implementing this interface,
        the return value must be the committed tid. It is used to set the
        serial for objects whose ids were passed to previous store calls
        in the same transaction.
        """

    def tpc_vote(transaction):
        """Provide a storage with an opportunity to veto a transaction

        See IStorage.store. For objects implementing this interface,
        the return value can be either None or a sequence of oids for which
        a conflict was resolved.
        """

class IStorageRestoreable(IStorage):
    """Copying Transactions

    The IStorageRestoreable interface supports copying
    already-committed transactions from one storage to another. This
    is typically done for replication or for moving data from one
    storage implementation to another.
    """

    def tpc_begin(transaction, tid=None):
        """Begin the two-phase commit process.

        If storage is already participating in a two-phase commit
        using the same transaction, the call is ignored.

        If the storage is already participating in a two-phase commit
        using a different transaction, the call blocks until the
        current transaction ends (commits or aborts).

        If a transaction id is given, then the transaction will use
        the given id rather than generating a new id.  This is used
        when copying already committed transactions from another
        storage.
        """

        # Note that the current implementation also accepts a status.
        # This is an artifact of:
        # - Earlier use of an undo status to undo revisions in place,
        #   and,
        # - Incorrect pack garbage-collection algorithms (possibly
        #   including the existing FileStorage implementation), that
        #   failed to take into account records after the pack time.


    def restore(oid, serial, data, version, prev_txn, transaction):
        """Write data already committed in a separate database

        The restore method is used when copying data from one database
        to a replica of the database.  It differs from store in that
        the data have already been committed, so there is no check for
        conflicts and no new transaction is is used for the data.

        Arguments:

        oid
             The object id for the record

        serial
             The transaction identifier that originally committed this object.

        data
             The record data.  This will be None if the transaction
             undid the creation of the object.

        prev_txn
             The identifier of a previous transaction that held the
             object data.  The target storage can sometimes use this
             as a hint to save space.

        transaction
             The current transaction.

        Nothing is returned.
        """


class IStorageRecordInformation(Interface):
    """Provide information about a single storage record
    """

    oid = Attribute("The object id")
    tid = Attribute("The transaction id")
    data = Attribute("The data record")
    version = Attribute("The version id")
    data_txn = Attribute("The previous transaction id")


class IStorageTransactionInformation(Interface):
    """Provide information about a storage transaction.

    Can be iterated over to retrieve the records modified in the transaction.

    """

    tid = Attribute("Transaction id")
    status = Attribute("Transaction Status") # XXX what are valid values?
    user = Attribute("Transaction user")
    description = Attribute("Transaction Description")
    extension = Attribute(
        "A dictionary carrying the transaction's extension data")

    def __iter__():
        """Iterate over the transaction's records given as
        IStorageRecordInformation objects.

        """


class IStorageIteration(Interface):
    """API for iterating over the contents of a storage."""

    def iterator(start=None, stop=None):
        """Return an IStorageTransactionInformation iterator.

        If the start argument is not None, then iteration will start
        with the first transaction whose identifier is greater than or
        equal to start.

        If the stop argument is not None, then iteration will end with
        the last transaction whose identifier is less than or equal to
        stop.

        The iterator provides access to the data as available at the time when
        the iterator was retrieved.

        """


class IStorageUndoable(IStorage):
    """A storage supporting transactional undo.
    """

    def supportsUndo():
        """Return True, indicating that the storage supports undo.
        """

    def undo(transaction_id, transaction):
        """Undo the transaction corresponding to the given transaction id.

        The transaction id is a value returned from undoInfo or
        undoLog, which may not be a stored transaction identifier as
        used elsewhere in the storage APIs.

        This method must only be called in the first phase of
        two-phase commit (after tpc_begin but before tpc_vote). It
        returns a serial (transaction id) and a sequence of object ids
        for objects affected by the transaction. The serial is ignored
        and may be None. The return from this method may be None.
        """
        # Used by DB (Actually, by TransactionalUndo)

    def undoLog(first, last, filter=None):
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
        # DB pass through

    def undoInfo(first=0, last=-20, specification=None):
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
        # DB pass-through


class IMVCCStorage(IStorage):
    """A storage that provides MVCC semantics internally.

    MVCC (multi-version concurrency control) means each user of a
    database has a snapshot view of the database. The snapshot view
    does not change, even if concurrent connections commit
    transactions, until a transaction boundary. Relational databases
    that support serializable transaction isolation provide MVCC.

    Storages that implement IMVCCStorage, such as RelStorage, provide
    MVCC semantics at the ZODB storage layer. When ZODB.Connection uses
    a storage that implements IMVCCStorage, each connection uses a
    connection-specific storage instance, and that storage instance
    provides a snapshot of the database.

    By contrast, storages that do not implement IMVCCStorage, such as
    FileStorage, rely on ZODB.Connection to provide MVCC semantics, so
    in that case, one storage instance is shared by many
    ZODB.Connections. Applications that use ZODB.Connection always have
    a snapshot view of the database; IMVCCStorage only modifies which
    layer of ZODB provides MVCC.

    Furthermore, IMVCCStorage changes the way object invalidation
    works. An essential feature of ZODB is the propagation of object
    invalidation messages to keep in-memory caches up to date. Storages
    like FileStorage and ZEO.ClientStorage send invalidation messages
    to all other Connection instances at transaction commit time.
    Storages that implement IMVCCStorage, on the other hand, expect the
    ZODB.Connection to poll for a list of invalidated objects.

    Certain methods of IMVCCStorage implementations open persistent
    back end database sessions and retain the sessions even after the
    method call finishes::

        load
        loadEx
        loadSerial
        loadBefore
        store
        restore
        new_oid
        history
        tpc_begin
        tpc_vote
        tpc_abort
        tpc_finish

    If you know that the storage instance will no longer be used after
    calling any of these methods, you should call the release method to
    release the persistent sessions. The persistent sessions will be
    reopened as necessary if you call one of those methods again.

    Other storage methods open short lived back end sessions and close
    the back end sessions before returning. These include::

        __len__
        getSize
        undoLog
        undo
        pack
        iterator

    These methods do not provide MVCC semantics, so these methods
    operate on the most current view of the database, rather than the
    snapshot view that the other methods use.
    """

    def new_instance():
        """Creates and returns another storage instance.

        The returned instance provides IMVCCStorage and connects to the
        same back-end database. The database state visible by the
        instance will be a snapshot that varies independently of other
        storage instances.
        """

    def release():
        """Release all persistent sessions used by this storage instance.

        After this call, the storage instance can still be used;
        calling methods that use persistent sessions will cause the
        persistent sessions to be reopened.
        """

    def poll_invalidations():
        """Poll the storage for external changes.

        Returns either a sequence of OIDs that have changed, or None.  When a
        sequence is returned, the corresponding objects should be removed
        from the ZODB in-memory cache.  When None is returned, the storage is
        indicating that so much time has elapsed since the last poll that it
        is no longer possible to enumerate all of the changed OIDs, since the
        previous transaction seen by the connection has already been packed.
        In that case, the ZODB in-memory cache should be cleared.
        """

    def sync(force=True):
        """Updates the internal snapshot to the current state of the database.

        If the force parameter is False, the storage may choose to
        ignore this call. By ignoring this call, a storage can reduce
        the frequency of database polls, thus reducing database load.
        """


class IStorageCurrentRecordIteration(IStorage):

    def record_iternext(next=None):
        """Iterate over the records in a storage

        Use like this:

            >>> next = None
            >>> while 1:
            ...     oid, tid, data, next = storage.record_iternext(next)
            ...     # do things with oid, tid, and data
            ...     if next is None:
            ...         break

        """

class IExternalGC(IStorage):

   def deleteObject(oid, serial, transaction):
       """Mark an object as deleted

       This method marks an object as deleted via a new object
       revision.  Subsequent attempts to load current data for the
       object will fail with a POSKeyError, but loads for
       non-current data will suceed if there are previous
       non-delete records.  The object will be removed from the
       storage when all not-delete records are removed.

       The serial argument must match the most recently committed
       serial for the object. This is a seat belt.

       This method can only be called in the first phase of 2-phase
       commit.
       """

class ReadVerifyingStorage(IStorage):

    def checkCurrentSerialInTransaction(oid, serial, transaction):
        """Check whether the given serial number is current.

        The method is called during the first phase of 2-phase commit
        to verify that data read in a transaction is current.

        The storage should raise a ReadConflictError if the serial is not
        current, although it may raise the exception later, in a call
        to store or in a call to tpc_vote.

        If no exception is raised, then the serial must remain current
        through the end of the transaction.
        """

class IBlob(Interface):
    """A BLOB supports efficient handling of large data within ZODB."""

    def open(mode):
        """Open a blob

        Returns a file(-like) object for handling the blob data.

        mode: Mode to open the file with. Possible values: r,w,r+,a,c

        The mode 'c' is similar to 'r', except that an orinary file
        object is returned and may be used in a separate transaction
        and after the blob's database connection has been closed.

        """

    def committed():
        """Return a file name for committed data.

        The returned file name may be opened for reading or handed to
        other processes for reading.  The file name isn't guarenteed
        to be valid indefinately.  The file may be removed in the
        future as a result of garbage collection depending on system
        configuration.

        A BlobError will be raised if the blob has any uncommitted data.
        """

    def consumeFile(filename):
        """Consume a file.

        Replace the current data of the blob with the file given under
        filename.

        The blob must not be opened for reading or writing when consuming a
        file.

        The blob will take over ownership of the file and will either
        rename or copy and remove it.  The file must not be open.

        """


class IBlobStorage(Interface):
    """A storage supporting BLOBs."""

    def storeBlob(oid, oldserial, data, blobfilename, version, transaction):
        """Stores data that has a BLOB attached.

        The blobfilename argument names a file containing blob data.
        The storage will take ownership of the file and will rename it
        (or copy and remove it) immediately, or at transaction-commit
        time.  The file must not be open.

        The new serial for the object is returned, but not necessarily
        immediately.  It may be returned directly, or on a subsequent
        store or tpc_vote call.

        The return value may be:

        - None

        - A new serial (string) for the object, or

        - An iterable of object-id and serial pairs giving new serials
          for objects.

        A serial, returned as a string or in a sequence of oid/serial
        pairs, may be the special value
        ZODB.ConflictResolution.ResolvedSerial to indicate that a
        conflict occured and that the object should be invalidated.

        Several different exceptions may be raised when an error occurs.

        ConflictError
          is raised when serial does not match the most recent serial
          number for object oid and the conflict was not resolved by
          the storage.

        StorageTransactionError
          is raised when transaction does not match the current
          transaction.

        StorageError or, more often, a subclass of it
          is raised when an internal error occurs while the storage is
          handling the store() call.

        """

    def loadBlob(oid, serial):
        """Return the filename of the Blob data for this OID and serial.

        Returns a filename.

        Raises POSKeyError if the blobfile cannot be found.
        """

    def openCommittedBlobFile(oid, serial, blob=None):
        """Return a file for committed data for the given object id and serial

        If a blob is provided, then a BlobFile object is returned,
        otherwise, an ordinary file is returned.  In either case, the
        file is opened for binary reading.

        This method is used to allow storages that cache blob data to
        make sure that data are available at least long enough for the
        file to be opened.
        """

    def temporaryDirectory():
        """Return a directory that should be used for uncommitted blob data.

        If Blobs use this, then commits can be performed with a simple rename.
        """

class IBlobStorageRestoreable(IBlobStorage, IStorageRestoreable):

    def restoreBlob(oid, serial, data, blobfilename, prev_txn, transaction):
        """Write blob data already committed in a separate database

        See the restore and storeBlob methods.
        """


class IBroken(Interface):
    """Broken objects are placeholders for objects that can no longer be
    created because their class has gone away.

    They cannot be modified, but they retain their state. This allows them to
    be rebuild should the missing class be found again.

    A broken object's __class__ can be used to determine the original
    class' name (__name__) and module (__module__).

    The original object's state and initialization arguments are
    available in broken object attributes to aid analysis and
    reconstruction.

    """

    def __setattr__(name, value):
        """You cannot modify broken objects. This will raise a
        ZODB.broken.BrokenModified exception.
        """

    __Broken_newargs__ = Attribute("Arguments passed to __new__.")
    __Broken_initargs__ = Attribute("Arguments passed to __init__.")
    __Broken_state__ = Attribute("Value passed to __setstate__.")

class BlobError(Exception):
    pass


class StorageStopIteration(IndexError, StopIteration):
    """A combination of StopIteration and IndexError to provide a
    backwards-compatible exception.
    """
