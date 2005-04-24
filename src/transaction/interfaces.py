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
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Transaction Interfaces

$Id$
"""

import zope.interface

class ITransactionManager(zope.interface.Interface):
    """An object that manages a sequence of transactions

    Applications use transaction managers to establish transaction boundaries.
    """


    def begin():
        """Begin a new transaction.

        If an existing transaction is in progress, it will be aborted.
        """

    def get():
        """Get the current transaction.
        """

    def commit():
        """Commit the current transaction
        """

    def abort(self):
        """Abort the current transaction
        """

    def registerSynch(synch):
        """Register an ISynchronizer.

        Synchronizers are notified at the beginning and end of
        transaction completion.
        
        """

    def unregisterSynch(synch):
        """Unregister an ISynchronizer.

        Synchronizers are notified at the beginning and end of
        transaction completion.
        
        """

class ITransaction(zope.interface.Interface):
    """Object representing a running transaction.

    Objects with this interface may represent different transactions
    during their lifetime (.begin() can be called to start a new
    transaction using the same instance).
    """

    user = zope.interface.Attribute(
        """A user name associated with the transaction.

        The format of the user name is defined by the application.  The value
        is of Python type str.  Storages record the user value, as meta-data,
        when a transaction commits.

        A storage may impose a limit on the size of the value; behavior is
        undefined if such a limit is exceeded (for example, a storage may
        raise an exception, or truncate the value).
        """)

    description = zope.interface.Attribute(
        """A textual description of the transaction.

        The value is of Python type str.  Method note() is the intended
        way to set the value.  Storages record the description, as meta-data,
        when a transaction commits.

        A storage may impose a limit on the size of the description; behavior
        is undefined if such a limit is exceeded (for example, a storage may
        raise an exception, or truncate the value).
        """)

    def commit(subtransaction=None):
        """Finalize the transaction.

        This executes the two-phase commit algorithm for all
        IDataManager objects associated with the transaction.
        """

    def abort(subtransaction=0, freeme=1):
        """Abort the transaction.

        This is called from the application.  This can only be called
        before the two-phase commit protocol has been started.
        """

    def join(datamanager):
        """Add a datamanager to the transaction.

        The if the data manager supports savepoints, it must call this
        *before* making any changes.  If the transaction has had any
        savepoints, then it will take a savepoint of the data manager
        when join is called and this savepoint must reflct the state
        of the data manager before any changes that caused the data
        manager to join the transaction.

        The datamanager must implement the
        transactions.interfaces.IDataManager interface, and be
        adaptable to ZODB.interfaces.IDataManager.
        
        """

    def note(text):
        """Add text to the transaction description.

        If a description has already been set, text is added to the
        end of the description following two newline characters.
        Surrounding whitespace is stripped from text.
        """
        # Unsure:  does impl do the right thing with ''?  Not clear what
        # the "right thing" is.

    def setUser(user_name, path="/"):
        """Set the user name.

        path should be provided if needed to further qualify the
        identified user.  This is a convenience method used by Zope.
        It sets the .user attribute to str(path) + " " + str(user_name).
        """

    def setExtendedInfo(name, value):
        """Add extension data to the transaction.

        name is the name of the extension property to set; value must
        be a picklable value.

        Storage implementations may limit the amount of extension data
        which can be stored.
        """
        # Unsure:  is this allowed to cause an exception here, during
        # the two-phase commit, or can it toss data silently?

    def beforeCommitHook(hook, *args, **kws):
        """Register a hook to call before the transaction is committed.

        The specified hook function will be called after the transaction's
        commit method has been called, but before the commit process has been
        started.  The hook will be passed the specified positional and keyword
        arguments.

        Multiple hooks can be registered and will be called in the order they
        were registered (first registered, first called).  This method can
        also be called from a hook:  an executing hook can register more
        hooks.  Applications should take care to avoid creating infinite loops
        by recursively registering hooks.

        Hooks are called only for a top-level commit.  A subtransaction
        commit does not call any hooks.  If the transaction is aborted, hooks
        are not called, and are discarded.  Calling a hook "consumes" its
        registration too:  hook registrations do not persist across
        transactions.  If it's desired to call the same hook on every
        transaction commit, then beforeCommitHook() must be called with that
        hook during every transaction; in such a case consider registering a
        synchronizer object via a TransactionManager's registerSynch() method
        instead.
        """

class ITransactionDeprecated(zope.interface.Interface):
    """Deprecated parts of the transaction API."""

    # TODO: deprecated36
    def begin(info=None):
        """Begin a new transaction.

        If the transaction is in progress, it is aborted and a new
        transaction is started using the same transaction object.
        """

    # TODO: deprecate this for 3.6.
    def register(object):
        """Register the given object for transaction control."""

class IDataManager(zope.interface.Interface):
    """Objects that manage transactional storage.

    These objects may manage data for other objects, or they may manage
    non-object storages, such as relational databases.

    IDataManagerOriginal is the interface currently provided by ZODB
    database connections, but the intent is to move to the newer
    IDataManager.

    Note that when data are modified, data managers should join a
    transaction so that data can be committed when the user commits
    the transaction.

    """

    # Two-phase commit protocol.  These methods are called by the
    # ITransaction object associated with the transaction being
    # committed.

    def abort(transaction):
        """Abort a transaction and forget all changes.

        Abort must be called outside of a two-phase commit.

        Abort is called by the transaction manager to abort transactions
        that are not yet in a two-phase commit.
        """

    def tpc_begin(transaction):
        """Begin commit of a transaction, starting the two-phase commit.

        transaction is the ITransaction instance associated with the
        transaction being committed.

        subtransaction is a Boolean flag indicating whether the
        two-phase commit is being invoked for a subtransaction.

        Important note: Subtransactions are modelled in the sense that
        when you commit a subtransaction, subsequent commits should be
        for subtransactions as well.  That is, there must be a
        commit_sub() call between a tpc_begin() call with the
        subtransaction flag set to true and a tpc_begin() with the
        flag set to false.

        """

    def commit(transaction):
        """Commit modifications to registered objects.

        Save the object as part of the data to be made persistent if
        the transaction commits.

        This includes conflict detection and handling. If no conflicts or
        errors occur it saves the objects in the storage.
        """

    def tpc_abort(transaction):
        """Abort a transaction.

        This is called by a transaction manager to end a two-phase commit on
        the data manager.

        This is always called after a tpc_begin call.

        transaction is the ITransaction instance associated with the
        transaction being committed.

        This should never fail.
        """

    def tpc_vote(transaction):
        """Verify that a data manager can commit the transaction

        This is the last chance for a data manager to vote 'no'.  A
        data manager votes 'no' by raising an exception.

        transaction is the ITransaction instance associated with the
        transaction being committed.
        """

    def tpc_finish(transaction):
        """Indicate confirmation that the transaction is done.

        transaction is the ITransaction instance associated with the
        transaction being committed.

        This should never fail. If this raises an exception, the
        database is not expected to maintain consistency; it's a
        serious error.

        It's important that the storage calls the passed function
        while it still has its lock.  We don't want another thread
        to be able to read any updated data until we've had a chance
        to send an invalidation message to all of the other
        connections!
        """

    def sortKey():
        """Return a key to use for ordering registered DataManagers

        ZODB uses a global sort order to prevent deadlock when it commits
        transactions involving multiple resource managers.  The resource
        manager must define a sortKey() method that provides a global ordering
        for resource managers.
        """
        # Alternate version:
        #"""Return a consistent sort key for this connection.
        #
        #This allows ordering multiple connections that use the same storage in
        #a consistent manner. This is unique for the lifetime of a connection,
        #which is good enough to avoid ZEO deadlocks.
        #"""

class ISavepointDataManager(IDataManager):

    def savepoint():
        """Return a savepoint (ISavepoint)
        """

class ISavepoint(zope.interface.Interface):
    """A transaction savepoint
    """

    def rollback():
        """Rollback any work done since the savepoint

        An InvalidSavepointRollbackError is raised if the savepoint
        isn't valid.
        
        """

    valid = zope.interface.Attribute(
        "Boolean indicating whether the savepoint is valid")

class InvalidSavepointRollbackError(Exception):
    """Attempt to rollback an invalid savepoint

    A savepoint may be invalid because:

    - The surrounding transaction has committed or aborted

    - An earlier savepoint in the same transaction has been rolled back
    """

class ISynchronizer(zope.interface.Interface):
    """Objects that participate in the transaction-boundary notification API.
    """

    def beforeCompletion(transaction):
        """Hook that is called by the transaction at the start of a commit.
        """

    def afterCompletion(transaction):
        """Hook that is called by the transaction after completing a commit.
        """

