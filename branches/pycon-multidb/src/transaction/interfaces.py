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

class IResourceManager(zope.interface.Interface):
    """Objects that manage resources transactionally.

    These objects may manage data for other objects, or they may manage
    non-object storages, such as relational databases.

    IDataManagerOriginal is the interface currently provided by ZODB
    database connections, but the intent is to move to the newer
    IDataManager.
    """

    # Two-phase commit protocol.  These methods are called by the
    # ITransaction object associated with the transaction being
    # committed.

    def tpc_begin(transaction):
        """Begin two-phase commit, to save data changes.

        An implementation should do as much work as possible without
        making changes permanent.  Changes should be made permanent
        when tpc_finish is called (or aborted if tpc_abort is called).
        The work can be divided between tpc_begin() and tpc_vote(), and
        the intent is that tpc_vote() be as fast as possible (to minimize
        the period of uncertainty).

        transaction is the ITransaction instance associated with the
        transaction being committed.
        """

    def tpc_vote(transaction):
        """Verify that a resource manager can commit the transaction.

        This is the last chance for a resource manager to vote 'no'.  A
        resource manager votes 'no' by raising an exception.

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
        """

    def tpc_abort(transaction):
        """Abort a transaction.

        transaction is the ITransaction instance associated with the
        transaction being committed.

        All changes made by the current transaction are aborted.  Note
        that this includes all changes stored in any savepoints that may
        be associated with the current transaction.

        tpc_abort() can be called at any time, either in or out of the
        two-phase commit.

        This should never fail.
        """

    # The savepoint/rollback API.

    def savepoint(transaction):
        """Save partial transaction changes.

        There are two purposes:

        1) To allow discarding partial changes without discarding all
           dhanges.

        2) To checkpoint changes to disk that would otherwise live in
           memory for the duration of the transaction.

        Returns an object implementing ISavePoint2 that can be used
        to discard changes made since the savepoint was captured.

        An implementation that doesn't support savepoints should implement
        this method by returning a savepoint object that raises an
        exception when its rollback method is called.  The savepoint method
        shouldn't raise an error.  This way, transactions that create
        savepoints can proceed as long as an attempt is never made to roll
        back a savepoint.
        """

    def discard(transaction):
        """Discard changes within the transaction since the last savepoint.

        That means changes made since the last savepoint if one exists, or
        since the start of the transaction.
        """

class IDataManagerOriginal(zope.interface.Interface):
    """Objects that manage transactional storage.

    These objects may manage data for other objects, or they may manage
    non-object storages, such as relational databases.

    IDataManagerOriginal is the interface currently provided by ZODB
    database connections, but the intent is to move to the newer
    IDataManager.
    """

    def abort_sub(transaction):
        """Discard all subtransaction data.

        See subtransaction.txt

        This is called when top-level transactions are aborted.

        No further subtransactions can be started once abort_sub()
        has been called; this is only used when the transaction is
        being aborted.

        abort_sub also implies the abort of a 2-phase commit.

        This should never fail.
        """

    def commit_sub(transaction):
        """Commit all changes made in subtransactions and begin 2-phase commit

        Data are saved *as if* they are part of the current transaction.
        That is, they will not be persistent unless the current transaction
        is committed.

        This is called when the current top-level transaction is committed.

        No further subtransactions can be started once commit_sub()
        has been called; this is only used when the transaction is
        being committed.

        This call also implied the beginning of 2-phase commit.
        """

    # Two-phase commit protocol.  These methods are called by the
    # ITransaction object associated with the transaction being
    # committed.

    def tpc_begin(transaction, subtransaction=False):
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


    def tpc_abort(transaction):
        """Abort a transaction.

        This is always called after a tpc_begin call.

        transaction is the ITransaction instance associated with the
        transaction being committed.

        This should never fail.
        """

    def tpc_finish(transaction):
        """Indicate confirmation that the transaction is done.

        transaction is the ITransaction instance associated with the
        transaction being committed.

        This should never fail. If this raises an exception, the
        database is not expected to maintain consistency; it's a
        serious error.

        """

    def tpc_vote(transaction):
        """Verify that a data manager can commit the transaction

        This is the last chance for a data manager to vote 'no'.  A
        data manager votes 'no' by raising an exception.

        transaction is the ITransaction instance associated with the
        transaction being committed.
        """

    def commit(object, transaction):
        """CCCommit changes to an object

        Save the object as part of the data to be made persistent if
        the transaction commits.
        """

    def abort(object, transaction):
        """Abort changes to an object

        Only changes made since the last transaction or
        sub-transaction boundary are discarded.

        This method may be called either:

        o Outside of two-phase commit, or

        o In the first phase of two-phase commit

        """

    def sortKey():
        """
        Return a key to use for ordering registered DataManagers

        ZODB uses a global sort order to prevent deadlock when it commits
        transactions involving multiple resource managers.  The resource
        manager must define a sortKey() method that provides a global ordering
        for resource managers.
        """

class IDataManager(zope.interface.Interface):
    """Data management interface for storing objects transactionally.

    ZODB database connections currently provides the older
    IDataManagerOriginal interface, but the intent is to move to this newer
    IDataManager interface.

    Our hope is that this interface will evolve and become the standard
    interface.  There are some issues to be resolved first, like:

    - Probably want separate abort methods for use in and out of
      two-phase commit.

    - The savepoint api may need some more thought.

    """

    def prepare(transaction):
        """Perform the first phase of a 2-phase commit

        The data manager prepares for commit any changes to be made
        persistent.  A normal return from this method indicated that
        the data manager is ready to commit the changes.

        The data manager must raise an exception if it is not prepared
        to commit the transaction after executing prepare().

        The transaction must match that used for preceeding
        savepoints, if any.
        """

        # This is equivalent to zodb3's tpc_begin, commit, and
        # tpc_vote combined.

    def abort(transaction):
        """Abort changes made by transaction

        This may be called before two-phase commit or in the second
        phase of two-phase commit.

        The transaction must match that used for preceeding
        savepoints, if any.

        """

        # This is equivalent to *both* zodb3's abort and tpc_abort
        # calls. This should probably be split into 2 methods.

    def commit(transaction):
        """Finish two-phase commit

        The prepare method must be called, with the same transaction,
        before calling commit.

        """

        # This is equivalent to zodb3's tpc_finish

    def savepoint(transaction):
        """Do tentative commit of changes to this point.

        Should return an object implementing IRollback that can be used
        to rollback to the savepoint.

        Note that (unlike zodb3) this doesn't use a 2-phase commit
        protocol.  If this call fails, or if a rollback call on the
        result fails, the (containing) transaction should be
        aborted.  Aborting the containing transaction is *not* the
        responsibility of the data manager, however.

        An implementation that doesn't support savepoints should
        implement this method by returning a rollback implementation
        that always raises an error when it's rollback method is
        called. The savepoing method shouldn't raise an error. This
        way, transactions that create savepoints can proceed as long
        as an attempt is never made to roll back a savepoint.

        """

    def sortKey():
        """
        Return a key to use for ordering registered DataManagers

        ZODB uses a global sort order to prevent deadlock when it commits
        transactions involving multiple resource managers.  The resource
        manager must define a sortKey() method that provides a global ordering
        for resource managers.
        """

class ITransaction(zope.interface.Interface):
    """Object representing a running transaction.

    Objects with this interface may represent different transactions
    during their lifetime (.begin() can be called to start a new
    transaction using the same instance).
    """

    user = zope.interface.Attribute(
        "user",
        "The name of the user on whose behalf the transaction is being\n"
        "performed.  The format of the user name is defined by the\n"
        "application.")
    # Unsure: required to be a string?

    description = zope.interface.Attribute(
        "description",
        "Textual description of the transaction.")

    def begin(info=None, subtransaction=None):
        """Begin a new transaction.

        If the transaction is in progress, it is aborted and a new
        transaction is started using the same transaction object.
        """

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

        The datamanager must implement the
        transactions.interfaces.IDataManager interface, and be
        adaptable to ZODB.interfaces.IDataManager.
        """

    def register(object):
        """Register the given object for transaction control."""

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
        identified user.
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

class ISavePoint(zope.interface.Interface):
    """ISavePoint objects represent partial transaction changes.

    Sequences of savepoint objects are associated with transactions,
    and with IResourceManagers.
    """

    def rollback():
        """Discard changes made after this savepoint.

        This includes discarding (call the discard method on) all
        subsequent savepoints.
        """

    def discard():
        """Discard changes saved by this savepoint.

        That means changes made since the immediately preceding
        savepoint if one exists, or since the start of the transaction,
        until this savepoint.

        Once a savepoint has been discarded, it's an error to attempt
        to rollback or discard it again.
        """

    next_savepoint = zope.interface.Attribute(
        """The next savepoint (later in time), or None if self is the
           most recent savepoint.""")

class IRollback(zope.interface.Interface):

    def rollback():
        """Rollback changes since savepoint.

        IOW, rollback to the last savepoint.

        It is an error to rollback to a savepoint if:

        - An earlier savepoint within the same transaction has been
          rolled back to, or

        - The transaction has ended.
        """
