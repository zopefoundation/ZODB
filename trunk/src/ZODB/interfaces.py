##############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Interfaces for ZODB.

$Id: interfaces.py,v 1.4 2004/04/19 21:19:05 tim_one Exp $
"""

try:
    from zope.interface import Interface, Attribute
except ImportError:
    class Interface:
        pass

    class Attribute:
        def __init__(self, __name__, __doc__):
            self.__name__ = __name__
            self.__doc__ = __doc__


class IDataManager(Interface):
    """Objects that manage transactional storage.

    These object's may manage data for other objects, or they may manage
    non-object storages, such as relational databases.
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


class ITransaction(Interface):
    """Object representing a running transaction.

    Objects with this interface may represent different transactions
    during their lifetime (.begin() can be called to start a new
    transaction using the same instance).
    """

    user = Attribute(
        "user",
        "The name of the user on whose behalf the transaction is being\n"
        "performed.  The format of the user name is defined by the\n"
        "application.")
    # XXX required to be a string?

    description = Attribute(
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
        # XXX does impl do the right thing with ''?  Not clear what
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
        # XXX is this this allowed to cause an exception here, during
        # the two-phase commit, or can it toss data silently?
