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
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
from zope.interface import Interface

class TransactionError(StandardError):
    """An error occured due to normal transaction processing."""

class ConflictError(TransactionError):
    """Two transactions tried to modify the same object at once

    This transaction should be resubmitted.
    """

class IllegalStateError(TransactionError):
    """An operation was invoked that wasn't valid in the current
    transaction state.
    """

    def __init__(self, verb, state):
        self._verb = verb
        self._state = state

    def __str__(self):
        return "Can't %s transaction in %s state" % (self._verb,
                                                     self._state)

class AbortError(TransactionError):
    """Transaction commit failed and the application must abort."""

    def __init__(self, datamgr):
        self.datamgr = datamgr

    def __str__(self):
        str = self.__class__.__doc__ + " Failed data manager: %s"
        return str % self.datamgr

class RollbackError(TransactionError):
    """An error occurred rolling back a savepoint."""

class IDataManager(Interface):
    """Data management interface for storing objects transactionally

    This is currently implemented by ZODB database connections.
    """

    def prepare(transaction):
        """Begin two-phase commit of a transaction.

        DataManager should return True or False.
        """

    def abort(transaction):
        """Abort changes made by transaction."""

    def commit(transaction):
        """Commit changes made by transaction."""

    def savepoint(transaction):
        """Do tentative commit of changes to this point.

        Should return an object implementing IRollback
        """

class IRollback(Interface):

    def rollback():
        """Rollback changes since savepoint."""

class ITransaction(Interface):
    """Transaction objects

    Application code typically gets these by calling
    get_transaction().
    """

    def abort():
        """Abort the current transaction."""

    def begin():
        """Begin a transaction."""

    def commit():
        """Commit a transaction."""

    def join(resource):
        """Join a resource manager to the current transaction."""

    def status():
        """Return status of the current transaction."""

    def suspend():
        """Suspend the current transaction.

        If a transaction is suspended, the transaction manager no
        longer treats it as active.  The resume() method must be
        called before the transaction can be used.
        """

    def resume():
        """Resume the current transaction.

        If another transaction is active, it must be suspended before
        resume() is called.
        """

class ITransactionManager(Interface):
    """Coordinates application use of transactional resources."""

    def get():
        """Return the curren transaction.

        Calls new() to start a new transaction if one does not exist.
        """

    def begin():
        """Return a new transaction.

        If a transaction is currently active for the calling thread,
        it is aborted.
        """

    def commit(txn):
        """Commit txn."""

    def abort(txn):
        """Abort txn."""

    def savepoint(txn):
        """Return rollback object that can restore txn to current state."""
