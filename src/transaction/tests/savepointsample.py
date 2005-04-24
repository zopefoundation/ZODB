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
"""Savepoint data manager implementation example

Sample data manager implementation that illustrates how to implement savepoints

See savepoint.txt in the transaction package.

$Id$
"""

import UserDict
from zope import interface
import transaction.interfaces

class SampleDataManager(UserDict.DictMixin):
    """Sample implementation of data manager that doesn't support savepoints

    This data manager stores named simple values, like strings and numbers.
    """
    
    interface.implements(transaction.interfaces.ISavepointDataManager)

    def __init__(self, transaction_manager = None):
        if transaction_manager is None:
            # Use the thread-local transaction manager if none is provided:
            transaction_manager = transaction.manager
        self.transaction_manager = transaction_manager

        # Our committed and uncommitted data:
        self.committed = {}
        self.uncommitted = self.committed.copy()

        # Our transaction state:

        #   If our uncommitted data is modified, we'll join a transaction
        #   and keep track of the transaction we joined.  Any commit
        #   related messages we get should be for this same transaction
        self.transaction = None

        #   What phase, if any, of two-phase commit we are in:
        self.tpc_phase = None


    #######################################################################
    # Provide a mapping interface to uncommitted data.  We provide
    # a basic subset of the interface. DictMixin does the rest.
    
    def __getitem__(self, name):
        return self.uncommitted[name]
    
    def __setitem__(self, name, value):
        self._join() # join the current transaction, if we haven't already
        self.uncommitted[name] = value
    
    def __delitem__(self, name):
        self._join() # join the current transaction, if we haven't already
        del self.uncommitted[name]

    def keys(self):
        return self.uncommitted.keys()

    #
    #######################################################################

    #######################################################################
    # Transaction methods

    def _join(self):
        # If this is the first change in the transaction, join the transaction
        if self.transaction is None:
            self.transaction = self.transaction_manager.get()
            self.transaction.join(self)

    def _resetTransaction(self):
        self.transaction = None
        self.tpc_phase = None

    def abort(self, transaction):
        """Throw away changes made before the commit process has started
        """
        assert ((transaction is self.transaction) or (self.transaction is None)
                ), "Must not change transactions"
        assert self.tpc_phase is None, "Must be called outside of tpc"
        self.uncommitted = self.committed.copy()
        self._resetTransaction()

    def tpc_begin(self, transaction):
        """Enter two-phase commit
        """
        assert transaction is self.transaction, "Must not change transactions"
        assert self.tpc_phase is None, "Must be called outside of tpc"
        self.tpc_phase = 1

    def commit(self, transaction):
        """Record data modified during the transaction
        """
        assert transaction is self.transaction, "Must not change transactions"
        assert self.tpc_phase == 1, "Must be called in first phase of tpc"

        # In our simple example, we don't need to do anything.
        # A more complex data manager would typically write to some sort
        # of log.

    def tpc_vote(self, transaction):
        assert transaction is self.transaction, "Must not change transactions"
        assert self.tpc_phase == 1, "Must be called in first phase of tpc"
        # This particular data manager is always ready to vote.
        # Real data managers will usually need to take some steps to
        # make sure that the finish will succeed
        self.tpc_phase = 2

    def tpc_finish(self, transaction):
        assert transaction is self.transaction, "Must not change transactions"
        assert self.tpc_phase == 2, "Must be called in second phase of tpc"
        self.committed = self.uncommitted.copy()
        self._resetTransaction()
        
    def tpc_abort(self, transaction):
        assert transaction is self.transaction, "Must not change transactions"
        assert self.tpc_phase is not None, "Must be called inside of tpc"
        self.uncommitted = self.committed.copy()
        self._resetTransaction()
    
    #
    #######################################################################

    #######################################################################
    # Other data manager methods

    def sortKey(self):
        # Commit operations on multiple data managers are performed in
        # sort key order.  This important to avoid deadlock when data
        # managers are shared among multiple threads or processes and
        # use locks to manage that sharing.  We aren't going to bother
        # with that here.
        return str(id(self))

    #
    #######################################################################

class SampleSavepointDataManager(SampleDataManager):
    """Sample implementation of a savepoint-supporting data manager

    This extends the basic data manager with savepoint support.
    """

    def savepoint(self):
        # When we create the savepoint, we save the existing database state
        return SampleSavepoint(self, self.uncommitted.copy())

    def _rollback_savepoint(self, savepoint):
        # when we rollback the savepoint, we restore the saved data
        self.uncommitted = savepoint.data

class SampleSavepoint:

    def __init__(self, data_manager, data):
        self.data_manager = data_manager
        self.data = data

    def rollback(self):
        self.data_manager._rollback_savepoint(self)
