##############################################################################
#
# Copyright (c) 2004 Zope Foundation and Contributors.
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
"""Sample objects for use in tests

$Id$
"""

class ResourceManager(object):
    """Sample resource manager.

       This class provides a trivial resource-manager implementation and doc
       strings to illustrate the protocol and to provide a tool for writing
       tests.

       Our sample resource manager has state that is updated through an inc
       method and through transaction operations.

       When we create a sample resource manager:

       >>> rm = ResourceManager()

       It has two pieces state, state and delta, both initialized to 0:

       >>> rm.state
       0
       >>> rm.delta
       0

       state is meant to model committed state, while delta represents
       tentative changes within a transaction.  We change the state by
       calling inc:

       >>> rm.inc()

       which updates delta:

       >>> rm.delta
       1

       but state isn't changed until we commit the transaction:

       >>> rm.state
       0

       To commit the changes, we use 2-phase commit.  We execute the first
       stage by calling prepare.  We need to pass a transation. Our
       sample resource managers don't really use the transactions for much,
       so we'll be lazy and use strings for transactions.  The sample
       resource manager updates the state when we call tpc_vote:


       >>> t1 = '1'
       >>> rm.tpc_begin(t1)
       >>> rm.state, rm.delta
       (0, 1)

       >>> rm.tpc_vote(t1)
       >>> rm.state, rm.delta
       (1, 1)

       Now if we call tpc_finish:

       >>> rm.tpc_finish(t1)

       Our changes are "permanent".  The state reflects the changes and the
       delta has been reset to 0.

       >>> rm.state, rm.delta
       (1, 0)
       """

    def __init__(self):
        self.state = 0
        self.sp = 0
        self.transaction = None
        self.delta = 0
        self.txn_state = None

    def _check_state(self, *ok_states):
        if self.txn_state not in ok_states:
            raise ValueError("txn in state %r but expected one of %r" %
                             (self.txn_state, ok_states))

    def _checkTransaction(self, transaction):
        if (transaction is not self.transaction
            and self.transaction is not None):
            raise TypeError("Transaction missmatch",
                            transaction, self.transaction)

    def inc(self, n=1):
        self.delta += n

    def tpc_begin(self, transaction):
        """Prepare to commit data.

        >>> rm = ResourceManager()
        >>> rm.inc()
        >>> t1 = '1'
        >>> rm.tpc_begin(t1)
        >>> rm.tpc_vote(t1)
        >>> rm.tpc_finish(t1)
        >>> rm.state
        1
        >>> rm.inc()
        >>> t2 = '2'
        >>> rm.tpc_begin(t2)
        >>> rm.tpc_vote(t2)
        >>> rm.tpc_abort(t2)
        >>> rm.state
        1

        It is an error to call tpc_begin more than once without completing
        two-phase commit:

        >>> rm.tpc_begin(t1)

        >>> rm.tpc_begin(t1)
        Traceback (most recent call last):
        ...
        ValueError: txn in state 'tpc_begin' but expected one of (None,)
        >>> rm.tpc_abort(t1)

        If there was a preceeding savepoint, the transaction must match:

        >>> rollback = rm.savepoint(t1)
        >>> rm.tpc_begin(t2)
        Traceback (most recent call last):
        ,,,
        TypeError: ('Transaction missmatch', '2', '1')

        >>> rm.tpc_begin(t1)

        """
        self._checkTransaction(transaction)
        self._check_state(None)
        self.transaction = transaction
        self.txn_state = 'tpc_begin'

    def tpc_vote(self, transaction):
        """Verify that a data manager can commit the transaction.

        This is the last chance for a data manager to vote 'no'.  A
        data manager votes 'no' by raising an exception.

        transaction is the ITransaction instance associated with the
        transaction being committed.
        """
        self._checkTransaction(transaction)
        self._check_state('tpc_begin')
        self.state += self.delta
        self.txn_state = 'tpc_vote'

    def tpc_finish(self, transaction):
        """Complete two-phase commit

        >>> rm = ResourceManager()
        >>> rm.state
        0
        >>> rm.inc()

        We start two-phase commit by calling prepare:

        >>> t1 = '1'
        >>> rm.tpc_begin(t1)
        >>> rm.tpc_vote(t1)

        We complete it by calling tpc_finish:

        >>> rm.tpc_finish(t1)
        >>> rm.state
        1

        It is an error ro call tpc_finish without calling tpc_vote:

        >>> rm.inc()
        >>> t2 = '2'
        >>> rm.tpc_begin(t2)
        >>> rm.tpc_finish(t2)
        Traceback (most recent call last):
        ...
        ValueError: txn in state 'tpc_begin' but expected one of ('tpc_vote',)

        >>> rm.tpc_abort(t2)  # clean slate

        >>> rm.tpc_begin(t2)
        >>> rm.tpc_vote(t2)
        >>> rm.tpc_finish(t2)

        Of course, the transactions given to tpc_begin and tpc_finish must
        be the same:

        >>> rm.inc()
        >>> t3 = '3'
        >>> rm.tpc_begin(t3)
        >>> rm.tpc_vote(t3)
        >>> rm.tpc_finish(t2)
        Traceback (most recent call last):
        ...
        TypeError: ('Transaction missmatch', '2', '3')
        """
        self._checkTransaction(transaction)
        self._check_state('tpc_vote')
        self.delta = 0
        self.transaction = None
        self.prepared = False
        self.txn_state = None

    def tpc_abort(self, transaction):
        """Abort a transaction

        The abort method can be called before two-phase commit to
        throw away work done in the transaction:

        >>> rm = ResourceManager()
        >>> rm.inc()
        >>> rm.state, rm.delta
        (0, 1)
        >>> t1 = '1'
        >>> rm.tpc_abort(t1)
        >>> rm.state, rm.delta
        (0, 0)

        The abort method also throws away work done in savepoints:

        >>> rm.inc()
        >>> r = rm.savepoint(t1)
        >>> rm.inc()
        >>> r = rm.savepoint(t1)
        >>> rm.state, rm.delta
        (0, 2)
        >>> rm.tpc_abort(t1)
        >>> rm.state, rm.delta
        (0, 0)

        If savepoints are used, abort must be passed the same
        transaction:

        >>> rm.inc()
        >>> r = rm.savepoint(t1)
        >>> t2 = '2'
        >>> rm.tpc_abort(t2)
        Traceback (most recent call last):
        ...
        TypeError: ('Transaction missmatch', '2', '1')

        >>> rm.tpc_abort(t1)

        The abort method is also used to abort a two-phase commit:

        >>> rm.inc()
        >>> rm.state, rm.delta
        (0, 1)
        >>> rm.tpc_begin(t1)
        >>> rm.state, rm.delta
        (0, 1)
        >>> rm.tpc_vote(t1)
        >>> rm.state, rm.delta
        (1, 1)
        >>> rm.tpc_abort(t1)
        >>> rm.state, rm.delta
        (0, 0)

        Of course, the transactions passed to prepare and abort must
        match:

        >>> rm.tpc_begin(t1)
        >>> rm.tpc_abort(t2)
        Traceback (most recent call last):
        ...
        TypeError: ('Transaction missmatch', '2', '1')

        >>> rm.tpc_abort(t1)

        This should never fail.
        """

        self._checkTransaction(transaction)
        if self.transaction is not None:
            self.transaction = None

        if self.txn_state == 'tpc_vote':
            self.state -= self.delta

        self.txn_state = None
        self.delta = 0

    def savepoint(self, transaction):
        """Provide the ability to rollback transaction state

        Savepoints provide a way to:

        - Save partial transaction work. For some resource managers, this
          could allow resources to be used more efficiently.

        - Provide the ability to revert state to a point in a
          transaction without aborting the entire transaction.  In
          other words, savepoints support partial aborts.

        Savepoints don't use two-phase commit. If there are errors in
        setting or rolling back to savepoints, the application should
        abort the containing transaction.  This is *not* the
        responsibility of the resource manager.

        Savepoints are always associated with a transaction. Any work
        done in a savepoint's transaction is tentative until the
        transaction is committed using two-phase commit.

        >>> rm = ResourceManager()
        >>> rm.inc()
        >>> t1 = '1'
        >>> r = rm.savepoint(t1)
        >>> rm.state, rm.delta
        (0, 1)
        >>> rm.inc()
        >>> rm.state, rm.delta
        (0, 2)
        >>> r.rollback()
        >>> rm.state, rm.delta
        (0, 1)
        >>> rm.tpc_begin(t1)
        >>> rm.tpc_vote(t1)
        >>> rm.tpc_finish(t1)
        >>> rm.state, rm.delta
        (1, 0)

        Savepoints must have the same transaction:

        >>> r1 = rm.savepoint(t1)
        >>> rm.state, rm.delta
        (1, 0)
        >>> rm.inc()
        >>> rm.state, rm.delta
        (1, 1)
        >>> t2 = '2'
        >>> r2 = rm.savepoint(t2)
        Traceback (most recent call last):
        ...
        TypeError: ('Transaction missmatch', '2', '1')

        >>> r2 = rm.savepoint(t1)
        >>> rm.inc()
        >>> rm.state, rm.delta
        (1, 2)

        If we rollback to an earlier savepoint, we discard all work
        done later:

        >>> r1.rollback()
        >>> rm.state, rm.delta
        (1, 0)

        and we can no longer rollback to the later savepoint:

        >>> r2.rollback()
        Traceback (most recent call last):
        ...
        TypeError: ('Attempt to roll back to invalid save point', 3, 2)

        We can roll back to a savepoint as often as we like:

        >>> r1.rollback()
        >>> r1.rollback()
        >>> r1.rollback()
        >>> rm.state, rm.delta
        (1, 0)

        >>> rm.inc()
        >>> rm.inc()
        >>> rm.inc()
        >>> rm.state, rm.delta
        (1, 3)
        >>> r1.rollback()
        >>> rm.state, rm.delta
        (1, 0)

        But we can't rollback to a savepoint after it has been
        committed:

        >>> rm.tpc_begin(t1)
        >>> rm.tpc_vote(t1)
        >>> rm.tpc_finish(t1)

        >>> r1.rollback()
        Traceback (most recent call last):
        ...
        TypeError: Attempt to rollback stale rollback

        """
        if self.txn_state is not None:
            raise TypeError("Can't get savepoint during two-phase commit")
        self._checkTransaction(transaction)
        self.transaction = transaction
        self.sp += 1
        return SavePoint(self)

    def discard(self, transaction):
        pass

class SavePoint(object):

    def __init__(self, rm):
        self.rm = rm
        self.sp = rm.sp
        self.delta = rm.delta
        self.transaction = rm.transaction

    def rollback(self):
        if self.transaction is not self.rm.transaction:
            raise TypeError("Attempt to rollback stale rollback")
        if self.rm.sp < self.sp:
            raise TypeError("Attempt to roll back to invalid save point",
                            self.sp, self.rm.sp)
        self.rm.sp = self.sp
        self.rm.delta = self.delta

    def discard(self):
        pass

def test_suite():
    from doctest import DocTestSuite
    return DocTestSuite()

if __name__ == '__main__':
    unittest.main()
