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
"""


class DataManager(object):
    """Sample data manager

       This class provides a trivial data-manager implementation and doc
       strings to illustrate the the protocol and to provide a tool for
       writing tests.

       Our sample data manager has state that is updated through an inc
       method and through transaction operations.

       When we create a sample data manager:

       >>> dm = DataManager()

       It has two bits of state, state:

       >>> dm.state
       0

       and delta:

       >>> dm.delta
       0

       Both of which are initialized to 0.  state is meant to model
       committed state, while delta represents tentative changes within a
       transaction.  We change the state by calling inc:

       >>> dm.inc()

       which updates delta:

       >>> dm.delta
       1

       but state isn't changed until we commit the transaction:

       >>> dm.state
       0

       To commit the changes, we use 2-phase commit. We execute the first
       stage by calling prepare.  We need to pass a transation. Our
       sample data managers don't really use the transactions for much,
       so we'll be lazy and use strings for transactions:

       >>> t1 = '1'
       >>> dm.prepare(t1)

       The sample data manager updates the state when we call prepare:

       >>> dm.state
       1
       >>> dm.delta
       1

       This is mainly so we can detect some affect of calling the methods.

       Now if we call commit:

       >>> dm.commit(t1)

       Our changes are"permanent".  The state reflects the changes and the
       delta has been reset to 0.

       >>> dm.state
       1
       >>> dm.delta
       0
       """

    def __init__(self):
        self.state = 0
        self.sp = 0
        self.transaction = None
        self.delta = 0
        self.prepared = False

    def inc(self, n=1):
        self.delta += n

    def prepare(self, transaction):
        """Prepare to commit data

        >>> dm = DataManager()
        >>> dm.inc()
        >>> t1 = '1'
        >>> dm.prepare(t1)
        >>> dm.commit(t1)
        >>> dm.state
        1
        >>> dm.inc()
        >>> t2 = '2'
        >>> dm.prepare(t2)
        >>> dm.abort(t2)
        >>> dm.state
        1

        It is en error to call prepare more than once without an intervening
        commit or abort:

        >>> dm.prepare(t1)

        >>> dm.prepare(t1)
        Traceback (most recent call last):
        ...
        TypeError: Already prepared

        >>> dm.prepare(t2)
        Traceback (most recent call last):
        ...
        TypeError: Already prepared

        >>> dm.abort(t1)

        If there was a preceeding savepoint, the transaction must match:

        >>> rollback = dm.savepoint(t1)
        >>> dm.prepare(t2)
        Traceback (most recent call last):
        ,,,
        TypeError: ('Transaction missmatch', '2', '1')

        >>> dm.prepare(t1)

        """
        if self.prepared:
            raise TypeError('Already prepared')
        self._checkTransaction(transaction)
        self.prepared = True
        self.transaction = transaction
        self.state += self.delta

    def _checkTransaction(self, transaction):
        if (transaction is not self.transaction
                and self.transaction is not None):
            raise TypeError("Transaction missmatch",
                            transaction, self.transaction)

    def abort(self, transaction):
        """Abort a transaction

        The abort method can be called before two-phase commit to
        throw away work done in the transaction:

        >>> dm = DataManager()
        >>> dm.inc()
        >>> dm.state, dm.delta
        (0, 1)
        >>> t1 = '1'
        >>> dm.abort(t1)
        >>> dm.state, dm.delta
        (0, 0)

        The abort method also throws away work done in savepoints:

        >>> dm.inc()
        >>> r = dm.savepoint(t1)
        >>> dm.inc()
        >>> r = dm.savepoint(t1)
        >>> dm.state, dm.delta
        (0, 2)
        >>> dm.abort(t1)
        >>> dm.state, dm.delta
        (0, 0)

        If savepoints are used, abort must be passed the same
        transaction:

        >>> dm.inc()
        >>> r = dm.savepoint(t1)
        >>> t2 = '2'
        >>> dm.abort(t2)
        Traceback (most recent call last):
        ...
        TypeError: ('Transaction missmatch', '2', '1')

        >>> dm.abort(t1)

        The abort method is also used to abort a two-phase commit:

        >>> dm.inc()
        >>> dm.state, dm.delta
        (0, 1)
        >>> dm.prepare(t1)
        >>> dm.state, dm.delta
        (1, 1)
        >>> dm.abort(t1)
        >>> dm.state, dm.delta
        (0, 0)

        Of course, the transactions passed to prepare and abort must
        match:

        >>> dm.prepare(t1)
        >>> dm.abort(t2)
        Traceback (most recent call last):
        ...
        TypeError: ('Transaction missmatch', '2', '1')

        >>> dm.abort(t1)


        """
        self._checkTransaction(transaction)
        if self.transaction is not None:
            self.transaction = None

        if self.prepared:
            self.state -= self.delta
            self.prepared = False

        self.delta = 0

    def commit(self, transaction):
        """Complete two-phase commit

        >>> dm = DataManager()
        >>> dm.state
        0
        >>> dm.inc()

        We start two-phase commit by calling prepare:

        >>> t1 = '1'
        >>> dm.prepare(t1)

        We complete it by calling commit:

        >>> dm.commit(t1)
        >>> dm.state
        1

        It is an error ro call commit without calling prepare first:

        >>> dm.inc()
        >>> t2 = '2'
        >>> dm.commit(t2)
        Traceback (most recent call last):
        ...
        TypeError: Not prepared to commit

        >>> dm.prepare(t2)
        >>> dm.commit(t2)

        If course, the transactions given to prepare and commit must
        be the same:

        >>> dm.inc()
        >>> t3 = '3'
        >>> dm.prepare(t3)
        >>> dm.commit(t2)
        Traceback (most recent call last):
        ...
        TypeError: ('Transaction missmatch', '2', '3')

        """
        if not self.prepared:
            raise TypeError('Not prepared to commit')
        self._checkTransaction(transaction)
        self.delta = 0
        self.transaction = None
        self.prepared = False

    def savepoint(self, transaction):
        """Provide the ability to rollback transaction state

        Savepoints provide a way to:

        - Save partial transaction work. For some data managers, this
          could allow resources to be used more efficiently.

        - Provide the ability to revert state to a point in a
          transaction without aborting the entire transaction.  In
          other words, savepoints support partial aborts.

        Savepoints don't use two-phase commit. If there are errors in
        setting or rolling back to savepoints, the application should
        abort the containing transaction.  This is *not* the
        responsibility of the data manager.

        Savepoints are always associated with a transaction. Any work
        done in a savepoint's transaction is tentative until the
        transaction is committed using two-phase commit.

        >>> dm = DataManager()
        >>> dm.inc()
        >>> t1 = '1'
        >>> r = dm.savepoint(t1)
        >>> dm.state, dm.delta
        (0, 1)
        >>> dm.inc()
        >>> dm.state, dm.delta
        (0, 2)
        >>> r.rollback()
        >>> dm.state, dm.delta
        (0, 1)
        >>> dm.prepare(t1)
        >>> dm.commit(t1)
        >>> dm.state, dm.delta
        (1, 0)

        Savepoints must have the same transaction:

        >>> r1 = dm.savepoint(t1)
        >>> dm.state, dm.delta
        (1, 0)
        >>> dm.inc()
        >>> dm.state, dm.delta
        (1, 1)
        >>> t2 = '2'
        >>> r2 = dm.savepoint(t2)
        Traceback (most recent call last):
        ...
        TypeError: ('Transaction missmatch', '2', '1')

        >>> r2 = dm.savepoint(t1)
        >>> dm.inc()
        >>> dm.state, dm.delta
        (1, 2)

        If we rollback to an earlier savepoint, we discard all work
        done later:

        >>> r1.rollback()
        >>> dm.state, dm.delta
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
        >>> dm.state, dm.delta
        (1, 0)

        >>> dm.inc()
        >>> dm.inc()
        >>> dm.inc()
        >>> dm.state, dm.delta
        (1, 3)
        >>> r1.rollback()
        >>> dm.state, dm.delta
        (1, 0)

        But we can't rollback to a savepoint after it has been
        committed:

        >>> dm.prepare(t1)
        >>> dm.commit(t1)

        >>> r1.rollback()
        Traceback (most recent call last):
        ...
        TypeError: Attempt to rollback stale rollback

        """
        if self.prepared:
            raise TypeError("Can't get savepoint during two-phase commit")
        self._checkTransaction(transaction)
        self.transaction = transaction
        self.sp += 1
        return Rollback(self)


class Rollback(object):

    def __init__(self, dm):
        self.dm = dm
        self.sp = dm.sp
        self.delta = dm.delta
        self.transaction = dm.transaction

    def rollback(self):
        if self.transaction is not self.dm.transaction:
            raise TypeError("Attempt to rollback stale rollback")
        if self.dm.sp < self.sp:
            raise TypeError("Attempt to roll back to invalid save point",
                            self.sp, self.dm.sp)
        self.dm.sp = self.sp
        self.dm.delta = self.delta


def test_suite():
    from doctest import DocTestSuite
    return DocTestSuite()
