##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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
"""XXX short summary goes here.

$Id: test_datamanageradapter.py,v 1.2 2004/02/19 02:59:10 jeremy Exp $
"""
import unittest
from doctest import DocTestSuite
from ZODB.Transaction import DataManagerAdapter
from ZODB.tests.sampledm import DataManager

def test_normal_commit():
    """
    So, we have a data manager:

    >>> dm = DataManager()

    and we do some work that modifies uncommited state:

    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 1)

    Now we'll commit the changes.  When the data manager joins a transaction,
    the transaction will create an adapter.
    
    >>> dma = DataManagerAdapter(dm)

    and register it as a modified object. At commit time, the
    transaction will get the "jar" like this:

    >>> jar = getattr(dma, '_p_jar', dma)

    and, of course, the jar and the adapter will be the same:

    >>> jar is dma
    True

    The transaction will call tpc_begin:

    >>> t1 = '1'
    >>> jar.tpc_begin(t1)

    Then the transaction will call commit on the jar:

    >>> jar.commit(dma, t1)

    This doesn't actually do anything. :)

    >>> dm.state, dm.delta
    (0, 1)

    The transaction will then call tpc_vote:

    >>> jar.tpc_vote(t1)

    This prepares the data manager:

    >>> dm.state, dm.delta
    (1, 1)
    >>> dm.prepared
    True

    Finally, tpc_finish is called:

    >>> jar.tpc_finish(t1)

    and the data manager finishes the two-phase commit:
    
    >>> dm.state, dm.delta
    (1, 0)
    >>> dm.prepared
    False
    """

def test_abort():
    """
    So, we have a data manager:

    >>> dm = DataManager()

    and we do some work that modifies uncommited state:

    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 1)

    When the data manager joins a transaction,
    the transaction will create an adapter.
    
    >>> dma = DataManagerAdapter(dm)

    and register it as a modified object.

    Now we'll abort the transaction. The transaction will get the
    "jar" like this:

    >>> jar = getattr(dma, '_p_jar', dma)

    and, of course, the jar and the adapter will be the same:

    >>> jar is dma
    True

    Then the transaction will call abort on the jar:

    >>> t1 = '1'
    >>> jar.abort(dma, t1)

    Which aborts the changes in the data manager:

    >>> dm.state, dm.delta
    (0, 0)
    """

def test_tpc_abort_phase1():
    """
    So, we have a data manager:

    >>> dm = DataManager()

    and we do some work that modifies uncommited state:

    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 1)

    Now we'll commit the changes.  When the data manager joins a transaction,
    the transaction will create an adapter.
    
    >>> dma = DataManagerAdapter(dm)

    and register it as a modified object. At commit time, the
    transaction will get the "jar" like this:

    >>> jar = getattr(dma, '_p_jar', dma)

    and, of course, the jar and the adapter will be the same:

    >>> jar is dma
    True

    The transaction will call tpc_begin:

    >>> t1 = '1'
    >>> jar.tpc_begin(t1)

    Then the transaction will call commit on the jar:

    >>> jar.commit(dma, t1)

    This doesn't actually do anything. :)

    >>> dm.state, dm.delta
    (0, 1)

    At this point, the transaction decides to abort. It calls tpc_abort:

    >>> jar.tpc_abort(t1)

    Which causes the state of the data manager to be restored:

    >>> dm.state, dm.delta
    (0, 0)
    """

def test_tpc_abort_phase2():
    """
    So, we have a data manager:

    >>> dm = DataManager()

    and we do some work that modifies uncommited state:

    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 1)

    Now we'll commit the changes.  When the data manager joins a transaction,
    the transaction will create an adapter.
    
    >>> dma = DataManagerAdapter(dm)

    and register it as a modified object. At commit time, the
    transaction will get the "jar" like this:

    >>> jar = getattr(dma, '_p_jar', dma)

    and, of course, the jar and the adapter will be the same:

    >>> jar is dma
    True

    The transaction will call tpc_begin:

    >>> t1 = '1'
    >>> jar.tpc_begin(t1)

    Then the transaction will call commit on the jar:

    >>> jar.commit(dma, t1)

    This doesn't actually do anything. :)

    >>> dm.state, dm.delta
    (0, 1)

    The transaction calls vote:

    >>> jar.tpc_vote(t1)

    This prepares the data manager:

    >>> dm.state, dm.delta
    (1, 1)
    >>> dm.prepared
    True

    At this point, the transaction decides to abort. It calls tpc_abort:

    >>> jar.tpc_abort(t1)

    Which causes the state of the data manager to be restored:

    >>> dm.state, dm.delta
    (0, 0)
    >>> dm.prepared
    False
    """

def test_commit_w_subtransactions():
    """
    So, we have a data manager:

    >>> dm = DataManager()

    and we do some work that modifies uncommited state:

    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 1)

    Now we'll commit the changes in a subtransaction.  When the data
    manager joins a transaction, the transaction will create an
    adapter.
    
    >>> dma = DataManagerAdapter(dm)

    and register it as a modified object. At commit time, the
    transaction will get the "jar" like this:

    >>> jar = getattr(dma, '_p_jar', dma)

    and, of course, the jar and the adapter will be the same:

    >>> jar is dma
    True

    The transaction will call tpc_begin:

    >>> t1 = '1'
    >>> jar.tpc_begin(t1, 1) # 1 -> subtxn

    Then the transaction will call commit on the jar:

    >>> jar.commit(dma, t1)

    This doesn't actually do anything. :)

    >>> dm.state, dm.delta
    (0, 1)

    The transaction will then call tpc_vote:

    >>> jar.tpc_vote(t1)

    This doesn't do anything either, because zodb4 data managers don't
    actually do two-phase commit for subtransactions.

    >>> dm.state, dm.delta
    (0, 1)

    Finally, we call tpc_finish. This does actally create a savepoint,
    but we can't really tell that from outside.

    >>> jar.tpc_finish(t1)
    >>> dm.state, dm.delta
    (0, 1)

    We'll do more of the above:

    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 2)
    >>> jar.tpc_begin(t1, 1) # 1 -> subtxn
    >>> jar.commit(dma, t1)
    >>> jar.tpc_vote(t1)
    >>> jar.tpc_finish(t1)
    >>> dm.state, dm.delta
    (0, 2)
    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 3)
    >>> jar.tpc_begin(t1, 1) # 1 -> subtxn
    >>> jar.commit(dma, t1)
    >>> jar.tpc_vote(t1)
    >>> jar.tpc_finish(t1)
    >>> dm.state, dm.delta
    (0, 3)

    Note that the bove works *because* the same transaction is used
    for each subtransaction.

    Finally, we'll do a little more work:

    >>> dm.inc()
    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 5)
    
    and then commit the top-level transaction.

    The transaction  will actually go through the steps for a subtransaction:

    >>> jar.tpc_begin(t1, 1) # 1 -> subtxn
    >>> jar.commit(dma, t1)
    >>> jar.tpc_vote(t1)
    >>> jar.tpc_finish(t1)

    And then call commit_sub:

    >>> jar.commit_sub(t1)

    As usual, this doesn't actually do anything. ;)

    >>> dm.state, dm.delta
    (0, 5)

    The transaction manager doesn's call tpc_begin, because commit_sub
    implies the start of two-phase commit. Next, it does call commit:

    >>> jar.commit(dma, t1)

    which doesn't do anything.

    Finally, the transaction calls tpc_vote:

    >>> jar.tpc_vote(t1)

    which actually does something (because this is the top-level txn):

    >>> dm.state, dm.delta
    (5, 5)
    >>> dm.prepared
    True

    Finally, tpc_finish is called:

    >>> jar.tpc_finish(t1)

    and the data manager finishes the two-phase commit:
    
    >>> dm.state, dm.delta
    (5, 0)
    >>> dm.prepared
    False
    """

def test_commit_w_subtransactions_featuring_subtransaction_abort():
    """
    So, we have a data manager:

    >>> dm = DataManager()

    and we do some work that modifies uncommited state:

    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 1)

    Now we'll commit the changes in a subtransaction.  When the data
    manager joins a transaction, the transaction will create an
    adapter.
    
    >>> dma = DataManagerAdapter(dm)

    and register it as a modified object. At commit time, the
    transaction will get the "jar" like this:

    >>> jar = getattr(dma, '_p_jar', dma)

    and, of course, the jar and the adapter will be the same:

    >>> jar is dma
    True

    The transaction will call tpc_begin:

    >>> t1 = '1'
    >>> jar.tpc_begin(t1, 1) # 1 -> subtxn

    Then the transaction will call commit on the jar:

    >>> jar.commit(dma, t1)

    This doesn't actually do anything. :)

    >>> dm.state, dm.delta
    (0, 1)

    The transaction will then call tpc_vote:

    >>> jar.tpc_vote(t1)

    This doesn't do anything either, because zodb4 data managers don't
    actually do two-phase commit for subtransactions.

    >>> dm.state, dm.delta
    (0, 1)

    Finally, we call tpc_finish. This does actally create a savepoint,
    but we can't really tell that from outside.

    >>> jar.tpc_finish(t1)
    >>> dm.state, dm.delta
    (0, 1)

    We'll do more of the above:

    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 2)
    >>> jar.tpc_begin(t1, 1) # 1 -> subtxn
    >>> jar.commit(dma, t1)
    >>> jar.tpc_vote(t1)
    >>> jar.tpc_finish(t1)
    >>> dm.state, dm.delta
    (0, 2)

    
    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 3)

    But then we'll decide to abort a subtransaction.

    The transaction will just call abort as usual:

    >>> jar.abort(dma, t1)

    This will cause a rollback to the last savepoint:
    >>> dm.state, dm.delta
    (0, 2)

    Then we do more work:
    
    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 3)
    >>> jar.tpc_begin(t1, 1) # 1 -> subtxn
    >>> jar.commit(dma, t1)
    >>> jar.tpc_vote(t1)
    >>> jar.tpc_finish(t1)
    >>> dm.state, dm.delta
    (0, 3)

    Note that the bove works *because* the same transaction is used
    for each subtransaction.

    Finally, we'll do a little more work:

    >>> dm.inc()
    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 5)
    
    and then commit the top-level transaction.

    The transaction  will actually go through the steps for a subtransaction:

    >>> jar.tpc_begin(t1, 1) # 1 -> subtxn
    >>> jar.commit(dma, t1)
    >>> jar.tpc_vote(t1)
    >>> jar.tpc_finish(t1)

    And then call commit_sub:

    >>> jar.commit_sub(t1)

    As usual, this doesn't actually do anything. ;)

    >>> dm.state, dm.delta
    (0, 5)

    The transaction manager doesn's call tpc_begin, because commit_sub
    implies the start of two-phase commit. Next, it does call commit:

    >>> jar.commit(dma, t1)

    which doesn't do anything.

    Finally, the transaction calls tpc_vote:

    >>> jar.tpc_vote(t1)

    which actually does something (because this is the top-level txn):

    >>> dm.state, dm.delta
    (5, 5)
    >>> dm.prepared
    True

    Finally, tpc_finish is called:

    >>> jar.tpc_finish(t1)

    and the data manager finishes the two-phase commit:
    
    >>> dm.state, dm.delta
    (5, 0)
    >>> dm.prepared
    False
    """

def test_abort_w_subtransactions():
    """
    So, we have a data manager:

    >>> dm = DataManager()

    and we do some work that modifies uncommited state:

    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 1)

    Now we'll commit the changes in a subtransaction.  When the data
    manager joins a transaction, the transaction will create an
    adapter.
    
    >>> dma = DataManagerAdapter(dm)

    and register it as a modified object. At commit time, the
    transaction will get the "jar" like this:

    >>> jar = getattr(dma, '_p_jar', dma)

    and, of course, the jar and the adapter will be the same:

    >>> jar is dma
    True

    The transaction will call tpc_begin:

    >>> t1 = '1'
    >>> jar.tpc_begin(t1, 1) # 1 -> subtxn

    Then the transaction will call commit on the jar:

    >>> jar.commit(dma, t1)

    This doesn't actually do anything. :)

    >>> dm.state, dm.delta
    (0, 1)

    The transaction will then call tpc_vote:

    >>> jar.tpc_vote(t1)

    This doesn't do anything either, because zodb4 data managers don't
    actually do two-phase commit for subtransactions.

    >>> dm.state, dm.delta
    (0, 1)

    Finally, we call tpc_finish. This does actally create a savepoint,
    but we can't really tell that from outside.

    >>> jar.tpc_finish(t1)
    >>> dm.state, dm.delta
    (0, 1)

    We'll do more of the above:

    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 2)
    >>> jar.tpc_begin(t1, 1) # 1 -> subtxn
    >>> jar.commit(dma, t1)
    >>> jar.tpc_vote(t1)
    >>> jar.tpc_finish(t1)
    >>> dm.state, dm.delta
    (0, 2)
    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 3)
    >>> jar.tpc_begin(t1, 1) # 1 -> subtxn
    >>> jar.commit(dma, t1)
    >>> jar.tpc_vote(t1)
    >>> jar.tpc_finish(t1)
    >>> dm.state, dm.delta
    (0, 3)

    Note that the bove works *because* the same transaction is used
    for each subtransaction.

    Finally, we'll do a little more work:

    >>> dm.inc()
    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 5)
    
    and then abort the top-level transaction.

    The transaction first call abort on the jar:

    >>> jar.abort(dma, t1)

    This will have the effect of aborting the subtrancation:

    >>> dm.state, dm.delta
    (0, 3)
    
    Then the transaction will call abort_sub:

    >>> jar.abort_sub(t1)

    This will abort all of the subtransactions:

    >>> dm.state, dm.delta
    (0, 0)
    """


def test_tpc_abort_w_subtransactions_featuring_subtransaction_abort():
    """
    So, we have a data manager:

    >>> dm = DataManager()

    and we do some work that modifies uncommited state:

    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 1)

    Now we'll commit the changes in a subtransaction.  When the data
    manager joins a transaction, the transaction will create an
    adapter.
    
    >>> dma = DataManagerAdapter(dm)

    and register it as a modified object. At commit time, the
    transaction will get the "jar" like this:

    >>> jar = getattr(dma, '_p_jar', dma)

    and, of course, the jar and the adapter will be the same:

    >>> jar is dma
    True

    The transaction will call tpc_begin:

    >>> t1 = '1'
    >>> jar.tpc_begin(t1, 1) # 1 -> subtxn

    Then the transaction will call commit on the jar:

    >>> jar.commit(dma, t1)

    This doesn't actually do anything. :)

    >>> dm.state, dm.delta
    (0, 1)

    The transaction will then call tpc_vote:

    >>> jar.tpc_vote(t1)

    This doesn't do anything either, because zodb4 data managers don't
    actually do two-phase commit for subtransactions.

    >>> dm.state, dm.delta
    (0, 1)

    Finally, we call tpc_finish. This does actally create a savepoint,
    but we can't really tell that from outside.

    >>> jar.tpc_finish(t1)
    >>> dm.state, dm.delta
    (0, 1)

    We'll do more of the above:

    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 2)
    >>> jar.tpc_begin(t1, 1) # 1 -> subtxn
    >>> jar.commit(dma, t1)
    >>> jar.tpc_vote(t1)
    >>> jar.tpc_finish(t1)
    >>> dm.state, dm.delta
    (0, 2)

    
    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 3)

    But then we'll decide to abort a subtransaction.

    The transaction will just call abort as usual:

    >>> jar.abort(dma, t1)

    This will cause a rollback to the last savepoint:
    >>> dm.state, dm.delta
    (0, 2)

    Then we do more work:
    
    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 3)
    >>> jar.tpc_begin(t1, 1) # 1 -> subtxn
    >>> jar.commit(dma, t1)
    >>> jar.tpc_vote(t1)
    >>> jar.tpc_finish(t1)
    >>> dm.state, dm.delta
    (0, 3)

    Note that the bove works *because* the same transaction is used
    for each subtransaction.

    Finally, we'll do a little more work:

    >>> dm.inc()
    >>> dm.inc()
    >>> dm.state, dm.delta
    (0, 5)
    
    and then commit the top-level transaction.

    The transaction  will actually go through the steps for a subtransaction:

    >>> jar.tpc_begin(t1, 1) # 1 -> subtxn
    >>> jar.commit(dma, t1)
    >>> jar.tpc_vote(t1)
    >>> jar.tpc_finish(t1)

    And then call commit_sub:

    >>> jar.commit_sub(t1)

    As usual, this doesn't actually do anything. ;)

    >>> dm.state, dm.delta
    (0, 5)

    The transaction manager doesn's call tpc_begin, because commit_sub
    implies the start of two-phase commit. Next, it does call commit:

    >>> jar.commit(dma, t1)

    which doesn't do anything.

    Finally, the transaction calls tpc_vote:

    >>> jar.tpc_vote(t1)

    which actually does something (because this is the top-level txn):

    >>> dm.state, dm.delta
    (5, 5)
    >>> dm.prepared
    True

    Now, at the last minute, the transaction is aborted (possibly due
    to a "no vote" from another data manager):

    >>> jar.tpc_abort(t1)

    An the changes are undone:
    
    >>> dm.state, dm.delta
    (0, 0)
    >>> dm.prepared
    False
    """

def test_suite():
    return unittest.TestSuite((
        DocTestSuite(),
        ))

if __name__ == '__main__': unittest.main()
