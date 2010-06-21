##############################################################################
#
# Copyright (c) 2003 Zope Foundation and Contributors.
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
"""
$Id$
"""
import unittest
from zope.testing.doctest import DocTestSuite
from transaction._transaction import DataManagerAdapter
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

    >>> jar.commit(t1)

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
    >>> jar.abort(t1)

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

    >>> jar.commit(t1)

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

    >>> jar.commit(t1)

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

def test_suite():
    return DocTestSuite()

if __name__ == '__main__':
    unittest.main()
