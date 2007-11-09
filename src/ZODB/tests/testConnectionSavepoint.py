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
"""Tests of savepoint feature

$Id$
"""
import unittest
from zope.testing import doctest
import persistent.dict, transaction

def testAddingThenModifyThenAbort():
    """\
We ran into a problem in which abort failed after adding an object in
a savepoint and then modifying the object.  The problem was that, on
commit, the savepoint was aborted before the modifications were
aborted.  Because the object was added in the savepoint, its _p_oid
and _p_jar were cleared when the savepoint was aborted.  The object
was in the registered-object list.  There's an invariant for this
list that states that all objects in the list should have an oid and
(correct) jar.

The fix was to abort work done after the savepoint before aborting the
savepoint.

    >>> import ZODB.tests.util
    >>> db = ZODB.tests.util.DB()
    >>> connection = db.open()
    >>> root = connection.root()

    >>> ob = persistent.dict.PersistentDict()
    >>> root['ob'] = ob
    >>> sp = transaction.savepoint()
    >>> ob.x = 1
    >>> transaction.abort()
"""

def testModifyThenSavePointThenModifySomeMoreThenCommit():
    """\
We got conflict errors when we committed after we modified an object
in a savepoint, and then modified it some more after the last
savepoint.

The problem was that we were effectively commiting the object twice --
when commiting the current data and when committing the savepoint.
The fix was to first make a new savepoint to move new changes to the
savepoint storage and *then* to commit the savepoint storage.

    >>> import ZODB.tests.util
    >>> db = ZODB.tests.util.DB()
    >>> connection = db.open()
    >>> root = connection.root()
    >>> sp = transaction.savepoint()
    >>> root['a'] = 1
    >>> sp = transaction.savepoint()
    >>> root['a'] = 2
    >>> transaction.commit()
"""

def testCantCloseConnectionWithActiveSavepoint():
    """
    >>> import ZODB.tests.util
    >>> db = ZODB.tests.util.DB()
    >>> connection = db.open()
    >>> root = connection.root()
    >>> root['a'] = 1
    >>> sp = transaction.savepoint()
    >>> connection.close()
    Traceback (most recent call last):
    ...
    ConnectionStateError: Cannot close a connection joined to a transaction

    >>> db.close()
    """

def testSavepointDoesCacheGC():
    """\
Although the interface doesn't guarantee this internal detail, making a
savepoint should do incremental gc on connection memory caches.  Indeed,
one traditional use for savepoints is simply to free memory space midstream
during a long transaction.  Before ZODB 3.4.2, making a savepoint failed
to trigger cache gc, and this test verifies that it now does.

    >>> import ZODB
    >>> from ZODB.tests.MinPO import MinPO
    >>> from ZODB.MappingStorage import MappingStorage
    >>> import transaction
    >>> CACHESIZE = 5  # something tiny
    >>> LOOPCOUNT = CACHESIZE * 10
    >>> st = MappingStorage("Test")
    >>> db = ZODB.DB(st, cache_size=CACHESIZE)
    >>> cn = db.open()
    >>> rt = cn.root()

Now attach substantially more than CACHESIZE persistent objects to the root:

    >>> for i in range(LOOPCOUNT):
    ...     rt[i] = MinPO(i)
    >>> transaction.commit()

Now modify all of them; the cache should contain LOOPCOUNT MinPO objects
then, + 1 for the root object:

    >>> for i in range(LOOPCOUNT):
    ...     obj = rt[i]
    ...     obj.value = -i
    >>> len(cn._cache) == LOOPCOUNT + 1
    True

Making a savepoint at this time used to leave the cache holding the same
number of objects.  Make sure the cache shrinks now instead.

    >>> dummy = transaction.savepoint()
    >>> len(cn._cache) <= CACHESIZE + 1
    True

Verify all the values are as expected:

    >>> failures = []
    >>> for i in range(LOOPCOUNT):
    ...     obj = rt[i]
    ...     if obj.value != -i:
    ...         failures.append(obj)
    >>> failures
    []

    >>> transaction.abort()
    >>> db.close()
"""

def test_suite():
    return unittest.TestSuite((
        doctest.DocFileSuite('testConnectionSavepoint.txt'),
        doctest.DocTestSuite(),
        ))

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
