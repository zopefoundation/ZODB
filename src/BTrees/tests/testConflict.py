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
# FOR A PARTICULAR PURPOSE
#
##############################################################################
import os
from unittest import TestCase, TestSuite, makeSuite
from types import ClassType

from BTrees.OOBTree import OOBTree, OOBucket, OOSet, OOTreeSet
from BTrees.IOBTree import IOBTree, IOBucket, IOSet, IOTreeSet
from BTrees.IIBTree import IIBTree, IIBucket, IISet, IITreeSet
from BTrees.IFBTree import IFBTree, IFBucket, IFSet, IFTreeSet
from BTrees.OIBTree import OIBTree, OIBucket, OISet, OITreeSet
from BTrees.LOBTree import LOBTree, LOBucket, LOSet, LOTreeSet
from BTrees.LLBTree import LLBTree, LLBucket, LLSet, LLTreeSet
from BTrees.LFBTree import LFBTree, LFBucket, LFSet, LFTreeSet
from BTrees.OLBTree import OLBTree, OLBucket, OLSet, OLTreeSet

import transaction
from ZODB.POSException import ConflictError

class Base:
    """ Tests common to all types: sets, buckets, and BTrees """

    storage = None

    def setUp(self):
        self.t = self.t_type()

    def tearDown(self):
        transaction.abort()
        del self.t
        if self.storage is not None:
            self.storage.close()
            self.storage.cleanup()

    def openDB(self):
        from ZODB.FileStorage import FileStorage
        from ZODB.DB import DB
        n = 'fs_tmp__%s' % os.getpid()
        self.storage = FileStorage(n)
        self.db = DB(self.storage)
        return self.db

class MappingBase(Base):
    """ Tests common to mappings (buckets, btrees) """

    def _deletefail(self):
        del self.t[1]

    def _setupConflict(self):

        l=[ -5124, -7377, 2274, 8801, -9901, 7327, 1565, 17, -679,
            3686, -3607, 14, 6419, -5637, 6040, -4556, -8622, 3847, 7191,
            -4067]


        e1=[(-1704, 0), (5420, 1), (-239, 2), (4024, 3), (-6984, 4)]
        e2=[(7745, 0), (4868, 1), (-2548, 2), (-2711, 3), (-3154, 4)]


        base=self.t
        base.update([(i, i*i) for i in l[:20]])
        b1=base.__class__(base)
        b2=base.__class__(base)
        bm=base.__class__(base)

        items=base.items()

        return  base, b1, b2, bm, e1, e2, items

    def testSimpleConflict(self):
        # Unlike all the other tests, invoke conflict resolution
        # by committing a transaction and catching a conflict
        # in the storage.
        self.openDB()

        r1 = self.db.open().root()
        r1["t"] = self.t
        transaction.commit()

        r2 = self.db.open().root()
        copy = r2["t"]
        list(copy)    # unghostify

        self.assertEqual(self.t._p_serial, copy._p_serial)

        self.t.update({1:2, 2:3})
        transaction.commit()

        copy.update({3:4})
        transaction.commit()


    def testMergeDelete(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()
        del b1[items[1][0]]
        del b2[items[5][0]]
        del b1[items[-1][0]]
        del b2[items[-2][0]]
        del bm[items[1][0]]
        del bm[items[5][0]]
        del bm[items[-1][0]]
        del bm[items[-2][0]]
        test_merge(base, b1, b2, bm, 'merge  delete')

    def testMergeDeleteAndUpdate(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()
        del b1[items[1][0]]
        b2[items[5][0]]=1
        del b1[items[-1][0]]
        b2[items[-2][0]]=2
        del bm[items[1][0]]
        bm[items[5][0]]=1
        del bm[items[-1][0]]
        bm[items[-2][0]]=2
        test_merge(base, b1, b2, bm, 'merge update and delete')

    def testMergeUpdate(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()
        b1[items[0][0]]=1
        b2[items[5][0]]=2
        b1[items[-1][0]]=3
        b2[items[-2][0]]=4
        bm[items[0][0]]=1
        bm[items[5][0]]=2
        bm[items[-1][0]]=3
        bm[items[-2][0]]=4
        test_merge(base, b1, b2, bm, 'merge update')

    def testFailMergeDelete(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()
        del b1[items[0][0]]
        del b2[items[0][0]]
        test_merge(base, b1, b2, bm, 'merge conflicting delete',
                   should_fail=1)

    def testFailMergeUpdate(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()
        b1[items[0][0]]=1
        b2[items[0][0]]=2
        test_merge(base, b1, b2, bm, 'merge conflicting update',
                   should_fail=1)

    def testFailMergeDeleteAndUpdate(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()
        del b1[items[0][0]]
        b2[items[0][0]]=-9
        test_merge(base, b1, b2, bm, 'merge conflicting update and delete',
                   should_fail=1)

    def testMergeInserts(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()

        b1[-99999]=-99999
        b1[e1[0][0]]=e1[0][1]
        b2[99999]=99999
        b2[e1[2][0]]=e1[2][1]

        bm[-99999]=-99999
        bm[e1[0][0]]=e1[0][1]
        bm[99999]=99999
        bm[e1[2][0]]=e1[2][1]
        test_merge(base, b1, b2, bm, 'merge insert')

    def testMergeInsertsFromEmpty(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()

        base.clear()
        b1.clear()
        b2.clear()
        bm.clear()

        b1.update(e1)
        bm.update(e1)
        b2.update(e2)
        bm.update(e2)

        test_merge(base, b1, b2, bm, 'merge insert from empty')

    def testFailMergeEmptyAndFill(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()

        b1.clear()
        bm.clear()
        b2.update(e2)
        bm.update(e2)

        test_merge(base, b1, b2, bm, 'merge insert from empty', should_fail=1)

    def testMergeEmpty(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()

        b1.clear()
        bm.clear()

        test_merge(base, b1, b2, bm, 'empty one and not other', should_fail=1)

    def testFailMergeInsert(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()
        b1[-99999]=-99999
        b1[e1[0][0]]=e1[0][1]
        b2[99999]=99999
        b2[e1[0][0]]=e1[0][1]
        test_merge(base, b1, b2, bm, 'merge conflicting inserts',
                   should_fail=1)

class SetTests(Base):
    "Set (as opposed to TreeSet) specific tests."

    def _setupConflict(self):
        l=[ -5124, -7377, 2274, 8801, -9901, 7327, 1565, 17, -679,
            3686, -3607, 14, 6419, -5637, 6040, -4556, -8622, 3847, 7191,
            -4067]

        e1=[-1704, 5420, -239, 4024, -6984]
        e2=[7745, 4868, -2548, -2711, -3154]


        base=self.t
        base.update(l)
        b1=base.__class__(base)
        b2=base.__class__(base)
        bm=base.__class__(base)

        items=base.keys()

        return  base, b1, b2, bm, e1, e2, items

    def testMergeDelete(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()
        b1.remove(items[1])
        b2.remove(items[5])
        b1.remove(items[-1])
        b2.remove(items[-2])
        bm.remove(items[1])
        bm.remove(items[5])
        bm.remove(items[-1])
        bm.remove(items[-2])
        test_merge(base, b1, b2, bm, 'merge  delete')

    def testFailMergeDelete(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()
        b1.remove(items[0])
        b2.remove(items[0])
        test_merge(base, b1, b2, bm, 'merge conflicting delete',
                   should_fail=1)

    def testMergeInserts(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()

        b1.insert(-99999)
        b1.insert(e1[0])
        b2.insert(99999)
        b2.insert(e1[2])

        bm.insert(-99999)
        bm.insert(e1[0])
        bm.insert(99999)
        bm.insert(e1[2])
        test_merge(base, b1, b2, bm, 'merge insert')

    def testMergeInsertsFromEmpty(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()

        base.clear()
        b1.clear()
        b2.clear()
        bm.clear()

        b1.update(e1)
        bm.update(e1)
        b2.update(e2)
        bm.update(e2)

        test_merge(base, b1, b2, bm, 'merge insert from empty')

    def testFailMergeEmptyAndFill(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()

        b1.clear()
        bm.clear()
        b2.update(e2)
        bm.update(e2)

        test_merge(base, b1, b2, bm, 'merge insert from empty', should_fail=1)

    def testMergeEmpty(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()

        b1.clear()
        bm.clear()

        test_merge(base, b1, b2, bm, 'empty one and not other', should_fail=1)

    def testFailMergeInsert(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()
        b1.insert(-99999)
        b1.insert(e1[0])
        b2.insert(99999)
        b2.insert(e1[0])
        test_merge(base, b1, b2, bm, 'merge conflicting inserts',
                   should_fail=1)


def test_merge(o1, o2, o3, expect, message='failed to merge', should_fail=0):
    s1 = o1.__getstate__()
    s2 = o2.__getstate__()
    s3 = o3.__getstate__()
    expected = expect.__getstate__()
    if expected is None:
        expected = ((((),),),)

    if should_fail:
        try:
            merged = o1._p_resolveConflict(s1, s2, s3)
        except ConflictError, err:
            pass
        else:
            assert 0, message
    else:
        merged = o1._p_resolveConflict(s1, s2, s3)
        assert merged == expected, message

class NastyConfict(Base, TestCase):

    t_type = OOBTree

    # This tests a problem that cropped up while trying to write
    # testBucketSplitConflict (below):  conflict resolution wasn't
    # working at all in non-trivial cases.  Symptoms varied from
    # strange complaints about pickling (despite that the test isn't
    # doing any *directly*), thru SystemErrors from Python and
    # AssertionErrors inside the BTree code.
    def testResolutionBlowsUp(self):
        b = self.t
        for i in range(0, 200, 4):
            b[i] = i
        # bucket 0 has 15 values: 0, 4 .. 56
        # bucket 1 has 15 values: 60, 64 .. 116
        # bucket 2 has 20 values: 120, 124 .. 196
        state = b.__getstate__()
        # Looks like:  ((bucket0, 60, bucket1, 120, bucket2), firstbucket)
        # If these fail, the *preconditions* for running the test aren't
        # satisfied -- the test itself hasn't been run yet.
        self.assertEqual(len(state), 2)
        self.assertEqual(len(state[0]), 5)
        self.assertEqual(state[0][1], 60)
        self.assertEqual(state[0][3], 120)

        # Invoke conflict resolution by committing a transaction.
        self.openDB()

        r1 = self.db.open().root()
        r1["t"] = self.t
        transaction.commit()

        r2 = self.db.open().root()
        copy = r2["t"]
        # Make sure all of copy is loaded.
        list(copy.values())

        self.assertEqual(self.t._p_serial, copy._p_serial)

        self.t.update({1:2, 2:3})
        transaction.commit()

        copy.update({3:4})
        transaction.commit()  # if this doesn't blow up
        list(copy.values())         # and this doesn't either, then fine

    def testBucketSplitConflict(self):
        # Tests that a bucket split is viewed as a conflict.
        # It's (almost necessarily) a white-box test, and sensitive to
        # implementation details.
        b = self.t
        for i in range(0, 200, 4):
            b[i] = i
        # bucket 0 has 15 values: 0, 4 .. 56
        # bucket 1 has 15 values: 60, 64 .. 116
        # bucket 2 has 20 values: 120, 124 .. 196
        state = b.__getstate__()
        # Looks like:  ((bucket0, 60, bucket1, 120, bucket2), firstbucket)
        # If these fail, the *preconditions* for running the test aren't
        # satisfied -- the test itself hasn't been run yet.
        self.assertEqual(len(state), 2)
        self.assertEqual(len(state[0]), 5)
        self.assertEqual(state[0][1], 60)
        self.assertEqual(state[0][3], 120)

        # Invoke conflict resolution by committing a transaction.
        self.openDB()

        tm1 = transaction.TransactionManager()
        r1 = self.db.open(transaction_manager=tm1).root()
        r1["t"] = self.t
        tm1.commit()

        tm2 = transaction.TransactionManager()
        r2 = self.db.open(transaction_manager=tm2).root()
        copy = r2["t"]
        # Make sure all of copy is loaded.
        list(copy.values())

        self.assertEqual(self.t._p_serial, copy._p_serial)

        # In one transaction, add 16 new keys to bucket1, to force a bucket
        # split.
        b = self.t
        numtoadd = 16
        candidate = 60
        while numtoadd:
            if not b.has_key(candidate):
                b[candidate] = candidate
                numtoadd -= 1
            candidate += 1
        # bucket 0 has 15 values: 0, 4 .. 56
        # bucket 1 has 15 values: 60, 61 .. 74
        # bucket 2 has 16 values: [75, 76 .. 81] + [84, 88 ..116]
        # bucket 3 has 20 values: 120, 124 .. 196
        state = b.__getstate__()
        # Looks like:  ((b0, 60, b1, 75, b2, 120, b3), firstbucket)
        # The next block is still verifying preconditions.
        self.assertEqual(len(state) , 2)
        self.assertEqual(len(state[0]), 7)
        self.assertEqual(state[0][1], 60)
        self.assertEqual(state[0][3], 75)
        self.assertEqual(state[0][5], 120)

        tm1.commit()

        # In the other transaction, add 3 values near the tail end of bucket1.
        # This doesn't cause a split.
        b = copy
        for i in range(112, 116):
            b[i] = i
        # bucket 0 has 15 values: 0, 4 .. 56
        # bucket 1 has 18 values: 60, 64 .. 112, 113, 114, 115, 116
        # bucket 2 has 20 values: 120, 124 .. 196
        state = b.__getstate__()
        # Looks like:  ((bucket0, 60, bucket1, 120, bucket2), firstbucket)
        # The next block is still verifying preconditions.
        self.assertEqual(len(state), 2)
        self.assertEqual(len(state[0]), 5)
        self.assertEqual(state[0][1], 60)
        self.assertEqual(state[0][3], 120)

        self.assertRaises(ConflictError, tm2.commit)

    def testEmptyBucketConflict(self):
        # Tests that an emptied bucket *created by* conflict resolution is
        # viewed as a conflict:  conflict resolution doesn't have enough
        # info to unlink the empty bucket from the BTree correctly.
        b = self.t
        for i in range(0, 200, 4):
            b[i] = i
        # bucket 0 has 15 values: 0, 4 .. 56
        # bucket 1 has 15 values: 60, 64 .. 116
        # bucket 2 has 20 values: 120, 124 .. 196
        state = b.__getstate__()
        # Looks like:  ((bucket0, 60, bucket1, 120, bucket2), firstbucket)
        # If these fail, the *preconditions* for running the test aren't
        # satisfied -- the test itself hasn't been run yet.
        self.assertEqual(len(state), 2)
        self.assertEqual(len(state[0]), 5)
        self.assertEqual(state[0][1], 60)
        self.assertEqual(state[0][3], 120)

        # Invoke conflict resolution by committing a transaction.
        self.openDB()

        tm1 = transaction.TransactionManager()
        r1 = self.db.open(transaction_manager=tm1).root()
        r1["t"] = self.t
        tm1.commit()

        tm2 = transaction.TransactionManager()
        r2 = self.db.open(transaction_manager=tm2).root()
        copy = r2["t"]
        # Make sure all of copy is loaded.
        list(copy.values())

        self.assertEqual(self.t._p_serial, copy._p_serial)

        # In one transaction, delete half of bucket 1.
        b = self.t
        for k in 60, 64, 68, 72, 76, 80, 84, 88:
            del b[k]
        # bucket 0 has 15 values: 0, 4 .. 56
        # bucket 1 has 7 values: 92, 96, 100, 104, 108, 112, 116
        # bucket 2 has 20 values: 120, 124 .. 196
        state = b.__getstate__()
        # Looks like:  ((bucket0, 60, bucket1, 120, bucket2), firstbucket)
        # The next block is still verifying preconditions.
        self.assertEqual(len(state) , 2)
        self.assertEqual(len(state[0]), 5)
        self.assertEqual(state[0][1], 92)
        self.assertEqual(state[0][3], 120)

        tm1.commit()

        # In the other transaction, delete the other half of bucket 1.
        b = copy
        for k in 92, 96, 100, 104, 108, 112, 116:
            del b[k]
        # bucket 0 has 15 values: 0, 4 .. 56
        # bucket 1 has 8 values: 60, 64, 68, 72, 76, 80, 84, 88
        # bucket 2 has 20 values: 120, 124 .. 196
        state = b.__getstate__()
        # Looks like:  ((bucket0, 60, bucket1, 120, bucket2), firstbucket)
        # The next block is still verifying preconditions.
        self.assertEqual(len(state), 2)
        self.assertEqual(len(state[0]), 5)
        self.assertEqual(state[0][1], 60)
        self.assertEqual(state[0][3], 120)

        # Conflict resolution empties bucket1 entirely.  This used to
        # create an "insane" BTree (a legit BTree cannot contain an empty
        # bucket -- it contains NULL pointers the BTree code doesn't
        # expect, and segfaults result).
        self.assertRaises(ConflictError, tm2.commit)


    def testEmptyBucketNoConflict(self):
        # Tests that a plain empty bucket (on input) is not viewed as a
        # conflict.
        b = self.t
        for i in range(0, 200, 4):
            b[i] = i
        # bucket 0 has 15 values: 0, 4 .. 56
        # bucket 1 has 15 values: 60, 64 .. 116
        # bucket 2 has 20 values: 120, 124 .. 196
        state = b.__getstate__()
        # Looks like:  ((bucket0, 60, bucket1, 120, bucket2), firstbucket)
        # If these fail, the *preconditions* for running the test aren't
        # satisfied -- the test itself hasn't been run yet.
        self.assertEqual(len(state), 2)
        self.assertEqual(len(state[0]), 5)
        self.assertEqual(state[0][1], 60)
        self.assertEqual(state[0][3], 120)

        # Invoke conflict resolution by committing a transaction.
        self.openDB()

        r1 = self.db.open().root()
        r1["t"] = self.t
        transaction.commit()

        r2 = self.db.open().root()
        copy = r2["t"]
        # Make sure all of copy is loaded.
        list(copy.values())

        self.assertEqual(self.t._p_serial, copy._p_serial)

        # In one transaction, just add a key.
        b = self.t
        b[1] = 1
        # bucket 0 has 16 values: [0, 1] + [4, 8 .. 56]
        # bucket 1 has 15 values: 60, 64 .. 116
        # bucket 2 has 20 values: 120, 124 .. 196
        state = b.__getstate__()
        # Looks like:  ((bucket0, 60, bucket1, 120, bucket2), firstbucket)
        # The next block is still verifying preconditions.
        self.assertEqual(len(state), 2)
        self.assertEqual(len(state[0]), 5)
        self.assertEqual(state[0][1], 60)
        self.assertEqual(state[0][3], 120)

        transaction.commit()

        # In the other transaction, delete bucket 2.
        b = copy
        for k in range(120, 200, 4):
            del b[k]
        # bucket 0 has 15 values: 0, 4 .. 56
        # bucket 1 has 15 values: 60, 64 .. 116
        state = b.__getstate__()
        # Looks like:  ((bucket0, 60, bucket1), firstbucket)
        # The next block is still verifying preconditions.
        self.assertEqual(len(state), 2)
        self.assertEqual(len(state[0]), 3)
        self.assertEqual(state[0][1], 60)

        # This shouldn't create a ConflictError.
        transaction.commit()
        # And the resulting BTree shouldn't have internal damage.
        b._check()

    # The snaky control flow in _bucket__p_resolveConflict ended up trying
    # to decref a NULL pointer if conflict resolution was fed 3 empty
    # buckets.  http://collector.zope.org/Zope/553
    def testThreeEmptyBucketsNoSegfault(self):
        self.openDB()

        tm1 = transaction.TransactionManager()
        r1 = self.db.open(transaction_manager=tm1).root()
        self.assertEqual(len(self.t), 0)
        r1["t"] = b = self.t  # an empty tree
        tm1.commit()

        tm2 = transaction.TransactionManager()
        r2 = self.db.open(transaction_manager=tm2).root()
        copy = r2["t"]
        # Make sure all of copy is loaded.
        list(copy.values())

        # In one transaction, add and delete a key.
        b[2] = 2
        del b[2]
        tm1.commit()

        # In the other transaction, also add and delete a key.
        b = copy
        b[1] = 1
        del b[1]
        # If the commit() segfaults, the C code is still wrong for this case.
        self.assertRaises(ConflictError, tm2.commit)

    def testCantResolveBTreeConflict(self):
        # Test that a conflict involving two different changes to
        # an internal BTree node is unresolvable.  An internal node
        # only changes when there are enough additions or deletions
        # to a child bucket that the bucket is split or removed.
        # It's (almost necessarily) a white-box test, and sensitive to
        # implementation details.
        b = self.t
        for i in range(0, 200, 4):
            b[i] = i
        # bucket 0 has 15 values: 0, 4 .. 56
        # bucket 1 has 15 values: 60, 64 .. 116
        # bucket 2 has 20 values: 120, 124 .. 196
        state = b.__getstate__()
        # Looks like:  ((bucket0, 60, bucket1, 120, bucket2), firstbucket)
        # If these fail, the *preconditions* for running the test aren't
        # satisfied -- the test itself hasn't been run yet.
        self.assertEqual(len(state), 2)
        self.assertEqual(len(state[0]), 5)
        self.assertEqual(state[0][1], 60)
        self.assertEqual(state[0][3], 120)

        # Set up database connections to provoke conflict.
        self.openDB()
        tm1 = transaction.TransactionManager()
        r1 = self.db.open(transaction_manager=tm1).root()
        r1["t"] = self.t
        tm1.commit()

        tm2 = transaction.TransactionManager()
        r2 = self.db.open(transaction_manager=tm2).root()
        copy = r2["t"]
        # Make sure all of copy is loaded.
        list(copy.values())

        self.assertEqual(self.t._p_serial, copy._p_serial)

        # Now one transaction should add enough keys to cause a split,
        # and another should remove all the keys in one bucket.

        for k in range(200, 300, 4):
            self.t[k] = k
        tm1.commit()

        for k in range(0, 60, 4):
            del copy[k]

        try:
            tm2.commit()
        except ConflictError, detail:
            self.assert_(str(detail).startswith('database conflict error'))
        else:
            self.fail("expected ConflictError")

    def testConflictWithOneEmptyBucket(self):
        # If one transaction empties a bucket, while another adds an item
        # to the bucket, all the changes "look resolvable":  bucket conflict
        # resolution returns a bucket containing (only) the item added by
        # the latter transaction, but changes from the former transaction
        # removing the bucket are uncontested:  the bucket is removed from
        # the BTree despite that resolution thinks it's non-empty!  This
        # was first reported by Dieter Maurer, to zodb-dev on 22 Mar 2005.
        b = self.t
        for i in range(0, 200, 4):
            b[i] = i
        # bucket 0 has 15 values: 0, 4 .. 56
        # bucket 1 has 15 values: 60, 64 .. 116
        # bucket 2 has 20 values: 120, 124 .. 196
        state = b.__getstate__()
        # Looks like:  ((bucket0, 60, bucket1, 120, bucket2), firstbucket)
        # If these fail, the *preconditions* for running the test aren't
        # satisfied -- the test itself hasn't been run yet.
        self.assertEqual(len(state), 2)
        self.assertEqual(len(state[0]), 5)
        self.assertEqual(state[0][1], 60)
        self.assertEqual(state[0][3], 120)

        # Set up database connections to provoke conflict.
        self.openDB()
        tm1 = transaction.TransactionManager()
        r1 = self.db.open(transaction_manager=tm1).root()
        r1["t"] = self.t
        tm1.commit()

        tm2 = transaction.TransactionManager()
        r2 = self.db.open(transaction_manager=tm2).root()
        copy = r2["t"]
        # Make sure all of copy is loaded.
        list(copy.values())

        self.assertEqual(self.t._p_serial, copy._p_serial)

        # Now one transaction empties the first bucket, and another adds a
        # key to the first bucket.

        for k in range(0, 60, 4):
            del self.t[k]
        tm1.commit()

        copy[1] = 1

        try:
            tm2.commit()
        except ConflictError, detail:
            self.assert_(str(detail).startswith('database conflict error'))
        else:
            self.fail("expected ConflictError")

        # Same thing, except commit the transactions in the opposite order.
        b = OOBTree()
        for i in range(0, 200, 4):
            b[i] = i

        tm1 = transaction.TransactionManager()
        r1 = self.db.open(transaction_manager=tm1).root()
        r1["t"] = b
        tm1.commit()

        tm2 = transaction.TransactionManager()
        r2 = self.db.open(transaction_manager=tm2).root()
        copy = r2["t"]
        # Make sure all of copy is loaded.
        list(copy.values())

        self.assertEqual(b._p_serial, copy._p_serial)

        # Now one transaction empties the first bucket, and another adds a
        # key to the first bucket.
        b[1] = 1
        tm1.commit()

        for k in range(0, 60, 4):
            del copy[k]
        try:
            tm2.commit()
        except ConflictError, detail:
            self.assert_(str(detail).startswith('database conflict error'))
        else:
            self.fail("expected ConflictError")

    def testConflictOfInsertAndDeleteOfFirstBucketItem(self):
        """
        Recently, BTrees became careful about removing internal keys
        (keys in internal aka BTree nodes) when they were deleted from
        buckets. This poses a problem for conflict resolution.

        We want to guard against a case in which the first key in a
        bucket is removed in one transaction while a key is added
        after that key but before the next key in another transaction
        with the result that the added key is unreachble

        original:

          Bucket(...), k1, Bucket((k1, v1), (k3, v3), ...)

        tran1

          Bucket(...), k3, Bucket(k3, v3), ...)

        tran2

          Bucket(...), k1, Bucket((k1, v1), (k2, v2), (k3, v3), ...)

          where k1 < k2 < k3

        We don't want:

          Bucket(...), k3, Bucket((k2, v2), (k3, v3), ...)

          as k2 would be unfindable, so we want a conflict.

        """
        mytype = self.t_type
        db = self.openDB()
        tm1 = transaction.TransactionManager()
        conn1 = db.open(tm1)
        conn1.root.t = t = mytype()
        for i in range(0, 200, 2):
            t[i] = i
        tm1.commit()
        k = t.__getstate__()[0][1]
        assert t.__getstate__()[0][2].keys()[0] == k

        tm2 = transaction.TransactionManager()
        conn2 = db.open(tm2)

        t[k+1] = k+1
        del conn2.root.t[k]
        for i in range(200,300):
            conn2.root.t[i] = i

        tm1.commit()
        self.assertRaises(ConflictError, tm2.commit)
        tm2.abort()

        k = t.__getstate__()[0][1]
        t[k+1] = k+1
        del conn2.root.t[k]

        tm2.commit()
        self.assertRaises(ConflictError, tm1.commit)
        tm1.abort()

def test_suite():
    suite = TestSuite()

    for kv in ('OO',
               'II', 'IO', 'OI', 'IF',
               'LL', 'LO', 'OL', 'LF',
               ):
        for name, bases in (('BTree', (MappingBase, TestCase)),
                            ('Bucket', (MappingBase, TestCase)),
                            ('TreeSet', (SetTests, TestCase)),
                            ('Set', (SetTests, TestCase)),
                            ):
            klass = ClassType(kv + name + 'Test', bases,
                              dict(t_type=globals()[kv+name]))
            suite.addTest(makeSuite(klass))
    
    suite.addTest(makeSuite(NastyConfict))
    return suite
