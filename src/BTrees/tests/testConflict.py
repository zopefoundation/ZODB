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
# FOR A PARTICULAR PURPOSE
#
##############################################################################
import os
from unittest import TestCase, TestSuite, makeSuite

from BTrees.OOBTree import OOBTree, OOBucket, OOSet, OOTreeSet
from BTrees.IOBTree import IOBTree, IOBucket, IOSet, IOTreeSet
from BTrees.IIBTree import IIBTree, IIBucket, IISet, IITreeSet
from BTrees.OIBTree import OIBTree, OIBucket, OISet, OITreeSet

from ZODB.POSException import ConflictError

class Base:
    """ Tests common to all types: sets, buckets, and BTrees """

    storage = None

    def tearDown(self):
        get_transaction().abort()
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
        get_transaction().commit()

        r2 = self.db.open().root()
        copy = r2["t"]
        list(copy)    # unghostify

        self.assertEqual(self.t._p_serial, copy._p_serial)

        self.t.update({1:2, 2:3})
        get_transaction().commit()

        copy.update({3:4})
        get_transaction().commit()


    def testMergeDelete(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()
        del b1[items[0][0]]
        del b2[items[5][0]]
        del b1[items[-1][0]]
        del b2[items[-2][0]]
        del bm[items[0][0]]
        del bm[items[5][0]]
        del bm[items[-1][0]]
        del bm[items[-2][0]]
        test_merge(base, b1, b2, bm, 'merge  delete')

    def testMergeDeleteAndUpdate(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()
        del b1[items[0][0]]
        b2[items[5][0]]=1
        del b1[items[-1][0]]
        b2[items[-2][0]]=2
        del bm[items[0][0]]
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

    def testMergeEmptyAndFill(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()

        b1.clear()
        bm.clear()
        b2.update(e2)
        bm.update(e2)

        test_merge(base, b1, b2, bm, 'merge insert from empty')

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
        b1.remove(items[0])
        b2.remove(items[5])
        b1.remove(items[-1])
        b2.remove(items[-2])
        bm.remove(items[0])
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

    def testMergeEmptyAndFill(self):
        base, b1, b2, bm, e1, e2, items = self._setupConflict()

        b1.clear()
        bm.clear()
        b2.update(e2)
        bm.update(e2)

        test_merge(base, b1, b2, bm, 'merge insert from empty')

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

class BucketTests(MappingBase):
    """ Tests common to all buckets """

class BTreeTests(MappingBase):
    """ Tests common to all BTrees """

## BTree tests

class TestIOBTrees(BTreeTests, TestCase):
    def setUp(self):
        self.t = IOBTree()

class TestOOBTrees(BTreeTests, TestCase):
    def setUp(self):
        self.t = OOBTree()

class TestOIBTrees(BTreeTests, TestCase):
    def setUp(self):
        self.t = OIBTree()

class TestIIBTrees(BTreeTests, TestCase):
    def setUp(self):
        self.t = IIBTree()

## Set tests

class TestIOSets(SetTests, TestCase):
    def setUp(self):
        self.t = IOSet()

class TestOOSets(SetTests, TestCase):
    def setUp(self):
        self.t = OOSet()

class TestIISets(SetTests, TestCase):
    def setUp(self):
        self.t = IISet()

class TestOISets(SetTests, TestCase):
    def setUp(self):
        self.t = OISet()

class TestIOTreeSets(SetTests, TestCase):
    def setUp(self):
        self.t = IOTreeSet()

class TestOOTreeSets(SetTests, TestCase):
    def setUp(self):
        self.t = OOTreeSet()

class TestIITreeSets(SetTests, TestCase):
    def setUp(self):
        self.t = IITreeSet()

class TestOITreeSets(SetTests, TestCase):
    def setUp(self):
        self.t = OITreeSet()

## Bucket tests

class TestIOBuckets(BucketTests, TestCase):
    def setUp(self):
        self.t = IOBucket()

class TestOOBuckets(BucketTests, TestCase):
    def setUp(self):
        self.t = OOBucket()

class TestIIBuckets(BucketTests, TestCase):
    def setUp(self):
        self.t = IIBucket()

class TestOIBuckets(BucketTests, TestCase):
    def setUp(self):
        self.t = OIBucket()

class NastyConfict(Base, TestCase):
    def setUp(self):
        self.t = OOBTree()

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
        get_transaction().commit()

        r2 = self.db.open().root()
        copy = r2["t"]
        # Make sure all of copy is loaded.
        list(copy.values())

        self.assertEqual(self.t._p_serial, copy._p_serial)

        self.t.update({1:2, 2:3})
        get_transaction().commit()

        copy.update({3:4})
        get_transaction().commit()  # if this doesn't blow up
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

        r1 = self.db.open().root()
        r1["t"] = self.t
        get_transaction().commit()

        r2 = self.db.open().root()
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

        get_transaction().commit()

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

        self.assertRaises(ConflictError, get_transaction().commit)
        get_transaction().abort()   # horrible things happen w/o this

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

        r1 = self.db.open().root()
        r1["t"] = self.t
        get_transaction().commit()

        r2 = self.db.open().root()
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
        self.assertEqual(state[0][1], 60)
        self.assertEqual(state[0][3], 120)

        get_transaction().commit()

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
        self.assertRaises(ConflictError, get_transaction().commit)
        get_transaction().abort()   # horrible things happen w/o this


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
        get_transaction().commit()

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

        get_transaction().commit()

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
        get_transaction().commit()
        # And the resulting BTree shouldn't have internal damage.
        b._check()

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
        r1 = self.db.open().root()
        r1["t"] = self.t
        get_transaction().commit()

        r2 = self.db.open().root()
        copy = r2["t"]
        # Make sure all of copy is loaded.
        list(copy.values())

        self.assertEqual(self.t._p_serial, copy._p_serial)

        # Now one transaction should add enough keys to cause a split,
        # and another should remove all the keys in one bucket.

        for k in range(200, 300, 4):
            self.t[k] = k
        get_transaction().commit()

        for k in range(0, 60, 4):
            del copy[k]

        try:
            get_transaction().commit()
        except ConflictError, detail:
            self.assert_(str(detail).startswith('database conflict error'))
            get_transaction().abort()
        else:
            self.fail("expected ConflictError")


def test_suite():
    suite = TestSuite()
    for k in (TestIOBTrees,   TestOOBTrees,   TestOIBTrees,   TestIIBTrees,
              TestIOSets,     TestOOSets,     TestOISets,     TestIISets,
              TestIOTreeSets, TestOOTreeSets, TestOITreeSets, TestIITreeSets,
              TestIOBuckets,  TestOOBuckets,  TestOIBuckets,  TestIIBuckets,
              NastyConfict):
        suite.addTest(makeSuite(k))
    return suite

