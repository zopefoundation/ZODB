##############################################################################
#
# Copyright (c) 2002 Zope Corporation and Contributors.
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
import random
from unittest import TestCase, TestSuite, TextTestRunner, makeSuite

from BTrees.OOBTree import OOBTree, OOBucket, OOSet, OOTreeSet
from BTrees.IOBTree import IOBTree, IOBucket, IOSet, IOTreeSet
from BTrees.IIBTree import IIBTree, IIBucket, IISet, IITreeSet
from BTrees.OIBTree import OIBTree, OIBucket, OISet, OITreeSet

# Subclasses have to set up:
#     builders - functions to build inputs, taking an optional keys arg
#     intersection, union, difference - set to the type-correct versions
class SetResult(TestCase):
    def setUp(self):
        self.Akeys = [1,    3,    5, 6   ]
        self.Bkeys = [   2, 3, 4,    6, 7]
        self.As = [makeset(self.Akeys) for makeset in self.builders]
        self.Bs = [makeset(self.Bkeys) for makeset in self.builders]
        self.emptys = [makeset() for makeset in self.builders]

    # Slow but obviously correct Python implementations of basic ops.
    def _union(self, x, y):
        result = list(x.keys())
        for e in y.keys():
            if e not in result:
                result.append(e)
        result.sort()
        return result

    def _intersection(self, x, y):
        result = []
        ykeys = y.keys()
        for e in x.keys():
            if e in ykeys:
                result.append(e)
        return result

    def _difference(self, x, y):
        result = list(x.keys())
        for e in y.keys():
            if e in result:
                result.remove(e)
        # Difference preserves LHS values.
        if hasattr(x, "values"):
            result = [(k, x[k]) for k in result]
        return result

    def testNone(self):
        for op in self.union, self.intersection, self.difference:
            C = op(None, None)
            self.assert_(C is None)

        for op in self.union, self.intersection, self.difference:
            for A in self.As:
                C = op(A, None)
                self.assert_(C is A)

                C = op(None, A)
                if op is self.difference:
                    self.assert_(C is None)
                else:
                    self.assert_(C is A)

    def testEmptyUnion(self):
        for A in self.As:
            for E in self.emptys:
                C = self.union(A, E)
                self.assert_(not hasattr(C, "values"))
                self.assertEqual(list(C), self.Akeys)

                C = self.union(E, A)
                self.assert_(not hasattr(C, "values"))
                self.assertEqual(list(C), self.Akeys)

    def testEmptyIntersection(self):
        for A in self.As:
            for E in self.emptys:
                C = self.intersection(A, E)
                self.assert_(not hasattr(C, "values"))
                self.assertEqual(list(C), [])

                C = self.intersection(E, A)
                self.assert_(not hasattr(C, "values"))
                self.assertEqual(list(C), [])

    def testEmptyDifference(self):
        for A in self.As:
            for E in self.emptys:
                C = self.difference(A, E)
                # Difference preserves LHS values.
                self.assertEqual(hasattr(C, "values"), hasattr(A, "values"))
                if hasattr(A, "values"):
                    self.assertEqual(list(C.items()), list(A.items()))
                else:
                    self.assertEqual(list(C), self.Akeys)

                C = self.difference(E, A)
                self.assertEqual(hasattr(C, "values"), hasattr(E, "values"))
                self.assertEqual(list(C.keys()), [])

    def testUnion(self):
        inputs = self.As + self.Bs
        for A in inputs:
            for B in inputs:
                C = self.union(A, B)
                self.assert_(not hasattr(C, "values"))
                self.assertEqual(list(C), self._union(A, B))

    def testIntersection(self):
        inputs = self.As + self.Bs
        for A in inputs:
            for B in inputs:
                C = self.intersection(A, B)
                self.assert_(not hasattr(C, "values"))
                self.assertEqual(list(C), self._intersection(A, B))

    def testDifference(self):
        inputs = self.As + self.Bs
        for A in inputs:
            for B in inputs:
                C = self.difference(A, B)
                # Difference preserves LHS values.
                self.assertEqual(hasattr(C, "values"), hasattr(A, "values"))
                want = self._difference(A, B)
                if hasattr(A, "values"):
                    self.assertEqual(list(C.items()), want)
                else:
                    self.assertEqual(list(C), want)

    def testLargerInputs(self):
        from random import randint
        MAXSIZE = 200
        MAXVAL = 400
        for i in range(3):
            n = randint(0, MAXSIZE)
            Akeys = [randint(1, MAXVAL) for j in range(n)]
            As = [makeset(Akeys) for makeset in self.builders]
            Akeys = IISet(Akeys)

            n = randint(0, MAXSIZE)
            Bkeys = [randint(1, MAXVAL) for j in range(n)]
            Bs = [makeset(Bkeys) for makeset in self.builders]
            Bkeys = IISet(Bkeys)

            for op, simulator in ((self.union, self._union),
                                  (self.intersection, self._intersection),
                                  (self.difference, self._difference)):
                for A in As:
                    for B in Bs:
                        got = op(A, B)
                        want = simulator(Akeys, Bkeys)
                        self.assertEqual(list(got.keys()), want,
                                         (A, B,
                                          Akeys, Bkeys,
                                          list(got.keys()), want))

# Given a mapping builder (IIBTree, OOBucket, etc), return a function
# that builds an object of that type given only a list of keys.
def makeBuilder(mapbuilder):
    def result(keys=[], mapbuilder=mapbuilder):
        return mapbuilder(zip(keys, keys))
    return result

class PureII(SetResult):
    from BTrees.IIBTree import union, intersection, difference
    builders = IISet, IITreeSet, makeBuilder(IIBTree), makeBuilder(IIBucket)

class PureIO(SetResult):
    from BTrees.IOBTree import union, intersection, difference
    builders = IOSet, IOTreeSet, makeBuilder(IOBTree), makeBuilder(IOBucket)

class PureOO(SetResult):
    from BTrees.OOBTree import union, intersection, difference
    builders = OOSet, OOTreeSet, makeBuilder(OOBTree), makeBuilder(OOBucket)

class PureOI(SetResult):
    from BTrees.OIBTree import union, intersection, difference
    builders = OISet, OITreeSet, makeBuilder(OIBTree), makeBuilder(OIBucket)

# Subclasses must set up (as class variables):
#     multiunion, union
#     mkset, mktreeset
#     mkbucket, mkbtree
class MultiUnion(TestCase):

    def testEmpty(self):
        self.assertEqual(len(self.multiunion([])), 0)

    def testOne(self):
        for sequence in [3], range(20), range(-10, 0, 2) + range(1, 10, 2):
            seq1 = sequence[:]
            seq2 = sequence[:]
            seq2.reverse()
            seqsorted = sequence[:]
            seqsorted.sort()
            for seq in seq1, seq2, seqsorted:
                for builder in self.mkset, self.mktreeset:
                    input = builder(seq)
                    output = self.multiunion([input])
                    self.assertEqual(len(seq), len(output))
                    self.assertEqual(seqsorted, list(output))

    def testValuesIgnored(self):
        for builder in self.mkbucket, self.mkbtree:
            input = builder([(1, 2), (3, 4), (5, 6)])
            output = self.multiunion([input])
            self.assertEqual([1, 3, 5], list(output))

    def testBigInput(self):
        N = 100000
        input = self.mkset(range(N))
        output = self.multiunion([input] * 10)
        self.assertEqual(len(output), N)
        self.assertEqual(output.minKey(), 0)
        self.assertEqual(output.maxKey(), N-1)
        self.assertEqual(list(output), range(N))

    def testLotsOfLittleOnes(self):
        from random import shuffle
        N = 5000
        inputs = []
        mkset, mktreeset = self.mkset, self.mktreeset
        for i in range(N):
            base = i * 4 - N
            inputs.append(mkset([base, base+1]))
            inputs.append(mktreeset([base+2, base+3]))
        shuffle(inputs)
        output = self.multiunion(inputs)
        self.assertEqual(len(output), N*4)
        self.assertEqual(list(output), range(-N, 3*N))

    def testFunkyKeyIteration(self):
        # The internal set iteration protocol allows "iterating over" a
        # a single key as if it were a set.
        N = 100
        union, mkset = self.union, self.mkset
        slow = mkset()
        for i in range(N):
            slow = union(slow, mkset([i]))
        fast = self.multiunion(range(N))  # acts like N distinct singleton sets
        self.assertEqual(len(slow), N)
        self.assertEqual(len(fast), N)
        self.assertEqual(list(slow.keys()), list(fast.keys()))
        self.assertEqual(list(fast.keys()), range(N))

class TestIIMultiUnion(MultiUnion):
    from BTrees.IIBTree import multiunion, union
    from BTrees.IIBTree import IISet as mkset, IITreeSet as mktreeset
    from BTrees.IIBTree import IIBucket as mkbucket, IIBTree as mkbtree

class TestIOMultiUnion(MultiUnion):
    from BTrees.IOBTree import multiunion, union
    from BTrees.IOBTree import IOSet as mkset, IOTreeSet as mktreeset
    from BTrees.IOBTree import IOBucket as mkbucket, IOBTree as mkbtree

# Check that various special module functions are and aren't imported from
# the expected BTree modules.
class TestImports(TestCase):
    def testWeightedUnion(self):
        from BTrees.IIBTree import weightedUnion
        from BTrees.OIBTree import weightedUnion

        try:
            from BTrees.IOBTree import weightedUnion
        except ImportError:
            pass
        else:
            self.fail("IOBTree shouldn't have weightedUnion")

        try:
            from BTrees.OOBTree import weightedUnion
        except ImportError:
            pass
        else:
            self.fail("OOBTree shouldn't have weightedUnion")

    def testWeightedIntersection(self):
        from BTrees.IIBTree import weightedIntersection
        from BTrees.OIBTree import weightedIntersection

        try:
            from BTrees.IOBTree import weightedIntersection
        except ImportError:
            pass
        else:
            self.fail("IOBTree shouldn't have weightedIntersection")

        try:
            from BTrees.OOBTree import weightedIntersection
        except ImportError:
            pass
        else:
            self.fail("OOBTree shouldn't have weightedIntersection")


    def testMultiunion(self):
        from BTrees.IIBTree import multiunion
        from BTrees.IOBTree import multiunion

        try:
            from BTrees.OIBTree import multiunion
        except ImportError:
            pass
        else:
            self.fail("OIBTree shouldn't have multiunion")

        try:
            from BTrees.OOBTree import multiunion
        except ImportError:
            pass
        else:
            self.fail("OOBTree shouldn't have multiunion")

# Subclasses must set up (as class variables):
#     weightedUnion, weightedIntersection
#     builders -- sequence of constructors, taking items
#     union, intersection -- the module routines of those names
#     mkbucket -- the module bucket builder
class Weighted(TestCase):

    def setUp(self):
        self.Aitems = [(1, 10), (3, 30),  (5, 50), (6, 60)]
        self.Bitems = [(2, 21), (3, 31), (4, 41),  (6, 61), (7, 71)]

        self.As = [make(self.Aitems) for make in self.builders]
        self.Bs = [make(self.Bitems) for make in self.builders]
        self.emptys = [make([]) for make in self.builders]

        weights = []
        for w1 in 0, 1, 7:  # -3, -1, 0, 1, 7:  XXX negative weights buggy
            for w2 in 0, 1, 7:  # -3, -1, 0, 1, 7:  XXX negative weights buggy
                weights.append((w1, w2))
        self.weights = weights

    def testBothNone(self):
        for op in self.weightedUnion, self.weightedIntersection:
            w, C = op(None, None)
            self.assert_(C is None)
            self.assertEqual(w, 0)

            w, C = op(None, None, 42, 666)
            self.assert_(C is None)
            self.assertEqual(w, 0)

    def testLeftNone(self):
        for op in self.weightedUnion, self.weightedIntersection:
            for A in self.As + self.emptys:
                w, C = op(None, A)
                self.assert_(C is A)
                self.assertEqual(w, 1)

                w, C = op(None, A, 42, 666)
                self.assert_(C is A)
                self.assertEqual(w, 666)

    def testRightNone(self):
        for op in self.weightedUnion, self.weightedIntersection:
            for A in self.As + self.emptys:
                w, C = op(A, None)
                self.assert_(C is A)
                self.assertEqual(w, 1)

                w, C = op(A, None, 42, 666)
                self.assert_(C is A)
                self.assertEqual(w, 42)

    # If obj is a set, return a bucket with values all 1; else return obj.
    def _normalize(self, obj):
        if isaset(obj):
            obj = self.mkbucket(zip(obj.keys(), [1] * len(obj)))
        return obj

    # Python simulation of weightedUnion.
    def _wunion(self, A, B, w1=1, w2=1):
        if isaset(A) and isaset(B):
            return 1, self.union(A, B).keys()
        A = self._normalize(A)
        B = self._normalize(B)
        result = []
        for key in self.union(A, B):
            v1 = A.get(key, 0)
            v2 = B.get(key, 0)
            result.append((key, v1*w1 + v2*w2))
        return 1, result

    def testUnion(self):
        inputs = self.As + self.Bs + self.emptys
        for A in inputs:
            for B in inputs:
                want_w, want_s = self._wunion(A, B)
                got_w, got_s = self.weightedUnion(A, B)
                self.assertEqual(got_w, want_w)
                if isaset(got_s):
                    self.assertEqual(got_s.keys(), want_s)
                else:
                    self.assertEqual(got_s.items(), want_s)

                for w1, w2 in self.weights:
                    want_w, want_s = self._wunion(A, B, w1, w2)
                    got_w, got_s = self.weightedUnion(A, B, w1, w2)
                    self.assertEqual(got_w, want_w)
                    if isaset(got_s):
                        self.assertEqual(got_s.keys(), want_s)
                    else:
                        self.assertEqual(got_s.items(), want_s)

    # Python simulation weightedIntersection.
    def _wintersection(self, A, B, w1=1, w2=1):
        if isaset(A) and isaset(B):
            return w1 + w2, self.intersection(A, B).keys()
        A = self._normalize(A)
        B = self._normalize(B)
        result = []
        for key in self.intersection(A, B):
            result.append((key, A[key]*w1 + B[key]*w2))
        return 1, result

    def testIntersection(self):
        inputs = self.As + self.Bs + self.emptys
        for A in inputs:
            for B in inputs:
                want_w, want_s = self._wintersection(A, B)
                got_w, got_s = self.weightedIntersection(A, B)
                self.assertEqual(got_w, want_w)
                if isaset(got_s):
                    self.assertEqual(got_s.keys(), want_s)
                else:
                    self.assertEqual(got_s.items(), want_s)

                for w1, w2 in self.weights:
                    want_w, want_s = self._wintersection(A, B, w1, w2)
                    got_w, got_s = self.weightedIntersection(A, B, w1, w2)
                    self.assertEqual(got_w, want_w)
                    if isaset(got_s):
                        self.assertEqual(got_s.keys(), want_s)
                    else:
                        self.assertEqual(got_s.items(), want_s)

# Given a set builder (like OITreeSet or OISet), return a function that
# takes a list of (key, value) pairs and builds a set out of the keys.
def itemsToSet(setbuilder):
    def result(items, setbuilder=setbuilder):
        return setbuilder([key for key, value in items])
    return result

# 'thing' is a bucket, btree, set or treeset.  Return true iff it's one of the
# latter two.
def isaset(thing):
    return not hasattr(thing, 'values')

class TestWeightedII(Weighted):
    from BTrees.IIBTree import weightedUnion, weightedIntersection
    from BTrees.IIBTree import union, intersection
    from BTrees.IIBTree import IIBucket as mkbucket
    builders = IIBucket, IIBTree, itemsToSet(IISet), itemsToSet(IITreeSet)

class TestWeightedOI(Weighted):
    from BTrees.OIBTree import weightedUnion, weightedIntersection
    from BTrees.OIBTree import union, intersection
    from BTrees.OIBTree import OIBucket as mkbucket
    builders = OIBucket, OIBTree, itemsToSet(OISet), itemsToSet(OITreeSet)


def test_suite():
    s = TestSuite()
    for klass in (TestIIMultiUnion, TestIOMultiUnion,
                  TestImports,
                  PureII, PureIO, PureOI, PureOO,
                  TestWeightedII, TestWeightedOI):
        s.addTest(makeSuite(klass))
    return s

def main():
    TextTestRunner().run(test_suite())

if __name__ == '__main__':
    main()
