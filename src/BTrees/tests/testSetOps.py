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

from BTrees.IIBTree import multiunion

# XXX TODO Needs more tests.
# This file was created when multiunion was added.  The other set operations
# don't appear to be tested anywhere yet.

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

class TestMultiUnion(TestCase):

    def testEmpty(self):
        self.assertEqual(len(multiunion([])), 0)

    def testOne(self):
        for sequence in [3], range(20), range(-10, 0, 2) + range(1, 10, 2):
            seq1 = sequence[:]
            seq2 = sequence[:]
            seq2.reverse()
            seqsorted = sequence[:]
            seqsorted.sort()
            for seq in seq1, seq2, seqsorted:
                for builder in IISet, IITreeSet:
                    input = builder(seq)
                    output = multiunion([input])
                    self.assertEqual(len(seq), len(output))
                    self.assertEqual(seqsorted, list(output))

    def testValuesIgnored(self):
        for builder in IIBucket, IIBTree:
            input = builder([(1, 2), (3, 4), (5, 6)])
            output = multiunion([input])
            self.assertEqual([1, 3, 5], list(output))

    def testBigInput(self):
        N = 100000
        input = IISet(range(N))
        output = multiunion([input] * 10)
        self.assertEqual(len(output), N)
        self.assertEqual(output.minKey(), 0)
        self.assertEqual(output.maxKey(), N-1)
        self.assertEqual(list(output), range(N))

    def testLotsOfLittleOnes(self):
        from random import shuffle
        N = 5000
        inputs = []
        for i in range(N):
            base = i * 4 - N
            inputs.append(IISet([base, base+1]))
            inputs.append(IITreeSet([base+2, base+3]))
        shuffle(inputs)
        output = multiunion(inputs)
        self.assertEqual(len(output), N*4)
        self.assertEqual(list(output), range(-N, 3*N))

    def testFunkyKeyIteration(self):
        # The internal set iteration protocol allows "iterating over" a
        # a single key as if it were a set.
        from BTrees.IIBTree import union
        N = 100
        slow = IISet()
        for i in range(N):
            slow = union(slow, IISet([i]))
        fast = multiunion(range(N))  # acts like N distinct singleton sets
        self.assertEqual(len(slow), N)
        self.assertEqual(len(fast), N)
        self.assertEqual(list(slow.keys()), list(fast.keys()))
        self.assertEqual(list(fast.keys()), range(N))

def test_suite():
    s = TestSuite()
    for klass in (TestMultiUnion,
                  PureII, PureIO, PureOI, PureOO):
        s.addTest(makeSuite(klass))
    return s

def main():
    TextTestRunner().run(test_suite())

if __name__ == '__main__':
    main()
