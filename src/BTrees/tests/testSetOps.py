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
import sys, os, time, random
from unittest import TestCase, TestSuite, TextTestRunner, makeSuite

from BTrees.IIBTree import IIBTree, IIBucket, IISet, IITreeSet, \
    union, intersection, difference, weightedUnion, weightedIntersection, \
    multiunion

# XXX TODO Needs more tests.
# This file was created when multiunion was added.  The other set operations
# don't appear to be tested anywhere yet.

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
    return makeSuite(TestMultiUnion, 'test')

def main():
  TextTestRunner().run(test_suite())

if __name__ == '__main__':
    main()
