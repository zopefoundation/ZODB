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
        input = IISet(range(50000))
        reversed = range(50000)
        reversed.reverse()
        reversed = IISet(reversed)
        output = multiunion([input, reversed] * 5)
        self.assertEqual(len(output), 50000)
        self.assertEqual(list(output), range(50000))

    def testLotsOfLittleOnes:
        from random import shuffle
        N = 5000
        inputs = []
        for i in range(N):
            base = i * 4 - N
            inputs.append(IISet([base, base+1]))
            inputs.append(IITreeSet([base+2, base+3]))
        inputs.shuffle()
        output = multiunion(inputs)
        self.assertEqual(len(output), N*4)
        self.assertEqual(list(output), range(-N, 3*N))

def test_suite():
    alltests = TestSuite((makeSuite(TestMultiUnion, 'test'),
                        ))
    return alltests

def main():
  TextTestRunner().run(test_suite())

if __name__ == '__main__':
    main()
