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
"""Test the BTree check.check() function."""

import unittest

from BTrees.OOBTree import OOBTree
from BTrees.check import check

class CheckTest(unittest.TestCase):

    def setUp(self):
        self.t = t = OOBTree()
        for i in range(31):
            t[i] = 2*i
        self.state = t.__getstate__()

    def testNormal(self):
        s = self.state
        # Looks like (state, first_bucket)
        # where state looks like (bucket0, 15, bucket1).
        self.assertEqual(len(s), 2)
        self.assertEqual(len(s[0]), 3)
        self.assertEqual(s[0][1], 15)
        self.t._check() # shouldn't blow up
        check(self.t)   # shouldn't blow up

    def testKeyTooLarge(self):
        # Damage an invariant by dropping the BTree key to 14.
        s = self.state
        news = (s[0][0], 14, s[0][2]), s[1]
        self.t.__setstate__(news)
        self.t._check() # not caught
        try:
            # Expecting "... key %r >= upper bound %r at index %d"
            check(self.t)
        except AssertionError, detail:
            self.failUnless(str(detail).find(">= upper bound") > 0)
        else:
            self.fail("expected self.t_check() to catch the problem")

    def testKeyTooSmall(self):
        # Damage an invariant by bumping the BTree key to 16.
        s = self.state
        news = (s[0][0], 16, s[0][2]), s[1]
        self.t.__setstate__(news)
        self.t._check() # not caught
        try:
            # Expecting "... key %r < lower bound %r at index %d"
            check(self.t)
        except AssertionError, detail:
            self.failUnless(str(detail).find("< lower bound") > 0)
        else:
            self.fail("expected self.t_check() to catch the problem")

    def testKeysSwapped(self):
        # Damage an invariant by swapping two key/value pairs.
        s = self.state
        # Looks like (state, first_bucket)
        # where state looks like (bucket0, 15, bucket1).
        (b0, num, b1), firstbucket = s
        self.assertEqual(b0[4], 8)
        self.assertEqual(b0[5], 10)
        b0state = b0.__getstate__()
        self.assertEqual(len(b0state), 2)
        # b0state looks like
        # ((k0, v0, k1, v1, ...), nextbucket)
        pairs, nextbucket = b0state
        self.assertEqual(pairs[8], 4)
        self.assertEqual(pairs[9], 8)
        self.assertEqual(pairs[10], 5)
        self.assertEqual(pairs[11], 10)
        newpairs = pairs[:8] + (5, 10, 4, 8) + pairs[12:]
        b0.__setstate__((newpairs, nextbucket))
        self.t._check() # not caught
        try:
            check(self.t)
        except AssertionError, detail:
            self.failUnless(str(detail).find(
                "key 5 at index 4 >= key 4 at index 5") > 0)
        else:
            self.fail("expected self.t_check() to catch the problem")

def test_suite():
    return unittest.makeSuite(CheckTest)
