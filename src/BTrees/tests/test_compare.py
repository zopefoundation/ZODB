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
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Test errors during comparison of BTree keys."""

import unittest

from BTrees.OOBTree import OOBucket as Bucket, OOSet as Set

import transaction
from ZODB.MappingStorage import MappingStorage
from ZODB.DB import DB

class CompareTest(unittest.TestCase):

    s = "A string with hi-bit-set characters: \700\701"
    u = u"A unicode string"

    def setUp(self):
        # These defaults only make sense if the default encoding
        # prevents s from being promoted to Unicode.
        self.assertRaises(UnicodeError, unicode, self.s)

        # An object needs to be added to the database to
        self.db = DB(MappingStorage())
        root = self.db.open().root()
        self.bucket = root["bucket"] = Bucket()
        self.set = root["set"] = Set()
        transaction.commit()

    def tearDown(self):
        self.assert_(self.bucket._p_changed != 2)
        self.assert_(self.set._p_changed != 2)

    def assertUE(self, callable, *args):
        self.assertRaises(UnicodeError, callable, *args)

    def testBucketGet(self):
        self.bucket[self.s] = 1
        self.assertUE(self.bucket.get, self.u)

    def testSetGet(self):
        self.set.insert(self.s)
        self.assertUE(self.set.remove, self.u)

    def testBucketSet(self):
        self.bucket[self.s] = 1
        self.assertUE(self.bucket.__setitem__, self.u, 1)

    def testSetSet(self):
        self.set.insert(self.s)
        self.assertUE(self.set.insert, self.u)

    def testBucketMinKey(self):
        self.bucket[self.s] = 1
        self.assertUE(self.bucket.minKey, self.u)

    def testSetMinKey(self):
        self.set.insert(self.s)
        self.assertUE(self.set.minKey, self.u)

def test_suite():
    return unittest.makeSuite(CompareTest)
