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
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Test the routines to convert between long and 64-bit strings"""

# originally zodb.tests.test_utils

import random
import unittest

NUM = 100

from ZODB.zodb4.z4utils import p64, u64

class TestUtils(unittest.TestCase):

    small = [random.randrange(1, 1L<<32, int=long)
             for i in range(NUM)]
    large = [random.randrange(1L<<32, 1L<<64, int=long)
             for i in range(NUM)]
    all = small + large

    def test_LongToStringToLong(self):
        for num in self.all:
            s = p64(num)
            n2 = u64(s)
            self.assertEquals(num, n2, "u64() failed")

    def test_KnownConstants(self):
        self.assertEquals("\000\000\000\000\000\000\000\001", p64(1))
        self.assertEquals("\000\000\000\001\000\000\000\000", p64(1L<<32))
        self.assertEquals(u64("\000\000\000\000\000\000\000\001"), 1)
        self.assertEquals(u64("\000\000\000\001\000\000\000\000"), 1L<<32)

def test_suite():
    return unittest.makeSuite(TestUtils)

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
