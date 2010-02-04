##############################################################################
#
# Copyright (c) 2008 Zope Corporation and Contributors.
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
"""\
Test for BTrees.Length module.

"""
__docformat__ = "reStructuredText"

import BTrees.Length
import copy
import sys
import unittest


class LengthTestCase(unittest.TestCase):

    def test_length_overflows_to_long(self):
        length = BTrees.Length.Length(sys.maxint)
        self.assertEqual(length(), sys.maxint)
        self.assert_(type(length()) is int)
        length.change(+1)
        self.assertEqual(length(), sys.maxint + 1)
        self.assert_(type(length()) is long)

    def test_length_underflows_to_long(self):
        minint = (-sys.maxint) - 1
        length = BTrees.Length.Length(minint)
        self.assertEqual(length(), minint)
        self.assert_(type(length()) is int)
        length.change(-1)
        self.assertEqual(length(), minint - 1)
        self.assert_(type(length()) is long)

    def test_copy(self):
        # Test for https://bugs.launchpad.net/zodb/+bug/516653
        length = BTrees.Length.Length()
        other = copy.copy(length)
        self.assertEqual(other(), 0)


def test_suite():
    return unittest.makeSuite(LengthTestCase)
