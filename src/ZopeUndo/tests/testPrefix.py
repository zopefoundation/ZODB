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
from ZopeUndo.Prefix import Prefix

import unittest

class PrefixTest(unittest.TestCase):

    def test(self):
        p1 = Prefix("/a/b")
        for equal in ("/a/b", "/a/b/c", "/a/b/c/d"):
            self.assertEqual(p1, equal)
        for notEqual in ("", "/a/c", "/a/bbb", "///"):
            self.assertNotEqual(p1, notEqual)

        p2 = Prefix("")
        for equal in ("", "/", "/def", "/a/b", "/a/b/c", "/a/b/c/d"):
            self.assertEqual(p2, equal)

def test_suite():
    return unittest.makeSuite(PrefixTest)
