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
from ZopeUndo.Prefix import Prefix

import unittest

class PrefixTest(unittest.TestCase):

    def test(self):
        p1 = (Prefix("/a/b"),
              ("/a/b", "/a/b/c", "/a/b/c/d"),
              ("", "/a/c"))

        p2 = (Prefix(""),
              ("", "/def", "/a/b", "/a/b/c", "/a/b/c/d"),
              ())

        for prefix, equal, notequal in p1, p2:
            for s in equal:
                self.assertEqual(prefix, s)
            for s in notequal:
                self.assertNotEqual(prefix, s)

def test_suite():
    return unittest.makeSuite(PrefixTest)
