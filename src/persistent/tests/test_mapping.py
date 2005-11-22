##############################################################################
#
# Copyright (c) 2005 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Test the mapping interface to PersistentMapping
"""

import unittest
from persistent.mapping import PersistentMapping

l0 = {}
l1 = {0:0}
l2 = {0:0, 1:1}

class TestPMapping(unittest.TestCase):

    def testTheWorld(self):
        # Test constructors
        u = PersistentMapping()
        u0 = PersistentMapping(l0)
        u1 = PersistentMapping(l1)
        u2 = PersistentMapping(l2)

        uu = PersistentMapping(u)
        uu0 = PersistentMapping(u0)
        uu1 = PersistentMapping(u1)
        uu2 = PersistentMapping(u2)

        class OtherMapping:
            def __init__(self, initmapping):
                self.__data = initmapping
            def items(self):
                return self.__data.items()
        v0 = PersistentMapping(OtherMapping(u0))
        vv = PersistentMapping([(0, 0), (1, 1)])

        # Test __repr__
        eq = self.assertEqual

        eq(str(u0), str(l0), "str(u0) == str(l0)")
        eq(repr(u1), repr(l1), "repr(u1) == repr(l1)")
        eq(`u2`, `l2`, "`u2` == `l2`")

        # Test __cmp__ and __len__

        def mycmp(a, b):
            r = cmp(a, b)
            if r < 0: return -1
            if r > 0: return 1
            return r

        all = [l0, l1, l2, u, u0, u1, u2, uu, uu0, uu1, uu2]
        for a in all:
            for b in all:
                eq(mycmp(a, b), mycmp(len(a), len(b)),
                      "mycmp(a, b) == mycmp(len(a), len(b))")

        # Test __getitem__

        for i in range(len(u2)):
            eq(u2[i], i, "u2[i] == i")

        # Test get

        for i in range(len(u2)):
            eq(u2.get(i), i, "u2.get(i) == i")
            eq(u2.get(i, 5), i, "u2.get(i, 5) == i")

        for i in min(u2)-1, max(u2)+1:
            eq(u2.get(i), None, "u2.get(i) == None")
            eq(u2.get(i, 5), 5, "u2.get(i, 5) == 5")

        # Test __setitem__

        uu2[0] = 0
        uu2[1] = 100
        uu2[2] = 200

        # Test __delitem__

        del uu2[1]
        del uu2[0]
        try:
            del uu2[0]
        except KeyError:
            pass
        else:
            raise TestFailed("uu2[0] shouldn't be deletable")

        # Test __contains__
        for i in u2:
            self.failUnless(i in u2, "i in u2")
        for i in min(u2)-1, max(u2)+1:
            self.failUnless(i not in u2, "i not in u2")

        # Test update

        l = {"a":"b"}
        u = PersistentMapping(l)
        u.update(u2)
        for i in u:
            self.failUnless(i in l or i in u2, "i in l or i in u2")
        for i in l:
            self.failUnless(i in u, "i in u")
        for i in u2:
            self.failUnless(i in u, "i in u")

        # Test setdefault

        x = u2.setdefault(0, 5)
        eq(x, 0, "u2.setdefault(0, 5) == 0")

        x = u2.setdefault(5, 5)
        eq(x, 5, "u2.setdefault(5, 5) == 5")
        self.failUnless(5 in u2, "5 in u2")

        # Test pop

        x = u2.pop(1)
        eq(x, 1, "u2.pop(1) == 1")
        self.failUnless(1 not in u2, "1 not in u2")

        try:
            u2.pop(1)
        except KeyError:
            pass
        else:
            raise TestFailed("1 should not be poppable from u2")

        x = u2.pop(1, 7)
        eq(x, 7, "u2.pop(1, 7) == 7")

        # Test popitem

        items = u2.items()
        key, value = u2.popitem()
        self.failUnless((key, value) in items, "key, value in items")
        self.failUnless(key not in u2, "key not in u2")

        # Test clear

        u2.clear()
        eq(u2, {}, "u2 == {}")


def test_suite():
    return unittest.makeSuite(TestPMapping)

if __name__ == "__main__":
    loader = unittest.TestLoader()
    unittest.main(testLoader=loader)
