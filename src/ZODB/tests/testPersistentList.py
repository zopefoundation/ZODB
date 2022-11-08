##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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
"""Test the list interface to PersistentList
"""

import unittest

from six import PY2

from persistent.list import PersistentList


l0 = []
l1 = [0]
l2 = [0, 1]


class TestPList(unittest.TestCase):
    def checkTheWorld(self):
        # Test constructors
        u = PersistentList()
        u0 = PersistentList(l0)
        u1 = PersistentList(l1)
        u2 = PersistentList(l2)

        uu = PersistentList(u)
        uu0 = PersistentList(u0)
        uu1 = PersistentList(u1)
        uu2 = PersistentList(u2)

        v = PersistentList(tuple(u))

        class OtherList(object):
            def __init__(self, initlist):
                self.__data = initlist

            def __len__(self):
                return len(self.__data)

            def __getitem__(self, i):
                return self.__data[i]
        v0 = PersistentList(OtherList(u0))
        vv = PersistentList("this is also a sequence")

        # Test __repr__
        eq = self.assertEqual

        eq(str(u0), str(l0), "str(u0) == str(l0)")
        eq(repr(u1), repr(l1), "repr(u1) == repr(l1)")
        eq(repr(u2), repr(l2), "repr(u2) == repr(l2)")

        # Test __cmp__ and __len__

        # Py3: No cmp() or __cmp__ anymore.
        if PY2:
            def mycmp(a, b):
                r = cmp(a, b)  # noqa: F821 undefined name 'cmp'
                if r < 0:
                    return -1
                if r > 0:
                    return 1
                return r

            all = [l0, l1, l2, u, u0, u1, u2, v, v0, vv, uu, uu0, uu1, uu2]
            for a in all:
                for b in all:
                    eq(mycmp(a, b), mycmp(len(a), len(b)),
                       "mycmp(a, b) == mycmp(len(a), len(b))")

        # Test __getitem__

        for i in range(len(u2)):
            eq(u2[i], i, "u2[i] == i")

        # Test __setitem__

        uu2[0] = 0
        uu2[1] = 100
        try:
            uu2[2] = 200
        except IndexError:
            pass
        else:
            self.fail("uu2[2] shouldn't be assignable")

        # Test __delitem__

        del uu2[1]
        del uu2[0]
        try:
            del uu2[0]
        except IndexError:
            pass
        else:
            self.fail("uu2[0] shouldn't be deletable")

        # Test __getslice__

        for i in range(-3, 4):
            eq(u2[:i], l2[:i], "u2[:i] == l2[:i]")
            eq(u2[i:], l2[i:], "u2[i:] == l2[i:]")
            for j in range(-3, 4):
                eq(u2[i:j], l2[i:j], "u2[i:j] == l2[i:j]")

        # Test __setslice__

        for i in range(-3, 4):
            u2[:i] = l2[:i]
            eq(u2, l2, "u2 == l2")
            u2[i:] = l2[i:]
            eq(u2, l2, "u2 == l2")
            for j in range(-3, 4):
                u2[i:j] = l2[i:j]
                eq(u2, l2, "u2 == l2")

        uu2 = u2[:]
        uu2[:0] = [-2, -1]
        eq(uu2, [-2, -1, 0, 1], "uu2 == [-2, -1, 0, 1]")
        uu2[0:] = []
        eq(uu2, [], "uu2 == []")

        # Test __contains__
        for i in u2:
            self.assertTrue(i in u2, "i in u2")
        for i in min(u2)-1, max(u2)+1:
            self.assertTrue(i not in u2, "i not in u2")

        # Test __delslice__

        uu2 = u2[:]
        del uu2[1:2]
        del uu2[0:1]
        eq(uu2, [], "uu2 == []")

        uu2 = u2[:]
        del uu2[1:]
        del uu2[:1]
        eq(uu2, [], "uu2 == []")

        # Test __add__, __radd__, __mul__ and __rmul__

        # self.assertTrue(u1 + [] == [] + u1 == u1, "u1 + [] == [] + u1 == u1")
        self.assertTrue(u1 + [1] == u2, "u1 + [1] == u2")
        # self.assertTrue([-1] + u1 == [-1, 0], "[-1] + u1 == [-1, 0]")
        self.assertTrue(u2 == u2*1 == 1*u2, "u2 == u2*1 == 1*u2")
        self.assertTrue(u2+u2 == u2*2 == 2*u2, "u2+u2 == u2*2 == 2*u2")
        self.assertTrue(u2+u2+u2 == u2*3 == 3*u2, "u2+u2+u2 == u2*3 == 3*u2")

        # Test append

        u = u1[:]
        u.append(1)
        eq(u, u2, "u == u2")

        # Test insert

        u = u2[:]
        u.insert(0, -1)
        eq(u, [-1, 0, 1], "u == [-1, 0, 1]")

        # Test pop

        u = PersistentList([0, -1, 1])
        u.pop()
        eq(u, [0, -1], "u == [0, -1]")
        u.pop(0)
        eq(u, [-1], "u == [-1]")

        # Test remove

        u = u2[:]
        u.remove(1)
        eq(u, u1, "u == u1")

        # Test count
        u = u2*3
        eq(u.count(0), 3, "u.count(0) == 3")
        eq(u.count(1), 3, "u.count(1) == 3")
        eq(u.count(2), 0, "u.count(2) == 0")

        # Test index

        eq(u2.index(0), 0, "u2.index(0) == 0")
        eq(u2.index(1), 1, "u2.index(1) == 1")
        try:
            u2.index(2)
        except ValueError:
            pass
        else:
            self.fail("expected ValueError")

        # Test reverse

        u = u2[:]
        u.reverse()
        eq(u, [1, 0], "u == [1, 0]")
        u.reverse()
        eq(u, u2, "u == u2")

        # Test sort

        u = PersistentList([1, 0])
        u.sort()
        eq(u, u2, "u == u2")

        # Test extend

        u = u1[:]
        u.extend(u2)
        eq(u, u1 + u2, "u == u1 + u2")

    def checkBackwardCompat(self):
        # Verify that the sanest of the ZODB 3.2 dotted paths still works.
        from ZODB.PersistentList import PersistentList as oldPath
        self.assertTrue(oldPath is PersistentList)


def test_suite():
    return unittest.makeSuite(TestPList, 'check')
