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

__version__ = '$Id: testBTreesUnicode.py,v 1.8 2003/11/28 16:44:45 jim Exp $'

import unittest
from BTrees.OOBTree import OOBTree

# When an OOBtree contains unicode strings as keys,
# it is neccessary accessing non-unicode strings are
# either ascii strings or encoded as unicoded using the
# corresponding encoding

encoding = 'ISO-8859-1'

class TestBTreesUnicode(unittest.TestCase):
    """ test unicode"""

    def setUp(self):
        """setup an OOBTree with some unicode strings"""

        self.s = unicode('dreit\xe4gigen', 'latin1')

        self.data = [('alien', 1),
                     ('k\xf6nnten', 2),
                     ('fox', 3),
                     ('future', 4),
                     ('quick', 5),
                     ('zerst\xf6rt', 6),
                     (unicode('dreit\xe4gigen','latin1'), 7),
                    ]

        self.tree = OOBTree()
        for k, v in self.data:
            if isinstance(k, str):
                k = unicode(k, 'latin1')
            self.tree[k] = v

    def testAllKeys(self):
        # check every item of the tree
        for k, v in self.data:
            if isinstance(k, str):
                k = unicode(k, encoding)
            self.assert_(self.tree.has_key(k))
            self.assertEqual(self.tree[k], v)

    def testUnicodeKeys(self):
        # try to access unicode keys in tree
        k, v = self.data[-1]
        self.assertEqual(k, self.s)
        self.assertEqual(self.tree[k], v)
        self.assertEqual(self.tree[self.s], v)

    def testAsciiKeys(self):
        # try to access some "plain ASCII" keys in the tree
        for k, v in self.data[0], self.data[2]:
            self.assert_(isinstance(k, str))
            self.assertEqual(self.tree[k], v)

def test_suite():
    return unittest.makeSuite(TestBTreesUnicode)

def main():
    unittest.TextTestRunner().run(test_suite())

if __name__ == '__main__':
    main()
