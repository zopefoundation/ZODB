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

__version__ = '$Id: testBTreesUnicode.py,v 1.6 2002/02/11 23:40:40 gvanrossum Exp $'

import unittest,types
from BTrees.OOBTree import OOBTree


# When a OOBtree contains unicode strings as keys,
# it is neccessary accessing non-unicode strings are
# either ascii strings or encoded as unicoded using the
# corresponding encoding


encoding = 'ISO-8859-1'

class TestBTreesUnicode(unittest.TestCase):
    """ test unicode"""

    def setUp(self):
        """ setup an OOBTree with some unicode strings """

        self.s = unicode('dreit\xe4gigen','latin1')

        self.data = [('alien', 284708388), 
                ('k\xf6nnten', 284708389),
                ('fox', 284708387), 
                ('future', 284708388), 
                ('quick', 284708387), 
                ('zerst\xf6rt', 284708389), 
                (unicode('dreit\xe4gigen','latin1'), 284708391)
                ]

        self.tree = OOBTree()
        for k,v in self.data:
            if isinstance(k,types.StringType):
                self.tree[unicode(k,'latin1')]=v
            else:
                self.tree[k]=v



    def test1(self):
        """ check every item of the tree """

        for k, v in self.data:
            if isinstance(k,types.StringType):
                key = unicode(k,encoding)
            else:
                key = k

            if self.tree[key]!=v:
                print "fehler"


    def test2(self):
        """ try to access unicode keys in tree"""

        assert self.data[-1][0]== self.s
        assert self.tree[self.data[-1][0]]== self.data[-1][1]
        assert self.tree[self.s]== self.data[-1][1],''


def test_suite():
    return unittest.makeSuite(TestBTreesUnicode)

def main():
    unittest.TextTestRunner().run(test_suite())

if __name__ == '__main__':
    main()

