import os,sys,unittest
from BTrees.OOBTree import OOBTree


class TestBTreesUnicode(unittest.TestCase):
    """ test unicode"""

    def setUp(self):
        """ setup an OOBTree with some unicode strings """

        self.s = unicode('dreit\xe4gigen','latin1')

        self.data = [('alien', 284708388), 
                ('fox', 284708387), 
                ('future', 284708388), 
                ('k\xf6nnten', 284708389),
                ('quick', 284708387), 
                ('zerst\xf6rt', 284708389), 
                (unicode('dreit\xe4gigen','latin1'), 284708391)]

        self.tree = OOBTree()
        for k,v in self.data:   self.tree[k]=v


    def test1(self):
        """ check every item of the tree """

        for k,v in self.data:
            assert self.tree[k]==v

    def test2(self):
        """ try to access unicode keys in tree"""

        assert self.data[-1][0]==self.s
        assert self.tree[self.data[-1][0]] == self.data[-1][1] 
        assert self.tree[self.s] == self.data[-1][1] 



def test_suite():
    return unittest.makeSuite(TestBTreesUnicode,'test')

