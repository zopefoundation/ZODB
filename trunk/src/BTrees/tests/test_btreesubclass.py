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
from BTrees.OOBTree import OOBTree, OOBucket

class B(OOBucket):
    pass

class T(OOBTree):
    _bucket_type = B

import unittest

class SubclassTest(unittest.TestCase):

    def testSubclass(self):
        # test that a subclass that defines _bucket_type gets buckets
        # of that type
        t = T()

        # XXX there's no good way to get a bucket at the moment.
        # XXX __getstate__() is as good as it gets, but the default
        # XXX getstate explicitly includes the pickle of the bucket
        # XXX for small trees, so we have to be clever :-(

        # make sure there is more than one bucket in the tree
        for i in range(1000):
            t[i] = i

        state = t.__getstate__()
        self.assert_(state[0][0].__class__ is B)

def test_suite():
    return unittest.makeSuite(SubclassTest)
