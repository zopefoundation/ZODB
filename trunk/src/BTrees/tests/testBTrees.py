##############################################################################
# 
# Zope Public License (ZPL) Version 1.0
# -------------------------------------
# 
# Copyright (c) Digital Creations.  All rights reserved.
# 
# This license has been certified as Open Source(tm).
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
# 1. Redistributions in source code must retain the above copyright
#    notice, this list of conditions, and the following disclaimer.
# 
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions, and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
# 
# 3. Digital Creations requests that attribution be given to Zope
#    in any manner possible. Zope includes a "Powered by Zope"
#    button that is installed by default. While it is not a license
#    violation to remove this button, it is requested that the
#    attribution remain. A significant investment has been put
#    into Zope, and this effort will continue if the Zope community
#    continues to grow. This is one way to assure that growth.
# 
# 4. All advertising materials and documentation mentioning
#    features derived from or use of this software must display
#    the following acknowledgement:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    In the event that the product being advertised includes an
#    intact Zope distribution (with copyright and license included)
#    then this clause is waived.
# 
# 5. Names associated with Zope or Digital Creations must not be used to
#    endorse or promote products derived from this software without
#    prior written permission from Digital Creations.
# 
# 6. Modified redistributions of any form whatsoever must retain
#    the following acknowledgment:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    Intact (re-)distributions of any official Zope release do not
#    require an external acknowledgement.
# 
# 7. Modifications are encouraged but must be packaged separately as
#    patches to official Zope releases.  Distributions that do not
#    clearly separate the patches from the original work must be clearly
#    labeled as unofficial distributions.  Modifications which do not
#    carry the name Zope may be packaged in any form, as long as they
#    conform to all of the clauses above.
# 
# 
# Disclaimer
# 
#   THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS ``AS IS'' AND ANY
#   EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
#   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL CREATIONS OR ITS
#   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
#   USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#   ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#   OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
#   OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
#   SUCH DAMAGE.
# 
# 
# This software consists of contributions made by Digital Creations and
# many individuals on behalf of Digital Creations.  Specific
# attributions are listed in the accompanying credits file.
# 
##############################################################################
import sys, os, time, whrandom

print sys.path
try:
    sys.path.insert(0, '.')
    import ZODB
except:
    sys.path.insert(0, '../..')
    #os.chdir('../..')
    import ZODB

from BTrees.OOBTree import OOBTree
from unittest import TestCase, TestSuite, TextTestRunner, makeSuite

class TestBTrees(TestCase):
    def setUp(self):
        self.t = OOBTree()

    def tearDown(self):
        self.t = None
        del self.t
        
    def testGetItemFails(self):
        self.assertRaises(KeyError, self._getitemfail)

    def _getitemfail(self):
        return self.t[1]

    def testGetReturnsDefault(self):
        assert self.t.get(1) == None
        assert self.t.get(1, 'foo') == 'foo'

    def testSetItemGetItemWorks(self):
        self.t[1] = 1
        a = self.t[1]
        assert a == 1, `a`

    def testReplaceWorks(self):
        self.t[1] = 1
        assert self.t[1] == 1, self.t[1]
        self.t[1] = 2
        assert self.t[1] == 2, self.t[1]

    def testHasKeyWorks(self):
        self.t[1] = 1
        assert self.t.has_key(1)

    def testValuesWorks(self):
        for x in range(100):
            self.t[x] = x
        v = self.t.values()
        i = 0
        for x in v:
            assert x == i, (x,i)
            i = i + 1
            
    def testKeysWorks(self):
        for x in range(100):
            self.t[x] = x
        v = self.t.keys()
        i = 0
        for x in v:
            assert x == i, (x,i)
            i = i + 1
        # BTree items must lie about their lengths, so we convert to list
        assert len(v) == 100, len(v)
        #assert len(v) == 100, len(v)

    def testItemsWorks(self):
        for x in range(100):
            self.t[x] = x
        v = self.t.items()
        i = 0
        for x in v:
            assert x[0] == i, (x[0], i)
            assert x[1] == i, (x[0], i)
            i = i + 1

    def testDeleteInvalidKeyRaisesKeyError(self):
        self.assertRaises(KeyError, self._deletefail)

    def _deletefail(self):
        del self.t[1]

    def testDeleteNoChildrenWorks(self):
        self.t[5] = 6
        self.t[2] = 10
        self.t[6] = 12
        self.t[1] = 100
        self.t[3] = 200
        self.t[10] = 500
        self.t[4] = 99
        del self.t[4]
        diff = lsubtract(self.t.keys(), [1,2,3,5,6,10])
        assert diff == [], diff

    def testDeleteOneChildWorks(self):
        self.t[5] = 6
        self.t[2] = 10
        self.t[6] = 12
        self.t[1] = 100
        self.t[3] = 200
        self.t[10] = 500
        self.t[4] = 99
        del self.t[3]
        diff = lsubtract(self.t.keys(), [1,2,4,5,6,10])
        assert diff == [], diff

    def testDeleteTwoChildrenNoInorderSuccessorWorks(self):
        self.t[5] = 6
        self.t[2] = 10
        self.t[6] = 12
        self.t[1] = 100
        self.t[3] = 200
        self.t[10] = 500
        self.t[4] = 99
        del self.t[2]
        diff = lsubtract(self.t.keys(), [1,3,4,5,6,10])
        assert diff == [], diff
        
    def testDeleteTwoChildrenInorderSuccessorWorks(self):
        self.t[5] = 6
        self.t[2] = 10
        self.t[6] = 12
        self.t[1] = 100
        self.t[3] = 200
        self.t[10] = 500
        self.t[4] = 99
        self.t[2.5] = 150
        del self.t[2]
        diff = lsubtract(self.t.keys(), [1,2.5,3,4,5,6,10])
        assert diff == [], diff

    def testDeleteRootWorks(self):
        self.t[5] = 6
        self.t[2] = 10
        self.t[6] = 12
        self.t[1] = 100
        self.t[3] = 200
        self.t[10] = 500
        self.t[4] = 99
        self.t[2.5] = 150
        del self.t[5]
        diff = lsubtract(self.t.keys(), [1,2,2.5,3,4,6,10])
        assert diff == [], diff

    def testRandomNonOverlappingInserts(self):
        added = {}
        r = range(100)
        for x in r:
            k = whrandom.choice(r)
            if not added.has_key(k):
                self.t[k] = x
                added[k] = 1
        addl = added.keys()
        addl.sort()
        diff = lsubtract(list(self.t.keys()), addl)
        assert diff == [], (diff, addl, list(self.t.keys()))

    def testRandomOverlappingInserts(self):
        added = {}
        r = range(100)
        for x in r:
            k = whrandom.choice(r)
            self.t[k] = x
            added[k] = 1
        addl = added.keys()
        addl.sort()
        diff = lsubtract(self.t.keys(), addl)
        assert diff == [], diff

    def testLen(self):
        added = {}
        r = range(1000)
        for x in r:
            k = whrandom.choice(r)
            self.t[k] = x
            added[k] = x
        addl = added.keys()
        assert len(self.t) == len(addl), len(self.t)

    def testRandomDeletes(self):
        r = range(1000)
        added = []
        for x in r:
            k = whrandom.choice(r)
            self.t[k] = x
            added.append(k)
        deleted = []
        for x in r:
            k = whrandom.choice(r)
            if self.t.has_key(k):
                del self.t[k]
                deleted.append(k)
                if self.t.has_key(k):
                    raise "had problems deleting %s" % k
        badones = []
        for x in deleted:
            if self.t.has_key(x):
                badones.append(x)
        assert badones == [], (badones, added, deleted)

    def testTargetedDeletes(self):
        r = range(1000)
        for x in r:
            k = whrandom.choice(r)
            self.t[k] = x
        for x in r:
            try:
                del self.t[x]
            except KeyError:
                pass
        #assert realseq(self.t.keys()) == [], realseq(self.t.keys())
        # this fails 1/22/2001 by segfaulting
        
    def testPathologicalRightBranching(self):
        r = range(1000)
        for x in r:
            self.t[x] = 1
        assert realseq(self.t.keys()) == r, realseq(self.t.keys())
        for x in r:
            del self.t[x]
        assert realseq(self.t.keys()) == [], realseq(self.t.keys())

    def testPathologicalLeftBranching(self):
        r = range(1000)
        revr = r[:]
        revr.reverse()
        for x in revr:
            self.t[x] = 1
        assert realseq(self.t.keys()) == r, realseq(self.t.keys())

        for x in revr:
            del self.t[x]
        assert realseq(self.t.keys()) == [], realseq(self.t.keys())

    def testSuccessorChildParentRewriteExerciseCase(self):
        add_order = [
            85, 73, 165, 273, 215, 142, 233, 67, 86, 166, 235, 225, 255,
            73, 175, 171, 285, 162, 108, 28, 283, 258, 232, 199, 260,
            298, 275, 44, 261, 291, 4, 181, 285, 289, 216, 212, 129,
            243, 97, 48, 48, 159, 22, 285, 92, 110, 27, 55, 202, 294,
            113, 251, 193, 290, 55, 58, 239, 71, 4, 75, 129, 91, 111,
            271, 101, 289, 194, 218, 77, 142, 94, 100, 115, 101, 226,
            17, 94, 56, 18, 163, 93, 199, 286, 213, 126, 240, 245, 190,
            195, 204, 100, 199, 161, 292, 202, 48, 165, 6, 173, 40, 218,
            271, 228, 7, 166, 173, 138, 93, 22, 140, 41, 234, 17, 249,
            215, 12, 292, 246, 272, 260, 140, 58, 2, 91, 246, 189, 116,
            72, 259, 34, 120, 263, 168, 298, 118, 18, 28, 299, 192, 252,
            112, 60, 277, 273, 286, 15, 263, 141, 241, 172, 255, 52, 89,
            127, 119, 255, 184, 213, 44, 116, 231, 173, 298, 178, 196,
            89, 184, 289, 98, 216, 115, 35, 132, 278, 238, 20, 241, 128,
            179, 159, 107, 206, 194, 31, 260, 122, 56, 144, 118, 283,
            183, 215, 214, 87, 33, 205, 183, 212, 221, 216, 296, 40,
            108, 45, 188, 139, 38, 256, 276, 114, 270, 112, 214, 191,
            147, 111, 299, 107, 101, 43, 84, 127, 67, 205, 251, 38, 91,
            297, 26, 165, 187, 19, 6, 73, 4, 176, 195, 90, 71, 30, 82,
            139, 210, 8, 41, 253, 127, 190, 102, 280, 26, 233, 32, 257,
            194, 263, 203, 190, 111, 218, 199, 29, 81, 207, 18, 180,
            157, 172, 192, 135, 163, 275, 74, 296, 298, 265, 105, 191,
            282, 277, 83, 188, 144, 259, 6, 173, 81, 107, 292, 231,
            129, 65, 161, 113, 103, 136, 255, 285, 289, 1
            ]
        delete_order = [
            276, 273, 12, 275, 2, 286, 127, 83, 92, 33, 101, 195,
            299, 191, 22, 232, 291, 226, 110, 94, 257, 233, 215, 184,
            35, 178, 18, 74, 296, 210, 298, 81, 265, 175, 116, 261,
            212, 277, 260, 234, 6, 129, 31, 4, 235, 249, 34, 289, 105,
            259, 91, 93, 119, 7, 183, 240, 41, 253, 290, 136, 75, 292,
            67, 112, 111, 256, 163, 38, 126, 139, 98, 56, 282, 60, 26,
            55, 245, 225, 32, 52, 40, 271, 29, 252, 239, 89, 87, 205,
            213, 180, 97, 108, 120, 218, 44, 187, 196, 251, 202, 203,
            172, 28, 188, 77, 90, 199, 297, 282, 141, 100, 161, 216,
            73, 19, 17, 189, 30, 258
            ]
        for x in add_order:
            self.t[x] = 1
        for x in delete_order:
            try: del self.t[x]
            except KeyError:
                if self.t.has_key(x): assert 1==2,"failed to delete %s" % x

def lsubtract(l1, l2):
    l = filter(lambda x, l1=l1: x not in l1, l2)
    l = l + filter(lambda x, l2=l2: x not in l2, l1)
    return l

def realseq(itemsob):
    return map(lambda x: x, itemsob)

if __name__ == '__main__':
    testsuite = makeSuite(TestBTrees, 'test')
    runner = TextTestRunner()
    runner.run(testsuite)

