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
import sys, os, time, random
import os, sys

from BTrees.OOBTree import OOBTree, OOBucket, OOSet, OOTreeSet
from BTrees.IOBTree import IOBTree, IOBucket, IOSet, IOTreeSet
from BTrees.IIBTree import IIBTree, IIBucket, IISet, IITreeSet
from BTrees.OIBTree import OIBTree, OIBucket, OISet, OITreeSet
from unittest import TestCase, TestSuite, TextTestRunner, makeSuite

from glob import glob

class Base:
    """ Tests common to all types: sets, buckets, and BTrees """
    def tearDown(self):
        self.t = None
        del self.t

    def _getRoot(self):
        from ZODB.FileStorage import FileStorage
        from ZODB.DB import DB
        n = 'fs_tmp__%s' % os.getpid()
        s = FileStorage(n)
        db = DB(s)
        root = db.open().root()
        return root

    def _closeDB(self, root):
        root._p_jar._db.close()
        root = None

    def _delDB(self):
        for file in glob('fs_tmp__*'):
            os.remove(file)
        
    def testLoadAndStore(self):
        for i in 0, 10, 1000:
            t = self.t.__class__()
            self._populate(t, i)
            try:
                root = self._getRoot()
                root[i] = t
                get_transaction().commit()
            except:
                self._closeDB(root)
                self._delDB()
                raise

            self._closeDB(root)

            try:
                root = self._getRoot()
                #XXX BTree stuff doesn't implement comparison
                if hasattr(t, 'items'):
                    assert list(root[i].items()) == list(t.items())
                else:
                    assert list(root[i].keys()) == list(t.keys())
            finally:
                self._closeDB(root)
                self._delDB()
            
    def testGhostUnghost(self):
        for i in 0, 10, 1000:
            t = self.t.__class__()
            self._populate(t, i)
            try:
                root = self._getRoot()
                root[i] = t
                get_transaction().commit()
            except:
                self._closeDB(root)
                self._delDB()
                raise

            self._closeDB(root)

            try:
                root = self._getRoot()
                root[i]._p_changed = None
                get_transaction().commit()
                if hasattr(t,'items'):
                    assert list(root[i].items()) == list(t.items())
                else:
                    assert list(root[i].keys()) == list(t.keys())
            finally:
                self._closeDB(root)
                self._delDB()

class MappingBase(Base):
    """ Tests common to mappings (buckets, btrees) """

    def _populate(self, t, l):
        # Make some data
        for i in range(l): t[i]=i
    
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

    def testLen(self):
        added = {}
        r = range(1000)
        for x in r:
            k = random.choice(r)
            self.t[k] = x
            added[k] = x
        addl = added.keys()
        assert len(self.t) == len(addl), len(self.t)

    def testHasKeyWorks(self):
        self.t[1] = 1
        assert self.t.has_key(1)

    def testValuesWorks(self):
        for x in range(100):
            self.t[x] = x*x
        v = self.t.values()
        for i in range(100):
            assert v[i]==i*i , (i*i,i)

            
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

    def testMaxKeyMinKey(self):
        self.t[7] = 6
        self.t[3] = 10
        self.t[8] = 12
        self.t[1] = 100
        self.t[5] = 200
        self.t[10] = 500
        self.t[6] = 99
        self.t[4] = 150
        del self.t[7]
        t = self.t
        assert t.maxKey() == 10
        assert t.maxKey(6) == 6
        assert t.maxKey(9) == 8
        assert t.minKey() == 1
        assert t.minKey(3) == 3
        assert t.minKey(9) == 10

    def testClear(self):
        r = range(100)
        for x in r:
            rnd = random.choice(r)
            self.t[rnd] = 0
        self.t.clear()
        diff = lsubtract(list(self.t.keys()), [])
        assert diff == [], diff

    def testUpdate(self):
        "mapping update"
        d={}
        l=[]
        for i in range(10000):
            k=random.randrange(-2000, 2001)
            d[k]=i
            l.append((k, i))
            
        items=d.items()
        items.sort()

        self.t.update(d)
        assert list(self.t.items()) == items

        self.t.clear()
        assert list(self.t.items()) == []

        self.t.update(l)
        assert list(self.t.items()) == items

    def testEmptyRangeSearches(self):
        t=self.t
        t.update([(1,1),(5,5),(9,9)])
        assert list(t.keys(-6,-4))==[], list(t.keys(-6,-4))
        assert list(t.keys(2,4))==[], list(t.keys(2,4))
        assert list(t.keys(6,8))==[], list(t.keys(6,8))
        assert list(t.keys(10,12))==[], list(t.keys(10,12))
        

class NormalSetTests(Base):
    """ Test common to all set types """


    def _populate(self, t, l): 
        # Make some data
        t.update(range(l))


    def testInsertReturnsValue(self):
        t = self.t
        assert t.insert(5) == 1

    def testDuplicateInsert(self):
        t = self.t
        t.insert(5)
        assert t.insert(5) == 0
        
    def testInsert(self):
        t = self.t
        t.insert(1)
        assert t.has_key(1)

    def testBigInsert(self):
        t = self.t
        r = xrange(10000)
        for x in r:
            t.insert(x)
        for x in r:
            assert t.has_key(x)

    def testRemoveSucceeds(self):
        t = self.t
        r = xrange(10000)
        for x in r: t.insert(x)
        for x in r: t.remove(x)

    def testRemoveFails(self):
        self.assertRaises(KeyError, self._removenonexistent)

    def _removenonexistent(self):
        self.t.remove(1)

    def testHasKeyFails(self):
        t = self.t
        assert not t.has_key(1)

    def testKeys(self):
        t = self.t
        r = xrange(1000)
        for x in r: t.insert(x)
        diff = lsubtract(t.keys(), r)
        assert diff == [], diff

    def testClear(self):
        t = self.t
        r = xrange(1000)
        for x in r: t.insert(x)
        t.clear()
        diff = lsubtract(t.keys(), [])
        assert diff == [], diff

    def testMaxKeyMinKey(self):
        t = self.t
        t.insert(1)
        t.insert(2)
        t.insert(3)
        t.insert(8)
        t.insert(5)
        t.insert(10)
        t.insert(6)
        t.insert(4)
        assert t.maxKey() == 10
        assert t.maxKey(6) == 6
        assert t.maxKey(9) == 8
        assert t.minKey() == 1
        assert t.minKey(3) == 3
        assert t.minKey(9) == 10

    def testUpdate(self):
        "mapping update"
        d={}
        l=[]
        for i in range(10000):
            k=random.randrange(-2000, 2001)
            d[k]=i
            l.append(k)
            
        items=d.keys()
        items.sort()

        self.t.update(l)
        assert list(self.t.keys()) == items

    def testEmptyRangeSearches(self):
        t=self.t
        t.update([1,5,9])
        assert list(t.keys(-6,-4))==[], list(t.keys(-6,-4))
        assert list(t.keys(2,4))==[], list(t.keys(2,4))
        assert list(t.keys(6,8))==[], list(t.keys(6,8))
        assert list(t.keys(10,12))==[], list(t.keys(10,12))

class ExtendedSetTests(NormalSetTests):
    def testLen(self):
        t = self.t
        r = xrange(10000)
        for x in r: t.insert(x)
        assert len(t) == 10000, len(t)

    def testGetItem(self):
        t = self.t
        r = xrange(10000)
        for x in r: t.insert(x)
        for x in r:
            assert t[x] == x
        
class BucketTests(MappingBase):
    """ Tests common to all buckets """
    pass

class BTreeTests(MappingBase):
    """ Tests common to all BTrees """
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
        """ 7, 3, 8, 1, 5, 10, 6, 4 -- del 3 """
        self.t[7] = 6
        self.t[3] = 10
        self.t[8] = 12
        self.t[1] = 100
        self.t[5] = 200
        self.t[10] = 500
        self.t[6] = 99
        self.t[4] = 150
        del self.t[3]
        diff = lsubtract(self.t.keys(), [1,4,5,6,7,8,10])
        assert diff == [], diff

    def testDeleteRootWorks(self):
        """ 7, 3, 8, 1, 5, 10, 6, 4 -- del 7 """
        self.t[7] = 6
        self.t[3] = 10
        self.t[8] = 12
        self.t[1] = 100
        self.t[5] = 200
        self.t[10] = 500
        self.t[6] = 99
        self.t[4] = 150
        del self.t[7]
        diff = lsubtract(self.t.keys(), [1,3,4,5,6,8,10])
        assert diff == [], diff

    def testRandomNonOverlappingInserts(self):
        added = {}
        r = range(100)
        for x in r:
            k = random.choice(r)
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
            k = random.choice(r)
            self.t[k] = x
            added[k] = 1
        addl = added.keys()
        addl.sort()
        diff = lsubtract(self.t.keys(), addl)
        assert diff == [], diff

    def testRandomDeletes(self):
        r = range(1000)
        added = []
        for x in r:
            k = random.choice(r)
            self.t[k] = x
            added.append(k)
        deleted = []
        for x in r:
            k = random.choice(r)
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
            k = random.choice(r)
            self.t[k] = x
        for x in r:
            try:
                del self.t[x]
            except KeyError:
                pass
        assert realseq(self.t.keys()) == [], realseq(self.t.keys())
        
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

    def testRangeSearchAfterSequentialInsert(self):
        r = range(100)
        for x in r:
            self.t[x] = 0
        diff = lsubtract(list(self.t.keys(0, 100)), r)
        assert diff == [], diff

    def testRangeSearchAfterRandomInsert(self):
        r = range(100)
        a = {}
        for x in r:
            rnd = random.choice(r)
            self.t[rnd] = 0
            a[rnd] = 0
        diff = lsubtract(list(self.t.keys(0, 100)), a.keys())
        assert diff == [], diff

    def testInsertMethod(self):
        t = self.t
        t[0] = 1
        assert t.insert(0, 1) == 0
        assert t.insert(1, 1) == 1
        assert lsubtract(list(t.keys()), [0,1]) == []

## BTree tests

class TestIOBTrees(BTreeTests, TestCase):
    def setUp(self):
        self.t = IOBTree()

    def nonIntegerKeyRaises(self):
        self.assertRaises(TypeError, self._stringraises)
        self.assertRaises(TypeError, self._floatraises)
        self.assertRaises(TypeError, self._noneraises)

    def _stringraises(self):
        self.t['c'] = 1

    def _floatraises(self):
        self.t[2.5] = 1

    def _noneraises(self):
        self.t[None] = 1

class TestOOBTrees(BTreeTests, TestCase):
    def setUp(self):
        self.t = OOBTree()

class TestOIBTrees(BTreeTests, TestCase):
    def setUp(self):
        self.t = OIBTree()

    def testNonIntegerValueRaises(self):
        self.assertRaises(TypeError, self._stringraises)
        self.assertRaises(TypeError, self._floatraises)
        self.assertRaises(TypeError, self._noneraises)

    def _stringraises(self):
        self.t[1] = 'c'

    def _floatraises(self):
        self.t[1] = 1.4

    def _noneraises(self):
        self.t[1] = None

class TestIIBTrees(BTreeTests, TestCase):
    def setUp(self):
        self.t = IIBTree()

    def testNonIntegerKeyRaises(self):
        self.assertRaises(TypeError, self._stringraiseskey)
        self.assertRaises(TypeError, self._floatraiseskey)
        self.assertRaises(TypeError, self._noneraiseskey)

    def testNonIntegerValueRaises(self):
        self.assertRaises(TypeError, self._stringraisesvalue)
        self.assertRaises(TypeError, self._floatraisesvalue)
        self.assertRaises(TypeError, self._noneraisesvalue)

    def _stringraiseskey(self):
        self.t['c'] = 1

    def _floatraiseskey(self):
        self.t[2.5] = 1

    def _noneraiseskey(self):
        self.t[None] = 1

    def _stringraisesvalue(self):
        self.t[1] = 'c'

    def _floatraisesvalue(self):
        self.t[1] = 1.4

    def _noneraisesvalue(self):
        self.t[1] = None

## Set tests

class TestIOSets(ExtendedSetTests, TestCase):
    def setUp(self):
        self.t = IOSet()

    def testNonIntegerInsertRaises(self):
        self.assertRaises(TypeError,self._insertstringraises)
        self.assertRaises(TypeError,self._insertfloatraises)
        self.assertRaises(TypeError,self._insertnoneraises)

    def _insertstringraises(self):
        self.t.insert('a')

    def _insertfloatraises(self):
        self.t.insert(1.4)

    def _insertnoneraises(self):
        self.t.insert(None)

class TestOOSets(ExtendedSetTests, TestCase):
    def setUp(self):
        self.t = OOSet()

class TestIISets(ExtendedSetTests, TestCase):
    def setUp(self):
        self.t = IISet()

class TestOISets(ExtendedSetTests, TestCase):
    def setUp(self):
        self.t = OISet()

class TestIOTreeSets(NormalSetTests, TestCase):
    def setUp(self):
        self.t = IOTreeSet()
        
class TestOOTreeSets(NormalSetTests, TestCase):
    def setUp(self):
        self.t = OOTreeSet()

class TestIITreeSets(NormalSetTests, TestCase):
    def setUp(self):
        self.t = IITreeSet()

class TestOITreeSets(NormalSetTests, TestCase):
    def setUp(self):
        self.t = OITreeSet()
        
## Bucket tests

class TestIOBuckets(BucketTests, TestCase):
    def setUp(self):
        self.t = IOBucket()

class TestOOBuckets(BucketTests, TestCase):
    def setUp(self):
        self.t = OOBucket()

class TestIIBuckets(BucketTests, TestCase):
    def setUp(self):
        self.t = IIBucket()

class TestOIBuckets(BucketTests, TestCase):
    def setUp(self):
        self.t = OIBucket()

def test_suite():
    TIOBTree = makeSuite(TestIOBTrees, 'test')
    TOOBTree = makeSuite(TestOOBTrees, 'test')
    TOIBTree = makeSuite(TestOIBTrees, 'test')
    TIIBTree = makeSuite(TestIIBTrees, 'test')

    TIOSet = makeSuite(TestIOSets, 'test')
    TOOSet = makeSuite(TestOOSets, 'test')
    TOISet = makeSuite(TestIOSets, 'test')
    TIISet = makeSuite(TestOOSets, 'test')

    TIOTreeSet = makeSuite(TestIOTreeSets, 'test')
    TOOTreeSet = makeSuite(TestOOTreeSets, 'test')
    TOITreeSet = makeSuite(TestIOTreeSets, 'test')
    TIITreeSet = makeSuite(TestOOTreeSets, 'test')

    TIOBucket = makeSuite(TestIOBuckets, 'test')
    TOOBucket = makeSuite(TestOOBuckets, 'test')
    TOIBucket = makeSuite(TestOIBuckets, 'test')
    TIIBucket = makeSuite(TestIIBuckets, 'test')
    
    alltests = TestSuite((TIOSet, TOOSet, TOISet, TIISet,
                          TIOTreeSet, TOOTreeSet, TOITreeSet, TIITreeSet,
                          TIOBucket, TOOBucket, TOIBucket, TIIBucket,
                          TOOBTree, TIOBTree, TOIBTree, TIIBTree))

    return alltests



## utility functions

def lsubtract(l1, l2):
   l1=list(l1)
   l2=list(l2)
   l = filter(lambda x, l1=l1: x not in l1, l2)
   l = l + filter(lambda x, l2=l2: x not in l2, l1)
   return l

def realseq(itemsob):
    return map(lambda x: x, itemsob)


def main():
    TextTestRunner().run(test_suite())

if __name__ == '__main__':
    main()

