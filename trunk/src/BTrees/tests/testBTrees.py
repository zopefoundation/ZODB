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
import random
from unittest import TestCase, TestSuite, TextTestRunner, makeSuite

from BTrees.OOBTree import OOBTree, OOBucket, OOSet, OOTreeSet
from BTrees.IOBTree import IOBTree, IOBucket, IOSet, IOTreeSet
from BTrees.IIBTree import IIBTree, IIBucket, IISet, IITreeSet
from BTrees.OIBTree import OIBTree, OIBucket, OISet, OITreeSet

from BTrees.check import check

from ZODB import DB
from ZODB.MappingStorage import MappingStorage

class Base(TestCase):
    """ Tests common to all types: sets, buckets, and BTrees """

    db = None

    def tearDown(self):
        if self.db is not None:
            self.db.close()
        self.t = None
        del self.t

    def _getRoot(self):
        if self.db is None:
            # XXX On the next line, the ZODB4 flavor of this routine
            # XXX passes a cache_size argument:
            #     self.db = DB(MappingStorage(), cache_size=1)
            # XXX If that's done here, though, testLoadAndStore() and
            # XXX testGhostUnghost() both nail the CPU and seemingly
            # XXX never finish.
            self.db = DB(MappingStorage())
        return self.db.open().root()

    def _closeRoot(self, root):
        root._p_jar.close()

    def testLoadAndStore(self):
        for i in 0, 10, 1000:
            t = self.t.__class__()
            self._populate(t, i)
            root = None
            root = self._getRoot()
            root[i] = t
            get_transaction().commit()

            root2 = self._getRoot()
            if hasattr(t, 'items'):
                self.assertEqual(list(root2[i].items()) , list(t.items()))
            else:
                self.assertEqual(list(root2[i].keys()) , list(t.keys()))

            self._closeRoot(root)
            self._closeRoot(root2)

    def testGhostUnghost(self):
        for i in 0, 10, 1000:
            t = self.t.__class__()
            self._populate(t, i)
            root = self._getRoot()
            root[i] = t
            get_transaction().commit()

            root2 = self._getRoot()
            root2[i]._p_deactivate()
            get_transaction().commit()
            if hasattr(t, 'items'):
                self.assertEqual(list(root2[i].items()) , list(t.items()))
            else:
                self.assertEqual(list(root2[i].keys()) , list(t.keys()))

            self._closeRoot(root)
            self._closeRoot(root2)

    def testSimpleExclusiveKeyRange(self):
        t = self.t.__class__()
        self.assertEqual(list(t.keys()), [])
        self.assertEqual(list(t.keys(excludemin=True)), [])
        self.assertEqual(list(t.keys(excludemax=True)), [])
        self.assertEqual(list(t.keys(excludemin=True, excludemax=True)), [])

        self._populate(t, 1)
        self.assertEqual(list(t.keys()), [0])
        self.assertEqual(list(t.keys(excludemin=True)), [])
        self.assertEqual(list(t.keys(excludemax=True)), [])
        self.assertEqual(list(t.keys(excludemin=True, excludemax=True)), [])

        t.clear()
        self._populate(t, 2)
        self.assertEqual(list(t.keys()), [0, 1])
        self.assertEqual(list(t.keys(excludemin=True)), [1])
        self.assertEqual(list(t.keys(excludemax=True)), [0])
        self.assertEqual(list(t.keys(excludemin=True, excludemax=True)), [])

        t.clear()
        self._populate(t, 3)
        self.assertEqual(list(t.keys()), [0, 1, 2])
        self.assertEqual(list(t.keys(excludemin=True)), [1, 2])
        self.assertEqual(list(t.keys(excludemax=True)), [0, 1])
        self.assertEqual(list(t.keys(excludemin=True, excludemax=True)), [1])

        self.assertEqual(list(t.keys(-1, 3, excludemin=True, excludemax=True)),
                         [0, 1, 2])
        self.assertEqual(list(t.keys(0, 3, excludemin=True, excludemax=True)),
                         [1, 2])
        self.assertEqual(list(t.keys(-1, 2, excludemin=True, excludemax=True)),
                         [0, 1])
        self.assertEqual(list(t.keys(0, 2, excludemin=True, excludemax=True)),
                         [1])

class MappingBase(Base):
    """ Tests common to mappings (buckets, btrees) """

    def _populate(self, t, l):
        # Make some data
        for i in range(l): t[i]=i

    def testRepr(self):
        # test the repr because buckets have a complex repr implementation
        # internally the cutoff from a stack allocated buffer to a heap
        # allocated buffer is 10000.
        for i in range(1000):
            self.t[i] = i
        r = repr(self.t)
        # make sure the repr is 10000 bytes long for a bucket
        # XXX since we the test is also run for btrees, skip the length
        # XXX check if the repr starts with '<'
        if not r.startswith('<'):
            self.assert_(len(r) > 10000)

    def testGetItemFails(self):
        self.assertRaises(KeyError, self._getitemfail)

    def _getitemfail(self):
        return self.t[1]

    def testGetReturnsDefault(self):
        self.assertEqual(self.t.get(1) , None)
        self.assertEqual(self.t.get(1, 'foo') , 'foo')

    def testSetItemGetItemWorks(self):
        self.t[1] = 1
        a = self.t[1]
        self.assertEqual(a , 1, `a`)

    def testReplaceWorks(self):
        self.t[1] = 1
        self.assertEqual(self.t[1] , 1, self.t[1])
        self.t[1] = 2
        self.assertEqual(self.t[1] , 2, self.t[1])

    def testLen(self):
        added = {}
        r = range(1000)
        for x in r:
            k = random.choice(r)
            self.t[k] = x
            added[k] = x
        addl = added.keys()
        self.assertEqual(len(self.t) , len(addl), len(self.t))

    def testHasKeyWorks(self):
        self.t[1] = 1
        self.assert_(self.t.has_key(1))
        self.assert_(1 in self.t)
        self.assert_(0 not in self.t)
        self.assert_(2 not in self.t)

    def testValuesWorks(self):
        for x in range(100):
            self.t[x] = x*x
        v = self.t.values()
        for i in range(100):
            self.assertEqual(v[i], i*i)
        i = 0
        for value in self.t.itervalues():
            self.assertEqual(value, i*i)
            i += 1

    def testValuesWorks1(self):
        for x in range(100):
            self.t[99-x] = x

        for x in range(40):
            lst = list(self.t.values(0+x,99-x))
            lst.sort()
            self.assertEqual(lst,range(0+x,99-x+1))

            lst = list(self.t.values(max=99-x, min=0+x))
            lst.sort()
            self.assertEqual(lst,range(0+x,99-x+1))

    def testKeysWorks(self):
        for x in range(100):
            self.t[x] = x
        v = self.t.keys()
        i = 0
        for x in v:
            self.assertEqual(x,i)
            i = i + 1

        for x in range(40):
            lst = self.t.keys(0+x,99-x)
            self.assertEqual(list(lst), range(0+x, 99-x+1))

            lst = self.t.keys(max=99-x, min=0+x)
            self.assertEqual(list(lst), range(0+x, 99-x+1))

        self.assertEqual(len(v), 100)

    def testItemsWorks(self):
        for x in range(100):
            self.t[x] = 2*x
        v = self.t.items()
        i = 0
        for x in v:
            self.assertEqual(x[0], i)
            self.assertEqual(x[1], 2*i)
            i += 1

        i = 0
        for x in self.t.iteritems():
            self.assertEqual(x, (i, 2*i))
            i += 1

        items = list(self.t.items(min=12, max=20))
        self.assertEqual(items, zip(range(12, 21), range(24, 43, 2)))

        items = list(self.t.iteritems(min=12, max=20))
        self.assertEqual(items, zip(range(12, 21), range(24, 43, 2)))


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
        self.assertEqual(t.maxKey(), 10)
        self.assertEqual(t.maxKey(6), 6)
        self.assertEqual(t.maxKey(9), 8)
        self.assertEqual(t.minKey(), 1)
        self.assertEqual(t.minKey(3), 3)
        self.assertEqual(t.minKey(9), 10)

    def testClear(self):
        r = range(100)
        for x in r:
            rnd = random.choice(r)
            self.t[rnd] = 0
        self.t.clear()
        diff = lsubtract(list(self.t.keys()), [])
        self.assertEqual(diff, [])

    def testUpdate(self):
        d={}
        l=[]
        for i in range(10000):
            k=random.randrange(-2000, 2001)
            d[k]=i
            l.append((k, i))

        items=d.items()
        items.sort()

        self.t.update(d)
        self.assertEqual(list(self.t.items()), items)

        self.t.clear()
        self.assertEqual(list(self.t.items()), [])

        self.t.update(l)
        self.assertEqual(list(self.t.items()), items)

    def testEmptyRangeSearches(self):
        t = self.t
        t.update([(1,1), (5,5), (9,9)])
        self.assertEqual(list(t.keys(-6,-4)), [], list(t.keys(-6,-4)))
        self.assertEqual(list(t.keys(2,4)), [], list(t.keys(2,4)))
        self.assertEqual(list(t.keys(6,8)), [], list(t.keys(6,8)))
        self.assertEqual(list(t.keys(10,12)), [], list(t.keys(10,12)))
        self.assertEqual(list(t.keys(9, 1)), [], list(t.keys(9, 1)))

        # For IITreeSets, this one was returning 31 for len(keys), and
        # list(keys) produced a list with 100 elements.
        t.clear()
        t.update(zip(range(300), range(300)))
        keys = t.keys(200, 50)
        self.assertEqual(len(keys), 0)
        self.assertEqual(list(keys), [])
        self.assertEqual(list(t.iterkeys(200, 50)), [])

        keys = t.keys(max=50, min=200)
        self.assertEqual(len(keys), 0)
        self.assertEqual(list(keys), [])
        self.assertEqual(list(t.iterkeys(max=50, min=200)), [])

    def testSlicing(self):
        # Test that slicing of .keys()/.values()/.items() works exactly the
        # same way as slicing a Python list with the same contents.
        # This tests fixes to several bugs in this area, starting with
        # http://collector.zope.org/Zope/419,
        # "BTreeItems slice contains 1 too many elements".

        t = self.t
        for n in range(10):
            t.clear()
            self.assertEqual(len(t), 0)

            keys = []
            values = []
            items = []
            for key in range(n):
                value = -2 * key
                t[key] = value
                keys.append(key)
                values.append(value)
                items.append((key, value))
            self.assertEqual(len(t), n)

            kslice = t.keys()
            vslice = t.values()
            islice = t.items()
            self.assertEqual(len(kslice), n)
            self.assertEqual(len(vslice), n)
            self.assertEqual(len(islice), n)

            # Test whole-structure slices.
            x = kslice[:]
            self.assertEqual(list(x), keys[:])

            x = vslice[:]
            self.assertEqual(list(x), values[:])

            x = islice[:]
            self.assertEqual(list(x), items[:])

            for lo in range(-2*n, 2*n+1):
                # Test one-sided slices.
                x = kslice[:lo]
                self.assertEqual(list(x), keys[:lo])
                x = kslice[lo:]
                self.assertEqual(list(x), keys[lo:])

                x = vslice[:lo]
                self.assertEqual(list(x), values[:lo])
                x = vslice[lo:]
                self.assertEqual(list(x), values[lo:])

                x = islice[:lo]
                self.assertEqual(list(x), items[:lo])
                x = islice[lo:]
                self.assertEqual(list(x), items[lo:])

                for hi in range(-2*n, 2*n+1):
                    # Test two-sided slices.
                    x = kslice[lo:hi]
                    self.assertEqual(list(x), keys[lo:hi])

                    x = vslice[lo:hi]
                    self.assertEqual(list(x), values[lo:hi])

                    x = islice[lo:hi]
                    self.assertEqual(list(x), items[lo:hi])

        # The specific test case from Zope collector 419.
        t.clear()
        for i in xrange(100):
            t[i] = 1
        tslice = t.items()[20:80]
        self.assertEqual(len(tslice), 60)
        self.assertEqual(list(tslice), zip(range(20, 80), [1]*60))

    def testIterators(self):
        t = self.t

        for keys in [], [-2], [1, 4], range(-170, 2000, 6):
            t.clear()
            for k in keys:
                t[k] = -3 * k

            self.assertEqual(list(t), keys)

            x = []
            for k in t:
                x.append(k)
            self.assertEqual(x, keys)

            it = iter(t)
            self.assert_(it is iter(it))
            x = []
            try:
                while 1:
                    x.append(it.next())
            except StopIteration:
                pass
            self.assertEqual(x, keys)

            self.assertEqual(list(t.iterkeys()), keys)
            self.assertEqual(list(t.itervalues()), list(t.values()))
            self.assertEqual(list(t.iteritems()), list(t.items()))

    def testRangedIterators(self):
        t = self.t

        for keys in [], [-2], [1, 4], range(-170, 2000, 13):
            t.clear()
            values = []
            for k in keys:
                value = -3 * k
                t[k] = value
                values.append(value)
            items = zip(keys, values)

            self.assertEqual(list(t.iterkeys()), keys)
            self.assertEqual(list(t.itervalues()), values)
            self.assertEqual(list(t.iteritems()), items)

            if not keys:
                continue

            min_mid_max = (keys[0], keys[len(keys) >> 1], keys[-1])
            for key1 in min_mid_max:
                for lo in range(key1 - 1, key1 + 2):
                    # Test one-sided range iterators.
                    goodkeys = [k for k in keys if lo <= k]
                    got = t.iterkeys(lo)
                    self.assertEqual(goodkeys, list(got))

                    goodvalues = [t[k] for k in goodkeys]
                    got = t.itervalues(lo)
                    self.assertEqual(goodvalues, list(got))

                    gooditems = zip(goodkeys, goodvalues)
                    got = t.iteritems(lo)
                    self.assertEqual(gooditems, list(got))

                    for key2 in min_mid_max:
                        for hi in range(key2 - 1, key2 + 2):
                            goodkeys = [k for k in keys if lo <= k <= hi]
                            got = t.iterkeys(min=lo, max=hi)
                            self.assertEqual(goodkeys, list(got))

                            goodvalues = [t[k] for k in goodkeys]
                            got = t.itervalues(lo, max=hi)
                            self.assertEqual(goodvalues, list(got))

                            gooditems = zip(goodkeys, goodvalues)
                            got = t.iteritems(max=hi, min=lo)
                            self.assertEqual(gooditems, list(got))

    def testBadUpdateTupleSize(self):
        # This one silently ignored the excess in Zope3.
        try:
            self.t.update([(1, 2, 3)])
        except TypeError:
            pass
        else:
            self.fail("update() with 3-tuple didn't complain")

        # This one dumped core in Zope3.
        try:
            self.t.update([(1,)])
        except TypeError:
            pass
        else:
            self.fail("update() with 1-tuple didn't complain")

        # This one should simply succeed.
        self.t.update([(1, 2)])
        self.assertEqual(list(self.t.items()), [(1, 2)])

    def testSimpleExclusivRanges(self):
        def identity(x):
            return x
        def dup(x):
            return [(y, y) for y in x]

        for methodname, f in (("keys", identity),
                              ("values", identity),
                              ("items", dup),
                              ("iterkeys", identity),
                              ("itervalues", identity),
                              ("iteritems", dup)):

            t = self.t.__class__()
            meth = getattr(t, methodname, None)
            if meth is None:
                continue

            self.assertEqual(list(meth()), [])
            self.assertEqual(list(meth(excludemin=True)), [])
            self.assertEqual(list(meth(excludemax=True)), [])
            self.assertEqual(list(meth(excludemin=True, excludemax=True)), [])

            self._populate(t, 1)
            self.assertEqual(list(meth()), f([0]))
            self.assertEqual(list(meth(excludemin=True)), [])
            self.assertEqual(list(meth(excludemax=True)), [])
            self.assertEqual(list(meth(excludemin=True, excludemax=True)), [])

            t.clear()
            self._populate(t, 2)
            self.assertEqual(list(meth()), f([0, 1]))
            self.assertEqual(list(meth(excludemin=True)), f([1]))
            self.assertEqual(list(meth(excludemax=True)), f([0]))
            self.assertEqual(list(meth(excludemin=True, excludemax=True)), [])

            t.clear()
            self._populate(t, 3)
            self.assertEqual(list(meth()), f([0, 1, 2]))
            self.assertEqual(list(meth(excludemin=True)), f([1, 2]))
            self.assertEqual(list(meth(excludemax=True)), f([0, 1]))
            self.assertEqual(list(meth(excludemin=True, excludemax=True)),
                            f([1]))
            self.assertEqual(list(meth(-1, 3, excludemin=True,
                                       excludemax=True)),
                             f([0, 1, 2]))
            self.assertEqual(list(meth(0, 3, excludemin=True,
                                       excludemax=True)),
                             f([1, 2]))
            self.assertEqual(list(meth(-1, 2, excludemin=True,
                                       excludemax=True)),
                             f([0, 1]))
            self.assertEqual(list(meth(0, 2, excludemin=True,
                                       excludemax=True)),
                             f([1]))

class NormalSetTests(Base):
    """ Test common to all set types """

    def _populate(self, t, l):
        # Make some data
        t.update(range(l))

    def testInsertReturnsValue(self):
        t = self.t
        self.assertEqual(t.insert(5) , 1)

    def testDuplicateInsert(self):
        t = self.t
        t.insert(5)
        self.assertEqual(t.insert(5) , 0)

    def testInsert(self):
        t = self.t
        t.insert(1)
        self.assert_(t.has_key(1))
        self.assert_(1 in t)
        self.assert_(2 not in t)

    def testBigInsert(self):
        t = self.t
        r = xrange(10000)
        for x in r:
            t.insert(x)
        for x in r:
            self.assert_(t.has_key(x))
            self.assert_(x in t)

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
        self.assert_(not t.has_key(1))
        self.assert_(1 not in t)

    def testKeys(self):
        t = self.t
        r = xrange(1000)
        for x in r:
            t.insert(x)
        diff = lsubtract(t.keys(), r)
        self.assertEqual(diff, [])


    def testClear(self):
        t = self.t
        r = xrange(1000)
        for x in r: t.insert(x)
        t.clear()
        diff = lsubtract(t.keys(), [])
        self.assertEqual(diff , [], diff)

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
        self.assertEqual(t.maxKey() , 10)
        self.assertEqual(t.maxKey(6) , 6)
        self.assertEqual(t.maxKey(9) , 8)
        self.assertEqual(t.minKey() , 1)
        self.assertEqual(t.minKey(3) , 3)
        self.assertEqual(t.minKey(9) , 10)
        self.assert_(t.minKey() in t)
        self.assert_(t.minKey()-1 not in t)
        self.assert_(t.maxKey() in t)
        self.assert_(t.maxKey()+1 not in t)

    def testUpdate(self):
        d={}
        l=[]
        for i in range(10000):
            k=random.randrange(-2000, 2001)
            d[k]=i
            l.append(k)

        items = d.keys()
        items.sort()

        self.t.update(l)
        self.assertEqual(list(self.t.keys()), items)

    def testEmptyRangeSearches(self):
        t = self.t
        t.update([1, 5, 9])
        self.assertEqual(list(t.keys(-6,-4)), [], list(t.keys(-6,-4)))
        self.assertEqual(list(t.keys(2,4)), [], list(t.keys(2,4)))
        self.assertEqual(list(t.keys(6,8)), [], list(t.keys(6,8)))
        self.assertEqual(list(t.keys(10,12)), [], list(t.keys(10,12)))
        self.assertEqual(list(t.keys(9,1)), [], list(t.keys(9,1)))

        # For IITreeSets, this one was returning 31 for len(keys), and
        # list(keys) produced a list with 100 elements.
        t.clear()
        t.update(range(300))
        keys = t.keys(200, 50)
        self.assertEqual(len(keys), 0)
        self.assertEqual(list(keys), [])

        keys = t.keys(max=50, min=200)
        self.assertEqual(len(keys), 0)
        self.assertEqual(list(keys), [])

    def testSlicing(self):
        # Test that slicing of .keys() works exactly the same way as slicing
        # a Python list with the same contents.

        t = self.t
        for n in range(10):
            t.clear()
            self.assertEqual(len(t), 0)

            keys = range(10*n, 11*n)
            t.update(keys)
            self.assertEqual(len(t), n)

            kslice = t.keys()
            self.assertEqual(len(kslice), n)

            # Test whole-structure slices.
            x = kslice[:]
            self.assertEqual(list(x), keys[:])

            for lo in range(-2*n, 2*n+1):
                # Test one-sided slices.
                x = kslice[:lo]
                self.assertEqual(list(x), keys[:lo])
                x = kslice[lo:]
                self.assertEqual(list(x), keys[lo:])

                for hi in range(-2*n, 2*n+1):
                    # Test two-sided slices.
                    x = kslice[lo:hi]
                    self.assertEqual(list(x), keys[lo:hi])

    def testIterator(self):
        t = self.t

        for keys in [], [-2], [1, 4], range(-170, 2000, 6):
            t.clear()
            t.update(keys)

            self.assertEqual(list(t), keys)

            x = []
            for k in t:
                x.append(k)
            self.assertEqual(x, keys)

            it = iter(t)
            self.assert_(it is iter(it))
            x = []
            try:
                while 1:
                    x.append(it.next())
            except StopIteration:
                pass
            self.assertEqual(x, keys)

class ExtendedSetTests(NormalSetTests):
    def testLen(self):
        t = self.t
        r = xrange(10000)
        for x in r: t.insert(x)
        self.assertEqual(len(t) , 10000, len(t))

    def testGetItem(self):
        t = self.t
        r = xrange(10000)
        for x in r: t.insert(x)
        for x in r:
            self.assertEqual(t[x] , x)

class BTreeTests(MappingBase):
    """ Tests common to all BTrees """

    def tearDown(self):
        self.t._check()
        check(self.t)
        MappingBase.tearDown(self)

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
        self.assertEqual(diff , [], diff)

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
        self.assertEqual(diff , [], diff)

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
        self.assertEqual(diff , [], diff)

    def testDeleteTwoChildrenInorderSuccessorWorks(self):
        # 7, 3, 8, 1, 5, 10, 6, 4 -- del 3
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
        self.assertEqual(diff , [], diff)

    def testDeleteRootWorks(self):
        # 7, 3, 8, 1, 5, 10, 6, 4 -- del 7
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
        self.assertEqual(diff , [], diff)

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
        self.assertEqual(diff , [], (diff, addl, list(self.t.keys())))

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
        self.assertEqual(diff , [], diff)

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
                self.assert_(k in self.t)
                del self.t[k]
                deleted.append(k)
                if self.t.has_key(k):
                    self.fail( "had problems deleting %s" % k )
        badones = []
        for x in deleted:
            if self.t.has_key(x):
                badones.append(x)
        self.assertEqual(badones , [], (badones, added, deleted))

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
        self.assertEqual(realseq(self.t.keys()) , [], realseq(self.t.keys()))

    def testPathologicalRightBranching(self):
        r = range(1000)
        for x in r:
            self.t[x] = 1
        self.assertEqual(realseq(self.t.keys()) , r, realseq(self.t.keys()))
        for x in r:
            del self.t[x]
        self.assertEqual(realseq(self.t.keys()) , [], realseq(self.t.keys()))

    def testPathologicalLeftBranching(self):
        r = range(1000)
        revr = r[:]
        revr.reverse()
        for x in revr:
            self.t[x] = 1
        self.assertEqual(realseq(self.t.keys()) , r, realseq(self.t.keys()))

        for x in revr:
            del self.t[x]
        self.assertEqual(realseq(self.t.keys()) , [], realseq(self.t.keys()))

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
                if self.t.has_key(x): self.assertEqual(1,2,"failed to delete %s" % x)

    def testRangeSearchAfterSequentialInsert(self):
        r = range(100)
        for x in r:
            self.t[x] = 0
        diff = lsubtract(list(self.t.keys(0, 100)), r)
        self.assertEqual(diff , [], diff)

    def testRangeSearchAfterRandomInsert(self):
        r = range(100)
        a = {}
        for x in r:
            rnd = random.choice(r)
            self.t[rnd] = 0
            a[rnd] = 0
        diff = lsubtract(list(self.t.keys(0, 100)), a.keys())
        self.assertEqual(diff , [], diff)

    def testPathologicalRangeSearch(self):
        t = self.t
        # Build a 2-level tree with at least two buckets.
        for i in range(200):
            t[i] = i
        items, dummy = t.__getstate__()
        self.assert_(len(items) > 2)   # at least two buckets and a key
        # All values in the first bucket are < firstkey.  All in the
        # second bucket are >= firstkey, and firstkey is the first key in
        # the second bucket.
        firstkey = items[1]
        therange = t.keys(-1, firstkey)
        self.assertEqual(len(therange), firstkey + 1)
        self.assertEqual(list(therange), range(firstkey + 1))
        # Now for the tricky part.  If we delete firstkey, the second bucket
        # loses its smallest key, but firstkey remains in the BTree node.
        # If we then do a high-end range search on firstkey, the BTree node
        # directs us to look in the second bucket, but there's no longer any
        # key <= firstkey in that bucket.  The correct answer points to the
        # end of the *first* bucket.  The algorithm has to be smart enough
        # to "go backwards" in the BTree then; if it doesn't, it will
        # erroneously claim that the range is empty.
        del t[firstkey]
        therange = t.keys(min=-1, max=firstkey)
        self.assertEqual(len(therange), firstkey)
        self.assertEqual(list(therange), range(firstkey))

    def testInsertMethod(self):
        t = self.t
        t[0] = 1
        self.assertEqual(t.insert(0, 1) , 0)
        self.assertEqual(t.insert(1, 1) , 1)
        self.assertEqual(lsubtract(list(t.keys()), [0,1]) , [])

    def testDamagedIterator(self):
        # A cute one from Steve Alexander.  This caused the BTreeItems
        # object to go insane, accessing memory beyond the allocated part
        # of the bucket.  If it fails, the symptom is either a C-level
        # assertion error (if the BTree code was compiled without NDEBUG),
        # or most likely a segfault (if the BTree code was compiled with
        # NDEBUG).

        t = self.t.__class__()
        self._populate(t, 10)
        # In order for this to fail, it's important that k be a "lazy"
        # iterator, referring to the BTree by indirect position (index)
        # instead of a fully materialized list.  Then the position can
        # end up pointing into trash memory, if the bucket pointed to
        # shrinks.
        k = t.keys()
        for dummy in range(20):
            try:
                del t[k[0]]
            except RuntimeError, detail:
                self.assertEqual(str(detail), "the bucket being iterated "
                                              "changed size")
                break

# tests of various type errors

class TypeTest(TestCase):

    def testBadTypeRaises(self):
        self.assertRaises(TypeError, self._stringraises)
        self.assertRaises(TypeError, self._floatraises)
        self.assertRaises(TypeError, self._noneraises)

class TestIOBTrees(TypeTest):
    def setUp(self):
        self.t = IOBTree()

    def _stringraises(self):
        self.t['c'] = 1

    def _floatraises(self):
        self.t[2.5] = 1

    def _noneraises(self):
        self.t[None] = 1

class TestOIBTrees(TypeTest):
    def setUp(self):
        self.t = OIBTree()

    def _stringraises(self):
        self.t[1] = 'c'

    def _floatraises(self):
        self.t[1] = 1.4

    def _noneraises(self):
        self.t[1] = None

    def testEmptyFirstBucketReportedByGuido(self):
        b = self.t
        for i in xrange(29972): # reduce to 29971 and it works
            b[i] = i
        for i in xrange(30): # reduce to 29 and it works
            del b[i]
            b[i+40000] = i

        self.assertEqual(b.keys()[0], 30)

class TestIIBTrees(TestCase):
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

class TestIOSets(TestCase):
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

class DegenerateBTree(TestCase):
    # Build a degenerate tree (set).  Boxes are BTree nodes.  There are
    # 5 leaf buckets, each containing a single int.  Keys in the BTree
    # nodes don't appear in the buckets.  Seven BTree nodes are purely
    # indirection nodes (no keys).  Buckets aren't all at the same depth:
    #
    #     +------------------------+
    #     |          4             |
    #     +------------------------+
    #         |              |
    #         |              v
    #         |             +-+
    #         |             | |
    #         |             +-+
    #         |              |
    #         v              v
    #     +-------+   +-------------+
    #     |   2   |   |   6     10  |
    #     +-------+   +-------------+
    #      |     |     |     |     |
    #      v     v     v     v     v
    #     +-+   +-+   +-+   +-+   +-+
    #     | |   | |   | |   | |   | |
    #     +-+   +-+   +-+   +-+   +-+
    #      |     |     |     |     |
    #      v     v     v     v     v
    #      1     3    +-+    7     11
    #                 | |
    #                 +-+
    #                  |
    #                  v
    #                  5
    #
    # This is nasty for many algorithms.  Consider a high-end range search
    # for 4.  The BTree nodes direct it to the 5 bucket, but the correct
    # answer is the 3 bucket, which requires going in a different direction
    # at the very top node already.  Consider a low-end range search for
    # 9.  The BTree nodes direct it to the 7 bucket, but the correct answer
    # is the 11 bucket.  This is also a nasty-case tree for deletions.

    def _build_degenerate_tree(self):
        # Build the buckets and chain them together.
        bucket11 = IISet([11])

        bucket7 = IISet()
        bucket7.__setstate__(((7,), bucket11))

        bucket5 = IISet()
        bucket5.__setstate__(((5,), bucket7))

        bucket3 = IISet()
        bucket3.__setstate__(((3,), bucket5))

        bucket1 = IISet()
        bucket1.__setstate__(((1,), bucket3))

        # Build the deepest layers of indirection nodes.
        ts = IITreeSet
        tree1 = ts()
        tree1.__setstate__(((bucket1,), bucket1))

        tree3 = ts()
        tree3.__setstate__(((bucket3,), bucket3))

        tree5lower = ts()
        tree5lower.__setstate__(((bucket5,), bucket5))
        tree5 = ts()
        tree5.__setstate__(((tree5lower,), bucket5))

        tree7 = ts()
        tree7.__setstate__(((bucket7,), bucket7))

        tree11 = ts()
        tree11.__setstate__(((bucket11,), bucket11))

        # Paste together the middle layers.
        tree13 = ts()
        tree13.__setstate__(((tree1, 2, tree3), bucket1))

        tree5711lower = ts()
        tree5711lower.__setstate__(((tree5, 6, tree7, 10, tree11), bucket5))
        tree5711 = ts()
        tree5711.__setstate__(((tree5711lower,), bucket5))

        # One more.
        t = ts()
        t.__setstate__(((tree13, 4, tree5711), bucket1))
        t._check()
        check(t)
        return t, [1, 3, 5, 7, 11]

    def testBasicOps(self):
        t, keys = self._build_degenerate_tree()
        self.assertEqual(len(t), len(keys))
        self.assertEqual(list(t.keys()), keys)
        # has_key actually returns the depth of a bucket.
        self.assertEqual(t.has_key(1), 4)
        self.assertEqual(t.has_key(3), 4)
        self.assertEqual(t.has_key(5), 6)
        self.assertEqual(t.has_key(7), 5)
        self.assertEqual(t.has_key(11), 5)
        for i in 0, 2, 4, 6, 8, 9, 10, 12:
            self.assert_(i not in t)

    def _checkRanges(self, tree, keys):
        self.assertEqual(len(tree), len(keys))
        sorted_keys = keys[:]
        sorted_keys.sort()
        self.assertEqual(list(tree.keys()), sorted_keys)
        for k in keys:
            self.assert_(k in tree)
        if keys:
            lokey = sorted_keys[0]
            hikey = sorted_keys[-1]
            self.assertEqual(lokey, tree.minKey())
            self.assertEqual(hikey, tree.maxKey())
        else:
            lokey = hikey = 42

        # Try all range searches.
        for lo in range(lokey - 1, hikey + 2):
            for hi in range(lo - 1, hikey + 2):
                for skipmin in False, True:
                    for skipmax in False, True:
                        wantlo, wanthi = lo, hi
                        if skipmin:
                            wantlo += 1
                        if skipmax:
                            wanthi -= 1
                        want = [k for k in keys if wantlo <= k <= wanthi]
                        got = list(tree.keys(lo, hi, skipmin, skipmax))
                        self.assertEqual(want, got)

    def testRanges(self):
        t, keys = self._build_degenerate_tree()
        self._checkRanges(t, keys)

    def testDeletes(self):
        # Delete keys in all possible orders, checking each tree along
        # the way.

        # This is a tough test.  Previous failure modes included:
        # 1. A variety of assertion failures in _checkRanges.
        # 2. Assorted "Invalid firstbucket pointer" failures at
        #    seemingly random times, coming out of the BTree destructor.
        # 3. Under Python 2.3 CVS, some baffling
        #      RuntimeWarning: tp_compare didn't return -1 or -2 for exception
        #    warnings, possibly due to memory corruption after a BTree
        #    goes insane.

        t, keys = self._build_degenerate_tree()
        for oneperm in permutations(keys):
            t, keys = self._build_degenerate_tree()
            for key in oneperm:
                t.remove(key)
                keys.remove(key)
                t._check()
                check(t)
                self._checkRanges(t, keys)
            # We removed all the keys, so the tree should be empty now.
            self.assertEqual(t.__getstate__(), None)

            # A damaged tree may trigger an "invalid firstbucket pointer"
            # failure at the time its destructor is invoked.  Try to force
            # that to happen now, so it doesn't look like a baffling failure
            # at some unrelated line.
            del t   # trigger destructor

class IIBucketTest(MappingBase):
    def setUp(self):
        self.t = IIBucket()
class IOBucketTest(MappingBase):
    def setUp(self):
        self.t = IOBucket()
class OIBucketTest(MappingBase):
    def setUp(self):
        self.t = OIBucket()
class OOBucketTest(MappingBase):
    def setUp(self):
        self.t = OOBucket()

class IITreeSetTest(NormalSetTests):
    def setUp(self):
        self.t = IITreeSet()
class IOTreeSetTest(NormalSetTests):
    def setUp(self):
        self.t = IOTreeSet()
class OITreeSetTest(NormalSetTests):
    def setUp(self):
        self.t = OITreeSet()
class OOTreeSetTest(NormalSetTests):
    def setUp(self):
        self.t = OOTreeSet()

class IISetTest(ExtendedSetTests):
    def setUp(self):
        self.t = IISet()
class IOSetTest(ExtendedSetTests):
    def setUp(self):
        self.t = IOSet()
class OISetTest(ExtendedSetTests):
    def setUp(self):
        self.t = OISet()
class OOSetTest(ExtendedSetTests):
    def setUp(self):
        self.t = OOSet()

class IIBTreeTest(BTreeTests):
    def setUp(self):
        self.t = IIBTree()
class IOBTreeTest(BTreeTests):
    def setUp(self):
        self.t = IOBTree()
class OIBTreeTest(BTreeTests):
    def setUp(self):
        self.t = OIBTree()
class OOBTreeTest(BTreeTests):
    def setUp(self):
        self.t = OOBTree()

# cmp error propagation tests

class DoesntLikeBeingCompared:
    def __cmp__(self,other):
        raise ValueError('incomparable')

class TestCmpError(TestCase):
    def testFoo(self):
        t = OOBTree()
        t['hello world'] = None
        try:
            t[DoesntLikeBeingCompared()] = None
        except ValueError,e:
            self.assertEqual(str(e), 'incomparable')
        else:
            self.fail('incomarable objects should not be allowed into '
                      'the tree')

def test_suite():
    s = TestSuite()

    for klass in (IIBucketTest,  IOBucketTest,  OIBucketTest,  OOBucketTest,
                  IITreeSetTest, IOTreeSetTest, OITreeSetTest, OOTreeSetTest,
                  IISetTest,     IOSetTest,     OISetTest,     OOSetTest,
                  IIBTreeTest,   IOBTreeTest,   OIBTreeTest,   OOBTreeTest,
                  # Note:  there is no TestOOBTrees.  The next three are
                  # checking for assorted TypeErrors, and when both keys
                  # and values oare objects (OO), there's nothing to test.
                  TestIIBTrees,  TestIOBTrees,  TestOIBTrees,
                  TestIOSets,
                  DegenerateBTree,
                  TestCmpError):
        s.addTest(makeSuite(klass))

    return s

## utility functions

def lsubtract(l1, l2):
    l1 = list(l1)
    l2 = list(l2)
    l = filter(lambda x, l1=l1: x not in l1, l2)
    l = l + filter(lambda x, l2=l2: x not in l2, l1)
    return l

def realseq(itemsob):
    return [x for x in itemsob]

def permutations(x):
    # Return a list of all permutations of list x.
    n = len(x)
    if n <= 1:
        return [x]
    result = []
    x0 = x[0]
    for i in range(n):
        # Build the (n-1)! permutations with x[i] in the first position.
        xcopy = x[:]
        first, xcopy[i] = xcopy[i], x0
        result.extend([[first] + p for p in permutations(xcopy[1:])])
    return result


def main():
    TextTestRunner().run(test_suite())

if __name__ == '__main__':
    main()
