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
"""Check loadSerial() on storages that support historical revisions."""

from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import \
     zodb_unpickle, zodb_pickle, snooze, handle_serials
from ZODB.utils import p64, u64

import transaction

ZERO = '\0'*8

class RevisionStorage:

    def checkLoadSerial(self):
        oid = self._storage.new_oid()
        revid = ZERO
        revisions = {}
        for i in range(31, 38):
            revid = self._dostore(oid, revid=revid, data=MinPO(i))
            revisions[revid] = MinPO(i)
        # Now make sure all the revisions have the correct value
        for revid, value in revisions.items():
            data = self._storage.loadSerial(oid, revid)
            self.assertEqual(zodb_unpickle(data), value)

    def checkLoadBefore(self):
        # Store 10 revisions of one object and then make sure that we
        # can get all the non-current revisions back.
        oid = self._storage.new_oid()
        revs = []
        revid = None
        for i in range(10):
            # We need to ensure that successive timestamps are at least
            # two apart, so that a timestamp exists that's unambiguously
            # between successive timestamps.  Each call to snooze()
            # guarantees that the next timestamp will be at least one
            # larger (and probably much more than that) than the previous
            # one.
            snooze()
            snooze()
            revid = self._dostore(oid, revid, data=MinPO(i))
            revs.append(self._storage.loadEx(oid, ""))

        prev = u64(revs[0][1])
        for i in range(1, 10):
            tid = revs[i][1]
            cur = u64(tid)
            middle = prev + (cur - prev) // 2
            assert prev < middle < cur  # else the snooze() trick failed
            prev = cur
            t = self._storage.loadBefore(oid, p64(middle))
            self.assert_(t is not None)
            data, start, end = t
            self.assertEqual(revs[i-1][0], data)
            self.assertEqual(tid, end)

    def checkLoadBeforeEdges(self):
        # Check the edges cases for a non-current load.
        oid = self._storage.new_oid()

        self.assertRaises(KeyError, self._storage.loadBefore,
                          oid, p64(0))

        revid1 = self._dostore(oid, data=MinPO(1))

        self.assertEqual(self._storage.loadBefore(oid, p64(0)), None)
        self.assertEqual(self._storage.loadBefore(oid, revid1), None)

        cur = p64(u64(revid1) + 1)
        data, start, end = self._storage.loadBefore(oid, cur)
        self.assertEqual(zodb_unpickle(data), MinPO(1))
        self.assertEqual(start, revid1)
        self.assertEqual(end, None)

        revid2 = self._dostore(oid, revid=revid1, data=MinPO(2))
        data, start, end = self._storage.loadBefore(oid, cur)
        self.assertEqual(zodb_unpickle(data), MinPO(1))
        self.assertEqual(start, revid1)
        self.assertEqual(end, revid2)

    def checkLoadBeforeOld(self):
        # Look for a very old revision.  With the BaseStorage implementation
        # this should require multple history() calls.
        oid = self._storage.new_oid()
        revs = []
        revid = None
        for i in range(50):
            revid = self._dostore(oid, revid, data=MinPO(i))
            revs.append(revid)

        data, start, end = self._storage.loadBefore(oid, revs[12])
        self.assertEqual(zodb_unpickle(data), MinPO(11))
        self.assertEqual(start, revs[11])
        self.assertEqual(end, revs[12])


    # XXX Is it okay to assume everyone testing against RevisionStorage
    # implements undo?

    def checkLoadBeforeUndo(self):
        # Do several transactions then undo them.
        oid = self._storage.new_oid()
        revid = None
        for i in range(5):
            revid = self._dostore(oid, revid, data=MinPO(i))
        revs = []
        for i in range(4):
            info = self._storage.undoInfo()
            tid = info[0]["id"]
            # Always undo the most recent txn, so the value will
            # alternate between 3 and 4.
            self._undo(tid, [oid], note="undo %d" % i)
            revs.append(self._storage.loadEx(oid, ""))

        prev_tid = None
        for i, (data, tid, ver) in enumerate(revs):
            t = self._storage.loadBefore(oid, p64(u64(tid) + 1))
            self.assertEqual(data, t[0])
            self.assertEqual(tid, t[1])
            if prev_tid:
                self.assert_(prev_tid < t[1])
            prev_tid = t[1]
            if i < 3:
                self.assertEqual(revs[i+1][1], t[2])
            else:
                self.assertEqual(None, t[2])

    def checkLoadBeforeConsecutiveTids(self):
        eq = self.assertEqual
        oid = self._storage.new_oid()
        def helper(tid, revid, x):
            data = zodb_pickle(MinPO(x))
            t = transaction.Transaction()
            try:
                self._storage.tpc_begin(t, p64(tid))
                r1 = self._storage.store(oid, revid, data, '', t)
                # Finish the transaction
                r2 = self._storage.tpc_vote(t)
                newrevid = handle_serials(oid, r1, r2)
                self._storage.tpc_finish(t)
            except:
                self._storage.tpc_abort(t)
                raise
            return newrevid
        revid1 = helper(1, None, 1)
        revid2 = helper(2, revid1, 2)
        revid3 = helper(3, revid2, 3)
        data, start_tid, end_tid = self._storage.loadBefore(oid, p64(2))
        eq(zodb_unpickle(data), MinPO(1))
        eq(u64(start_tid), 1)
        eq(u64(end_tid), 2)

    def checkLoadBeforeCreation(self):
        eq = self.assertEqual
        oid1 = self._storage.new_oid()
        oid2 = self._storage.new_oid()
        revid1 = self._dostore(oid1)
        revid2 = self._dostore(oid2)
        results = self._storage.loadBefore(oid2, revid2)
        eq(results, None)

    # XXX There are other edge cases to handle, including pack.
