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
"""Check transactionalUndo().

Any storage that supports transactionalUndo() must pass these tests.
"""

import time
import types
from ZODB import POSException
from ZODB.Transaction import Transaction
from ZODB.referencesf import referencesf
from ZODB.utils import u64, p64
from ZODB import DB

from Persistence import Persistent
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_pickle, zodb_unpickle

ZERO = '\0'*8

class C(Persistent):
    pass

def snooze():
    # In Windows, it's possible that two successive time.time() calls return
    # the same value.  Tim guarantees that time never runs backwards.  You
    # usually want to call this before you pack a storage, or must make other
    # guarantees about increasing timestamps.
    now = time.time()
    while now == time.time():
        time.sleep(0.1)

def listeq(L1, L2):
    """Return True if L1.sort() == L2.sort()"""
    c1 = L1[:]
    c2 = L2[:]
    c1.sort()
    c2.sort()
    return c1 == c2

class TransactionalUndoStorage:

    def _transaction_begin(self):
        self.__serials = {}

    def _transaction_store(self, oid, rev, data, vers, trans):
        r = self._storage.store(oid, rev, data, vers, trans)
        if r:
            if type(r) == types.StringType:
                self.__serials[oid] = r
            else:
                for oid, serial in r:
                    self.__serials[oid] = serial

    def _transaction_vote(self, trans):
        r = self._storage.tpc_vote(trans)
        if r:
            for oid, serial in r:
                self.__serials[oid] = serial

    def _transaction_newserial(self, oid):
        return self.__serials[oid]

    def _multi_obj_transaction(self, objs):
        newrevs = {}
        t = Transaction()
        self._storage.tpc_begin(t)
        self._transaction_begin()
        for oid, rev, data in objs:
            self._transaction_store(oid, rev, data, '', t)
            newrevs[oid] = None
        self._transaction_vote(t)
        self._storage.tpc_finish(t)
        for oid in newrevs.keys():
            newrevs[oid] = self._transaction_newserial(oid)
        return newrevs

    def _iterate(self):
        """Iterate over the storage in its final state."""
        # This is testing that the iterator() code works correctly.
        # The hasattr() guards against ZEO, which doesn't support iterator.
        if not hasattr(self._storage, "iterator"):
            return
        iter = self._storage.iterator()
        for txn in iter:
            for rec in txn:
                pass

    def checkSimpleTransactionalUndo(self):
        eq = self.assertEqual
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=MinPO(23))
        revid = self._dostore(oid, revid=revid, data=MinPO(24))
        revid = self._dostore(oid, revid=revid, data=MinPO(25))

        info = self._storage.undoInfo()
        tid = info[0]['id']
        # Now start an undo transaction
        t = Transaction()
        t.note('undo1')
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(24))
        # Do another one
        info = self._storage.undoInfo()
        tid = info[2]['id']
        t = Transaction()
        t.note('undo2')
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(23))
        # Try to undo the first record
        info = self._storage.undoInfo()
        tid = info[4]['id']
        t = Transaction()
        t.note('undo3')
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 1)
        eq(oids[0], oid)
        # This should fail since we've undone the object's creation
        self.assertRaises(KeyError,
                          self._storage.load, oid, '')
        # And now let's try to redo the object's creation
        info = self._storage.undoInfo()
        tid = info[0]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(23))
        self._iterate()

    def checkCreationUndoneGetSerial(self):
        # create an object
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=MinPO(23))
        # undo its creation
        info = self._storage.undoInfo()
        tid = info[0]['id']
        t = Transaction()
        t.note('undo1')
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        # Check that calling getSerial on an uncreated object raises a KeyError
        # The current version of FileStorage fails this test
        self.assertRaises(KeyError, self._storage.getSerial, oid)

    def checkUndoCreationBranch1(self):
        eq = self.assertEqual
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=MinPO(11))
        revid = self._dostore(oid, revid=revid, data=MinPO(12))
        # Undo the last transaction
        info = self._storage.undoInfo()
        tid = info[0]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(11))
        # Now from here, we can either redo the last undo, or undo the object
        # creation.  Let's undo the object creation.
        info = self._storage.undoInfo()
        tid = info[2]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 1)
        eq(oids[0], oid)
        self.assertRaises(KeyError, self._storage.load, oid, '')
        self._iterate()

    def checkUndoCreationBranch2(self):
        eq = self.assertEqual
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=MinPO(11))
        revid = self._dostore(oid, revid=revid, data=MinPO(12))
        # Undo the last transaction
        info = self._storage.undoInfo()
        tid = info[0]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(11))
        # Now from here, we can either redo the last undo, or undo the object
        # creation.  Let's redo the last undo
        info = self._storage.undoInfo()
        tid = info[0]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(12))
        self._iterate()

    def checkTwoObjectUndo(self):
        eq = self.assertEqual
        # Convenience
        p31, p32, p51, p52 = map(zodb_pickle,
                                 map(MinPO, (31, 32, 51, 52)))
        oid1 = self._storage.new_oid()
        oid2 = self._storage.new_oid()
        revid1 = revid2 = ZERO
        # Store two objects in the same transaction
        t = Transaction()
        self._storage.tpc_begin(t)
        self._transaction_begin()
        self._transaction_store(oid1, revid1, p31, '', t)
        self._transaction_store(oid2, revid2, p51, '', t)
        # Finish the transaction
        self._transaction_vote(t)
        revid1 = self._transaction_newserial(oid1)
        revid2 = self._transaction_newserial(oid2)
        self._storage.tpc_finish(t)
        eq(revid1, revid2)
        # Update those same two objects
        t = Transaction()
        self._storage.tpc_begin(t)
        self._transaction_begin()
        self._transaction_store(oid1, revid1, p32, '', t)
        self._transaction_store(oid2, revid2, p52, '', t)
        # Finish the transaction
        self._transaction_vote(t)
        revid1 = self._transaction_newserial(oid1)
        revid2 = self._transaction_newserial(oid2)
        self._storage.tpc_finish(t)
        eq(revid1, revid2)
        # Make sure the objects have the current value
        data, revid1 = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(32))
        data, revid2 = self._storage.load(oid2, '')
        eq(zodb_unpickle(data), MinPO(52))
        # Now attempt to undo the transaction containing two objects
        info = self._storage.undoInfo()
        tid = info[0]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 2)
        self.failUnless(oid1 in oids)
        self.failUnless(oid2 in oids)
        data, revid1 = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(31))
        data, revid2 = self._storage.load(oid2, '')
        eq(zodb_unpickle(data), MinPO(51))
        self._iterate()

    def checkTwoObjectUndoAtOnce(self):
        # Convenience
        eq = self.assertEqual
        unless = self.failUnless
        p30, p31, p32, p50, p51, p52 = map(zodb_pickle,
                                           map(MinPO,
                                               (30, 31, 32, 50, 51, 52)))
        oid1 = self._storage.new_oid()
        oid2 = self._storage.new_oid()
        revid1 = revid2 = ZERO
        # Store two objects in the same transaction
        d = self._multi_obj_transaction([(oid1, revid1, p30),
                                         (oid2, revid2, p50),
                                         ])
        eq(d[oid1], d[oid2])
        # Update those same two objects
        d = self._multi_obj_transaction([(oid1, d[oid1], p31),
                                         (oid2, d[oid2], p51),
                                         ])
        eq(d[oid1], d[oid2])
        # Update those same two objects
        d = self._multi_obj_transaction([(oid1, d[oid1], p32),
                                         (oid2, d[oid2], p52),
                                         ])
        eq(d[oid1], d[oid2])
        revid1 = self._transaction_newserial(oid1)
        revid2 = self._transaction_newserial(oid2)
        eq(revid1, revid2)
        # Make sure the objects have the current value
        data, revid1 = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(32))
        data, revid2 = self._storage.load(oid2, '')
        eq(zodb_unpickle(data), MinPO(52))
        # Now attempt to undo the transaction containing two objects
        info = self._storage.undoInfo()
        tid = info[0]['id']
        tid1 = info[1]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        oids1 = self._storage.transactionalUndo(tid1, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        # We get the finalization stuff called an extra time:
##        self._storage.tpc_vote(t)
##        self._storage.tpc_finish(t)
        eq(len(oids), 2)
        eq(len(oids1), 2)
        unless(oid1 in oids)
        unless(oid2 in oids)
        data, revid1 = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(30))
        data, revid2 = self._storage.load(oid2, '')
        eq(zodb_unpickle(data), MinPO(50))
        # Now try to undo the one we just did to undo, whew
        info = self._storage.undoInfo()
        tid = info[0]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 2)
        unless(oid1 in oids)
        unless(oid2 in oids)
        data, revid1 = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(32))
        data, revid2 = self._storage.load(oid2, '')
        eq(zodb_unpickle(data), MinPO(52))
        self._iterate()

    def checkTwoObjectUndoAgain(self):
        eq = self.assertEqual
        p31, p32, p33, p51, p52, p53 = map(
            zodb_pickle,
            map(MinPO, (31, 32, 33, 51, 52, 53)))
        # Like the above, but the first revision of the objects are stored in
        # different transactions.
        oid1 = self._storage.new_oid()
        oid2 = self._storage.new_oid()
        revid1 = self._dostore(oid1, data=p31, already_pickled=1)
        revid2 = self._dostore(oid2, data=p51, already_pickled=1)
        # Update those same two objects
        t = Transaction()
        self._storage.tpc_begin(t)
        self._transaction_begin()
        self._transaction_store(oid1, revid1, p32, '', t)
        self._transaction_store(oid2, revid2, p52, '', t)
        # Finish the transaction
        self._transaction_vote(t)
        self._storage.tpc_finish(t)
        revid1 = self._transaction_newserial(oid1)
        revid2 = self._transaction_newserial(oid2)
        eq(revid1, revid2)
        # Now attempt to undo the transaction containing two objects
        info = self._storage.undoInfo()
        tid = info[0]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 2)
        self.failUnless(oid1 in oids)
        self.failUnless(oid2 in oids)
        data, revid1 = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(31))
        data, revid2 = self._storage.load(oid2, '')
        eq(zodb_unpickle(data), MinPO(51))
        # Like the above, but this time, the second transaction contains only
        # one object.
        t = Transaction()
        self._storage.tpc_begin(t)
        self._transaction_begin()
        self._transaction_store(oid1, revid1, p33, '', t)
        self._transaction_store(oid2, revid2, p53, '', t)
        # Finish the transaction
        self._transaction_vote(t)
        self._storage.tpc_finish(t)
        revid1 = self._transaction_newserial(oid1)
        revid2 = self._transaction_newserial(oid2)
        eq(revid1, revid2)
        # Update in different transactions
        revid1 = self._dostore(oid1, revid=revid1, data=MinPO(34))
        revid2 = self._dostore(oid2, revid=revid2, data=MinPO(54))
        # Now attempt to undo the transaction containing two objects
        info = self._storage.undoInfo()
        tid = info[1]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 1)
        self.failUnless(oid1 in oids)
        self.failUnless(not oid2 in oids)
        data, revid1 = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(33))
        data, revid2 = self._storage.load(oid2, '')
        eq(zodb_unpickle(data), MinPO(54))
        self._iterate()


    def checkNotUndoable(self):
        eq = self.assertEqual
        # Set things up so we've got a transaction that can't be undone
        oid = self._storage.new_oid()
        revid_a = self._dostore(oid, data=MinPO(51))
        revid_b = self._dostore(oid, revid=revid_a, data=MinPO(52))
        revid_c = self._dostore(oid, revid=revid_b, data=MinPO(53))
        # Start the undo
        info = self._storage.undoInfo()
        tid = info[1]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        self.assertRaises(POSException.UndoError,
                          self._storage.transactionalUndo,
                          tid, t)
        self._storage.tpc_abort(t)
        # Now have more fun: object1 and object2 are in the same transaction,
        # which we'll try to undo to, but one of them has since modified in
        # different transaction, so the undo should fail.
        oid1 = oid
        revid1 = revid_c
        oid2 = self._storage.new_oid()
        revid2 = ZERO
        p81, p82, p91, p92 = map(zodb_pickle,
                                 map(MinPO, (81, 82, 91, 92)))

        t = Transaction()
        self._storage.tpc_begin(t)
        self._transaction_begin()
        self._transaction_store(oid1, revid1, p81, '', t)
        self._transaction_store(oid2, revid2, p91, '', t)
        self._transaction_vote(t)
        self._storage.tpc_finish(t)
        revid1 = self._transaction_newserial(oid1)
        revid2 = self._transaction_newserial(oid2)
        eq(revid1, revid2)
        # Make sure the objects have the expected values
        data, revid_11 = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(81))
        data, revid_22 = self._storage.load(oid2, '')
        eq(zodb_unpickle(data), MinPO(91))
        eq(revid_11, revid1)
        eq(revid_22, revid2)
        # Now modify oid2
        revid2 = self._dostore(oid2, revid=revid2, data=MinPO(92))
        self.assertNotEqual(revid1, revid2)
        self.assertNotEqual(revid2, revid_22)
        info = self._storage.undoInfo()
        tid = info[1]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        self.assertRaises(POSException.UndoError,
                          self._storage.transactionalUndo,
                          tid, t)
        self._storage.tpc_abort(t)
        self._iterate()

    def checkTransactionalUndoAfterPack(self):
        eq = self.assertEqual
        # Add a few object revisions
        oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=MinPO(51))
        packtime = time.time()
        snooze()                # time.time() now distinct from packtime
        revid2 = self._dostore(oid, revid=revid1, data=MinPO(52))
        revid3 = self._dostore(oid, revid=revid2, data=MinPO(53))
        # Now get the undo log
        info = self._storage.undoInfo()
        eq(len(info), 3)
        tid = info[0]['id']
        # Now pack just the initial revision of the object.  We need the
        # second revision otherwise we won't be able to undo the third
        # revision!
        self._storage.pack(packtime, referencesf)
        # Make some basic assertions about the undo information now
        info2 = self._storage.undoInfo()
        eq(len(info2), 2)
        # And now attempt to undo the last transaction
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, '')
        # The object must now be at the second state
        eq(zodb_unpickle(data), MinPO(52))
        self._iterate()

    def checkTransactionalUndoAfterPackWithObjectUnlinkFromRoot(self):
        eq = self.assertEqual
        db = DB(self._storage)
        conn = db.open()
        root = conn.root()

        o1 = C()
        o2 = C()
        root['obj'] = o1
        o1.obj = o2
        txn = get_transaction()
        txn.note('o1 -> o2')
        txn.commit()
        now = packtime = time.time()
        while packtime <= now:
            packtime = time.time()

        o3 = C()
        o2.obj = o3
        txn = get_transaction()
        txn.note('o1 -> o2 -> o3')
        txn.commit()

        o1.obj = o3
        txn = get_transaction()
        txn.note('o1 -> o3')
        txn.commit()

        log = self._storage.undoLog()
        eq(len(log), 4)
        for entry in zip(log, ('o1 -> o3', 'o1 -> o2 -> o3',
                               'o1 -> o2', 'initial database creation')):
            eq(entry[0]['description'], entry[1])

        self._storage.pack(packtime, referencesf)

        log = self._storage.undoLog()
        for entry in zip(log, ('o1 -> o3', 'o1 -> o2 -> o3')):
            eq(entry[0]['description'], entry[1])

        tid = log[0]['id']
        db.undo(tid)
        txn = get_transaction()
        txn.note('undo')
        txn.commit()
        # undo does a txn-undo, but doesn't invalidate
        conn.sync()

        log = self._storage.undoLog()
        for entry in zip(log, ('undo', 'o1 -> o3', 'o1 -> o2 -> o3')):
            eq(entry[0]['description'], entry[1])

        eq(o1.obj, o2)
        eq(o1.obj.obj, o3)
        self._iterate()

    def checkPackAfterUndoDeletion(self):
        db = DB(self._storage)
        cn = db.open()
        root = cn.root()

        pack_times = []
        def set_pack_time():
            snooze()
            pack_times.append(time.time())

        root["key0"] = MinPO(0)
        root["key1"] = MinPO(1)
        root["key2"] = MinPO(2)
        txn = get_transaction()
        txn.note("create 3 keys")
        txn.commit()

        set_pack_time()

        del root["key1"]
        txn = get_transaction()
        txn.note("delete 1 key")
        txn.commit()

        set_pack_time()

        root._p_deactivate()
        cn.sync()
        self.assert_(listeq(root.keys(), ["key0", "key2"]))

        L = db.undoInfo()
        db.undo(L[0]["id"])
        txn = get_transaction()
        txn.note("undo deletion")
        txn.commit()

        set_pack_time()

        root._p_deactivate()
        cn.sync()
        self.assert_(listeq(root.keys(), ["key0", "key1", "key2"]))

        for t in pack_times:
            self._storage.pack(t, referencesf)

            root._p_deactivate()
            cn.sync()
            self.assert_(listeq(root.keys(), ["key0", "key1", "key2"]))
            for i in range(3):
                obj = root["key%d" % i]
                self.assertEqual(obj.value, i)
            root.items()
            self._inter_pack_pause()

    def checkPackAfterUndoManyTimes(self):
        db = DB(self._storage)
        cn = db.open()
        rt = cn.root()

        rt["test"] = MinPO(1)
        get_transaction().commit()
        rt["test2"] = MinPO(2)
        get_transaction().commit()
        rt["test"] = MinPO(3)
        txn = get_transaction()
        txn.note("root of undo")
        txn.commit()

        packtimes = []
        for i in range(10):
            L = db.undoInfo()
            db.undo(L[0]["id"])
            txn = get_transaction()
            txn.note("undo %d" % i)
            txn.commit()
            rt._p_deactivate()
            cn.sync()

            self.assertEqual(rt["test"].value, i % 2 and 3 or 1)
            self.assertEqual(rt["test2"].value, 2)

            packtimes.append(time.time())
            snooze()

        for t in packtimes:
            self._storage.pack(t, referencesf)
            cn.sync()
            cn._cache.clear()
            # The last undo set the value to 3 and pack should
            # never change that.
            self.assertEqual(rt["test"].value, 3)
            self.assertEqual(rt["test2"].value, 2)
            self._inter_pack_pause()

    def _inter_pack_pause(self):
        # DirectoryStorage needs a pause between packs,
        # most other storages dont.
        pass

    def checkTransactionalUndoIterator(self):
        # check that data_txn set in iterator makes sense
        if not hasattr(self._storage, "iterator"):
            return

        s = self._storage

        BATCHES = 4
        OBJECTS = 4

        orig = []
        for i in range(BATCHES):
            t = Transaction()
            tid = p64(i + 1)
            s.tpc_begin(t, tid)
            for j in range(OBJECTS):
                oid = s.new_oid()
                obj = MinPO(i * OBJECTS + j)
                revid = s.store(oid, None, zodb_pickle(obj), '', t)
                orig.append((tid, oid, revid))
            s.tpc_vote(t)
            s.tpc_finish(t)

        i = 0
        for tid, oid, revid in orig:
            self._dostore(oid, revid=revid, data=MinPO(revid),
                          description="update %s" % i)

        # Undo the OBJECTS transactions that modified objects created
        # in the ith original transaction.

        def undo(i):
            info = s.undoInfo()
            t = Transaction()
            s.tpc_begin(t)
            base = i * OBJECTS + i
            for j in range(OBJECTS):
                tid = info[base + j]['id']
                s.transactionalUndo(tid, t)
            s.tpc_vote(t)
            s.tpc_finish(t)

        for i in range(BATCHES):
            undo(i)

        # There are now (2 + OBJECTS) * BATCHES transactions:
        #     BATCHES original transactions, followed by
        #     OBJECTS * BATCHES modifications, followed by
        #     BATCHES undos

        iter = s.iterator()
        offset = 0

        eq = self.assertEqual

        for i in range(BATCHES):
            txn = iter[offset]
            offset += 1

            tid = p64(i + 1)
            eq(txn.tid, tid)

            L1 = [(rec.oid, rec.serial, rec.data_txn) for rec in txn]
            L2 = [(oid, revid, None) for _tid, oid, revid in orig
                  if _tid == tid]

            eq(L1, L2)

        for i in range(BATCHES * OBJECTS):
            txn = iter[offset]
            offset += 1
            eq(len([rec for rec in txn if rec.data_txn is None]), 1)

        for i in range(BATCHES):
            txn = iter[offset]
            offset += 1

            # The undos are performed in reverse order.
            otid = p64(BATCHES - i)
            L1 = [(rec.oid, rec.data_txn) for rec in txn]
            L2 = [(oid, otid) for _tid, oid, revid in orig
                  if _tid == otid]
            L1.sort()
            L2.sort()
            eq(L1, L2)

        self.assertRaises(IndexError, iter.__getitem__, offset)

    def checkUndoLogMetadata(self):
        # test that the metadata is correct in the undo log
        t = get_transaction()
        t.note('t1')
        t.setExtendedInfo('k2','this is transaction metadata')
        t.setUser('u3',path='p3')
        db = DB(self._storage)
        conn = db.open()
        root = conn.root()
        o1 = C()
        root['obj'] = o1
        txn = get_transaction()
        txn.commit()
        l = self._storage.undoLog()
        self.assertEqual(len(l),2)
        d = l[0]
        self.assertEqual(d['description'],'t1')
        self.assertEqual(d['k2'],'this is transaction metadata')
        self.assertEqual(d['user_name'],'p3 u3')
