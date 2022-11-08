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
"""Check undo().

Any storage that supports undo() must pass these tests.
"""
import time

from six import PY3

import transaction
from persistent import Persistent

from ZODB import DB
from ZODB import POSException
from ZODB.Connection import TransactionMetaData
from ZODB.serialize import referencesf
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import ZERO
from ZODB.tests.StorageTestBase import zodb_pickle
from ZODB.tests.StorageTestBase import zodb_unpickle
from ZODB.utils import load_current
from ZODB.utils import p64


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
    """Return True if L1.sort() == L2.sort()

    Also support iterators.
    """
    return sorted(L1) == sorted(L2)


class TransactionalUndoStorage(object):

    def _multi_obj_transaction(self, objs):
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        for oid, rev, data in objs:
            self._storage.store(oid, rev, data, '', t)
        self._storage.tpc_vote(t)
        return self._storage.tpc_finish(t)

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

    def _begin_undos_vote(self, t, *tids):
        self._storage.tpc_begin(t)
        oids = set()
        for tid in tids:
            undo_result = self._storage.undo(tid, t)
            if undo_result:
                oids.update(undo_result[1])
        oids.update(self._storage.tpc_vote(t) or ())
        return oids

    def undo(self, tid, note=None):
        t = TransactionMetaData()
        if note is not None:
            t.note(note)
        oids = self._begin_undos_vote(t, tid)
        self._storage.tpc_finish(t)
        return oids

    def checkSimpleTransactionalUndo(self):
        eq = self.assertEqual
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=MinPO(23))
        revid = self._dostore(oid, revid=revid, data=MinPO(24))
        revid = self._dostore(oid, revid=revid, data=MinPO(25))

        info = self._storage.undoInfo()
        # Now start an undo transaction
        self._undo(info[0]["id"], [oid], note="undo1")
        data, revid = load_current(self._storage, oid)
        eq(zodb_unpickle(data), MinPO(24))

        # Do another one
        info = self._storage.undoInfo()
        self._undo(info[2]["id"], [oid], note="undo2")
        data, revid = load_current(self._storage, oid)
        eq(zodb_unpickle(data), MinPO(23))

        # Try to undo the first record
        info = self._storage.undoInfo()
        self._undo(info[4]["id"], [oid], note="undo3")
        # This should fail since we've undone the object's creation
        self.assertRaises(KeyError, load_current, self._storage, oid)

        # And now let's try to redo the object's creation
        info = self._storage.undoInfo()
        self._undo(info[0]["id"], [oid])
        data, revid = load_current(self._storage, oid)
        eq(zodb_unpickle(data), MinPO(23))
        self._iterate()

    def checkCreationUndoneGetTid(self):
        # create an object
        oid = self._storage.new_oid()
        self._dostore(oid, data=MinPO(23))
        # undo its creation
        info = self._storage.undoInfo()
        tid = info[0]['id']
        self.undo(tid, 'undo1')
        # Check that calling getTid on an uncreated object raises a KeyError
        # The current version of FileStorage fails this test
        self.assertRaises(KeyError, self._storage.getTid, oid)

    def checkUndoCreationBranch1(self):
        eq = self.assertEqual
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=MinPO(11))
        revid = self._dostore(oid, revid=revid, data=MinPO(12))
        # Undo the last transaction
        info = self._storage.undoInfo()
        self._undo(info[0]['id'], [oid])
        data, revid = load_current(self._storage, oid)
        eq(zodb_unpickle(data), MinPO(11))

        # Now from here, we can either redo the last undo, or undo the object
        # creation.  Let's undo the object creation.
        info = self._storage.undoInfo()
        self._undo(info[2]['id'], [oid])
        self.assertRaises(KeyError, load_current, self._storage, oid)

        # Loading current data via loadBefore should raise a POSKeyError too:
        self.assertRaises(KeyError, self._storage.loadBefore, oid,
                          b'\x7f\xff\xff\xff\xff\xff\xff\xff')
        self._iterate()

    def checkUndoCreationBranch2(self):
        eq = self.assertEqual
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=MinPO(11))
        revid = self._dostore(oid, revid=revid, data=MinPO(12))
        # Undo the last transaction
        info = self._storage.undoInfo()
        self._undo(info[0]['id'], [oid])
        data, revid = load_current(self._storage, oid)
        eq(zodb_unpickle(data), MinPO(11))
        # Now from here, we can either redo the last undo, or undo the object
        # creation.  Let's redo the last undo
        info = self._storage.undoInfo()
        self._undo(info[0]['id'], [oid])
        data, revid = load_current(self._storage, oid)
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
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        self._storage.store(oid1, revid1, p31, '', t)
        self._storage.store(oid2, revid2, p51, '', t)
        # Finish the transaction
        self._storage.tpc_vote(t)
        tid = self._storage.tpc_finish(t)
        # Update those same two objects
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        self._storage.store(oid1, tid, p32, '', t)
        self._storage.store(oid2, tid, p52, '', t)
        # Finish the transaction
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        # Make sure the objects have the current value
        data, revid1 = load_current(self._storage, oid1)
        eq(zodb_unpickle(data), MinPO(32))
        data, revid2 = load_current(self._storage, oid2)
        eq(zodb_unpickle(data), MinPO(52))

        # Now attempt to undo the transaction containing two objects
        info = self._storage.undoInfo()
        self._undo(info[0]['id'], [oid1, oid2])
        data, revid1 = load_current(self._storage, oid1)
        eq(zodb_unpickle(data), MinPO(31))
        data, revid2 = load_current(self._storage, oid2)
        eq(zodb_unpickle(data), MinPO(51))
        self._iterate()

    def checkTwoObjectUndoAtOnce(self):
        # Convenience
        eq = self.assertEqual
        p30, p31, p32, p50, p51, p52 = map(zodb_pickle,
                                           map(MinPO,
                                               (30, 31, 32, 50, 51, 52)))
        oid1 = self._storage.new_oid()
        oid2 = self._storage.new_oid()
        # Store two objects in the same transaction
        tid = self._multi_obj_transaction([(oid1, ZERO, p30),
                                           (oid2, ZERO, p50),
                                           ])
        # Update those same two objects
        tid = self._multi_obj_transaction([(oid1, tid, p31),
                                           (oid2, tid, p51),
                                           ])
        # Update those same two objects
        tid = self._multi_obj_transaction([(oid1, tid, p32),
                                           (oid2, tid, p52),
                                           ])
        # Make sure the objects have the current value
        data, revid1 = load_current(self._storage, oid1)
        eq(zodb_unpickle(data), MinPO(32))
        data, revid2 = load_current(self._storage, oid2)
        eq(zodb_unpickle(data), MinPO(52))
        # Now attempt to undo the transaction containing two objects
        info = self._storage.undoInfo()
        tid = info[0]['id']
        tid1 = info[1]['id']
        t = TransactionMetaData()
        oids = self._begin_undos_vote(t, tid, tid1)
        serial = self._storage.tpc_finish(t)
        # We may get the finalization stuff called an extra time,
        # depending on the implementation.
        if serial is None:
            self.assertEqual(oids, {oid1, oid2})
        data, revid1 = load_current(self._storage, oid1)
        eq(zodb_unpickle(data), MinPO(30))
        data, revid2 = load_current(self._storage, oid2)
        eq(zodb_unpickle(data), MinPO(50))

        # Now try to undo the one we just did to undo, whew
        info = self._storage.undoInfo()
        self._undo(info[0]['id'], [oid1, oid2])
        data, revid1 = load_current(self._storage, oid1)
        eq(zodb_unpickle(data), MinPO(32))
        data, revid2 = load_current(self._storage, oid2)
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
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        self._storage.store(oid1, revid1, p32, '', t)
        self._storage.store(oid2, revid2, p52, '', t)
        # Finish the transaction
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        # Now attempt to undo the transaction containing two objects
        info = self._storage.undoInfo()
        self._undo(info[0]["id"], [oid1, oid2])
        data, revid1 = load_current(self._storage, oid1)
        eq(zodb_unpickle(data), MinPO(31))
        data, revid2 = load_current(self._storage, oid2)
        eq(zodb_unpickle(data), MinPO(51))
        # Like the above, but this time, the second transaction contains only
        # one object.
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        self._storage.store(oid1, revid1, p33, '', t)
        self._storage.store(oid2, revid2, p53, '', t)
        # Finish the transaction
        self._storage.tpc_vote(t)
        tid = self._storage.tpc_finish(t)
        # Update in different transactions
        revid1 = self._dostore(oid1, revid=tid, data=MinPO(34))
        revid2 = self._dostore(oid2, revid=tid, data=MinPO(54))
        # Now attempt to undo the transaction containing two objects
        info = self._storage.undoInfo()
        self.undo(info[1]['id'])
        data, revid1 = load_current(self._storage, oid1)
        eq(zodb_unpickle(data), MinPO(33))
        data, revid2 = load_current(self._storage, oid2)
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
        t = TransactionMetaData()
        self.assertRaises(POSException.UndoError,
                          self._begin_undos_vote, t, tid)
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

        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        self._storage.store(oid1, revid1, p81, '', t)
        self._storage.store(oid2, revid2, p91, '', t)
        self._storage.tpc_vote(t)
        tid = self._storage.tpc_finish(t)
        # Make sure the objects have the expected values
        data, revid_11 = load_current(self._storage, oid1)
        eq(zodb_unpickle(data), MinPO(81))
        data, revid_22 = load_current(self._storage, oid2)
        eq(zodb_unpickle(data), MinPO(91))
        eq(revid_11, tid)
        eq(revid_22, tid)
        # Now modify oid2
        revid2 = self._dostore(oid2, tid, MinPO(92))
        self.assertNotEqual(tid, revid2)
        info = self._storage.undoInfo()
        tid = info[1]['id']
        t = TransactionMetaData()
        self.assertRaises(POSException.UndoError,
                          self._begin_undos_vote, t, tid)
        self._storage.tpc_abort(t)
        self._iterate()

    def checkTransactionalUndoAfterPack(self):
        # bwarsaw Date: Thu Mar 28 21:04:43 2002 UTC
        # This is a test which should provoke the underlying bug in
        # transactionalUndo() on a standby storage.  If our hypothesis
        # is correct, the bug is in FileStorage, and is caused by
        # encoding the file position in the `id' field of the undoLog
        # information.  Note that Full just encodes the tid, but this
        # is a problem for FileStorage (we have a strategy for fixing
        # this).

        # So, basically, this makes sure that undo info doesn't depend
        # on file positions.  We change the file positions in an undo
        # record by packing.

        # Add a few object revisions
        oid = b'\0'*8
        revid0 = self._dostore(oid, data=MinPO(50))
        revid1 = self._dostore(oid, revid=revid0, data=MinPO(51))
        snooze()
        packtime = time.time()
        snooze()                # time.time() now distinct from packtime
        revid2 = self._dostore(oid, revid=revid1, data=MinPO(52))
        self._dostore(oid, revid=revid2, data=MinPO(53))
        # Now get the undo log
        info = self._storage.undoInfo()
        self.assertEqual(len(info), 4)
        tid = info[0]['id']
        # Now pack just the initial revision of the object.  We need the
        # second revision otherwise we won't be able to undo the third
        # revision!
        self._storage.pack(packtime, referencesf)
        # Make some basic assertions about the undo information now
        info2 = self._storage.undoInfo()
        self.assertEqual(len(info2), 2)
        # And now attempt to undo the last transaction
        undone, = self.undo(tid)
        self.assertEqual(undone, oid)
        data, revid = load_current(self._storage, oid)
        # The object must now be at the second state
        self.assertEqual(zodb_unpickle(data), MinPO(52))
        self._iterate()

    def checkTransactionalUndoAfterPackWithObjectUnlinkFromRoot(self):
        eq = self.assertEqual
        db = DB(self._storage)
        conn = db.open()
        try:
            root = conn.root()

            o1 = C()
            o2 = C()
            root['obj'] = o1
            o1.obj = o2
            txn = transaction.get()
            txn.note(u'o1 -> o2')
            txn.commit()
            now = packtime = time.time()
            while packtime <= now:
                packtime = time.time()

            o3 = C()
            o2.obj = o3
            txn = transaction.get()
            txn.note(u'o1 -> o2 -> o3')
            txn.commit()

            o1.obj = o3
            txn = transaction.get()
            txn.note(u'o1 -> o3')
            txn.commit()

            log = self._storage.undoLog()
            eq(len(log), 4)
            for entry in zip(log, (b'o1 -> o3', b'o1 -> o2 -> o3',
                                   b'o1 -> o2', b'initial database creation')):
                eq(entry[0]['description'], entry[1])

            self._storage.pack(packtime, referencesf)

            log = self._storage.undoLog()
            for entry in zip(log, (b'o1 -> o3', b'o1 -> o2 -> o3')):
                eq(entry[0]['description'], entry[1])

            tid = log[0]['id']
            db.undo(tid)
            txn = transaction.get()
            txn.note(u'undo')
            txn.commit()
            # undo does a txn-undo, but doesn't invalidate
            conn.sync()

            log = self._storage.undoLog()
            for entry in zip(log, (b'undo', b'o1 -> o3', b'o1 -> o2 -> o3')):
                eq(entry[0]['description'], entry[1])

            eq(o1.obj, o2)
            eq(o1.obj.obj, o3)
            self._iterate()
        finally:
            conn.close()
            db.close()

    def checkPackAfterUndoDeletion(self):
        db = DB(self._storage)
        cn = db.open()
        try:
            root = cn.root()

            pack_times = []

            def set_pack_time():
                pack_times.append(time.time())
                snooze()

            root["key0"] = MinPO(0)
            root["key1"] = MinPO(1)
            root["key2"] = MinPO(2)
            txn = transaction.get()
            txn.note(u"create 3 keys")
            txn.commit()

            set_pack_time()

            del root["key1"]
            txn = transaction.get()
            txn.note(u"delete 1 key")
            txn.commit()

            set_pack_time()

            root._p_deactivate()
            cn.sync()
            self.assertTrue(listeq(root.keys(), ["key0", "key2"]))

            L = db.undoInfo()
            db.undo(L[0]["id"])
            txn = transaction.get()
            txn.note(u"undo deletion")
            txn.commit()

            set_pack_time()

            root._p_deactivate()
            cn.sync()
            self.assertTrue(listeq(root.keys(), ["key0", "key1", "key2"]))

            for t in pack_times:
                self._storage.pack(t, referencesf)

                root._p_deactivate()
                cn.sync()
                self.assertTrue(listeq(root.keys(), ["key0", "key1", "key2"]))
                for i in range(3):
                    obj = root["key%d" % i]
                    self.assertEqual(obj.value, i)
                root.items()
                self._inter_pack_pause()
        finally:
            cn.close()
            db.close()

    def checkPackAfterUndoManyTimes(self):
        db = DB(self._storage)
        cn = db.open()
        try:
            rt = cn.root()

            rt["test"] = MinPO(1)
            transaction.commit()
            rt["test2"] = MinPO(2)
            transaction.commit()
            rt["test"] = MinPO(3)
            txn = transaction.get()
            txn.note(u"root of undo")
            txn.commit()

            packtimes = []
            for i in range(10):
                L = db.undoInfo()
                db.undo(L[0]["id"])
                txn = transaction.get()
                txn.note(u"undo %d" % i)
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

                # TODO:  Is _cache supposed to have a clear() method, or not?
                # cn._cache.clear()

                # The last undo set the value to 3 and pack should
                # never change that.
                self.assertEqual(rt["test"].value, 3)
                self.assertEqual(rt["test2"].value, 2)
                self._inter_pack_pause()
        finally:
            cn.close()
            db.close()

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
            t = TransactionMetaData()
            tid = p64(i + 1)
            s.tpc_begin(t, tid)
            for j in range(OBJECTS):
                oid = s.new_oid()
                obj = MinPO(i * OBJECTS + j)
                s.store(oid, None, zodb_pickle(obj), '', t)
                orig.append((tid, oid))
            s.tpc_vote(t)
            s.tpc_finish(t)

        orig = [(tid, oid, s.getTid(oid)) for tid, oid in orig]

        i = 0
        for tid, oid, revid in orig:
            self._dostore(oid, revid=revid, data=MinPO(revid),
                          description="update %s" % i)

        # Undo the OBJECTS transactions that modified objects created
        # in the ith original transaction.

        def undo(i):
            info = s.undoInfo()
            t = TransactionMetaData()
            s.tpc_begin(t)
            base = i * OBJECTS + i
            for j in range(OBJECTS):
                tid = info[base + j]['id']
                s.undo(tid, t)
            s.tpc_vote(t)
            s.tpc_finish(t)

        for i in range(BATCHES):
            undo(i)

        # There are now (2 + OBJECTS) * BATCHES transactions:
        #     BATCHES original transactions, followed by
        #     OBJECTS * BATCHES modifications, followed by
        #     BATCHES undos

        transactions = s.iterator()
        eq = self.assertEqual

        for i in range(BATCHES):
            txn = next(transactions)

            tid = p64(i + 1)
            eq(txn.tid, tid)

            L1 = {(rec.oid, rec.tid, rec.data_txn) for rec in txn}
            L2 = {(oid, revid, None) for _tid, oid, revid in orig
                  if _tid == tid}

            eq(L1, L2)

        for i in range(BATCHES * OBJECTS):
            txn = next(transactions)
            eq(len([rec for rec in txn if rec.data_txn is None]), 1)

        for i in range(BATCHES):
            txn = next(transactions)

            # The undos are performed in reverse order.
            otid = p64(BATCHES - i)
            L1 = [(rec.oid, rec.data_txn) for rec in txn]
            L2 = [(oid, otid) for _tid, oid, revid in orig
                  if _tid == otid]
            L1.sort()
            L2.sort()
            eq(L1, L2)

        self.assertRaises(StopIteration, next, transactions)

    def checkUndoLogMetadata(self):
        # test that the metadata is correct in the undo log
        t = transaction.get()
        t.note(u't1')
        t.setExtendedInfo('k2', 'this is transaction metadata')
        t.setUser(u'u3', path=u'p3')
        db = DB(self._storage)
        conn = db.open()
        try:
            root = conn.root()
            o1 = C()
            root['obj'] = o1
            txn = transaction.get()
            txn.commit()
            log = self._storage.undoLog()
            self.assertEqual(len(log), 2)
            d = log[0]
            self.assertEqual(d['description'], b't1')
            self.assertEqual(d['k2'], 'this is transaction metadata')
            self.assertEqual(d['user_name'], b'p3 u3')
        finally:
            conn.close()
            db.close()

    # A common test body for index tests on undoInfo and undoLog.  Before
    # ZODB 3.4, they always returned a wrong number of results (one too
    # few _or_ too many, depending on how they were called).
    def _exercise_info_indices(self, method_name):
        db = DB(self._storage)
        info_func = getattr(db, method_name)
        cn = db.open()
        rt = cn.root()

        # Do some transactions.
        for key in "abcdefghijklmnopqrstuvwxyz":
            rt[key] = ord(key)
            transaction.commit()

        # 26 letters = 26 transactions, + the hidden transaction to make
        # the root object, == 27 expected.
        allofem = info_func(0, 100000)
        self.assertEqual(len(allofem), 27)

        # Asking for no more than 100000 should do the same.
        redundant = info_func(last=-1000000)
        self.assertEqual(allofem, redundant)

        # By default, we should get only 20 back.
        default = info_func()
        self.assertEqual(len(default), 20)
        # And they should be the most recent 20.
        self.assertEqual(default, allofem[:20])

        # If we ask for only one, we should get only the most recent.
        fresh = info_func(last=1)
        self.assertEqual(len(fresh), 1)
        self.assertEqual(fresh[0], allofem[0])

        # Another way of asking for only the most recent.
        redundant = info_func(last=-1)
        self.assertEqual(fresh, redundant)

        # Try a slice that doesn't start at 0.
        oddball = info_func(first=11, last=17)
        self.assertEqual(len(oddball), 17-11)
        self.assertEqual(oddball, allofem[11: 11+len(oddball)])

        # And another way to spell the same thing.
        redundant = info_func(first=11, last=-6)
        self.assertEqual(oddball, redundant)

        cn.close()
        # Caution:  don't close db; the framework does that.  If you close
        # it here, the ZODB tests still work, but the ZRS RecoveryStorageTests
        # fail (closing the DB here in those tests closes the ZRS primary
        # before a ZRS secondary even starts, and then the latter can't
        # find a server to recover from).

    def checkIndicesInUndoInfo(self):
        self._exercise_info_indices("undoInfo")

    def checkIndicesInUndoLog(self):
        self._exercise_info_indices("undoLog")

    def checkUndoMultipleConflictResolution(self, reverse=False):
        from .ConflictResolution import PCounter
        db = DB(self._storage)
        cn = db.open()
        try:
            cn.root.x = PCounter()
            transaction.commit()

            for i in range(4):
                with db.transaction() as conn:
                    conn.transaction_manager.get().note(
                        (str if PY3 else unicode)(i))  # noqa: F821 undef name
                    conn.root.x.inc()

            ids = [log['id'] for log in db.undoLog(1, 3)]
            if reverse:
                ids.reverse()

            db.undoMultiple(ids)
            transaction.commit()

            self.assertEqual(cn.root.x._value, 2)
        finally:
            cn.close()
            db.close()

    def checkUndoMultipleConflictResolutionReversed(self):
        self.checkUndoMultipleConflictResolution(True)
