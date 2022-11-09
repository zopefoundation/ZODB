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
import unittest

import transaction
from BTrees.OOBTree import OOBTree
from persistent import Persistent
from persistent.mapping import PersistentMapping

import ZODB
import ZODB.FileStorage
import ZODB.MappingStorage
import ZODB.tests.util
from ZODB.POSException import TransactionFailedError


class P(Persistent):
    pass


class ZODBTests(ZODB.tests.util.TestCase):

    def setUp(self):
        ZODB.tests.util.TestCase.setUp(self)
        self._storage = ZODB.FileStorage.FileStorage(
            'ZODBTests.fs', create=1)
        self._db = ZODB.DB(self._storage)

    def tearDown(self):
        self._db.close()
        ZODB.tests.util.TestCase.tearDown(self)

    def populate(self):
        transaction.begin()
        conn = self._db.open()
        root = conn.root()
        root['test'] = pm = PersistentMapping()
        for n in range(100):
            pm[n] = PersistentMapping({0: 100 - n})
        transaction.get().note(u'created test data')
        transaction.commit()
        conn.close()

    def checkExportImport(self, abort_it=False):
        self.populate()
        conn = self._db.open()
        try:
            self.duplicate(conn, abort_it)
        finally:
            conn.close()
        conn = self._db.open()
        try:
            self.verify(conn, abort_it)
        finally:
            conn.close()

    def duplicate(self, conn, abort_it):
        transaction.begin()
        transaction.get().note(u'duplication')
        root = conn.root()
        ob = root['test']
        assert len(ob) > 10, 'Insufficient test data'
        try:
            import tempfile
            with tempfile.TemporaryFile(prefix="DUP") as f:
                ob._p_jar.exportFile(ob._p_oid, f)
                assert f.tell() > 0, 'Did not export correctly'
                f.seek(0)
                new_ob = ob._p_jar.importFile(f)
                self.assertEqual(new_ob, ob)
                root['dup'] = new_ob
            if abort_it:
                transaction.abort()
            else:
                transaction.commit()
        except:  # noqa: E722 do not use bare 'except'
            transaction.abort()
            raise

    def verify(self, conn, abort_it):
        transaction.begin()
        root = conn.root()
        ob = root['test']
        try:
            ob2 = root['dup']
        except KeyError:
            if abort_it:
                # Passed the test.
                return
            else:
                raise
        else:
            self.assertTrue(not abort_it, 'Did not abort duplication')
        l1 = list(ob.items())
        l1.sort()
        l2 = list(ob2.items())
        l2.sort()
        l1 = list(map(lambda k_v: (k_v[0], k_v[1][0]), l1))
        l2 = list(map(lambda k_v1: (k_v1[0], k_v1[1][0]), l2))
        self.assertEqual(l1, l2)
        self.assertTrue(ob._p_oid != ob2._p_oid)
        self.assertEqual(ob._p_jar, ob2._p_jar)
        oids = {}
        for v in ob.values():
            oids[v._p_oid] = 1
        for v in ob2.values():
            assert v._p_oid not in oids, (
                'Did not fully separate duplicate from original')
        transaction.commit()

    def checkExportImportAborted(self):
        self.checkExportImport(abort_it=True)

    def checkResetCache(self):
        # The cache size after a reset should be 0.  Note that
        # _resetCache is not a public API, but the resetCaches()
        # function is, and resetCaches() causes _resetCache() to be
        # called.
        self.populate()
        conn = self._db.open()
        conn.root()
        self.assertTrue(len(conn._cache) > 0)  # Precondition
        conn._resetCache()
        self.assertEqual(len(conn._cache), 0)

    def checkResetCachesAPI(self):
        # Checks the resetCaches() API.
        # (resetCaches used to be called updateCodeTimestamp.)
        self.populate()
        conn = self._db.open()
        conn.root()
        self.assertTrue(len(conn._cache) > 0)  # Precondition
        ZODB.Connection.resetCaches()
        conn.close()
        self.assertTrue(len(conn._cache) > 0)  # Still not flushed
        conn.open()  # simulate the connection being reopened
        self.assertEqual(len(conn._cache), 0)

    def checkExplicitTransactionManager(self):
        # Test of transactions that apply to only the connection,
        # not the thread.
        tm1 = transaction.TransactionManager()
        conn1 = self._db.open(transaction_manager=tm1)
        tm2 = transaction.TransactionManager()
        conn2 = self._db.open(transaction_manager=tm2)
        try:
            r1 = conn1.root()
            r2 = conn2.root()
            if 'item' in r1:
                del r1['item']
                tm1.get().commit()
            r1.get('item')
            r2.get('item')
            r1['item'] = 1
            tm1.get().commit()
            self.assertEqual(r1['item'], 1)
            # r2 has not seen a transaction boundary,
            # so it should be unchanged.
            self.assertEqual(r2.get('item'), None)
            conn2.sync()
            # Now r2 is updated.
            self.assertEqual(r2['item'], 1)

            # Now, for good measure, send an update in the other direction.
            r2['item'] = 2
            tm2.get().commit()
            self.assertEqual(r1['item'], 1)
            self.assertEqual(r2['item'], 2)
            conn1.sync()
            conn2.sync()
            self.assertEqual(r1['item'], 2)
            self.assertEqual(r2['item'], 2)
        finally:
            conn1.close()
            conn2.close()

    def checkSavepointDoesntGetInvalidations(self):
        # Prior to ZODB 3.2.9 and 3.4, Connection.tpc_finish() processed
        # invalidations even for a subtxn commit.  This could make
        # inconsistent state visible after a subtxn commit.  There was a
        # suspicion that POSKeyError was possible as a result, but I wasn't
        # able to construct a case where that happened.
        # Subtxns are deprecated now, but it's good to check that the
        # same kind of thing doesn't happen when making savepoints either.

        # Set up the database, to hold
        # root --> "p" -> value = 1
        #      --> "q" -> value = 2
        tm1 = transaction.TransactionManager()
        conn = self._db.open(transaction_manager=tm1)
        r1 = conn.root()
        p = P()
        p.value = 1
        r1["p"] = p
        q = P()
        q.value = 2
        r1["q"] = q
        tm1.commit()

        # Now txn T1 changes p.value to 3 locally (subtxn commit).
        p.value = 3
        tm1.savepoint()

        # Start new txn T2 with a new connection.
        tm2 = transaction.TransactionManager()
        cn2 = self._db.open(transaction_manager=tm2)
        r2 = cn2.root()
        p2 = r2["p"]
        self.assertEqual(p._p_oid, p2._p_oid)
        # T2 shouldn't see T1's change of p.value to 3, because T1 didn't
        # commit yet.
        self.assertEqual(p2.value, 1)
        # Change p.value to 4, and q.value to 5.  Neither should be visible
        # to T1, because T1 is still in progress.
        p2.value = 4
        q2 = r2["q"]
        self.assertEqual(q._p_oid, q2._p_oid)
        self.assertEqual(q2.value, 2)
        q2.value = 5
        tm2.commit()

        # Back to T1.  p and q still have the expected values.
        rt = conn.root()
        self.assertEqual(rt["p"].value, 3)
        self.assertEqual(rt["q"].value, 2)

        # Now make another savepoint in T1.  This shouldn't change what
        # T1 sees for p and q.
        rt["r"] = P()
        tm1.savepoint()

        # Making that savepoint in T1 should not process invalidations
        # from T2's commit.  p.value should still be 3 here (because that's
        # what T1 savepointed earlier), and q.value should still be 2.
        # Prior to ZODB 3.2.9 and 3.4, q.value was 5 here.
        rt = conn.root()
        try:
            self.assertEqual(rt["p"].value, 3)
            self.assertEqual(rt["q"].value, 2)
        finally:
            tm1.abort()

    def checkTxnBeginImpliesAbort(self):
        # begin() should do an abort() first, if needed.
        cn = self._db.open()
        rt = cn.root()
        rt['a'] = 1

        transaction.begin()  # should abort adding 'a' to the root
        rt = cn.root()
        self.assertRaises(KeyError, rt.__getitem__, 'a')

        transaction.begin()
        rt = cn.root()
        self.assertRaises(KeyError, rt.__getitem__, 'a')

        # One more time.
        transaction.begin()
        rt = cn.root()
        rt['a'] = 3

        transaction.begin()
        rt = cn.root()
        self.assertRaises(KeyError, rt.__getitem__, 'a')
        self.assertRaises(KeyError, rt.__getitem__, 'b')

        # That used methods of the default transaction *manager*.  Alas,
        # that's not necessarily the same as using methods of the current
        # transaction, and, in fact, when this test was written,
        # Transaction.begin() didn't do anything (everything from here
        # down failed).
        # Later (ZODB 3.6):  Transaction.begin() no longer exists, so the
        # rest of this test was tossed.

    def checkFailingCommitSticks(self):
        # See also checkFailingSavepointSticks.
        cn = self._db.open()
        rt = cn.root()
        rt['a'] = 1

        # Arrange for commit to fail during tpc_vote.
        poisoned_jar = PoisonedJar(break_tpc_vote=True)
        PoisonedObject(poisoned_jar)
        transaction.get().join(poisoned_jar)

        self.assertRaises(PoisonedError, transaction.get().commit)
        # Trying to commit again fails too.
        self.assertRaises(TransactionFailedError, transaction.commit)
        self.assertRaises(TransactionFailedError, transaction.commit)
        self.assertRaises(TransactionFailedError, transaction.commit)

        # The change to rt['a'] is lost.
        self.assertRaises(KeyError, rt.__getitem__, 'a')

        # Trying to modify an object also fails, because Transaction.join()
        # also raises TransactionFailedError.
        self.assertRaises(TransactionFailedError, rt.__setitem__, 'b', 2)

        # Clean up via abort(), and try again.
        transaction.abort()
        rt['a'] = 1
        transaction.commit()
        self.assertEqual(rt['a'], 1)

        # Cleaning up via begin() should also work.
        rt['a'] = 2
        transaction.get().join(poisoned_jar)
        self.assertRaises(PoisonedError, transaction.commit)
        self.assertRaises(TransactionFailedError, transaction.commit)
        # The change to rt['a'] is lost.
        self.assertEqual(rt['a'], 1)
        # Trying to modify an object also fails.
        self.assertRaises(TransactionFailedError, rt.__setitem__, 'b', 2)
        # Clean up via begin(), and try again.
        transaction.begin()
        rt['a'] = 2
        transaction.commit()
        self.assertEqual(rt['a'], 2)

        cn.close()

    def checkSavepointRollbackAndReadCurrent(self):
        '''
        savepoint rollback after readcurrent was called on a new object
        should not raise POSKeyError
        '''
        cn = self._db.open()
        try:
            transaction.begin()
            root = cn.root()
            added_before_savepoint = P()
            root['added_before_savepoint'] = added_before_savepoint
            sp = transaction.savepoint()
            added_before_savepoint.btree = new_btree = OOBTree()
            cn.add(new_btree)
            new_btree['change_to_trigger_read_current'] = P()
            sp.rollback()
            transaction.commit()
            self.assertTrue('added_before_savepoint' in root)
        finally:
            transaction.abort()
            cn.close()

    def checkFailingSavepointSticks(self):
        cn = self._db.open()
        rt = cn.root()
        rt['a'] = 1
        transaction.savepoint()
        self.assertEqual(rt['a'], 1)

        rt['b'] = 2

        # Make a jar that raises PoisonedError when making a savepoint.
        poisoned = PoisonedJar(break_savepoint=True)
        transaction.get().join(poisoned)
        self.assertRaises(PoisonedError, transaction.savepoint)
        # Trying to make a savepoint again fails too.
        self.assertRaises(TransactionFailedError, transaction.savepoint)
        self.assertRaises(TransactionFailedError, transaction.savepoint)
        # Top-level commit also fails.
        self.assertRaises(TransactionFailedError, transaction.commit)

        # The changes to rt['a'] and rt['b'] are lost.
        self.assertRaises(KeyError, rt.__getitem__, 'a')
        self.assertRaises(KeyError, rt.__getitem__, 'b')

        # Trying to modify an object also fails, because Transaction.join()
        # also raises TransactionFailedError.
        self.assertRaises(TransactionFailedError, rt.__setitem__, 'b', 2)

        # Clean up via abort(), and try again.
        transaction.abort()
        rt['a'] = 1
        transaction.commit()
        self.assertEqual(rt['a'], 1)

        # Cleaning up via begin() should also work.
        rt['a'] = 2
        transaction.get().join(poisoned)
        self.assertRaises(PoisonedError, transaction.savepoint)
        # Trying to make a savepoint again fails too.
        self.assertRaises(TransactionFailedError, transaction.savepoint)

        # The change to rt['a'] is lost.
        self.assertEqual(rt['a'], 1)
        # Trying to modify an object also fails.
        self.assertRaises(TransactionFailedError, rt.__setitem__, 'b', 2)

        # Clean up via begin(), and try again.
        transaction.begin()
        rt['a'] = 2
        transaction.savepoint()
        self.assertEqual(rt['a'], 2)
        transaction.commit()

        cn2 = self._db.open()
        rt = cn.root()
        self.assertEqual(rt['a'], 2)

        cn.close()
        cn2.close()

    def checkMultipleUndoInOneTransaction(self):
        # Verify that it's possible to perform multiple undo
        # operations within a transaction.  If ZODB performs the undo
        # operations in a nondeterministic order, this test will often
        # fail.

        conn = self._db.open()
        try:
            root = conn.root()

            # Add transactions that set root["state"] to (0..5)
            for state_num in range(6):
                transaction.begin()
                root['state'] = state_num
                transaction.get().note(u'root["state"] = %d' % state_num)
                transaction.commit()

            # Undo all but the first.  Note that no work is actually
            # performed yet.
            transaction.begin()
            log = self._db.undoLog()
            self._db.undoMultiple([log[i]['id'] for i in range(5)])

            transaction.get().note(u'undo states 1 through 5')

            # Now attempt all those undo operations.
            transaction.commit()

            # Sanity check: we should be back to the first state.
            self.assertEqual(root['state'], 0)
        finally:
            transaction.abort()
            conn.close()


class PoisonedError(Exception):
    pass

# PoisonedJar arranges to raise PoisonedError from interesting places.


class PoisonedJar(object):
    def __init__(self, break_tpc_begin=False, break_tpc_vote=False,
                 break_savepoint=False):
        self.break_tpc_begin = break_tpc_begin
        self.break_tpc_vote = break_tpc_vote
        self.break_savepoint = break_savepoint

    def sortKey(self):
        return str(id(self))

    def tpc_begin(self, *args):
        if self.break_tpc_begin:
            raise PoisonedError("tpc_begin fails")

    # A way to poison a top-level commit.
    def tpc_vote(self, *args):
        if self.break_tpc_vote:
            raise PoisonedError("tpc_vote fails")

    # A way to poison a savepoint -- also a way to poison a subtxn commit.
    def savepoint(self):
        if self.break_savepoint:
            raise PoisonedError("savepoint fails")

    def commit(*args):
        pass

    def abort(*self):
        pass


class PoisonedObject(object):
    def __init__(self, poisonedjar):
        self._p_jar = poisonedjar


def test_suite():
    return unittest.TestSuite((
        unittest.makeSuite(ZODBTests, 'check'),
    ))
