##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
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

import ZODB
import ZODB.FileStorage
from ZODB.POSException import ReadConflictError, ConflictError
from ZODB.tests.warnhook import WarningsHook

from persistent import Persistent
from persistent.mapping import PersistentMapping
import transaction

class P(Persistent):
    pass

class Independent(Persistent):

    def _p_independent(self):
        return 1

class DecoyIndependent(Persistent):

    def _p_independent(self):
        return 0

class ZODBTests(unittest.TestCase):

    def setUp(self):
        self._storage = ZODB.FileStorage.FileStorage(
            'ZODBTests.fs', create=1)
        self._db = ZODB.DB(self._storage)

    def populate(self):
        transaction.begin()
        conn = self._db.open()
        root = conn.root()
        root['test'] = pm = PersistentMapping()
        for n in range(100):
            pm[n] = PersistentMapping({0: 100 - n})
        transaction.get().note('created test data')
        transaction.commit()
        conn.close()

    def tearDown(self):
        self._db.close()
        self._storage.cleanup()

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
        transaction.get().note('duplication')
        root = conn.root()
        ob = root['test']
        assert len(ob) > 10, 'Insufficient test data'
        try:
            import tempfile
            f = tempfile.TemporaryFile()
            ob._p_jar.exportFile(ob._p_oid, f)
            assert f.tell() > 0, 'Did not export correctly'
            f.seek(0)
            new_ob = ob._p_jar.importFile(f)
            self.assertEqual(new_ob, ob)
            root['dup'] = new_ob
            f.close()
            if abort_it:
                transaction.abort()
            else:
                transaction.commit()
        except:
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
            self.failUnless(not abort_it, 'Did not abort duplication')
        l1 = list(ob.items())
        l1.sort()
        l2 = list(ob2.items())
        l2.sort()
        l1 = map(lambda (k, v): (k, v[0]), l1)
        l2 = map(lambda (k, v): (k, v[0]), l2)
        self.assertEqual(l1, l2)
        self.assert_(ob._p_oid != ob2._p_oid)
        self.assertEqual(ob._p_jar, ob2._p_jar)
        oids = {}
        for v in ob.values():
            oids[v._p_oid] = 1
        for v in ob2.values():
            assert not oids.has_key(v._p_oid), (
                'Did not fully separate duplicate from original')
        transaction.commit()

    def checkExportImportAborted(self):
        self.checkExportImport(abort_it=True)

    def checkVersionOnly(self):
        # Make sure the changes to make empty transactions a no-op
        # still allow things like abortVersion().  This should work
        # because abortVersion() calls tpc_begin() itself.
        conn = self._db.open("version")
        try:
            r = conn.root()
            r[1] = 1
            transaction.commit()
        finally:
            conn.close()
        self._db.abortVersion("version")
        transaction.commit()

    def checkResetCache(self):
        # The cache size after a reset should be 0.  Note that
        # _resetCache is not a public API, but the resetCaches()
        # function is, and resetCaches() causes _resetCache() to be
        # called.
        self.populate()
        conn = self._db.open()
        conn.root()
        self.assert_(len(conn._cache) > 0)  # Precondition
        conn._resetCache()
        self.assertEqual(len(conn._cache), 0)

    def checkResetCachesAPI(self):
        # Checks the resetCaches() API.
        # (resetCaches used to be called updateCodeTimestamp.)
        self.populate()
        conn = self._db.open()
        conn.root()
        self.assert_(len(conn._cache) > 0)  # Precondition
        ZODB.Connection.resetCaches()
        conn.close()
        self.assert_(len(conn._cache) > 0)  # Still not flushed
        conn._setDB(self._db)  # simulate the connection being reopened
        self.assertEqual(len(conn._cache), 0)

    def checkExplicitTransactionManager(self):
        # Test of transactions that apply to only the connection,
        # not the thread.
        tm1 = transaction.TransactionManager()
        conn1 = self._db.open(txn_mgr=tm1)
        tm2 = transaction.TransactionManager()
        conn2 = self._db.open(txn_mgr=tm2)
        try:
            r1 = conn1.root()
            r2 = conn2.root()
            if r1.has_key('item'):
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

    def checkLocalTransactions(self):
        # Test of transactions that apply to only the connection,
        # not the thread.
        conn1 = self._db.open()
        conn2 = self._db.open()
        hook = WarningsHook()
        hook.install()
        try:
            conn1.setLocalTransaction()
            conn2.setLocalTransaction()
            r1 = conn1.root()
            r2 = conn2.root()
            if r1.has_key('item'):
                del r1['item']
                conn1.getTransaction().commit()
            r1.get('item')
            r2.get('item')
            r1['item'] = 1
            conn1.getTransaction().commit()
            self.assertEqual(r1['item'], 1)
            # r2 has not seen a transaction boundary,
            # so it should be unchanged.
            self.assertEqual(r2.get('item'), None)
            conn2.sync()
            # Now r2 is updated.
            self.assertEqual(r2['item'], 1)

            # Now, for good measure, send an update in the other direction.
            r2['item'] = 2
            conn2.getTransaction().commit()
            self.assertEqual(r1['item'], 1)
            self.assertEqual(r2['item'], 2)
            conn1.sync()
            conn2.sync()
            self.assertEqual(r1['item'], 2)
            self.assertEqual(r2['item'], 2)
            for msg, obj, filename, lineno in hook.warnings:
                self.assert_(
                    msg.startswith("setLocalTransaction() is deprecated.") or
                    msg.startswith("getTransaction() is deprecated."))
        finally:
            conn1.close()
            conn2.close()
            hook.uninstall()

    def checkReadConflict(self):
        self.obj = P()
        self.readConflict()

    def readConflict(self, shouldFail=1):
        # Two transactions run concurrently.  Each reads some object,
        # then one commits and the other tries to read an object
        # modified by the first.  This read should fail with a conflict
        # error because the object state read is not necessarily
        # consistent with the objects read earlier in the transaction.

        tm1 = transaction.TransactionManager()
        conn = self._db.open(mvcc=False, txn_mgr=tm1)
        r1 = conn.root()
        r1["p"] = self.obj
        self.obj.child1 = P()
        tm1.get().commit()

        # start a new transaction with a new connection
        tm2 = transaction.TransactionManager()
        cn2 = self._db.open(mvcc=False, txn_mgr=tm2)
        # start a new transaction with the other connection
        r2 = cn2.root()

        self.assertEqual(r1._p_serial, r2._p_serial)

        self.obj.child2 = P()
        tm1.get().commit()

        # resume the transaction using cn2
        obj = r2["p"]
        # An attempt to access obj should fail, because r2 was read
        # earlier in the transaction and obj was modified by the othe
        # transaction.
        if shouldFail:
            self.assertRaises(ReadConflictError, lambda: obj.child1)
            # And since ReadConflictError was raised, attempting to commit
            # the transaction should re-raise it.  checkNotIndependent()
            # failed this part of the test for a long time.
            self.assertRaises(ReadConflictError, tm2.get().commit)
        else:
            # make sure that accessing the object succeeds
            obj.child1
        tm2.get().abort()

    def checkReadConflictIgnored(self):
        # Test that an application that catches a read conflict and
        # continues can not commit the transaction later.
        root = self._db.open(mvcc=False).root()
        root["real_data"] = real_data = PersistentMapping()
        root["index"] = index = PersistentMapping()

        real_data["a"] = PersistentMapping({"indexed_value": 0})
        real_data["b"] = PersistentMapping({"indexed_value": 1})
        index[1] = PersistentMapping({"b": 1})
        index[0] = PersistentMapping({"a": 1})
        transaction.commit()

        # load some objects from one connection
        tm = transaction.TransactionManager()
        cn2 = self._db.open(mvcc=False, txn_mgr=tm)
        r2 = cn2.root()
        real_data2 = r2["real_data"]
        index2 = r2["index"]

        real_data["b"]["indexed_value"] = 0
        del index[1]["b"]
        index[0]["b"] = 1
        transaction.commit()

        del real_data2["a"]
        try:
            del index2[0]["a"]
        except ReadConflictError:
            # This is the crux of the text.  Ignore the error.
            pass
        else:
            self.fail("No conflict occurred")

        # real_data2 still ready to commit
        self.assert_(real_data2._p_changed)

        # index2 values not ready to commit
        self.assert_(not index2._p_changed)
        self.assert_(not index2[0]._p_changed)
        self.assert_(not index2[1]._p_changed)

        self.assertRaises(ConflictError, tm.get().commit)
        transaction.abort()

    def checkIndependent(self):
        self.obj = Independent()
        self.readConflict(shouldFail=0)

    def checkNotIndependent(self):
        self.obj = DecoyIndependent()
        self.readConflict()

    def checkTxnBeginImpliesAbort(self):
        # begin() should do an abort() first, if needed.
        cn = self._db.open()
        rt = cn.root()
        rt['a'] = 1

        transaction.begin()  # should abort adding 'a' to the root
        rt = cn.root()
        self.assertRaises(KeyError, rt.__getitem__, 'a')

        # A longstanding bug:  this didn't work if changes were only in
        # subtransactions.
        transaction.begin()
        rt = cn.root()
        rt['a'] = 2
        transaction.commit(1)

        transaction.begin()
        rt = cn.root()
        self.assertRaises(KeyError, rt.__getitem__, 'a')

        # One more time, mixing "top level" and subtransaction changes.
        transaction.begin()
        rt = cn.root()
        rt['a'] = 3
        transaction.commit(1)
        rt['b'] = 4

        transaction.begin()
        rt = cn.root()
        self.assertRaises(KeyError, rt.__getitem__, 'a')
        self.assertRaises(KeyError, rt.__getitem__, 'b')

        # That used methods of the default transaction *manager*.  Alas,
        # that's not necessarily the same as using methods of the current
        # transaction, and, in fact, when this test was written,
        # Transaction.begin() didn't do anything (everything from here
        # down failed).

        # Oh, bleech.  Since Transaction.begin is also deprecated, we have
        # to goof around suppressing the deprecation warning.
        import warnings

        # First verify that Transaction.begin *is* deprecated, by turning
        # the warning into an error.
        warnings.filterwarnings("error", category=DeprecationWarning)
        self.assertRaises(DeprecationWarning, transaction.get().begin)
        del warnings.filters[0]

        # Now ignore DeprecationWarnings for the duration.  Use a
        # try/finally block to ensure we reenable DeprecationWarnings
        # no matter what.
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        try:
            cn = self._db.open()
            rt = cn.root()
            rt['a'] = 1

            transaction.get().begin()  # should abort adding 'a' to the root
            rt = cn.root()
            self.assertRaises(KeyError, rt.__getitem__, 'a')

            # A longstanding bug:  this didn't work if changes were only in
            # subtransactions.
            transaction.get().begin()
            rt = cn.root()
            rt['a'] = 2
            transaction.get().commit(1)

            transaction.get().begin()
            rt = cn.root()
            self.assertRaises(KeyError, rt.__getitem__, 'a')

            # One more time, mixing "top level" and subtransaction changes.
            transaction.get().begin()
            rt = cn.root()
            rt['a'] = 3
            transaction.get().commit(1)
            rt['b'] = 4

            transaction.get().begin()
            rt = cn.root()
            self.assertRaises(KeyError, rt.__getitem__, 'a')
            self.assertRaises(KeyError, rt.__getitem__, 'b')

            cn.close()

        finally:
            del warnings.filters[0]

def test_suite():
    return unittest.makeSuite(ZODBTests, 'check')

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
