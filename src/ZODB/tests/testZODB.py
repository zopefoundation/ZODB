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
from __future__ import nested_scopes

import unittest

import ZODB
import ZODB.FileStorage
from ZODB.PersistentMapping import PersistentMapping
from ZODB.POSException import ReadConflictError
from ZODB.tests.StorageTestBase import removefs
from Persistence import Persistent

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
        get_transaction().begin()
        conn = self._db.open()
        root = conn.root()
        root['test'] = pm = PersistentMapping()
        for n in range(100):
            pm[n] = PersistentMapping({0: 100 - n})
        get_transaction().note('created test data')
        get_transaction().commit()
        conn.close()

    def tearDown(self):
        self._db.close()
        removefs("ZODBTests.fs")

    def checkExportImport(self, abort_it=0, dup_name='test_duplicate'):
        self.populate()
        get_transaction().begin()
        get_transaction().note('duplication')
        # Duplicate the 'test' object.
        conn = self._db.open()
        try:
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
                root[dup_name] = new_ob
                f.close()
                if abort_it:
                    get_transaction().abort()
                else:
                    get_transaction().commit()
            except:
                get_transaction().abort()
                raise
        finally:
            conn.close()
        get_transaction().begin()
        # Verify the duplicate.
        conn = self._db.open()
        try:
            root = conn.root()
            ob = root['test']
            try:
                ob2 = root[dup_name]
            except KeyError:
                if abort_it:
                    # Passed the test.
                    return
                else:
                    raise
            else:
                if abort_it:
                    assert 0, 'Did not abort duplication'
            l1 = list(ob.items())
            l1.sort()
            l2 = list(ob2.items())
            l2.sort()
            l1 = map(lambda (k, v): (k, v[0]), l1)
            l2 = map(lambda (k, v): (k, v[0]), l2)
            assert l1 == l2, 'Duplicate did not match'
            assert ob._p_oid != ob2._p_oid, 'Did not duplicate'
            assert ob._p_jar == ob2._p_jar, 'Not same connection'
            oids = {}
            for v in ob.values():
                oids[v._p_oid] = 1
            for v in ob2.values():
                assert not oids.has_key(v._p_oid), (
                    'Did not fully separate duplicate from original')
            get_transaction().commit()
        finally:
            conn.close()

    def checkExportImportAborted(self):
        self.checkExportImport(abort_it=1, dup_name='test_duplicate_aborted')

    def checkVersionOnly(self):
        # Make sure the changes to make empty transactions a no-op
        # still allow things like abortVersion().  This should work
        # because abortVersion() calls tpc_begin() itself.
        conn = self._db.open("version")
        try:
            r = conn.root()
            r[1] = 1
            get_transaction().commit()
        finally:
            conn.close()
        self._db.abortVersion("version")
        get_transaction().commit()

    def checkResetCache(self):
        # The cache size after a reset should be 0 and the GC attributes
        # ought to be linked to it rather than the old cache.
        self.populate()
        conn = self._db.open()
        conn.root()
        self.assert_(len(conn._cache) > 0)  # Precondition
        conn._resetCache()
        self.assertEqual(len(conn._cache), 0)
        self.assert_(conn._incrgc == conn._cache.incrgc)
        self.assert_(conn.cacheGC == conn._cache.incrgc)

    def checkLocalTransactions(self):
        # Test of transactions that apply to only the connection,
        # not the thread.
        conn1 = self._db.open()
        conn2 = self._db.open()
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
        finally:
            conn1.close()
            conn2.close()

    def checkReadConflict(self):
        self.obj = P()
        self.readConflict()

    def readConflict(self, shouldFail=1):
        # Two transactions run concurrently.  Each reads some object,
        # then one commits and the other tries to read an object
        # modified by the first.  This read should fail with a conflict
        # error because the object state read is not necessarily
        # consistent with the objects read earlier in the transaction.

        conn = self._db.open()
        conn.setLocalTransaction()
        r1 = conn.root()
        r1["p"] = self.obj
        self.obj.child1 = P()
        conn.getTransaction().commit()

        # start a new transaction with a new connection
        cn2 = self._db.open()
        # start a new transaction with the other connection
        cn2.setLocalTransaction()
        r2 = cn2.root()

        self.assertEqual(r1._p_serial, r2._p_serial)
        
        self.obj.child2 = P()
        conn.getTransaction().commit()

        # resume the transaction using cn2
        obj = r2["p"]
        # An attempt to access obj should fail, because r2 was read
        # earlier in the transaction and obj was modified by the othe
        # transaction.
        if shouldFail:
            self.assertRaises(ReadConflictError, lambda: obj.child1)
        else:
            # make sure that accessing the object succeeds
            obj.child1
        cn2.getTransaction().abort()

    def testReadConflictIgnored(self):
        # Test that an application that catches a read conflict and
        # continues can not commit the transaction later.
        root = self._db.open().root()
        root["real_data"] = real_data = PersistentDict()
        root["index"] = index = PersistentDict()

        real_data["a"] = PersistentDict({"indexed_value": 0})
        real_data["b"] = PersistentDict({"indexed_value": 1})
        index[1] = PersistentDict({"b": 1})
        index[0] = PersistentDict({"a": 1})
        get_transaction().commit()

        # load some objects from one connection
        cn2 = self._db.open()
        cn2.setLocalTransaction()
        r2 = cn2.root()
        real_data2 = r2["real_data"]
        index2 = r2["index"]

        real_data["b"]["indexed_value"] = 0
        del index[1]["b"]
        index[0]["b"] = 1
        cn2.getTransaction().commit()

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

        self.assertRaises(ConflictError, get_transaction().commit)
        get_transaction().abort()

    def checkIndependent(self):
        self.obj = Independent()
        self.readConflict(shouldFail=0)

    def checkNotIndependent(self):
        self.obj = DecoyIndependent()
        self.readConflict()

def test_suite():
    return unittest.makeSuite(ZODBTests, 'check')
