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
"""Run tests against the iterator() interface for storages.

Any storage that supports the iterator() method should be able to pass
all these tests.
"""

from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_pickle, zodb_unpickle
from ZODB.utils import U64, p64
from ZODB.Transaction import Transaction


class IteratorCompare:

    def iter_verify(self, txniter, revids, val0):
        eq = self.assertEqual
        oid = self._oid
        val = val0
        for reciter, revid in zip(txniter, revids + [None]):
            eq(reciter.tid, revid)
            for rec in reciter:
                eq(rec.oid, oid)
                eq(rec.tid, revid)
                eq(rec.version, '')
                eq(zodb_unpickle(rec.data), MinPO(val))
                val = val + 1
        eq(val, val0 + len(revids))
        txniter.close()

class IteratorStorage(IteratorCompare):

    def checkSimpleIteration(self):
        # Store a bunch of revisions of a single object
        self._oid = oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=MinPO(11))
        revid2 = self._dostore(oid, revid=revid1, data=MinPO(12))
        revid3 = self._dostore(oid, revid=revid2, data=MinPO(13))
        # Now iterate over all the transactions and compare carefully
        txniter = self._storage.iterator()
        self.iter_verify(txniter, [revid1, revid2, revid3], 11)

    def checkClose(self):
        self._oid = oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=MinPO(11))
        txniter = self._storage.iterator()
        txniter.close()
        self.assertRaises(IOError, txniter.__getitem__, 0)

    def checkVersionIterator(self):
        if not self._storage.supportsVersions():
            return
        self._dostore()
        self._dostore(version='abort')
        self._dostore()
        self._dostore(version='abort')
        t = Transaction()
        self._storage.tpc_begin(t)
        self._storage.abortVersion('abort', t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)

        self._dostore(version='commit')
        self._dostore()
        self._dostore(version='commit')
        t = Transaction()
        self._storage.tpc_begin(t)
        self._storage.commitVersion('commit', '', t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)

        txniter = self._storage.iterator()
        for trans in txniter:
            for data in trans:
                pass

    def checkUndoZombieNonVersion(self):
        if not hasattr(self._storage, 'supportsTransactionalUndo'):
            return
        if not self._storage.supportsTransactionalUndo():
            return

        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=MinPO(94))
        # Get the undo information
        info = self._storage.undoInfo()
        tid = info[0]['id']
        # Undo the creation of the object, rendering it a zombie
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        # Now attempt to iterator over the storage
        iter = self._storage.iterator()
        for txn in iter:
            for rec in txn:
                pass

        # The last transaction performed an undo of the transaction that
        # created object oid.  (As Barry points out, the object is now in the
        # George Bailey state.)  Assert that the final data record contains
        # None in the data attribute.
        self.assertEqual(rec.oid, oid)
        self.assertEqual(rec.data, None)

    def checkTransactionExtensionFromIterator(self):
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=MinPO(1))
        iter = self._storage.iterator()
        count = 0
        for txn in iter:
            self.assertEqual(txn._extension, {})
            count +=1
        self.assertEqual(count, 1)

    def checkIterationIntraTransaction(self):
        # XXX try this test with logging enabled.  If you see something like
        #
        # ZODB FS FS21 warn: FileStorageTests.fs truncated, possibly due to
        # damaged records at 4
        #
        # Then the code in FileIterator.next() hasn't yet been fixed.
        oid = self._storage.new_oid()
        t = Transaction()
        data = zodb_pickle(MinPO(0))
        try:
            self._storage.tpc_begin(t)
            self._storage.store(oid, '\0'*8, data, '', t)
            self._storage.tpc_vote(t)
            # Don't do tpc_finish yet
            it = self._storage.iterator()
            for x in it:
                pass
        finally:
            self._storage.tpc_finish(t)

    def checkLoadEx(self):
        oid = self._storage.new_oid()
        self._dostore(oid, data=42)
        data, tid, ver = self._storage.loadEx(oid, "")
        self.assertEqual(zodb_unpickle(data), MinPO(42))
        match = False
        for txn in self._storage.iterator():
            for rec in txn:
                if rec.oid == oid and rec.tid == tid:
                    self.assertEqual(txn.tid, tid)
                    match = True
        if not match:
            self.fail("Could not find transaction with matching id")


class ExtendedIteratorStorage(IteratorCompare):

    def checkExtendedIteration(self):
        # Store a bunch of revisions of a single object
        self._oid = oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=MinPO(11))
        revid2 = self._dostore(oid, revid=revid1, data=MinPO(12))
        revid3 = self._dostore(oid, revid=revid2, data=MinPO(13))
        revid4 = self._dostore(oid, revid=revid3, data=MinPO(14))
        # Note that the end points are included
        # Iterate over all of the transactions with explicit start/stop
        txniter = self._storage.iterator(revid1, revid4)
        self.iter_verify(txniter, [revid1, revid2, revid3, revid4], 11)
        # Iterate over some of the transactions with explicit start
        txniter = self._storage.iterator(revid3)
        self.iter_verify(txniter, [revid3, revid4], 13)
        # Iterate over some of the transactions with explicit stop
        txniter = self._storage.iterator(None, revid2)
        self.iter_verify(txniter, [revid1, revid2], 11)
        # Iterate over some of the transactions with explicit start+stop
        txniter = self._storage.iterator(revid2, revid3)
        self.iter_verify(txniter, [revid2, revid3], 12)
        # Specify an upper bound somewhere in between values
        revid3a = p64((U64(revid3) + U64(revid4)) / 2)
        txniter = self._storage.iterator(revid2, revid3a)
        self.iter_verify(txniter, [revid2, revid3], 12)
        # Specify a lower bound somewhere in between values.
        # revid2 == revid1+1 is very likely on Windows.  Adding 1 before
        # dividing ensures that "the midpoint" we compute is strictly larger
        # than revid1.
        revid1a = p64((U64(revid1) + 1 + U64(revid2)) / 2)
        assert revid1 < revid1a
        txniter = self._storage.iterator(revid1a, revid3a)
        self.iter_verify(txniter, [revid2, revid3], 12)
        # Specify an empty range
        txniter = self._storage.iterator(revid3, revid2)
        self.iter_verify(txniter, [], 13)
        # Specify a singleton range
        txniter = self._storage.iterator(revid3, revid3)
        self.iter_verify(txniter, [revid3], 13)

class IteratorDeepCompare:
    def compare(self, storage1, storage2):
        eq = self.assertEqual
        iter1 = storage1.iterator()
        iter2 = storage2.iterator()
        for txn1, txn2 in zip(iter1, iter2):
            eq(txn1.tid,         txn2.tid)
            eq(txn1.status,      txn2.status)
            eq(txn1.user,        txn2.user)
            eq(txn1.description, txn2.description)
            eq(txn1._extension,  txn2._extension)
            for rec1, rec2 in zip(txn1, txn2):
                eq(rec1.oid,     rec2.oid)
                eq(rec1.tid,  rec2.tid)
                eq(rec1.version, rec2.version)
                eq(rec1.data,    rec2.data)
            # Make sure there are no more records left in rec1 and rec2,
            # meaning they were the same length.
            self.assertRaises(IndexError, txn1.next)
            self.assertRaises(IndexError, txn2.next)
        # Make sure ther are no more records left in txn1 and txn2, meaning
        # they were the same length
        self.assertRaises(IndexError, iter1.next)
        self.assertRaises(IndexError, iter2.next)
        iter1.close()
        iter2.close()
