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
"""Run tests against the iterator() interface for storages.

Any storage that supports the iterator() method should be able to pass
all these tests.

"""

import ZODB.blob
from ZODB.Connection import TransactionMetaData
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import ZERO
from ZODB.tests.StorageTestBase import zodb_pickle
from ZODB.tests.StorageTestBase import zodb_unpickle
from ZODB.utils import U64
from ZODB.utils import load_current
from ZODB.utils import p64


try:
    from itertools import izip as zip
except ImportError:
    # Py3: zip() already returns an iterable.
    pass


class IteratorCompare(object):

    def iter_verify(self, txniter, revids, val0):
        eq = self.assertEqual
        oid = self._oid
        val = val0
        for reciter, revid in zip(txniter, revids + [None]):
            eq(reciter.tid, revid)
            for rec in reciter:
                eq(rec.oid, oid)
                eq(rec.tid, revid)
                eq(zodb_unpickle(rec.data), MinPO(val))
                val = val + 1
        eq(val, val0 + len(revids))


class IteratorStorage(IteratorCompare):

    # Override this in tests for storages that delegate to TransactionMetaData
    # the task to (de)serialize extension data.
    use_extension_bytes = False

    def checkSimpleIteration(self):
        # Store a bunch of revisions of a single object
        self._oid = oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=MinPO(11))
        revid2 = self._dostore(oid, revid=revid1, data=MinPO(12))
        revid3 = self._dostore(oid, revid=revid2, data=MinPO(13))
        # Now iterate over all the transactions and compare carefully
        txniter = self._storage.iterator()
        self.iter_verify(txniter, [revid1, revid2, revid3], 11)

    def checkUndoZombie(self):
        oid = self._storage.new_oid()
        self._dostore(oid, data=MinPO(94))
        # Get the undo information
        info = self._storage.undoInfo()
        tid = info[0]['id']
        # Undo the creation of the object, rendering it a zombie
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        self._storage.undo(tid, t)
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
        # It will be deserialized into a simple dict, which will be serialized
        # differently. This simulates that 'dumps(loads(x), ...)' does not
        # always return x.
        class ext(dict):
            def __reduce__(self):
                return dict, (tuple(self.items()),)
        ext = ext(foo=1)
        oid = self._storage.new_oid()
        self._dostore(oid, data=MinPO(1), extension=ext)
        txn, = self._storage.iterator()
        self.assertEqual(txn.extension, ext)
        try:
            extension_bytes = txn.extension_bytes
        except AttributeError:
            # Volatile storages often don't serialize extension because it
            # would be useless.
            return
        txn.extension = ext
        if self.use_extension_bytes:
            self.assertEqual(extension_bytes, txn.extension_bytes)
        else:
            self.assertNotEqual(extension_bytes, txn.extension_bytes)

    def checkIterationIntraTransaction(self):
        # TODO:  Try this test with logging enabled.  If you see something
        # like
        #
        # ZODB FS FS21 warn: FileStorageTests.fs truncated, possibly due to
        # damaged records at 4
        #
        # Then the code in FileIterator.next() hasn't yet been fixed.
        # Should automate that check.
        oid = self._storage.new_oid()
        t = TransactionMetaData()
        data = zodb_pickle(MinPO(0))
        try:
            self._storage.tpc_begin(t)
            self._storage.store(oid, ZERO, data, '', t)
            self._storage.tpc_vote(t)
            # Don't do tpc_finish yet
            it = self._storage.iterator()
            for x in it:
                pass
        finally:
            self._storage.tpc_finish(t)

    def checkLoad_was_checkLoadEx(self):
        oid = self._storage.new_oid()
        self._dostore(oid, data=42)
        data, tid = load_current(self._storage, oid)
        self.assertEqual(zodb_unpickle(data), MinPO(42))
        match = False
        for txn in self._storage.iterator():
            for rec in txn:
                if rec.oid == oid and rec.tid == tid:
                    self.assertEqual(txn.tid, tid)
                    match = True
        if not match:
            self.fail("Could not find transaction with matching id")

    def checkIterateRepeatedly(self):
        self._dostore()
        transactions = self._storage.iterator()
        self.assertEqual(1, len(list(transactions)))
        # The iterator can only be consumed once:
        self.assertEqual(0, len(list(transactions)))

    def checkIterateRecordsRepeatedly(self):
        self._dostore()
        it = self._storage.iterator()
        tinfo = next(it)
        self.assertEqual(1, len(list(tinfo)))
        self.assertEqual(1, len(list(tinfo)))
        if hasattr(it, 'close'):
            it.close()

    def checkIterateWhileWriting(self):
        self._dostore()
        iterator = self._storage.iterator()
        # We have one transaction with 1 modified object.
        txn_1 = next(iterator)
        self.assertEqual(1, len(list(txn_1)))

        # We store another transaction with 1 object, the already running
        # iterator does not pick this up.
        self._dostore()
        with self.assertRaises(StopIteration):
            next(iterator)


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
        revid3a = p64((U64(revid3) + U64(revid4)) // 2)
        txniter = self._storage.iterator(revid2, revid3a)
        self.iter_verify(txniter, [revid2, revid3], 12)
        # Specify a lower bound somewhere in between values.
        # revid2 == revid1+1 is very likely on Windows.  Adding 1 before
        # dividing ensures that "the midpoint" we compute is strictly larger
        # than revid1.
        revid1a = p64((U64(revid1) + 1 + U64(revid2)) // 2)
        assert revid1 < revid1a
        txniter = self._storage.iterator(revid1a, revid3a)
        self.iter_verify(txniter, [revid2, revid3], 12)
        # Specify an empty range
        txniter = self._storage.iterator(revid3, revid2)
        self.iter_verify(txniter, [], 13)
        # Specify a singleton range
        txniter = self._storage.iterator(revid3, revid3)
        self.iter_verify(txniter, [revid3], 13)


class IteratorDeepCompare(object):

    def compare(self, storage1, storage2):
        eq = self.assertEqual
        iter1 = storage1.iterator()
        iter2 = storage2.iterator()
        for txn1, txn2 in zip(iter1, iter2):
            eq(txn1.tid,         txn2.tid)
            eq(txn1.status,      txn2.status)
            eq(txn1.user,        txn2.user)
            eq(txn1.description, txn2.description)
            eq(txn1.extension,  txn2.extension)
            eq(txn1.extension_bytes, txn2.extension_bytes)
            itxn1 = iter(txn1)
            itxn2 = iter(txn2)
            for rec1, rec2 in zip(itxn1, itxn2):
                eq(rec1.oid,     rec2.oid)
                eq(rec1.tid,  rec2.tid)
                eq(rec1.data,    rec2.data)
                if ZODB.blob.is_blob_record(rec1.data):
                    try:
                        fn1 = storage1.loadBlob(rec1.oid, rec1.tid)
                    except ZODB.POSException.POSKeyError:
                        self.assertRaises(
                            ZODB.POSException.POSKeyError,
                            storage2.loadBlob, rec1.oid, rec1.tid)
                    else:
                        fn2 = storage2.loadBlob(rec1.oid, rec1.tid)
                        self.assertTrue(fn1 != fn2)
                        with open(fn1, 'rb') as fp1:
                            with open(fn2, 'rb') as fp2:
                                eq(fp1.read(), fp2.read())

            # Make sure there are no more records left in rec1 and rec2,
            # meaning they were the same length.
            # Additionally, check that we're backwards compatible to the
            # IndexError we used to raise before.
            self.assertRaises(StopIteration, next, itxn1)
            self.assertRaises(StopIteration, next, itxn2)
        # Make sure ther are no more records left in txn1 and txn2, meaning
        # they were the same length
        self.assertRaises(StopIteration, next, iter1)
        self.assertRaises(StopIteration, next, iter2)
