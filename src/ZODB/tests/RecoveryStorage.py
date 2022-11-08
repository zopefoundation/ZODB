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
"""More recovery and iterator tests."""

import time

import transaction

from ZODB import DB
from ZODB.Connection import TransactionMetaData
from ZODB.serialize import referencesf
from ZODB.tests.IteratorStorage import IteratorDeepCompare
from ZODB.tests.StorageTestBase import MinPO
from ZODB.tests.StorageTestBase import snooze
from ZODB.tests.util import time_monotonically_increases
from ZODB.utils import load_current


class RecoveryStorage(IteratorDeepCompare):

    # Requires a setUp() that creates a self._dst destination storage
    def checkSimpleRecovery(self):
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=11)
        revid = self._dostore(oid, revid=revid, data=12)
        revid = self._dostore(oid, revid=revid, data=13)
        self._dst.copyTransactionsFrom(self._storage)
        self.compare(self._storage, self._dst)

    def checkRestoreAcrossPack(self):
        db = DB(self._storage)
        c = db.open()
        r = c.root()
        r["obj1"] = MinPO(1)
        transaction.commit()
        r["obj2"] = MinPO(1)
        transaction.commit()

        self._dst.copyTransactionsFrom(self._storage)
        self._dst.pack(time.time(), referencesf)

        self._undo(self._storage.undoInfo()[0]['id'])

        # copy the final transaction manually.  even though there
        # was a pack, the restore() ought to succeed.
        it = self._storage.iterator()
        # Get the last transaction and its record iterator. Record iterators
        # can't be accessed out-of-order, so we need to do this in a bit
        # complicated way:
        for final in it:
            records = list(final)

        self._dst.tpc_begin(final, final.tid, final.status)
        for r in records:
            self._dst.restore(r.oid, r.tid, r.data, '', r.data_txn,
                              final)
        self._dst.tpc_vote(final)
        self._dst.tpc_finish(final)

    @time_monotonically_increases
    def checkPackWithGCOnDestinationAfterRestore(self):
        raises = self.assertRaises
        db = DB(self._storage)
        conn = db.open()
        root = conn.root()
        root.obj = obj1 = MinPO(1)
        txn = transaction.get()
        txn.note(u'root -> obj')
        txn.commit()
        root.obj.obj = obj2 = MinPO(2)
        txn = transaction.get()
        txn.note(u'root -> obj -> obj')
        txn.commit()
        del root.obj
        txn = transaction.get()
        txn.note(u'root -X->')
        txn.commit()
        # Now copy the transactions to the destination
        self._dst.copyTransactionsFrom(self._storage)
        # Now pack the destination.
        snooze()
        self._dst.pack(time.time(),  referencesf)
        # And check to see that the root object exists, but not the other
        # objects.
        data, serial = load_current(self._dst, root._p_oid)
        raises(KeyError, load_current, self._dst, obj1._p_oid)
        raises(KeyError, load_current, self._dst, obj2._p_oid)

    def checkRestoreWithMultipleObjectsInUndoRedo(self):
        from ZODB.FileStorage import FileStorage

        # Undo creates backpointers in (at least) FileStorage.  ZODB 3.2.1
        # FileStorage._data_find() had an off-by-8 error, neglecting to
        # account for the size of the backpointer when searching a
        # transaction with multiple data records.  The results were
        # unpredictable.  For example, it could raise a Python exception
        # due to passing a negative offset to file.seek(), or could
        # claim that a transaction didn't have data for an oid despite
        # that it actually did.
        #
        # The former failure mode was seen in real life, in a ZRS secondary
        # doing recovery.  On my box today, the second failure mode is
        # what happens in this test (with an unpatched _data_find, of
        # course).  Note that the error can only "bite" if more than one
        # data record is in a transaction, and the oid we're looking for
        # follows at least one data record with a backpointer.
        #
        # Unfortunately, _data_find() is a low-level implementation detail,
        # and this test does some horrid white-box abuse to test it.

        is_filestorage = isinstance(self._storage, FileStorage)

        db = DB(self._storage)
        c = db.open()
        r = c.root()

        # Create some objects.
        r["obj1"] = MinPO(1)
        r["obj2"] = MinPO(1)
        transaction.commit()

        # Add x attributes to them.
        r["obj1"].x = 'x1'
        r["obj2"].x = 'x2'
        transaction.commit()

        r = db.open().root()
        self.assertEqual(r["obj1"].x, 'x1')
        self.assertEqual(r["obj2"].x, 'x2')

        # Dirty tricks.
        if is_filestorage:
            obj1_oid = r["obj1"]._p_oid
            obj2_oid = r["obj2"]._p_oid
            # This will be the offset of the next transaction, which
            # will contain two backpointers.
            pos = self._storage.getSize()

        # Undo the attribute creation.
        info = self._storage.undoInfo()
        tid = info[0]['id']
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        self._storage.undo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)

        r = db.open().root()
        self.assertRaises(AttributeError, getattr, r["obj1"], 'x')
        self.assertRaises(AttributeError, getattr, r["obj2"], 'x')

        if is_filestorage:
            # _data_find should find data records for both objects in that
            # transaction.  Without the patch, the second assert failed
            # (it claimed it couldn't find a data record for obj2) on my
            # box, but other failure modes were possible.
            self.assertTrue(self._storage._data_find(pos, obj1_oid, '') > 0)
            self.assertTrue(self._storage._data_find(pos, obj2_oid, '') > 0)

            # The offset of the next ("redo") transaction.
            pos = self._storage.getSize()

        # Undo the undo (restore the attributes).
        info = self._storage.undoInfo()
        tid = info[0]['id']
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        self._storage.undo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)

        r = db.open().root()
        self.assertEqual(r["obj1"].x, 'x1')
        self.assertEqual(r["obj2"].x, 'x2')

        if is_filestorage:
            # Again _data_find should find both objects in this txn, and
            # again the second assert failed on my box.
            self.assertTrue(self._storage._data_find(pos, obj1_oid, '') > 0)
            self.assertTrue(self._storage._data_find(pos, obj2_oid, '') > 0)

        # Indirectly provoke .restore().  .restore in turn indirectly
        # provokes _data_find too, but not usefully for the purposes of
        # the specific bug this test aims at:  copyTransactionsFrom() uses
        # storage iterators that chase backpointers themselves, and
        # return the data they point at instead.  The result is that
        # _data_find didn't actually see anything dangerous in this
        # part of the test.
        self._dst.copyTransactionsFrom(self._storage)
        self.compare(self._storage, self._dst)
