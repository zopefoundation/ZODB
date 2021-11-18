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
"""Run the basic tests for a storage as described in the official storage API

The most complete and most out-of-date description of the interface is:
http://www.zope.org/Documentation/Developer/Models/ZODB/ZODB_Architecture_Storage_Interface_Info.html

All storages should be able to pass these tests.
"""
import transaction
from ZODB import DB, POSException
from ZODB.Connection import TransactionMetaData
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle, zodb_pickle
from ZODB.tests.StorageTestBase import ZERO
from ZODB.tests.util import with_high_concurrency

import threading
import time
import zope.interface
import zope.interface.verify
from random import randint

from .. import utils


class BasicStorage(object):
    def checkBasics(self):
        self.assertEqual(self._storage.lastTransaction(), ZERO)

        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        self.assertRaises(POSException.StorageTransactionError,
                          self._storage.tpc_begin, t)
        # Aborting is easy
        self._storage.tpc_abort(t)
        # Test a few expected exceptions when we're doing operations giving a
        # different Transaction object than the one we've begun on.
        self._storage.tpc_begin(t)
        self.assertRaises(
            POSException.StorageTransactionError,
            self._storage.store,
            ZERO, ZERO, b'', '', TransactionMetaData())

        self.assertRaises(
            POSException.StorageTransactionError,
            self._storage.store,
            ZERO, 1, b'2', '', TransactionMetaData())

        self.assertRaises(
            POSException.StorageTransactionError,
            self._storage.tpc_vote, TransactionMetaData())
        self._storage.tpc_abort(t)

    def checkSerialIsNoneForInitialRevision(self):
        eq = self.assertEqual
        oid = self._storage.new_oid()
        txn = TransactionMetaData()
        self._storage.tpc_begin(txn)
        # Use None for serial.  Don't use _dostore() here because that coerces
        # serial=None to serial=ZERO.
        self._storage.store(oid, None, zodb_pickle(MinPO(11)),
                            '', txn)
        self._storage.tpc_vote(txn)
        newrevid = self._storage.tpc_finish(txn)
        data, revid = utils.load_current(self._storage, oid)
        value = zodb_unpickle(data)
        eq(value, MinPO(11))
        eq(revid, newrevid)

    def checkStore(self):
        revid = ZERO
        newrevid = self._dostore(revid=None)
        # Finish the transaction.
        self.assertNotEqual(newrevid, revid)

    def checkStoreAndLoad(self):
        eq = self.assertEqual
        oid = self._storage.new_oid()
        self._dostore(oid=oid, data=MinPO(7))
        data, revid = utils.load_current(self._storage, oid)
        value = zodb_unpickle(data)
        eq(value, MinPO(7))
        # Now do a bunch of updates to an object
        for i in range(13, 22):
            revid = self._dostore(oid, revid=revid, data=MinPO(i))
        # Now get the latest revision of the object
        data, revid = utils.load_current(self._storage, oid)
        eq(zodb_unpickle(data), MinPO(21))

    def checkConflicts(self):
        oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=MinPO(11))
        self._dostore(oid, revid=revid1, data=MinPO(12))
        self.assertRaises(POSException.ConflictError,
                          self._dostore,
                          oid, revid=revid1, data=MinPO(13))

    def checkWriteAfterAbort(self):
        oid = self._storage.new_oid()
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        self._storage.store(oid, ZERO, zodb_pickle(MinPO(5)), '', t)
        # Now abort this transaction
        self._storage.tpc_abort(t)
        # Now start all over again
        oid = self._storage.new_oid()
        self._dostore(oid=oid, data=MinPO(6))

    def checkAbortAfterVote(self):
        oid1 = self._storage.new_oid()
        revid1 = self._dostore(oid=oid1, data=MinPO(-2))
        oid = self._storage.new_oid()
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        self._storage.store(oid, ZERO, zodb_pickle(MinPO(5)), '', t)
        # Now abort this transaction
        self._storage.tpc_vote(t)
        self._storage.tpc_abort(t)
        # Now start all over again
        oid = self._storage.new_oid()
        revid = self._dostore(oid=oid, data=MinPO(6))

        for oid, revid in [(oid1, revid1), (oid, revid)]:
            data, _revid = utils.load_current(self._storage, oid)
            self.assertEqual(revid, _revid)

    def checkStoreTwoObjects(self):
        noteq = self.assertNotEqual
        p31, p32, p51, p52 = map(MinPO, (31, 32, 51, 52))
        oid1 = self._storage.new_oid()
        oid2 = self._storage.new_oid()
        noteq(oid1, oid2)
        revid1 = self._dostore(oid1, data=p31)
        revid2 = self._dostore(oid2, data=p51)
        noteq(revid1, revid2)
        revid3 = self._dostore(oid1, revid=revid1, data=p32)
        revid4 = self._dostore(oid2, revid=revid2, data=p52)
        noteq(revid3, revid4)

    def checkGetTid(self):
        if not hasattr(self._storage, 'getTid'):
            return
        eq = self.assertEqual
        p41, p42 = map(MinPO, (41, 42))
        oid = self._storage.new_oid()
        self.assertRaises(KeyError, self._storage.getTid, oid)
        # Now store a revision
        revid1 = self._dostore(oid, data=p41)
        eq(revid1, self._storage.getTid(oid))
        # And another one
        revid2 = self._dostore(oid, revid=revid1, data=p42)
        eq(revid2, self._storage.getTid(oid))

    def checkLen(self):
        # len(storage) reports the number of objects.
        # check it is zero when empty
        self.assertEqual(len(self._storage), 0)
        # check it is correct when the storage contains two object.
        # len may also be zero, for storages that do not keep track
        # of this number
        self._dostore(data=MinPO(22))
        self._dostore(data=MinPO(23))
        self.assertTrue(len(self._storage) in [0, 2])

    def checkGetSize(self):
        self._dostore(data=MinPO(25))
        size = self._storage.getSize()
        # The storage API doesn't make any claims about what size
        # means except that it ought to be printable.
        str(size)

    def checkNote(self):
        oid = self._storage.new_oid()
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        t.note(u'this is a test')
        self._storage.store(oid, ZERO, zodb_pickle(MinPO(5)), '', t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)

    def checkInterfaces(self):
        for iface in zope.interface.providedBy(self._storage):
            zope.interface.verify.verifyObject(iface, self._storage)

    def checkMultipleEmptyTransactions(self):
        # There was a bug in handling empty transactions in mapping
        # storage that caused the commit lock not to be released. :(
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        t = TransactionMetaData()
        self._storage.tpc_begin(t)      # Hung here before
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)

    def _do_store_in_separate_thread(self, oid, revid, voted):
        # We'll run the competing trans in a separate thread:
        thread = threading.Thread(name='T2',
                                  target=self._dostore, args=(oid,),
                                  kwargs=dict(revid=revid))
        thread.daemon = True
        thread.start()
        thread.join(.1)
        return thread

    def check_checkCurrentSerialInTransaction(self):
        oid = b'\0\0\0\0\0\0\0\xf0'
        tid = self._dostore(oid)
        tid2 = self._dostore(oid, revid=tid)
        data = b'cpersistent\nPersistent\nq\x01.N.'  # a simple persistent obj

        # ---------------------------------------------------------------------
        # stale read
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        try:
            self._storage.store(b'\0\0\0\0\0\0\0\xf1',
                                b'\0\0\0\0\0\0\0\0', data, '', t)
            self._storage.checkCurrentSerialInTransaction(oid, tid, t)
            self._storage.tpc_vote(t)
        except POSException.ReadConflictError as v:
            self.assertEqual(v.oid, oid)
            self.assertEqual(v.serials, (tid2, tid))
        else:
            if 0:
                self.assertTrue(False, "No conflict error")

        self._storage.tpc_abort(t)

        # ---------------------------------------------------------------------
        # non-stale read, no stress. :)
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        self._storage.store(b'\0\0\0\0\0\0\0\xf2',
                            b'\0\0\0\0\0\0\0\0', data, '', t)
        self._storage.checkCurrentSerialInTransaction(oid, tid2, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)

        # ---------------------------------------------------------------------
        # non-stale read, competition after vote.  The competing
        # transaction must produce a tid > this transaction's tid
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        self._storage.store(b'\0\0\0\0\0\0\0\xf3',
                            b'\0\0\0\0\0\0\0\0', data, '', t)
        self._storage.checkCurrentSerialInTransaction(oid, tid2, t)
        self._storage.tpc_vote(t)

        # We'll run the competing trans in a separate thread:
        thread = self._do_store_in_separate_thread(oid, tid2, True)
        self._storage.tpc_finish(t)
        thread.join(33)

        tid3 = utils.load_current(self._storage, oid)[1]
        self.assertTrue(tid3 >
                        utils.load_current(
                            self._storage, b'\0\0\0\0\0\0\0\xf3')[1])

        # ---------------------------------------------------------------------
        # non-stale competing trans after checkCurrentSerialInTransaction
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        self._storage.store(b'\0\0\0\0\0\0\0\xf4',
                            b'\0\0\0\0\0\0\0\0', data, '', t)
        self._storage.checkCurrentSerialInTransaction(oid, tid3, t)

        thread = self._do_store_in_separate_thread(oid, tid3, False)

        # There are 2 possibilities:
        # 1. The store happens before this transaction completes,
        #    in which case, the vote below fails.
        # 2. The store happens after this trans, in which case, the
        #    tid of the object is greater than this transaction's tid.
        try:
            self._storage.tpc_vote(t)
        except POSException.ReadConflictError:
            thread.join()  # OK :)
        else:
            self._storage.tpc_finish(t)
            thread.join()
            tid4 = utils.load_current(self._storage, oid)[1]
            self.assertTrue(
                tid4 >
                utils.load_current(self._storage, b'\0\0\0\0\0\0\0\xf4')[1])

    def check_tid_ordering_w_commit(self):

        # It's important that storages always give a consistent
        # ordering for revisions, tids.  This is most likely to fail
        # around commit.  Here we'll do some basic tests to check this.

        # We'll use threads to arrange for ordering to go wrong and
        # verify that a storage gets it right.

        # First, some initial data.
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        self._storage.store(ZERO, ZERO, b'x', '', t)
        self._storage.tpc_vote(t)
        tids = []
        self._storage.tpc_finish(t, lambda tid: tids.append(tid))

        # OK, now we'll start a new transaction, take it to finish,
        # and then block finish while we do some other operations.

        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        self._storage.store(ZERO, tids[0], b'y', '', t)
        self._storage.tpc_vote(t)

        to_join = []

        def run_in_thread(func):
            t = threading.Thread(target=func)
            t.daemon = True
            t.start()
            to_join.append(t)

        started = threading.Event()
        finish = threading.Event()

        @run_in_thread
        def commit():
            def callback(tid):
                started.set()
                tids.append(tid)
                finish.wait()

            self._storage.tpc_finish(t, callback)

        results = {}
        started.wait()
        attempts = []
        attempts_cond = utils.Condition()

        def update_attempts():
            with attempts_cond:
                attempts.append(1)
                attempts_cond.notify_all()

        @run_in_thread
        def load():
            update_attempts()
            results['load'] = utils.load_current(self._storage, ZERO)[1]
            results['lastTransaction'] = self._storage.lastTransaction()

        expected_attempts = 1

        if hasattr(self._storage, 'getTid'):
            expected_attempts += 1

            @run_in_thread
            def getTid():
                update_attempts()
                results['getTid'] = self._storage.getTid(ZERO)

        if hasattr(self._storage, 'lastInvalidations'):
            expected_attempts += 1

            @run_in_thread
            def lastInvalidations():
                update_attempts()
                invals = self._storage.lastInvalidations(1)
                if invals:
                    results['lastInvalidations'] = invals[0][0]

        with attempts_cond:
            while len(attempts) < expected_attempts:
                attempts_cond.wait()

        time.sleep(.01)  # for good measure :)
        finish.set()

        for t in to_join:
            t.join(1)

        self.assertEqual(results.pop('load'), tids[1])
        self.assertEqual(results.pop('lastTransaction'), tids[1])
        for m, tid in results.items():
            self.assertEqual(tid, tids[1])

    # verify storage/Connection for race in between load/open and local
    # invalidations.
    # https://github.com/zopefoundation/ZEO/issues/166
    # https://github.com/zopefoundation/ZODB/issues/290

    @with_high_concurrency
    def check_race_loadopen_vs_local_invalidate(self):
        db = DB(self._storage)

        # init initializes the database with two integer objects - obj1/obj2
        # that are set to 0.
        def init():
            transaction.begin()
            zconn = db.open()

            root = zconn.root()
            root['obj1'] = MinPO(0)
            root['obj2'] = MinPO(0)

            transaction.commit()
            zconn.close()

        # verify accesses obj1/obj2 and verifies that obj1.value == obj2.value
        #
        # access to obj1 is organized to always trigger loading from zstor.
        # access to obj2 goes through zconn cache and so verifies whether the
        # cache is not stale.
        failed = threading.Event()
        failure = [None]

        def verify():
            transaction.begin()
            zconn = db.open()

            root = zconn.root()
            obj1 = root['obj1']
            obj2 = root['obj2']

            # obj1 - reload it from zstor
            # obj2 - get it from zconn cache
            obj1._p_invalidate()

            # both objects must have the same values
            v1 = obj1.value
            v2 = obj2.value
            if v1 != v2:
                failure[0] = "verify: obj1.value (%d)  !=  obj2.value (%d)" % (
                    v1, v2)
                failed.set()

            # we did not changed anything; also fails with commit:
            transaction.abort()
            zconn.close()

        # modify changes obj1/obj2 by doing `objX.value += 1`.
        #
        # Since both objects start from 0, the invariant that
        # `obj1.value == obj2.value` is always preserved.
        def modify():
            transaction.begin()
            zconn = db.open()

            root = zconn.root()
            obj1 = root['obj1']
            obj2 = root['obj2']
            obj1.value += 1
            obj2.value += 1
            assert obj1.value == obj2.value

            transaction.commit()
            zconn.close()

        # xrun runs f in a loop until either N iterations, or until failed is
        # set.
        def xrun(f, N):
            try:
                for i in range(N):
                    # print('%s.%d' % (f.__name__, i))
                    f()
                    if failed.is_set():
                        break
            except:  # noqa: E722 do not use bare 'except'
                failed.set()
                raise

        # loop verify and modify concurrently.
        init()

        N = 500
        tverify = threading.Thread(
            name='Tverify', target=xrun, args=(verify, N))
        tmodify = threading.Thread(
            name='Tmodify', target=xrun, args=(modify, N))
        tverify.start()
        tmodify.start()
        tverify.join(60)
        tmodify.join(60)

        if failed.is_set():
            self.fail(failure[0])

    # client-server storages like ZEO, NEO and RelStorage allow several storage
    # clients to be connected to single storage server.
    #
    # For client-server storages test subclasses should implement
    # _new_storage_client to return new storage client that is connected to the
    # same storage server self._storage is connected to.

    def _new_storage_client(self):
        raise NotImplementedError

    # verify storage for race in between load and external invalidations.
    # https://github.com/zopefoundation/ZEO/issues/155
    #
    # This test is similar to check_race_loadopen_vs_local_invalidate but does
    # not reuse its code because the probability to reproduce external
    # invalidation bug with only 1 mutator + 1 verifier is low.
    @with_high_concurrency
    def check_race_load_vs_external_invalidate(self):
        # dbopen creates new client storage connection and wraps it with DB.
        def dbopen():
            try:
                zstor = self._new_storage_client()
            except NotImplementedError:
                # the test will be skipped from main thread because dbopen is
                # first used in init on the main thread before any other thread
                # is spawned.
                self.skipTest(
                    "%s does not implement _new_storage_client" % type(self))
            return DB(zstor)

        # init initializes the database with two integer objects - obj1/obj2
        # that are set to 0.
        def init():
            db = dbopen()

            transaction.begin()
            zconn = db.open()

            root = zconn.root()
            root['obj1'] = MinPO(0)
            root['obj2'] = MinPO(0)

            transaction.commit()
            zconn.close()

            db.close()

        # we'll run 8 T workers concurrently. As of 20210416, due to race
        # conditions in ZEO, it triggers the bug where T sees stale obj2 with
        # obj1.value != obj2.value
        #
        # The probability to reproduce the bug is significantly reduced with
        # decreasing n(workers): almost never with nwork=2 and sometimes with
        # nwork=4.
        nwork = 8

        # T is a worker that accesses obj1/obj2 in a loop and verifies
        # `obj1.value == obj2.value` invariant.
        #
        # access to obj1 is organized to always trigger loading from zstor.
        # access to obj2 goes through zconn cache and so verifies whether the
        # cache is not stale.
        #
        # Once in a while T tries to modify obj{1,2}.value maintaining the
        # invariant as test source of changes for other workers.
        failed = threading.Event()
        failure = [None] * nwork  # [tx] is failure from T(tx)

        def T(tx, N):
            db = dbopen()

            def t_():
                transaction.begin()
                zconn = db.open()

                root = zconn.root()
                obj1 = root['obj1']
                obj2 = root['obj2']

                # obj1 - reload it from zstor
                # obj2 - get it from zconn cache
                obj1._p_invalidate()

                # both objects must have the same values
                i1 = obj1.value
                i2 = obj2.value
                if i1 != i2:
                    # print('FAIL')
                    failure[tx] = (
                        "T%s: obj1.value (%d)  !=  obj2.value (%d)" % (
                            tx, i1, i2))
                    failed.set()

                # change objects once in a while
                if randint(0, 4) == 0:
                    # print("T%s: modify" % tx)
                    obj1.value += 1
                    obj2.value += 1

                try:
                    transaction.commit()
                except POSException.ConflictError:
                    # print('conflict -> ignore')
                    transaction.abort()

                zconn.close()

            try:
                for i in range(N):
                    # print('T%s.%d' % (tx, i))
                    t_()
                    if failed.is_set():
                        break
            except:  # noqa: E722 do not use bare 'except'
                failed.set()
                raise
            finally:
                db.close()

        # run the workers concurrently.
        init()

        N = 100
        tg = []
        for x in range(nwork):
            t = threading.Thread(name='T%d' % x, target=T, args=(x, N))
            t.start()
            tg.append(t)

        for t in tg:
            t.join(60)

        if failed.is_set():
            self.fail([_ for _ in failure if _])
