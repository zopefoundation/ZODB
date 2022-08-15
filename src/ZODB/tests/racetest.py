##############################################################################
#
# Copyright (c) 2019 - 2022 Zope Foundation and Contributors.
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
"""Module racetest provides infrastructure and tests to verify storages against
data corruptions caused by race conditions in storages implementations.
"""

import transaction
from ZODB import DB, POSException
from ZODB.tests.MinPO import MinPO
from ZODB.tests.util import with_high_concurrency

import threading
from random import randint


class RaceTests(object):

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
