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

It works by combining

  1) testing models for application behaviour, with
  2) model checkers, that drive provided application model through particular
     scenarios that are likely to hit specific race conditions in storage
     implementation.

If a race condition is hit, it is detected as a breakage of invariant defined
in the model specification.

A model defines application behaviour by specifying initial database state, a
"next" step representing database modification, and an invariant, that should
always be true, no matter how and in which order, simultaneously or serially,
the next steps are applied by database clients. A model specification is
represented by ISpec interface.

A checker drives the model through particular usage scenario where probability
of specific race condition is likely to be high. For example
_check_race_loadopen_vs_local_invalidate runs two client threads, that use
shared storage connection, where one thread repeatedly modifies the database,
and the other thread repeatedly checks the database for breakage of the model
invariant. This checker verifies storages and ZODB.Connection for races in
between load/open and local invalidations to catch bugs similar to
https://github.com/zopefoundation/ZODB/issues/290 and
https://github.com/zopefoundation/ZEO/issues/166.
"""

import transaction
from ZODB import DB, POSException
from ZODB.tests.MinPO import MinPO
from ZODB.tests.util import with_high_concurrency

import threading
from random import randint


class ISpec:
    """ISpec interface represents testing specification used by check_race_*"""

    def init(root):
        """init should initialize database state."""

    def next(root):
        """next should modify database state."""

    def assertStateOK(root):
        """assertStateOK should verify whether database state follows
        intended invariant.

        If not - it should raise AssertionError with details.
        """


class T2ObjectsInc(ISpec):
    """T2ObjectsInc is specification with behaviour where two objects obj1
    and obj2 are incremented synchronously.

    It is used in tests where bugs can be immedeately observed after the race.

    invariant:  obj1 == obj2
    """
    def init(_, root):
        root['obj1'] = MinPO(0)
        root['obj2'] = MinPO(0)

    def next(_, root):
        root['obj1'].value += 1
        root['obj2'].value += 1

    def assertStateOK(_, root):
        # both objects must have the same values
        i1 = root['obj1'].value
        i2 = root['obj2'].value

        if not (i1 == i2):
            raise AssertionError("obj1 (%d)  !=  obj2 (%d)" % (i1, i2))


class RaceTests(object):

    # verify storage/Connection for race in between load/open and local
    # invalidations.
    # https://github.com/zopefoundation/ZEO/issues/166
    # https://github.com/zopefoundation/ZODB/issues/290
    def check_race_loadopen_vs_local_invalidate(self):
        return self._check_race_loadopen_vs_local_invalidate(T2ObjectsInc())

    @with_high_concurrency
    def _check_race_loadopen_vs_local_invalidate(self, spec):
        assert isinstance(spec, ISpec)
        db = DB(self._storage)

        # init initializes the database according to the spec.
        def init():
            _state_init(db, spec)

        # verify accesses the database and verifies spec invariant.
        #
        # Access to half of the objects is organized to always trigger loading
        # from zstor. Access to the other half goes through zconn cache and so
        # verifies whether the cache is not stale.
        failed = threading.Event()
        failure = [None]

        def verify():
            transaction.begin()
            zconn = db.open()
            root = zconn.root()

            # reload some objects from zstor, while getting others from
            # zconn cache
            _state_invalidate_half1(root)

            try:
                spec.assertStateOK(root)
            except AssertionError as e:
                msg = "verify: %s\n" % e
                failure[0] = msg
                failed.set()

            # we did not changed anything; also fails with commit:
            transaction.abort()
            zconn.close()

        # modify changes the database by executing "next" step.
        #
        # Spec invariant should be preserved.
        def modify():
            transaction.begin()
            zconn = db.open()

            root = zconn.root()
            spec.next(root)
            spec.assertStateOK(root)

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
    def check_race_load_vs_external_invalidate(self):
        return self._check_race_load_vs_external_invalidate(T2ObjectsInc())

    @with_high_concurrency
    def _check_race_load_vs_external_invalidate(self, spec):
        assert isinstance(spec, ISpec)

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

        # init initializes the database according to the spec.
        def init():
            db = dbopen()
            _state_init(db, spec)
            db.close()

        # we'll run 8 T workers concurrently. As of 20210416, due to race
        # conditions in ZEO, it triggers the bug where T sees stale obj2 with
        # obj1.value != obj2.value
        #
        # The probability to reproduce the bug is significantly reduced with
        # decreasing n(workers): almost never with nwork=2 and sometimes with
        # nwork=4.
        nwork = 8

        # T is a worker that accesses database in a loop and verifies
        # spec invariant.
        #
        # Access to half of the objects is organized to always trigger loading
        # from zstor. Access to the other half goes through zconn cache and so
        # verifies whether the cache is not stale.
        #
        # Once in a while T tries to modify the database executing spec "next"
        # as test source of changes for other workers.
        failed = threading.Event()
        failure = [None] * nwork  # [tx] is failure from T(tx)

        def T(tx, N):
            db = dbopen()

            def t_():
                transaction.begin()
                zconn = db.open()
                root = zconn.root()

                # reload some objects from zstor, while getting others from
                # zconn cache
                _state_invalidate_half1(root)

                try:
                    spec.assertStateOK(root)
                except AssertionError as e:
                    msg = "T%s: %s\n" % (tx, e)
                    failure[tx] = msg
                    failed.set()

                # change objects once in a while
                if randint(0, 4) == 0:
                    # print("T%s: modify" % tx)
                    spec.next(root)
                    spec.assertStateOK(root)

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


# _state_init initializes the database according to the spec.
def _state_init(db, spec):
    transaction.begin()
    zconn = db.open()
    root = zconn.root()
    spec.init(root)
    spec.assertStateOK(root)
    transaction.commit()
    zconn.close()


# _state_invalidate_half1 invalidatates first 50% of database objects, so that
# the next time they are accessed, they are reloaded from the storage.
def _state_invalidate_half1(root):
    keys = list(sorted(root.keys()))
    for k in keys[:len(keys)//2]:
        obj = root[k]
        obj._p_invalidate()
