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
represented by IModelSpec interface.

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
from __future__ import print_function

import threading
from random import randint

import transaction
from zope.interface import Interface
from zope.interface import implementer

from ZODB import DB
from ZODB import POSException
from ZODB.tests.MinPO import MinPO
from ZODB.tests.util import long_test
from ZODB.tests.util import with_high_concurrency
from ZODB.utils import at2before
from ZODB.utils import tid_repr


class IModelSpec(Interface):
    """IModelSpec interface represents testing specification used by
    check_race_*"""

    def init(root):
        """init should initialize database state."""

    def next(root):
        """next should modify database state."""

    def assertStateOK(root):
        """assertStateOK should verify whether database state follows
        intended invariant.

        If not - it should raise AssertionError with details.
        """


@implementer(IModelSpec)
class T2ObjectsInc:
    """T2ObjectsInc is specification with behaviour where two objects obj1
    and obj2 are incremented synchronously.

    It is used in tests where bugs can be immediately observed after the race.

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


@implementer(IModelSpec)
class T2ObjectsInc2Phase:
    """T2ObjectsInc2Phase is specification with behaviour where two objects
    obj1 and obj2 are incremented in lock-step.

    It is used in tests where bugs can be observed on the next transaction
    after the race.

    invariant:  obj1 - obj2 == phase
    """
    def init(_, root):
        root['obj1'] = MinPO(0)
        root['obj2'] = MinPO(0)
        root['phase'] = MinPO(0)

    def next(_, root):
        phase = root['phase']
        if phase.value == 0:
            root['obj1'].value += 1
        else:
            root['obj2'].value += 1
        phase.value += 1
        phase.value %= 2

    def assertStateOK(_, root):
        i1 = root['obj1'].value
        i2 = root['obj2'].value
        p = root['phase'].value

        if not (i1 - i2 == p):
            raise AssertionError("obj1 (%d) - obj2(%d) != phase (%d)" %
                                 (i1, i2, p))


class RaceTests(object):

    # verify storage/Connection for race in between load/open and local
    # invalidations.
    # https://github.com/zopefoundation/ZEO/issues/166
    # https://github.com/zopefoundation/ZODB/issues/290
    def check_race_loadopen_vs_local_invalidate(self):
        return self._check_race_loadopen_vs_local_invalidate(T2ObjectsInc())

    @with_high_concurrency
    def _check_race_loadopen_vs_local_invalidate(self, spec):
        assert IModelSpec.providedBy(spec)
        db = DB(self._storage)

        # `init` initializes the database according to the spec.
        def init():
            _state_init(db, spec)

        # `verify` accesses objects in the database and verifies spec
        # invariant.
        #
        # Access to half of the objects is organized to always trigger loading
        # from zstor. Access to the other half goes through zconn cache and so
        # verifies whether the cache is not stale.
        def verify(tg):
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
                msg += _state_details(root)
                tg.fail(msg)

            # we did not changed anything; also fails with commit:
            transaction.abort()
            zconn.close()

        # `modify` changes objects in the database by executing "next" step.
        #
        # Spec invariant should be preserved.
        def modify(tg):
            transaction.begin()
            zconn = db.open()

            root = zconn.root()
            spec.next(root)
            spec.assertStateOK(root)

            transaction.commit()
            zconn.close()

        # `xrun` runs f in a loop until either N iterations, or until failed is
        # set.
        def xrun(tg, tx, f, N):
            for i in range(N):
                # print('%s.%d' % (f.__name__, i))
                f(tg)
                if tg.failed():
                    break

        # loop verify and modify concurrently.
        init()

        N = 500
        tg = TestWorkGroup(self)
        tg.go(xrun, verify, N, name='Tverify')
        tg.go(xrun, modify, N, name='Tmodify')
        tg.wait(120)

    # client-server storages like ZEO, NEO and RelStorage allow several storage
    # clients to be connected to single storage server.
    #
    # For client-server storages test subclasses should implement
    # _new_storage_client to return new storage client that is connected to the
    # same storage server self._storage is connected to.

    def _new_storage_client(self):
        raise NotImplementedError

    # `dbopen` creates new client storage connection and wraps it with DB.
    def dbopen(self):
        try:
            zstor = self._new_storage_client()
        except NotImplementedError:
            # the test will be skipped from main thread because dbopen is
            # first used in init on the main thread before any other thread
            # is spawned.
            self.skipTest(
                "%s does not implement _new_storage_client" % type(self))
        return DB(zstor)

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
        assert IModelSpec.providedBy(spec)

        # `init` initializes the database according to the spec.
        def init():
            db = self.dbopen()
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

        # `T` is a worker that accesses database in a loop and verifies
        # spec invariant.
        #
        # Access to half of the objects is organized to always trigger loading
        # from zstor. Access to the other half goes through zconn cache and so
        # verifies whether the cache is not stale.
        #
        # Once in a while T tries to modify the database executing spec "next"
        # as test source of changes for other workers.
        def T(tg, tx, N):
            db = self.dbopen()

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
                    msg += _state_details(root)
                    tg.fail(msg)

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
                    if tg.failed():
                        break
            finally:
                db.close()

        # run the workers concurrently.
        init()

        N = 100
        tg = TestWorkGroup(self)
        for _ in range(nwork):
            tg.go(T, N)
        tg.wait(120)

    # verify storage for race in between client disconnect and external
    # invalidations. https://github.com/zopefoundation/ZEO/issues/209
    #
    # This test is similar to check_race_load_vs_external_invalidate, but
    # increases the number of workers and also makes every worker to repeatedly
    # reconnect to the storage, so that the probability of disconnection is
    # high. It also uses T2ObjectsInc2Phase instead of T2ObjectsInc because if
    # an invalidation is skipped due to the disconnect/invalidation race,
    # T2ObjectsInc won't catch the bug as both objects will be either in old
    # state, or in new state after the next transaction. Contrary to that, with
    # T2ObjectsInc2Phase the invariant will be detected to be broken on the
    # next transaction.
    @long_test
    def check_race_external_invalidate_vs_disconnect(self):
        return self._check_race_external_invalidate_vs_disconnect(
                                                T2ObjectsInc2Phase())

    @with_high_concurrency
    def _check_race_external_invalidate_vs_disconnect(self, spec):
        assert IModelSpec.providedBy(spec)

        # `init` initializes the database according to the spec.
        def init():
            db = self.dbopen()
            _state_init(db, spec)
            db.close()

        nwork = 8*8   # nwork^2 from _check_race_load_vs_external_invalidate

        # `T` is similar to the T from _check_race_load_vs_external_invalidate
        # but reconnects to the database often.
        def T(tg, tx, N):
            def t_():
                def work1(db):
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
                        msg += _state_details(root)
                        tg.fail(msg)

                        zconn.close()
                        transaction.abort()
                        return

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

                db = self.dbopen()
                try:
                    for i in range(4):
                        if tg.failed():
                            break
                        work1(db)
                finally:
                    db.close()

            for i in range(N):
                # print('T%s.%d' % (tx, i))
                if tg.failed():
                    break
                t_()

        # run the workers concurrently.
        init()

        N = 100 // (2*4)  # N reduced to save time
        tg = TestWorkGroup(self)
        for _ in range(nwork):
            tg.go(T, N)
        tg.wait(120)


# `_state_init` initializes the database according to the spec.
def _state_init(db, spec):
    transaction.begin()
    zconn = db.open()
    root = zconn.root()
    spec.init(root)
    spec.assertStateOK(root)
    transaction.commit()
    zconn.close()


# `_state_invalidate_half1` invalidates first 50% of database objects, so
# that the next time they are accessed, they are reloaded from the storage.
def _state_invalidate_half1(root):
    keys = list(sorted(root.keys()))
    for k in keys[:len(keys)//2]:
        obj = root[k]
        obj._p_invalidate()


# `_state_details` returns text details about ZODB objects directly referenced
# by root.
def _state_details(root):  # -> txt
    # serial for all objects
    keys = list(sorted(root.keys()))
    txt = ''
    txt += '  '.join('%s._p_serial: %s' % (k, tid_repr(root[k]._p_serial))
                     for k in keys)
    txt += '\n'

    # zconn.at approximated as max(serials)
    # XXX better retrieve real zconn.at, but currently there is no way to
    # retrieve it for all kind of storages.
    zconn = root._p_jar
    zconn_at = max(root[k]._p_serial for k in keys)
    txt += 'zconn_at: %s  # approximated as max(serials)\n' % \
           tid_repr(zconn_at)

    # zstor.loadBefore(obj, @zconn_at)
    zstor = zconn.db().storage

    def load(key):
        load_txt = 'zstor.loadBefore(%s, @zconn.at)\t->  ' % key
        obj = root[key]
        x = zstor.loadBefore(obj._p_oid, at2before(zconn_at))
        if x is None:
            load_txt += 'None'
        else:
            _, serial, next_serial = x
            load_txt += 'serial: %s  next_serial: %s' % (
                        tid_repr(serial), tid_repr(next_serial))
        load_txt += '\n'
        return load_txt

    for k in keys:
        txt += load(k)

    # try to reset storage cache and retry loading
    # it helps to see if an error was due to the cache getting out of sync
    zcache = getattr(zstor, '_cache', None)  # works for ZEO and NEO
    if zcache is not None:
        zcache.clear()
        txt += 'zstor._cache.clear()\n'
        for k in keys:
            txt += load(k)

    return txt


class TestWorkGroup(object):
    """TestWorkGroup represents group of threads that run together to verify
       something.

       - .go() adds test thread to the group.
       - .wait() waits for all spawned threads to finish and reports all
         collected failures to containing testcase.
       - a test should indicate failure by call to .fail(), it
         can check for a failure with .failed()
    """

    def __init__(self, testcase):
        self.testcase = testcase
        self.failed_event = threading.Event()
        self.fail_mu = threading.Lock()
        self.failv = []           # failures registered by .fail
        self.threadv = []         # spawned threads
        self.waitg = WaitGroup()  # to wait for spawned threads

    def fail(self, msg):
        """fail adds failure to test result."""
        with self.fail_mu:
            self.failv.append(msg)
        self.failed_event.set()

    def failed(self):
        """did the test already fail."""
        return self.failed_event.is_set()

    def go(self, f, *argv, **kw):
        """go spawns f(self, #thread, *argv, **kw) in new test thread."""
        self.waitg.add(1)
        tx = len(self.threadv)
        tname = kw.pop('name', 'T%d' % tx)
        t = Daemon(name=tname, target=self._run, args=(f, tx, argv, kw))
        self.threadv.append(t)
        t.start()

    def _run(self, f, tx, argv, kw):
        tname = self.threadv[tx].name
        try:
            f(self, tx, *argv, **kw)
        except Exception as e:
            self.fail("Unhandled exception %r in thread %s"
                      % (e, tname))
            raise
        finally:
            self.waitg.done()

    def wait(self, timeout):
        """wait waits for all test threads to complete and reports all
           collected failures to containing testcase."""
        if not self.waitg.wait(timeout):
            self.fail("test did not finish within %s seconds" % timeout)

        failed_to_finish = []
        for t in self.threadv:
            try:
                t.join(1)
            except AssertionError:
                self.failed_event.set()
                failed_to_finish.append(t.name)
        if failed_to_finish:
            self.fail("threads did not finish: %s" % failed_to_finish)
        del self.threadv  # avoid cyclic garbage

        if self.failed():
            self.testcase.fail('\n\n'.join(self.failv))


class Daemon(threading.Thread):
    """auxiliary class to create daemon threads and fail if not stopped.

    In addition, the class ensures that reports for uncaught exceptions
    are output holding a lock. This prevents that concurrent reports
    get intermixed and facilitates the exception analysis.
    """
    def __init__(self, **kw):
        super(Daemon, self).__init__(**kw)
        self.daemon = True
        if hasattr(self, "_invoke_excepthook"):
            # Python 3.8+
            ori_invoke_excepthook = self._invoke_excepthook

            def invoke_excepthook(*args, **kw):
                with exc_lock:
                    return ori_invoke_excepthook(*args, **kw)

            self._invoke_excepthook = invoke_excepthook
        else:
            # old Python
            ori_run = self.run

            def run():
                from threading import _format_exc
                from threading import _sys
                try:
                    ori_run()
                except SystemExit:
                    pass
                except BaseException:
                    if _sys and _sys.stderr is not None:
                        with exc_lock:
                            print("Exception in thread %s:\n%s" %
                                  (self.name, _format_exc()),
                                  file=_sys.stderr)
                    else:
                        raise
                finally:
                    del self.run

            self.run = run

    def join(self, *args, **kw):
        super(Daemon, self).join(*args, **kw)
        if self.is_alive():
            raise AssertionError("Thread %s did not stop" % self.name)


# lock to ensure that Daemon exception reports are output atomically
exc_lock = threading.Lock()


class WaitGroup(object):
    """WaitGroup provides service to wait for spawned workers to be done.

       - .add() adds workers
       - .done() indicates that one worker is done
       - .wait() waits until all workers are done
    """
    def __init__(self):
        self.n = 0
        self.condition = threading.Condition()

    def add(self, delta):
        with self.condition:
            self.n += delta
            if self.n < 0:
                raise AssertionError("#workers is negative")
            if self.n == 0:
                self.condition.notify_all()

    def done(self):
        self.add(-1)

    def wait(self, timeout):  # -> ok
        with self.condition:
            if self.n == 0:
                return True
            ok = self.condition.wait(timeout)
            if ok is None:  # py2
                ok = (self.n == 0)
            return ok
