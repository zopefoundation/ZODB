##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################

import threading
import time
from random import Random

import transaction

from BTrees.check import check, display
from BTrees.OOBTree import OOBTree

from ZEO.tests.TestThread import TestThread

from ZODB.DB import DB
from ZODB.POSException import ReadConflictError, ConflictError

# The tests here let several threads have a go at one or more database
# instances simultaneously.  Each thread appends a disjoint (from the
# other threads) sequence of increasing integers to an OOBTree, one at
# at time (per thread).  This provokes lots of conflicts, and BTrees
# work hard at conflict resolution too.  An OOBTree is used because
# that flavor has the smallest maximum bucket size, and so splits buckets
# more often than other BTree flavors.
#
# When these tests were first written, they provoked an amazing number
# of obscure timing-related bugs in cache consistency logic, revealed
# by failure of the BTree to pass internal consistency checks at the end,
# and/or by failure of the BTree to contain all the keys the threads
# thought they added (i.e., the keys for which transaction.commit()
# did not raise any exception).

class FailableThread(TestThread):

    # mixin class
    # subclass must provide
    # - self.stop attribute (an event)
    # - self._testrun() method

    # TestThread.run() invokes testrun().
    def testrun(self):
        try:
            self._testrun()
        except:
            # Report the failure here to all the other threads, so
            # that they stop quickly.
            self.stop.set()
            raise


class StressTask:
    # Append integers startnum, startnum + step, startnum + 2*step, ...
    # to 'tree'.  If sleep is given, sleep
    # that long after each append.  At the end, instance var .added_keys
    # is a list of the ints the thread believes it added successfully.
    def __init__(self, db, threadnum, startnum, step=2, sleep=None):
        self.db = db
        self.threadnum = threadnum
        self.startnum = startnum
        self.step = step
        self.sleep = sleep
        self.added_keys = []
        self.tm = transaction.TransactionManager()
        self.cn = self.db.open(transaction_manager=self.tm)
        self.cn.sync()

    def doStep(self):
        tree = self.cn.root()["tree"]
        key = self.startnum
        tree[key] = self.threadnum

    def commit(self):
        cn = self.cn
        key = self.startnum
        self.tm.get().note("add key %s" % key)
        try:
            self.tm.get().commit()
        except ConflictError, msg:
            self.tm.abort()
        else:
            if self.sleep:
                time.sleep(self.sleep)
            self.added_keys.append(key)
        self.startnum += self.step

    def cleanup(self):
        self.tm.get().abort()
        self.cn.close()

def _runTasks(rounds, *tasks):
    '''run *task* interleaved for *rounds* rounds.'''
    def commit(run, actions):
        actions.append(':')
        for t in run:
            t.commit()
        del run[:]
    r = Random()
    r.seed(1064589285) # make it deterministic
    run = []
    actions = []
    try:
        for i in range(rounds):
            t = r.choice(tasks)
            if t in run:
                commit(run, actions)
            run.append(t)
            t.doStep()
            actions.append(`t.startnum`)
        commit(run,actions)
        # stderr.write(' '.join(actions)+'\n')
    finally:
        for t in tasks:
            t.cleanup()


class StressThread(FailableThread):

    # Append integers startnum, startnum + step, startnum + 2*step, ...
    # to 'tree' until Event stop is set.  If sleep is given, sleep
    # that long after each append.  At the end, instance var .added_keys
    # is a list of the ints the thread believes it added successfully.
    def __init__(self, testcase, db, stop, threadnum, commitdict,
                 startnum, step=2, sleep=None):
        TestThread.__init__(self, testcase)
        self.db = db
        self.stop = stop
        self.threadnum = threadnum
        self.startnum = startnum
        self.step = step
        self.sleep = sleep
        self.added_keys = []
        self.commitdict = commitdict

    def _testrun(self):
        tm = transaction.TransactionManager()
        cn = self.db.open(transaction_manager=tm)
        while not self.stop.isSet():
            try:
                tree = cn.root()["tree"]
                break
            except (ConflictError, KeyError):
                tm.abort()
        key = self.startnum
        while not self.stop.isSet():
            try:
                tree[key] = self.threadnum
                tm.get().note("add key %s" % key)
                tm.commit()
                self.commitdict[self] = 1
                if self.sleep:
                    time.sleep(self.sleep)
            except (ReadConflictError, ConflictError), msg:
                tm.abort()
            else:
                self.added_keys.append(key)
            key += self.step
        cn.close()

class LargeUpdatesThread(FailableThread):

    # A thread that performs a lot of updates.  It attempts to modify
    # more than 25 objects so that it can test code that runs vote
    # in a separate thread when it modifies more than 25 objects.

    def __init__(self, test, db, stop, threadnum, commitdict, startnum,
                 step=2, sleep=None):
        TestThread.__init__(self, test)
        self.db = db
        self.stop = stop
        self.threadnum = threadnum
        self.startnum = startnum
        self.step = step
        self.sleep = sleep
        self.added_keys = []
        self.commitdict = commitdict

    def _testrun(self):
        cn = self.db.open()
        while not self.stop.isSet():
            try:
                tree = cn.root()["tree"]
                break
            except (ConflictError, KeyError):
                # print "%d getting tree abort" % self.threadnum
                transaction.abort()

        keys_added = {} # set of keys we commit
        tkeys = []
        while not self.stop.isSet():

            # The test picks 50 keys spread across many buckets.
            # self.startnum and self.step ensure that all threads use
            # disjoint key sets, to minimize conflict errors.

            nkeys = len(tkeys)
            if nkeys < 50:
                tkeys = range(self.startnum, 3000, self.step)
                nkeys = len(tkeys)
            step = max(int(nkeys / 50), 1)
            keys = [tkeys[i] for i in range(0, nkeys, step)]
            for key in keys:
                try:
                    tree[key] = self.threadnum
                except (ReadConflictError, ConflictError), msg:
                    # print "%d setting key %s" % (self.threadnum, msg)
                    transaction.abort()
                    break
            else:
                # print "%d set #%d" % (self.threadnum, len(keys))
                transaction.get().note("keys %s" % ", ".join(map(str, keys)))
                try:
                    transaction.commit()
                    self.commitdict[self] = 1
                    if self.sleep:
                        time.sleep(self.sleep)
                except ConflictError, msg:
                    # print "%d commit %s" % (self.threadnum, msg)
                    transaction.abort()
                    continue
                for k in keys:
                    tkeys.remove(k)
                    keys_added[k] = 1
        self.added_keys = keys_added.keys()
        cn.close()

class InvalidationTests:

    # Minimum # of seconds the main thread lets the workers run.  The
    # test stops as soon as this much time has elapsed, and all threads
    # have managed to commit a change.
    MINTIME = 10

    # Maximum # of seconds the main thread lets the workers run.  We
    # stop after this long has elapsed regardless of whether all threads
    # have managed to commit a change.
    MAXTIME = 300

    StressThread = StressThread

    def _check_tree(self, cn, tree):
        # Make sure the BTree is sane at the C level.
        retries = 3
        while retries:
            retries -= 1
            try:
                check(tree)
                tree._check()
            except ReadConflictError:
                if retries:
                    transaction.abort()
                else:
                    raise
            except:
                display(tree)
                raise

    def _check_threads(self, tree, *threads):
        # Make sure the thread's view of the world is consistent with
        # the actual database state.

        expected_keys = []
        errormsgs = []
        err = errormsgs.append

        for t in threads:
            if not t.added_keys:
                err("thread %d didn't add any keys" % t.threadnum)
            expected_keys.extend(t.added_keys)
        expected_keys.sort()

        for i in range(100):
            tree._p_jar.sync()
            actual_keys = list(tree.keys())
            if expected_keys == actual_keys:
                break
            time.sleep(.1)
        else:
            err("expected keys != actual keys")
            for k in expected_keys:
                if k not in actual_keys:
                    err("key %s expected but not in tree" % k)
            for k in actual_keys:
                if k not in expected_keys:
                    err("key %s in tree but not expected" % k)

            self.fail('\n'.join(errormsgs))

    def go(self, stop, commitdict, *threads):
        # Run the threads
        for t in threads:
            t.start()
        delay = self.MINTIME
        start = time.time()
        while time.time() - start <= self.MAXTIME:
            stop.wait(delay)
            if stop.isSet():
                # Some thread failed.  Stop right now.
                break
            delay = 2.0
            if len(commitdict) >= len(threads):
                break
            # Some thread still hasn't managed to commit anything.
        stop.set()
        # Give all the threads some time to stop before trying to clean up.
        # cleanup() will cause the test to fail if some thread ended with
        # an uncaught exception, and unittest will call the base class
        # tearDown then immediately, but if other threads are still
        # running that can lead to a cascade of spurious exceptions.
        for t in threads:
            t.join(30)
        for t in threads:
            t.cleanup(10)

    def checkConcurrentUpdates2Storages_emulated(self):
        self._storage = storage1 = self.openClientStorage()
        storage2 = self.openClientStorage()
        db1 = DB(storage1)
        db2 = DB(storage2)

        cn = db1.open()
        tree = cn.root()["tree"] = OOBTree()
        transaction.commit()
        # DM: allow time for invalidations to come in and process them
        time.sleep(0.1)

        # Run two threads that update the BTree
        t1 = StressTask(db1, 1, 1,)
        t2 = StressTask(db2, 2, 2,)
        _runTasks(100, t1, t2)

        cn.sync()
        self._check_tree(cn, tree)
        self._check_threads(tree, t1, t2)

        cn.close()
        db1.close()
        db2.close()

    def checkConcurrentUpdates2Storages(self):
        self._storage = storage1 = self.openClientStorage()
        storage2 = self.openClientStorage()
        db1 = DB(storage1)
        db2 = DB(storage2)
        stop = threading.Event()

        cn = db1.open()
        tree = cn.root()["tree"] = OOBTree()
        transaction.commit()
        cn.close()

        # Run two threads that update the BTree
        cd = {}
        t1 = self.StressThread(self, db1, stop, 1, cd, 1)
        t2 = self.StressThread(self, db2, stop, 2, cd, 2)
        self.go(stop, cd, t1, t2)

        while db1.lastTransaction() != db2.lastTransaction():
            db1._storage.sync()
            db2._storage.sync()

        cn = db1.open()
        tree = cn.root()["tree"]
        self._check_tree(cn, tree)
        self._check_threads(tree, t1, t2)

        cn.close()
        db1.close()
        db2.close()

    def checkConcurrentUpdates19Storages(self):
        n = 19
        dbs = [DB(self.openClientStorage()) for i in range(n)]
        self._storage = dbs[0].storage
        stop = threading.Event()

        cn = dbs[0].open()
        tree = cn.root()["tree"] = OOBTree()
        transaction.commit()
        cn.close()

        # Run threads that update the BTree
        cd = {}
        threads = [self.StressThread(self, dbs[i], stop, i, cd, i, n)
                   for i in range(n)]
        self.go(stop, cd, *threads)

        while len(set(db.lastTransaction() for db in dbs)) > 1:
            _ = [db._storage.sync() for db in dbs]

        cn = dbs[0].open()
        tree = cn.root()["tree"]
        self._check_tree(cn, tree)
        self._check_threads(tree, *threads)

        cn.close()
        _ = [db.close() for db in dbs]

    def checkConcurrentUpdates1Storage(self):
        self._storage = storage1 = self.openClientStorage()
        db1 = DB(storage1)
        stop = threading.Event()

        cn = db1.open()
        tree = cn.root()["tree"] = OOBTree()
        transaction.commit()
        cn.close()

        # Run two threads that update the BTree
        cd = {}
        t1 = self.StressThread(self, db1, stop, 1, cd, 1, sleep=0.01)
        t2 = self.StressThread(self, db1, stop, 2, cd, 2, sleep=0.01)
        self.go(stop, cd, t1, t2)

        cn = db1.open()
        tree = cn.root()["tree"]
        self._check_tree(cn, tree)
        self._check_threads(tree, t1, t2)

        cn.close()
        db1.close()

    def checkConcurrentUpdates2StoragesMT(self):
        self._storage = storage1 = self.openClientStorage()
        db1 = DB(storage1)
        db2 = DB(self.openClientStorage())
        stop = threading.Event()

        cn = db1.open()
        tree = cn.root()["tree"] = OOBTree()
        transaction.commit()
        cn.close()

        # Run three threads that update the BTree.
        # Two of the threads share a single storage so that it
        # is possible for both threads to read the same object
        # at the same time.

        cd = {}
        t1 = self.StressThread(self, db1, stop, 1, cd, 1, 3)
        t2 = self.StressThread(self, db2, stop, 2, cd, 2, 3, 0.01)
        t3 = self.StressThread(self, db2, stop, 3, cd, 3, 3, 0.01)
        self.go(stop, cd, t1, t2, t3)

        while db1.lastTransaction() != db2.lastTransaction():
            time.sleep(.1)

        time.sleep(.1)
        cn = db1.open()
        tree = cn.root()["tree"]
        self._check_tree(cn, tree)
        self._check_threads(tree, t1, t2, t3)

        cn.close()
        db1.close()
        db2.close()

    def checkConcurrentLargeUpdates(self):
        # Use 3 threads like the 2StorageMT test above.
        self._storage = storage1 = self.openClientStorage()
        db1 = DB(storage1)
        db2 = DB(self.openClientStorage())
        stop = threading.Event()

        cn = db1.open()
        tree = cn.root()["tree"] = OOBTree()
        for i in range(0, 3000, 2):
            tree[i] = 0
        transaction.commit()
        cn.close()

        # Run three threads that update the BTree.
        # Two of the threads share a single storage so that it
        # is possible for both threads to read the same object
        # at the same time.

        cd = {}
        t1 = LargeUpdatesThread(self, db1, stop, 1, cd, 1, 3, 0.02)
        t2 = LargeUpdatesThread(self, db2, stop, 2, cd, 2, 3, 0.01)
        t3 = LargeUpdatesThread(self, db2, stop, 3, cd, 3, 3, 0.01)
        self.go(stop, cd, t1, t2, t3)

        while db1.lastTransaction() != db2.lastTransaction():
            db1._storage.sync()
            db2._storage.sync()

        cn = db1.open()
        tree = cn.root()["tree"]
        self._check_tree(cn, tree)

        # Purge the tree of the dummy entries mapping to 0.
        losers = [k for k, v in tree.items() if v == 0]
        for k in losers:
            del tree[k]
        transaction.commit()

        self._check_threads(tree, t1, t2, t3)

        cn.close()
        db1.close()
        db2.close()
