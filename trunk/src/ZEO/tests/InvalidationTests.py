##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################

from thread import get_ident
import threading
import time

from BTrees.check import check, display
from BTrees.OOBTree import OOBTree

from ZEO.tests.TestThread import TestThread
from ZEO.tests.ConnectionTests import CommonSetupTearDown

from ZODB.DB import DB
from ZODB.POSException \
     import ReadConflictError, ConflictError, VersionLockError
import zLOG

class StressThread(TestThread):

    def __init__(self, testcase, db, stop, threadnum, startnum,
                 step=2, sleep=None):
        TestThread.__init__(self, testcase)
        self.db = db
        self.stop = stop
        self.threadnum = threadnum
        self.startnum = startnum
        self.step = step
        self.sleep = sleep
        self.added_keys = []

    def testrun(self):
        cn = self.db.open()
        while not self.stop.isSet():
            try:
                tree = cn.root()["tree"]
                break
            except (ConflictError, KeyError):
                get_transaction().abort()
                cn.sync()
        key = self.startnum
        while not self.stop.isSet():
            try:
                tree[key] = self.threadnum
                get_transaction().note("add key %s" % key)
                get_transaction().commit()
                if self.sleep:
                    time.sleep(self.sleep)
            except (ReadConflictError, ConflictError), msg:
                get_transaction().abort()
                # sync() is necessary here to process invalidations
                # if we get a read conflict.  In the read conflict case,
                # no objects were modified so cn never got registered
                # with the transaction.
                cn.sync()
            else:
                self.added_keys.append(key)
                key += self.step
        cn.close()

class VersionStressThread(TestThread):
    
    def __init__(self, testcase, db, stop, threadnum, startnum,
                 step=2, sleep=None):
        TestThread.__init__(self, testcase)
        self.db = db
        self.stop = stop
        self.threadnum = threadnum
        self.startnum = startnum
        self.step = step
        self.sleep = sleep
        self.added_keys = []

    def log(self, msg):
        zLOG.LOG("thread %d" % get_ident(), 0, msg)

    def testrun(self):
        self.log("thread begin")
        commit = 0
        key = self.startnum
        while not self.stop.isSet():
            version = "%s:%s" % (self.threadnum, key)
            commit = not commit
            self.log("attempt to add key=%s version=%s commit=%d" %
                     (key, version, commit))
            if self.oneupdate(version, key, commit):
                self.added_keys.append(key)
            key += self.step

    def oneupdate(self, version, key, commit=1):
        # The mess of sleeps below were added to reduce the number
        # of VersionLockErrors, based on empirical observation.
        # It looks like the threads don't switch enough without
        # the sleeps.
        
        cn = self.db.open(version)
        while not self.stop.isSet():
            try:
                tree = cn.root()["tree"]
                break
            except (ConflictError, KeyError):
                get_transaction().abort()
                cn.sync()
        while not self.stop.isSet():
            try:
                tree[key] = self.threadnum
                get_transaction().note("add key %d" % key)
                get_transaction().commit()
                if self.sleep:
                    time.sleep(self.sleep)
                break
            except (VersionLockError, ReadConflictError, ConflictError), msg:
                self.log(msg)
                get_transaction().abort()
                # sync() is necessary here to process invalidations
                # if we get a read conflict.  In the read conflict case,
                # no objects were modified so cn never got registered
                # with the transaction.
                cn.sync()
                if self.sleep:
                    time.sleep(self.sleep)
        try:
            while not self.stop.isSet():
                try:
                    if commit:
                        self.db.commitVersion(version)
                        get_transaction().note("commit version %s" % version)
                    else:
                        self.db.abortVersion(version)
                        get_transaction().note("abort version %s" % version)
                    get_transaction().commit()
                    if self.sleep:
                        time.sleep(self.sleep)
                    return commit
                except ConflictError, msg:
                    self.log(msg)
                    get_transaction().abort()
                    cn.sync()
        finally:
            cn.close()
        return 0

class InvalidationTests(CommonSetupTearDown):

    level = 2
    DELAY = 15

    def _check_tree(self, cn, tree):
        # Make sure the BTree is sane and that all the updates persisted
        retries = 3
        while retries:
            retries -= 1
            try:
                check(tree)
                tree._check()
            except ReadConflictError:
                if retries:
                    get_transaction().abort()
                    cn.sync()
                else:
                    raise
            except:
                display(tree)
                raise

    def _check_threads(self, tree, *threads):
        # Make sure the thread's view of the world is consistent with
        # the actual database state.
        for t in threads:
            # If the test didn't add any keys, it didn't do what we expected.
            self.assert_(t.added_keys)
            for key in t.added_keys:
                self.assert_(tree.has_key(key), key)

    def go(self, stop, *threads):
        # Run the threads
        for t in threads:
            t.start()
        time.sleep(self.DELAY)
        stop.set()
        for t in threads:
            t.cleanup()
    
    def checkConcurrentUpdates2Storages(self):
        self._storage = storage1 = self.openClientStorage()
        storage2 = self.openClientStorage(cache="2")
        db1 = DB(storage1)
        db2 = DB(storage2)
        stop = threading.Event()

        cn = db1.open()
        tree = cn.root()["tree"] = OOBTree()
        get_transaction().commit()

        # Run two threads that update the BTree
        t1 = StressThread(self, db1, stop, 1, 1)
        t2 = StressThread(self, db2, stop, 2, 2)
        self.go(stop, t1, t2)

        cn.sync()
        self._check_tree(cn, tree)
        self._check_threads(tree, t1, t2)

        cn.close()
        db1.close()
        db2.close()

    def checkConcurrentUpdates1Storage(self):
        self._storage = storage1 = self.openClientStorage()
        db1 = DB(storage1)
        stop = threading.Event()

        cn = db1.open()
        tree = cn.root()["tree"] = OOBTree()
        get_transaction().commit()

        # Run two threads that update the BTree
        t1 = StressThread(self, db1, stop, 1, 1, sleep=0.001)
        t2 = StressThread(self, db1, stop, 2, 2, sleep=0.001)
        self.go(stop, t1, t2)

        cn.sync()
        self._check_tree(cn, tree)
        self._check_threads(tree, t1, t2)

        cn.close()
        db1.close()

    def checkConcurrentUpdates2StoragesMT(self):
        self._storage = storage1 = self.openClientStorage()
        db1 = DB(storage1)
        stop = threading.Event()

        cn = db1.open()
        tree = cn.root()["tree"] = OOBTree()
        get_transaction().commit()

        db2 = DB(self.openClientStorage(cache="2"))
        # Run three threads that update the BTree.
        # Two of the threads share a single storage so that it
        # is possible for both threads to read the same object
        # at the same time.
        
        t1 = StressThread(self, db1, stop, 1, 1, 3)
        t2 = StressThread(self, db2, stop, 2, 2, 3, 0.001)
        t3 = StressThread(self, db2, stop, 3, 3, 3, 0.001)
        self.go(stop, t1, t2, t3)

        cn.sync()
        self._check_tree(cn, tree)
        self._check_threads(tree, t1, t2, t3)

        cn.close()
        db1.close()
        db2.close()

    def checkConcurrentUpdatesInVersions(self):
        self._storage = storage1 = self.openClientStorage()
        db1 = DB(storage1)
        db2 = DB(self.openClientStorage(cache="2"))
        stop = threading.Event()

        cn = db1.open()
        tree = cn.root()["tree"] = OOBTree()
        get_transaction().commit()

        # Run three threads that update the BTree.
        # Two of the threads share a single storage so that it
        # is possible for both threads to read the same object
        # at the same time.
        
        t1 = VersionStressThread(self, db1, stop, 1, 1, 3)
        t2 = VersionStressThread(self, db2, stop, 2, 2, 3, 0.001)
        t3 = VersionStressThread(self, db2, stop, 3, 3, 3, 0.001)
        self.go(stop, t1, t2, t3)

        cn.sync()
        self._check_tree(cn, tree)
        self._check_threads(tree, t1, t2, t3)

        cn.close()
        db1.close()
        db2.close()

