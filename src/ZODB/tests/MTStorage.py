import random
import sys
import threading
import time

import ZODB
from PersistentMapping import PersistentMapping

from ZODB.tests.StorageTestBase \
     import StorageTestBase, zodb_pickle, zodb_unpickle, handle_serials
from ZODB.tests.MinPO import MinPO
from ZODB.Transaction import Transaction
from ZODB.POSException import ConflictError

SHORT_DELAY = 0.01

def sort(l):
    "Sort a list in place and return it."
    l.sort()
    return l

class TestThread(threading.Thread):
    """Base class for defining threads that run from unittest.

    If the thread exits with an uncaught exception, catch it and
    re-raise it when the thread is joined.  The re-raise will cause
    the test to fail.

    The subclass should define a runtest() method instead of a run()
    method.
    """

    def __init__(self, test):
        threading.Thread.__init__(self)
        self.test = test
        self._fail = None
        self._exc_info = None

    def run(self):
        try:
            self.runtest()
        except:
            self._exc_info = sys.exc_info()

    def fail(self, msg=""):
        self._test.fail(msg)

    def join(self, timeout=None):
        threading.Thread.join(self, timeout)
        if self._exc_info:
            raise self._exc_info[0], self._exc_info[1], self._exc_info[2]

class ZODBClientThread(TestThread):

    __super_init = TestThread.__init__

    def __init__(self, db, test, commits=10, delay=SHORT_DELAY):
        self.__super_init(test)
        self.setDaemon(1)
        self.db = db
        self.test = test
        self.commits = commits
        self.delay = delay

    def runtest(self):
        conn = self.db.open()
        root = conn.root()
        d = self.get_thread_dict(root)
        if d is None:
            self.test.fail()
        else:
            for i in range(self.commits):
                self.commit(d, i)
        self.test.assertEqual(sort(d.keys()), range(self.commits))

    def commit(self, d, num):
        d[num] = time.time()
        time.sleep(self.delay)
        get_transaction().commit()
        time.sleep(self.delay)

    def get_thread_dict(self, root):
        name = self.getName()
        # arbitrarily limit to 10 re-tries
        for i in range(10):
            try:
                m = PersistentMapping()
                root[name] = m
                get_transaction().commit()
                break
            except ConflictError, err:
                get_transaction().abort()
                root._p_jar.sync()
        for i in range(10):
            try:
                return root.get(name)
            except ConflictError:
                get_transaction().abort()
                root._p_jar.sync()

class StorageClientThread(TestThread):

    __super_init = TestThread.__init__

    def __init__(self, storage, test, commits=10, delay=SHORT_DELAY):
        self.__super_init(test)
        self.storage = storage
        self.test = test
        self.commits = commits
        self.delay = delay
        self.oids = {}

    def runtest(self):
        for i in range(self.commits):
            self.dostore(i)
        self.check()

    def check(self):
        for oid, revid in self.oids.items():
            data, serial = self.storage.load(oid, '')
            self.test.assertEqual(serial, revid)
            obj = zodb_unpickle(data)
            self.test.assertEqual(obj.value[0], self.getName())

    def pause(self):
        time.sleep(self.delay)

    def oid(self):
        oid = self.storage.new_oid()
        self.oids[oid] = None
        return oid

    def dostore(self, i):
        data = zodb_pickle(MinPO((self.getName(), i)))
        t = Transaction()
        oid = self.oid()
        self.pause()

        self.storage.tpc_begin(t)
        self.pause()

        # Always create a new object, signified by None for revid
        r1 = self.storage.store(oid, None, data, '', t)
        self.pause()

        r2 = self.storage.tpc_vote(t)
        self.pause()

        self.storage.tpc_finish(t)
        self.pause()

        revid = handle_serials(oid, r1, r2)
        self.oids[oid] = revid

class ExtStorageClientThread(StorageClientThread):

    def runtest(self):
        # pick some other storage ops to execute
        ops = [getattr(self, meth) for meth in dir(ExtStorageClientThread)
               if meth.startswith('do_')]
        assert ops, "Didn't find an storage ops in %s" % self.storage
        # do a store to guarantee there's at least one oid in self.oids
        self.dostore(0)

        for i in range(self.commits - 1):
            meth = random.choice(ops)
            meth()
            self.dostore(i)
        self.check()

    def pick_oid(self):
        return random.choice(self.oids.keys())

    def do_load(self):
        oid = self.pick_oid()
        self.storage.load(oid, '')

    def do_loadSerial(self):
        oid = self.pick_oid()
        self.storage.loadSerial(oid, self.oids[oid])

    def do_modifiedInVersion(self):
        oid = self.pick_oid()
        self.storage.modifiedInVersion(oid)

    def do_undoLog(self):
        self.storage.undoLog(0, -20)

    def do_iterator(self):
        try:
            iter = self.storage.iterator()
        except AttributeError:
            # XXX It's hard to detect that a ZEO ClientStorage
            # doesn't have this method, but does have all the others.
            return
        for obj in iter:
            pass

class MTStorage:
    "Test a storage with multiple client threads executing concurrently."

    def _checkNThreads(self, n, constructor, *args):
        threads = [constructor(*args) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
        for t in threads:
            self.failIf(t.isAlive(), "thread failed to finish in 60 seconds")

    def check2ZODBThreads(self):
        db = ZODB.DB(self._storage)
        self._checkNThreads(2, ZODBClientThread, db, self)

    def check7ZODBThreads(self):
        db = ZODB.DB(self._storage)
        self._checkNThreads(7, ZODBClientThread, db, self)

    def check2StorageThreads(self):
        self._checkNThreads(2, StorageClientThread, self._storage, self)

    def check7StorageThreads(self):
        self._checkNThreads(7, StorageClientThread, self._storage, self)

    def check4ExtStorageThread(self):
        self._checkNThreads(4, ExtStorageClientThread, self._storage, self)
