import random
import sys
import threading
import time

import six

import transaction
from persistent.mapping import PersistentMapping

import ZODB
from ZODB.Connection import TransactionMetaData
from ZODB.POSException import ConflictError
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_pickle
from ZODB.tests.StorageTestBase import zodb_unpickle
from ZODB.utils import load_current


SHORT_DELAY = 0.01


class TestThread(threading.Thread):
    """Base class for defining threads that run from unittest.

    If the thread exits with an uncaught exception, catch it and
    re-raise it when the thread is joined.  The re-raise will cause
    the test to fail.

    The subclass should define a runtest() method instead of a run()
    method.
    """

    def __init__(self):
        threading.Thread.__init__(self)
        self._exc_info = None

    def run(self):
        try:
            self.runtest()
        except:  # noqa: E722 do not use bare 'except'
            self._exc_info = sys.exc_info()

    def join(self, timeout=None):
        threading.Thread.join(self, timeout)
        if self._exc_info:
            raise six.reraise(
                self._exc_info[0], self._exc_info[1], self._exc_info[2])


class ZODBClientThread(TestThread):

    __super_init = TestThread.__init__

    def __init__(self, db, test, commits=10, delay=SHORT_DELAY):
        self.__super_init()
        self.daemon = True
        self.db = db
        self.test = test
        self.commits = commits
        self.delay = delay

    def runtest(self):
        conn = self.db.open()
        conn.sync()
        root = conn.root()
        d = self.get_thread_dict(root)
        if d is None:
            self.test.fail()
        else:
            for i in range(self.commits):
                self.commit(d, i)
        self.test.assertEqual(sorted(d.keys()), list(range(self.commits)))
        conn.close()

    def commit(self, d, num):
        d[num] = time.time()
        time.sleep(self.delay)
        transaction.commit()
        time.sleep(self.delay)

    # Return a new PersistentMapping, and store it on the root object under
    # the name of the current thread.
    def get_thread_dict(self, root):
        # This is vicious:  multiple threads are slamming changes into the
        # root object, then trying to read the root object, simultaneously
        # and without any coordination.  Conflict errors are rampant.  It
        # used to go around at most 10 times, but that fairly often failed
        # to make progress in the 7-thread tests on some test boxes.  Going
        # around (at most) 1000 times was enough so that a 100-thread test
        # reliably passed on Tim's hyperthreaded WinXP box (but at the
        # original 10 retries, the same test reliably failed with 15 threads).
        name = self.name
        MAXRETRIES = 1000

        for i in range(MAXRETRIES):
            try:
                root[name] = PersistentMapping()
                transaction.commit()
                break
            except ConflictError:
                root._p_jar.sync()
        else:
            raise ConflictError("Exceeded %d attempts to store" % MAXRETRIES)

        for j in range(MAXRETRIES):
            try:
                return root.get(name)
            except ConflictError:
                root._p_jar.sync()

        raise ConflictError("Exceeded %d attempts to read" % MAXRETRIES)


class StorageClientThread(TestThread):

    __super_init = TestThread.__init__

    def __init__(self, storage, test, commits=10, delay=SHORT_DELAY):
        self.__super_init()
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
            data, serial = load_current(self.storage, oid)
            self.test.assertEqual(serial, revid)
            obj = zodb_unpickle(data)
            self.test.assertEqual(obj.value[0], self.name)

    def pause(self):
        time.sleep(self.delay)

    def oid(self):
        oid = self.storage.new_oid()
        self.oids[oid] = None
        return oid

    def dostore(self, i):
        data = zodb_pickle(MinPO((self.name, i)))
        t = TransactionMetaData()
        oid = self.oid()
        self.pause()

        self.storage.tpc_begin(t)
        self.pause()

        # Always create a new object, signified by None for revid
        self.storage.store(oid, None, data, '', t)
        self.pause()

        self.storage.tpc_vote(t)
        self.pause()

        revid = self.storage.tpc_finish(t)
        self.pause()
        self.oids[oid] = revid


class ExtStorageClientThread(StorageClientThread):

    def runtest(self):
        # pick some other storage ops to execute, depending in part
        # on the features provided by the storage.
        names = ["do_load"]

        storage = self.storage

        try:
            supportsUndo = storage.supportsUndo
        except AttributeError:
            pass
        else:
            if supportsUndo():
                names += ["do_loadSerial", "do_undoLog", "do_iterator"]

        ops = [getattr(self, meth) for meth in names]
        assert ops, "Didn't find an storage ops in %s" % self.storage
        # do a store to guarantee there's at least one oid in self.oids
        self.dostore(0)

        for i in range(self.commits - 1):
            meth = random.choice(ops)
            meth()
            self.dostore(i)
        self.check()

    def pick_oid(self):
        return random.choice(tuple(self.oids))

    def do_load(self):
        oid = self.pick_oid()
        load_current(self.storage, oid)

    def do_loadSerial(self):
        oid = self.pick_oid()
        self.storage.loadSerial(oid, self.oids[oid])

    def do_undoLog(self):
        self.storage.undoLog(0, -20)

    def do_iterator(self):
        try:
            iter = self.storage.iterator()
        except AttributeError:
            # It's hard to detect that a ZEO ClientStorage
            # doesn't have this method, but does have all the others.
            return
        for obj in iter:
            pass


class MTStorage(object):
    "Test a storage with multiple client threads executing concurrently."

    def _checkNThreads(self, n, constructor, *args):
        threads = [constructor(*args) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
        for t in threads:
            self.assertFalse(t.is_alive(),
                             "thread failed to finish in 60 seconds")

    def check2ZODBThreads(self):
        db = ZODB.DB(self._storage)
        self._checkNThreads(2, ZODBClientThread, db, self)
        db.close()

    def check7ZODBThreads(self):
        db = ZODB.DB(self._storage)
        self._checkNThreads(7, ZODBClientThread, db, self)
        db.close()

    def check2StorageThreads(self):
        self._checkNThreads(2, StorageClientThread, self._storage, self)

    def check7StorageThreads(self):
        self._checkNThreads(7, StorageClientThread, self._storage, self)

    def check4ExtStorageThread(self):
        self._checkNThreads(4, ExtStorageClientThread, self._storage, self)
