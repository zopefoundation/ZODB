"""Test suite for ZEO based on ZODB.tests"""

import asyncore
import os
import random
import socket
import sys
import tempfile
import time
import types
import unittest

import ZEO.ClientStorage, ZEO.StorageServer
import ThreadedAsync, ZEO.trigger
from ZODB.FileStorage import FileStorage
import thread

from ZEO.tests import forker, Cache
from ZEO.smac import Disconnected

# Sorry Jim...
from ZODB.tests import StorageTestBase, BasicStorage, VersionStorage, \
     TransactionalUndoStorage, TransactionalUndoVersionStorage, \
     PackableStorage, Synchronization, ConflictResolution
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle


ZERO = '\0'*8

class DummyDB:
    def invalidate(self, *args):
        pass

class PackWaitWrapper:
    def __init__(self, storage):
        self.storage = storage

    def __getattr__(self, attr):
        return getattr(self.storage, attr)

    def pack(self, t, f):
        self.storage.pack(t, f, wait=1)

class ZEOTestBase(StorageTestBase.StorageTestBase):
    """Version of the storage test class that supports ZEO.
    
    For ZEO, we don't always get the serialno/exception for a
    particular store as the return value from the store.   But we
    will get no later than the return value from vote.
    """
    
    def _dostore(self, oid=None, revid=None, data=None, version=None,
                 already_pickled=0):
        """Do a complete storage transaction.

        The defaults are:
         - oid=None, ask the storage for a new oid
         - revid=None, use a revid of ZERO
         - data=None, pickle up some arbitrary data (the integer 7)
         - version=None, use the empty string version
        
        Returns the object's new revision id.
        """
        if oid is None:
            oid = self._storage.new_oid()
        if revid is None:
            revid = ZERO
        if data is None:
            data = MinPO(7)
        if not already_pickled:
            data = StorageTestBase.zodb_pickle(data)
        if version is None:
            version = ''
        # Begin the transaction
        self._storage.tpc_begin(self._transaction)
        # Store an object
        r1 = self._storage.store(oid, revid, data, version,
                                 self._transaction)
        s1 = self._get_serial(r1)
        # Finish the transaction
        r2 = self._storage.tpc_vote(self._transaction)
        s2 = self._get_serial(r2)
        self._storage.tpc_finish(self._transaction)
        # s1, s2 can be None or dict
        assert not (s1 and s2)
        return s1 and s1[oid] or s2 and s2[oid]

    def _get_serial(self, r):
        """Return oid -> serialno dict from sequence of ZEO replies."""
        d = {}
        if r is None:
            return None
        if type(r) == types.StringType:
            raise RuntimeError, "unexpected ZEO response: no oid"
        else:
            for oid, serial in r:
                if isinstance(serial, Exception):
                    raise serial
                d[oid] = serial
        return d
        
class GenericTests(ZEOTestBase,
                   Cache.StorageWithCache,
                   Cache.TransUndoStorageWithCache,
                   BasicStorage.BasicStorage,
                   VersionStorage.VersionStorage,
                   PackableStorage.PackableStorage,
                   Synchronization.SynchronizedStorage,
                   ConflictResolution.ConflictResolvingStorage,
                   ConflictResolution.ConflictResolvingTransUndoStorage,
                   TransactionalUndoStorage.TransactionalUndoStorage,
      TransactionalUndoVersionStorage.TransactionalUndoVersionStorage,
                   ):
    """An abstract base class for ZEO tests

    A specific ZEO test run depends on having a real storage that the
    StorageServer provides access to.  The GenericTests must be
    subclassed to provide an implementation of getStorage() that
    returns a specific storage, e.g. FileStorage.
    """

    __super_setUp = StorageTestBase.StorageTestBase.setUp
    __super_tearDown = StorageTestBase.StorageTestBase.tearDown

    def setUp(self):
        """Start a ZEO server using a Unix domain socket

        The ZEO server uses the storage object returned by the
        getStorage() method.
        """
        self.running = 1
        client, exit, pid = forker.start_zeo(self.getStorage())
        self._pid = pid
        self._server = exit
        self._storage = PackWaitWrapper(client)
        client.registerDB(DummyDB(), None)
        self.__super_setUp()

    def tearDown(self):
        """Try to cause the tests to halt"""
        self.running = 0
        self._storage.close()
        self._server.close()
        os.waitpid(self._pid, 0)
        self.delStorage()
        self.__super_tearDown()

    def checkLargeUpdate(self):
        obj = MinPO("X" * (10 * 128 * 1024))
        self._dostore(data=obj)

class ZEOFileStorageTests(GenericTests):
    __super_setUp = GenericTests.setUp
    
    def setUp(self):
        self.__fs_base = tempfile.mktemp()
        self.__super_setUp()

    def getStorage(self):
        return FileStorage(self.__fs_base, create=1)

    def delStorage(self):
        # file storage appears to create four files
        for ext in '', '.index', '.lock', '.tmp':
            path = self.__fs_base + ext
            try:
                os.remove(path)
            except os.error:
                pass

class WindowsGenericTests(GenericTests):
    """Subclass to support server creation on Windows.

    On Windows, the getStorage() design won't work because the storage
    can't be created in the parent process and passed to the child.
    All the work has to be done in the server's process.
    """
    __super_setUp = StorageTestBase.StorageTestBase.setUp
    __super_tearDown = StorageTestBase.StorageTestBase.tearDown

    def setUp(self):
        self.__super_setUp()
        args = self.getStorageInfo()
        name = args[0]
        args = args[1:]
        zeo_addr, self.test_addr, self.test_pid = \
                  forker.start_zeo_server(name, args)
        storage = ZEO.ClientStorage.ClientStorage(zeo_addr, debug=1,
                                                  min_disconnect_poll=0.5)
        self._storage = PackWaitWrapper(storage)
        storage.registerDB(DummyDB(), None)

    def tearDown(self):
        self._storage.close()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(self.test_addr)
        # the connection should cause the storage server to die
##        os.waitpid(self.test_pid, 0)
        time.sleep(0.5)
        self.delStorage()
        self.__super_tearDown()

class WindowsZEOFileStorageTests(WindowsGenericTests):

    def getStorageInfo(self):
        self.__fs_base = tempfile.mktemp()
        return 'FileStorage', self.__fs_base, '1'

    def delStorage(self):
        # file storage appears to create four files
        for ext in '', '.index', '.lock', '.tmp':
            path = self.__fs_base + ext
            try:
                os.remove(path)
            except os.error:
                pass

class ConnectionTests(ZEOTestBase):
    """Tests that explicitly manage the server process.

    To test the cache or re-connection, these test cases explicit
    start and stop a ZEO storage server.
    """
    
    __super_setUp = StorageTestBase.StorageTestBase.setUp
    __super_tearDown = StorageTestBase.StorageTestBase.tearDown

    ports = range(29000, 30000, 10) # enough for 100 tests
    random.shuffle(ports)

    def setUp(self):
        """Start a ZEO server using a Unix domain socket

        The ZEO server uses the storage object returned by the
        getStorage() method.
        """
        self.running = 1
        self.__fs_base = tempfile.mktemp()
        self.addr = '', random.randrange(2000, 3000)
        pid, exit = self._startServer()
        self._pid = pid
        self._server = exit
        self.__super_setUp()

    def _startServer(self, create=1):
        fs = FileStorage(self.__fs_base, create=create)
        return forker.start_zeo_server(fs, self.addr)

    def openClientStorage(self, cache='', cache_size=200000, wait=1):
        base = ZEO.ClientStorage.ClientStorage(self.addr,
                                               client=cache,
                                               cache_size=cache_size,
                                               wait_for_server_on_startup=wait)
        storage = PackWaitWrapper(base)
        storage.registerDB(DummyDB(), None)
        return storage

    def shutdownServer(self):
        if self.running:
            self.running = 0
            self._server.close()
            os.waitpid(self._pid, 0)

    def tearDown(self):
        """Try to cause the tests to halt"""
        self.shutdownServer()
        # file storage appears to create four files
        for ext in '', '.index', '.lock', '.tmp':
            path = self.__fs_base + ext
            if os.path.exists(path):
                os.unlink(path)
        for i in 0, 1:
            path = "c1-test-%d.zec" % i
            if os.path.exists(path):
                os.unlink(path)
        self.__super_tearDown()

    def checkBasicPersistence(self):
        """Verify cached data persists across client storage instances.

        To verify that the cache is being used, the test closes the
        server and then starts a new client with the server down.
        """
        self._storage = self.openClientStorage('test', 100000, 1)
        oid = self._storage.new_oid()
        obj = MinPO(12)
        revid1 = self._dostore(oid, data=obj)
        self._storage.close()
        self.shutdownServer()
        self._storage = self.openClientStorage('test', 100000, 0)
        data, revid2 = self._storage.load(oid, '')
        assert zodb_unpickle(data) == MinPO(12)
        assert revid1 == revid2
        self._storage.close()

    def checkRollover(self):
        """Check that the cache works when the files are swapped.

        In this case, only one object fits in a cache file.  When the
        cache files swap, the first object is effectively uncached.
        """
        self._storage = self.openClientStorage('test', 1000, 1)
        oid1 = self._storage.new_oid()
        obj1 = MinPO("1" * 500)
        revid1 = self._dostore(oid1, data=obj1)
        oid2 = self._storage.new_oid()
        obj2 = MinPO("2" * 500)
        revid2 = self._dostore(oid2, data=obj2)
        self._storage.close()
        self.shutdownServer()
        self._storage = self.openClientStorage('test', 1000, 0)
        self._storage.load(oid2, '')
        self.assertRaises(Disconnected, self._storage.load, oid1, '')

    def checkReconnection(self):
        """Check that the client reconnects when a server restarts."""

        from ZEO.ClientStorage import ClientDisconnected
        self._storage = self.openClientStorage()
        oid = self._storage.new_oid()
        obj = MinPO(12)
        revid1 = self._dostore(oid, data=obj)
        self.shutdownServer()
        self.running = 1
        self._pid, self._server = self._startServer(create=0)
        oid = self._storage.new_oid()
        obj = MinPO(12)
        while 1:
            try:
                revid1 = self._dostore(oid, data=obj)
            except (ClientDisconnected, thread.error), err:
                get_transaction().abort()
                time.sleep(0.1)
            else:
                break

def get_methods(klass):
    l = [klass]
    meth = {}
    while l:
        klass = l.pop(0)
        for base in klass.__bases__:
            l.append(base)
        for k, v in klass.__dict__.items():
            if callable(v):
                meth[k] = 1
    return meth.keys()

if os.name == "posix":
    test_classes = ZEOFileStorageTests, ConnectionTests
elif os.name == "nt":
    test_classes = WindowsZEOFileStorageTests
else:
    raise RuntimeError, "unsupported os: %s" % os.name

def makeTestSuite(testname=''):
    suite = unittest.TestSuite()
    name = 'check' + testname
    for klass in ZEOFileStorageTests, ConnectionTests:
        for meth in get_methods(klass):
            if meth.startswith(name):
                suite.addTest(klass(meth))
    return suite

def test_suite():
    return unittest.makeSuite(WindowsZEOFileStorageTests, 'check')

def main():
    import sys, getopt

    name_of_test = ''

    opts, args = getopt.getopt(sys.argv[1:], 'n:')
    for flag, val in opts:
        if flag == '-n':
            name_of_test = val

    if args:
        print "Did not expect arguments.  Got %s" % args
        return 0
    
    tests = makeTestSuite(name_of_test)
    runner = unittest.TextTestRunner()
    runner.run(tests)

if __name__ == "__main__":
    main()
