##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
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
     PackableStorage, Synchronization, ConflictResolution, RevisionStorage
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
    """Version of the storage test class that supports ZEO."""
    pass
    
# Some of the ZEO tests depend on the version of FileStorage available
# for the tests.  If we run these tests using Zope 2.3, FileStorage
# doesn't support TransactionalUndo.

if hasattr(FileStorage, 'supportsTransactionalUndo'):
    # XXX Assume that a FileStorage that supports transactional undo
    # also supports conflict resolution.
    class VersionDependentTests(
        TransactionalUndoStorage.TransactionalUndoStorage,
        TransactionalUndoVersionStorage.TransactionalUndoVersionStorage,
        ConflictResolution.ConflictResolvingStorage,
        ConflictResolution.ConflictResolvingTransUndoStorage):
        pass
else:
    class VersionDependentTests:
        pass

class GenericTests(ZEOTestBase,
                   VersionDependentTests,
                   Cache.StorageWithCache,
                   Cache.TransUndoStorageWithCache,
                   BasicStorage.BasicStorage,
                   VersionStorage.VersionStorage,
                   RevisionStorage.RevisionStorage,
                   PackableStorage.PackableStorage,
                   Synchronization.SynchronizedStorage,
                   ):
    """An abstract base class for ZEO tests

    A specific ZEO test run depends on having a real storage that the
    StorageServer provides access to.  The GenericTests must be
    subclassed to provide an implementation of getStorage() that
    returns a specific storage, e.g. FileStorage.
    """

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

    def tearDown(self):
        """Try to cause the tests to halt"""
        self.running = 0
        self._storage.close()
        self._server.close()
        os.waitpid(self._pid, 0)
        self.delStorage()

    def checkLargeUpdate(self):
        obj = MinPO("X" * (10 * 128 * 1024))
        self._dostore(data=obj)

    def checkTwoArgBegin(self):
        pass # ZEO 1 doesn't support two-arg begin

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

    def setUp(self):
        args = self.getStorageInfo()
        name = args[0]
        args = args[1:]
        zeo_addr, self.test_addr, self.test_pid = \
                  forker.start_zeo_server(name, args)
        storage = ZEO.ClientStorage.ClientStorage(zeo_addr, debug=1,
                                                  min_disconnect_poll=0.1)
        self._storage = PackWaitWrapper(storage)
        storage.registerDB(DummyDB(), None)

    def tearDown(self):
        self._storage.close()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(self.test_addr)
        s.close()
        # the connection should cause the storage server to die
##        os.waitpid(self.test_pid, 0)
        time.sleep(0.5)
        self.delStorage()

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
    
    ports = []
    for i in range(200):
        ports.append(random.randrange(25000, 30000))
    del i

    def openClientStorage(self, cache='', cache_size=200000, wait=1):
        # defined by subclasses
        pass

    def shutdownServer(self):
        # defined by subclasses
        pass

    def tearDown(self):
        """Try to cause the tests to halt"""
        self._storage.close()
        self.shutdownServer()
        # file storage appears to create four files
        for ext in '', '.index', '.lock', '.tmp':
            path = self.file + ext
            if os.path.exists(path):
                os.unlink(path)
        for i in 0, 1:
            path = "c1-test-%d.zec" % i
            if os.path.exists(path):
                os.unlink(path)

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
        self._startServer(create=0)
        oid = self._storage.new_oid()
        obj = MinPO(12)
        while 1:
            try:
                revid1 = self._dostore(oid, data=obj)
            except (ClientDisconnected, thread.error, socket.error), err:
                get_transaction().abort()
                time.sleep(0.1)
            else:
                break
            # XXX This is a bloody pain.  We're placing a heavy burden
            # on users to catch a plethora of exceptions in order to
            # write robust code.  Need to think about implementing
            # John Heintz's suggestion to make sure all exceptions
            # inherit from POSException. 

class UnixConnectionTests(ConnectionTests):
    __super_setUp = StorageTestBase.StorageTestBase.setUp

    def setUp(self):
        """Start a ZEO server using a Unix domain socket

        The ZEO server uses the storage object returned by the
        getStorage() method.
        """
        self.running = 1
        self.file = tempfile.mktemp()
        self.addr = '', self.ports.pop()
        self._startServer()
        self.__super_setUp()

    def _startServer(self, create=1):
        fs = FileStorage(self.file, create=create)
        self._pid, self._server = forker.start_zeo_server(fs, self.addr)

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

class WindowsConnectionTests(ConnectionTests):
    __super_setUp = StorageTestBase.StorageTestBase.setUp

    def setUp(self):
        self.file = tempfile.mktemp()
        self._startServer()
        self.__super_setUp()

    def _startServer(self, create=1):
        if create == 0:
            port = self.addr[1]
        else:
            port = None
        self.addr, self.test_a, pid = forker.start_zeo_server('FileStorage',
                                                              (self.file,
                                                               str(create)),
                                                              port)
        self.running = 1

    def openClientStorage(self, cache='', cache_size=200000, wait=1):
        base = ZEO.ClientStorage.ClientStorage(self.addr,
                                               client=cache,
                                               cache_size=cache_size,
                                               debug=1,
                                               wait_for_server_on_startup=wait)
        storage = PackWaitWrapper(base)
        storage.registerDB(DummyDB(), None)
        return storage

    def shutdownServer(self):
        if self.running:
            self.running = 0
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(self.test_a)
            s.close()
            time.sleep(1.0)

    def tearDown(self):
        self.shutdownServer()


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
    test_classes = ZEOFileStorageTests, UnixConnectionTests
elif os.name == "nt":
    test_classes = WindowsZEOFileStorageTests, WindowsConnectionTests
else:
    raise RuntimeError, "unsupported os: %s" % os.name

def makeTestSuite(testname=''):
    suite = unittest.TestSuite()
    name = 'check' + testname
    lname = len(name)
    for klass in test_classes:
        for meth in get_methods(klass):
            if meth[:lname] == name:
                suite.addTest(klass(meth))
    return suite

def test_suite():
    return makeTestSuite()

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
