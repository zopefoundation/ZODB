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
import select
import socket
import sys
import tempfile
import thread
import time
import types
import unittest

import ZEO.ClientStorage, ZEO.StorageServer
from ZODB.FileStorage import FileStorage
from ZODB.Transaction import Transaction
from ZODB.tests.StorageTestBase import zodb_pickle, MinPO
import zLOG

try:
    from ZODB.tests.StorageTestBase import removefs
except ImportError:
    # for compatibility with Zope 2.5 &c.
    import errno

    def removefs(base):
        """Remove all files created by FileStorage with path base."""
        for ext in '', '.old', '.tmp', '.lock', '.index', '.pack':
            path = base + ext
            try:
                os.remove(path)
            except os.error, err:
                if err[0] != errno.ENOENT:
                    raise


from ZEO.tests import forker, Cache, CommitLockTests, ThreadTests
from ZEO.zrpc.smac import Disconnected

from ZODB.tests import StorageTestBase, BasicStorage, VersionStorage, \
     TransactionalUndoStorage, TransactionalUndoVersionStorage, \
     PackableStorage, Synchronization, ConflictResolution, RevisionStorage, \
     MTStorage, ReadOnlyStorage
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle

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

class GenericTests(StorageTestBase.StorageTestBase,
                   TransactionalUndoStorage.TransactionalUndoStorage,
            TransactionalUndoVersionStorage.TransactionalUndoVersionStorage,
                   ConflictResolution.ConflictResolvingStorage,
                   ConflictResolution.ConflictResolvingTransUndoStorage,
                   Cache.StorageWithCache,
                   Cache.TransUndoStorageWithCache,
                   BasicStorage.BasicStorage,
                   VersionStorage.VersionStorage,
                   RevisionStorage.RevisionStorage,
                   PackableStorage.PackableStorage,
                   Synchronization.SynchronizedStorage,
                   MTStorage.MTStorage,
                   ReadOnlyStorage.ReadOnlyStorage,
                   CommitLockTests.CommitLockTests,
                   ThreadTests.ThreadTests,
                   ):
    """An abstract base class for ZEO tests

    A specific ZEO test run depends on having a real storage that the
    StorageServer provides access to.  The GenericTests must be
    subclassed to provide an implementation of getStorage() that
    returns a specific storage, e.g. FileStorage.
    """

    def setUp(self):
        zLOG.LOG("testZEO", zLOG.INFO, "setUp() %s" % self.id())
        client, exit, pid = forker.start_zeo(*self.getStorage())
        self._pids = [pid]
        self._servers = [exit]
        self._storage = PackWaitWrapper(client)
        client.registerDB(DummyDB(), None)

    def tearDown(self):
        self._storage.close()
        for server in self._servers:
            server.close()
        for pid in self._pids:
            os.waitpid(pid, 0)
        self.delStorage()

    def open(self, read_only=0):
        # XXX Needed to support ReadOnlyStorage tests.  Ought to be a
        # cleaner way.

        addr = self._storage._addr
        self._storage.close()
        self._storage = ZEO.ClientStorage.ClientStorage(addr,
                                                        read_only=read_only,
                                                        wait=1)

    def checkLargeUpdate(self):
        obj = MinPO("X" * (10 * 128 * 1024))
        self._dostore(data=obj)

    def checkZEOInvalidation(self):
        addr = self._storage._addr
        storage2 = ZEO.ClientStorage.ClientStorage(addr, wait=1,
                                                   min_disconnect_poll=0.1)
        try:
            oid = self._storage.new_oid()
            ob = MinPO('first')
            revid1 = self._dostore(oid, data=ob)
            data, serial = storage2.load(oid, '')
            self.assertEqual(zodb_unpickle(data), MinPO('first'))
            self.assertEqual(serial, revid1)
            revid2 = self._dostore(oid, data=MinPO('second'), revid=revid1)
            for n in range(3):
                # Let the server and client talk for a moment.
                # Is there a better way to do this?
                asyncore.poll(0.1)
            data, serial = storage2.load(oid, '')
            self.assertEqual(zodb_unpickle(data), MinPO('second'),
                             'Invalidation message was not sent!')
            self.assertEqual(serial, revid2)
        finally:
            storage2.close()


class ZEOFileStorageTests(GenericTests):
    __super_setUp = GenericTests.setUp

    def setUp(self):
        self.__fs_base = tempfile.mktemp()
        self.__super_setUp()

    def getStorage(self):
        self.__fs_base = tempfile.mktemp()
        return 'FileStorage', (self.__fs_base, '1')

    def delStorage(self):
        removefs(self.__fs_base)

class WindowsGenericTests(GenericTests):
    """Subclass to support server creation on Windows.

    On Windows, the getStorage() design won't work because the storage
    can't be created in the parent process and passed to the child.
    All the work has to be done in the server's process.
    """

    def setUp(self):
        zLOG.LOG("testZEO", zLOG.INFO, "setUp() %s" % self.id())
        args = self.getStorageInfo()
        name = args[0]
        args = args[1]
        zeo_addr, self.test_addr, self.test_pid = \
                  forker.start_zeo_server(name, args)
        storage = ZEO.ClientStorage.ClientStorage(zeo_addr, wait=1,
                                                  min_disconnect_poll=0.1)
        self._storage = PackWaitWrapper(storage)
        storage.registerDB(DummyDB(), None)

    def tearDown(self):
        self._storage.close()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(self.test_addr)
        s.close()
        # the connection should cause the storage server to die
        time.sleep(0.5)
        self.delStorage()

class WindowsZEOFileStorageTests(WindowsGenericTests):

    def getStorageInfo(self):
        self.__fs_base = tempfile.mktemp()
        return 'FileStorage', (self.__fs_base, '1') # create=1

    def delStorage(self):
        removefs(self.__fs_base)

class ConnectionTests(StorageTestBase.StorageTestBase):
    """Tests that explicitly manage the server process.

    To test the cache or re-connection, these test cases explicit
    start and stop a ZEO storage server.
    """

    __super_tearDown = StorageTestBase.StorageTestBase.tearDown

    ports = []
    for i in range(200):
        ports.append(random.randrange(25000, 30000))
    del i

    def setUp(self):
        """Start a ZEO server using a Unix domain socket

        The ZEO server uses the storage object returned by the
        getStorage() method.
        """
        self.file = tempfile.mktemp()
        self.addr = []
        self._pids = []
        self._servers = []
        self._newAddr()
        self._startServer()

    def _newAddr(self):
        self.addr.append(self._getAddr())

    def _getAddr(self):
        return 'localhost', self.ports.pop()

    def openClientStorage(self, cache='', cache_size=200000, wait=1):
        base = ZEO.ClientStorage.ClientStorage(self.addr,
                                               client=cache,
                                               cache_size=cache_size,
                                               wait=wait,
                                               min_disconnect_poll=0.1)
        storage = PackWaitWrapper(base)
        storage.registerDB(DummyDB(), None)
        return storage

    def tearDown(self):
        """Try to cause the tests to halt"""
        if getattr(self, '_storage', None) is not None:
            self._storage.close()
        for i in range(len(self._servers)):
            self.shutdownServer(i)
        # file storage appears to create four files
        for i in range(len(self.addr)):
            for ext in '', '.index', '.lock', '.tmp':
                path = "%s.%s%s" % (self.file, i, ext)
                if os.path.exists(path):
                    try:
                        os.unlink(path)
                    except os.error:
                        pass
        for i in 0, 1:
            path = "c1-test-%d.zec" % i
            if os.path.exists(path):
                try:
                    os.unlink(path)
                except os.error:
                    pass
        self.__super_tearDown()

    def checkMultipleAddresses(self):
        for i in range(4):
            self._newAddr()
        self._storage = self.openClientStorage('test', 100000, wait=1)
        oid = self._storage.new_oid()
        obj = MinPO(12)
        revid1 = self._dostore(oid, data=obj)
        self._storage.close()

    def checkMultipleServers(self):
        # XXX crude test at first -- just start two servers and do a
        # commit at each one.

        self._newAddr()
        self._storage = self.openClientStorage('test', 100000, wait=1)
        self._dostore()

        self.shutdownServer(index=0)
        self._startServer(index=1)

        # If we can still store after shutting down one of the
        # servers, we must be reconnecting to the other server.

        for i in range(10):
            try:
                self._dostore()
                break
            except Disconnected:
                time.sleep(0.5)


    def checkDisconnectionError(self):
        # Make sure we get a Disconnected when we try to read an
        # object when we're not connected to a storage server and the
        # object is not in the cache.
        self.shutdownServer()
        self._storage = self.openClientStorage('test', 1000, wait=0)
        self.assertRaises(Disconnected, self._storage.load, 'fredwash', '')

    def checkBasicPersistence(self):
        # Verify cached data persists across client storage instances.

        # To verify that the cache is being used, the test closes the
        # server and then starts a new client with the server down.
        # When the server is down, a load() gets the data from its cache.

        self._storage = self.openClientStorage('test', 100000, wait=1)
        oid = self._storage.new_oid()
        obj = MinPO(12)
        revid1 = self._dostore(oid, data=obj)
        self._storage.close()
        self.shutdownServer()
        self._storage = self.openClientStorage('test', 100000, wait=0)
        data, revid2 = self._storage.load(oid, '')
        self.assertEqual(zodb_unpickle(data), MinPO(12))
        self.assertEqual(revid1, revid2)
        self._storage.close()

    def checkRollover(self):
        # Check that the cache works when the files are swapped.

        # In this case, only one object fits in a cache file.  When the
        # cache files swap, the first object is effectively uncached.

        self._storage = self.openClientStorage('test', 1000, wait=1)
        oid1 = self._storage.new_oid()
        obj1 = MinPO("1" * 500)
        revid1 = self._dostore(oid1, data=obj1)
        oid2 = self._storage.new_oid()
        obj2 = MinPO("2" * 500)
        revid2 = self._dostore(oid2, data=obj2)
        self._storage.close()
        self.shutdownServer()
        self._storage = self.openClientStorage('test', 1000, wait=0)
        self._storage.load(oid1, '')
        self._storage.load(oid2, '')

    def checkReconnection(self):
        # Check that the client reconnects when a server restarts.

        # XXX Seem to get occasional errors that look like this:
        # File ZEO/zrpc2.py, line 217, in handle_request
        # File ZEO/StorageServer.py, line 325, in storea
        # File ZEO/StorageServer.py, line 209, in _check_tid
        # StorageTransactionError: (None, <tid>)
        # could system reconnect and continue old transaction?

        self._storage = self.openClientStorage()
        oid = self._storage.new_oid()
        obj = MinPO(12)
        revid1 = self._dostore(oid, data=obj)
        zLOG.LOG("checkReconnection", zLOG.INFO,
                 "About to shutdown server")
        self.shutdownServer()
        zLOG.LOG("checkReconnection", zLOG.INFO,
                 "About to restart server")
        self._startServer(create=0)
        oid = self._storage.new_oid()
        obj = MinPO(12)
        while 1:
            try:
                revid1 = self._dostore(oid, data=obj)
                break
            except (Disconnected, select.error, thread.error, socket.error), \
                   err:
                zLOG.LOG("checkReconnection", zLOG.INFO,
                         "Error after server restart; retrying.",
                         error=sys.exc_info())
                get_transaction().abort()
                time.sleep(0.1) # XXX how long to sleep
            # XXX This is a bloody pain.  We're placing a heavy burden
            # on users to catch a plethora of exceptions in order to
            # write robust code.  Need to think about implementing
            # John Heintz's suggestion to make sure all exceptions
            # inherit from POSException.
        zLOG.LOG("checkReconnection", zLOG.INFO, "finished")

class UnixConnectionTests(ConnectionTests):

    def _startServer(self, create=1, index=0):
        path = "%s.%d" % (self.file, index)
        addr = self.addr[index]
        pid, server = forker.start_zeo_server('FileStorage',
                                              (path, create), addr)
        self._pids.append(pid)
        self._servers.append(server)

    def shutdownServer(self, index=0):
        self._servers[index].close()
        if self._pids[index] is not None:
            try:
                os.waitpid(self._pids[index], 0)
                self._pids[index] = None
            except os.error, err:
                print err

class WindowsConnectionTests(ConnectionTests):

    def _startServer(self, create=1, index=0):
        path = "%s.%d" % (self.file, index)
        addr = self.addr[index]
        _addr, test_addr, test_pid = forker.start_zeo_server('FileStorage',
                                                 (path, str(create)), addr)
        self._pids.append(test_pid)
        self._servers.append(test_addr)

    def shutdownServer(self, index=0):
        if self._servers[index] is not None:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(self._servers[index])
            s.close()
            self._servers[index] = None
            # XXX waitpid() isn't available until Python 2.3
            time.sleep(0.5)

if os.name == "posix":
    test_classes = ZEOFileStorageTests, UnixConnectionTests
elif os.name == "nt":
    test_classes = WindowsZEOFileStorageTests, WindowsConnectionTests
else:
    raise RuntimeError, "unsupported os: %s" % os.name

def test_suite():
    suite = unittest.TestSuite()
    for klass in test_classes:
        sub = unittest.makeSuite(klass, 'check')
        suite.addTest(sub)
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest='test_suite')
