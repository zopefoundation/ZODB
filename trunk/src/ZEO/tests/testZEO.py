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
import unittest

from ZODB.Transaction import get_transaction
from ZODB.POSException import ReadOnlyError
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


from ZEO.ClientStorage import ClientStorage
from ZEO.tests import forker, Cache, CommitLockTests, ThreadTests
from ZEO.tests.ConnectionTests import ConnectionTests
from ZEO.Exceptions import Disconnected

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
        self._storage = ClientStorage(addr, read_only=read_only, wait=1)

    def checkLargeUpdate(self):
        obj = MinPO("X" * (10 * 128 * 1024))
        self._dostore(data=obj)

    def checkZEOInvalidation(self):
        addr = self._storage._addr
        storage2 = ClientStorage(addr, wait=1, min_disconnect_poll=0.1)
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
        storage = ClientStorage(zeo_addr, wait=1, min_disconnect_poll=0.1)
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

class BaseConnectionTests(ConnectionTests):
    # provide an openClientStorage() method shared by Unix and Windows
    
    def openClientStorage(self, cache='', cache_size=200000, wait=1,
                          read_only=0, read_only_fallback=0):
        base = ClientStorage(self.addr,
                             client=cache,
                             cache_size=cache_size,
                             wait=wait,
                             min_disconnect_poll=0.1,
                             read_only=read_only,
                             read_only_fallback=read_only_fallback)
        storage = PackWaitWrapper(base)
        storage.registerDB(DummyDB(), None)
        return storage

class UnixConnectionTests(BaseConnectionTests):

    def startServer(self, create=1, index=0, read_only=0, ro_svr=0):
        zLOG.LOG("testZEO", zLOG.INFO,
                 "startServer(create=%d, index=%d, read_only=%d)" %
                 (create, index, read_only))
        path = "%s.%d" % (self.file, index)
        addr = self.addr[index]
        pid, server = forker.start_zeo_server(
            'FileStorage', (path, create, read_only), addr, ro_svr)
        self._pids.append(pid)
        self._servers.append(server)

    def shutdownServer(self, index=0):
        zLOG.LOG("testZEO", zLOG.INFO, "shutdownServer(index=%d)" % index)
        self._servers[index].close()
        if self._pids[index] is not None:
            try:
                os.waitpid(self._pids[index], 0)
                self._pids[index] = None
            except os.error, err:
                print err

class WindowsConnectionTests(BaseConnectionTests):

    def startServer(self, create=1, index=0, read_only=0, ro_svr=0):
        zLOG.LOG("testZEO", zLOG.INFO,
                 "startServer(create=%d, index=%d, read_only=%d)" %
                 (create, index, read_only))
        path = "%s.%d" % (self.file, index)
        addr = self.addr[index]
        args = (path, '='+str(create), '='+str(read_only))
        _addr, test_addr, test_pid = forker.start_zeo_server(
            'FileStorage', args, addr, ro_svr)
        self._pids.append(test_pid)
        self._servers.append(test_addr)

    def shutdownServer(self, index=0):
        zLOG.LOG("testZEO", zLOG.INFO, "shutdownServer(index=%d)" % index)
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

    # shutup warnings about mktemp
    import warnings
    warnings.filterwarnings("ignore", "mktemp")
    
    suite = unittest.TestSuite()
    for klass in test_classes:
        sub = unittest.makeSuite(klass, 'check')
        suite.addTest(sub)
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest='test_suite')
