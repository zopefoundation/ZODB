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
"""Test suite for ZEO based on ZODB.tests."""

# System imports
import os
import sys
import time
import socket
import asyncore
import tempfile
import unittest

# Zope/ZODB3 imports
import zLOG

# ZODB test support
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle

# Handle potential absence of removefs
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

# ZODB test mixin classes
from ZODB.tests import StorageTestBase, BasicStorage, VersionStorage, \
     TransactionalUndoStorage, TransactionalUndoVersionStorage, \
     PackableStorage, Synchronization, ConflictResolution, RevisionStorage, \
     MTStorage, ReadOnlyStorage

# ZEO imports
from ZEO.ClientStorage import ClientStorage
from ZEO.Exceptions import Disconnected

# ZEO test support
from ZEO.tests import forker, Cache

# ZEO test mixin classes
from ZEO.tests import CommitLockTests, ThreadTests

class DummyDB:
    def invalidate(self, *args):
        pass

class MiscZEOTests:

    """ZEO tests that don't fit in elsewhere."""

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

class GenericTests(
    # Base class for all ZODB tests
    StorageTestBase.StorageTestBase,

    # ZODB test mixin classes (in the same order as imported)
    BasicStorage.BasicStorage,
    VersionStorage.VersionStorage,
    TransactionalUndoStorage.TransactionalUndoStorage,
    TransactionalUndoVersionStorage.TransactionalUndoVersionStorage,
    PackableStorage.PackableStorage,
    Synchronization.SynchronizedStorage,
    ConflictResolution.ConflictResolvingStorage,
    ConflictResolution.ConflictResolvingTransUndoStorage,
    RevisionStorage.RevisionStorage,
    MTStorage.MTStorage,
    ReadOnlyStorage.ReadOnlyStorage,

    # ZEO test mixin classes (in the same order as imported)
    Cache.StorageWithCache,
    Cache.TransUndoStorageWithCache,
    CommitLockTests.CommitLockTests,
    ThreadTests.ThreadTests,
    MiscZEOTests # Locally defined (see above)
    ):

    """Combine tests from various origins in one class."""

    def open(self, read_only=0):
        # XXX Needed to support ReadOnlyStorage tests.  Ought to be a
        # cleaner way.
        addr = self._storage._addr
        self._storage.close()
        self._storage = ClientStorage(addr, read_only=read_only, wait=1)

class UnixTests(GenericTests):

    """Add Unix-specific scaffolding to the generic test suite."""

    def setUp(self):
        zLOG.LOG("testZEO", zLOG.INFO, "setUp() %s" % self.id())
        client, exit, pid = forker.start_zeo(*self.getStorage())
        self._pids = [pid]
        self._servers = [exit]
        self._storage = client
        client.registerDB(DummyDB(), None)

    def tearDown(self):
        self._storage.close()
        for server in self._servers:
            server.close()
        for pid in self._pids:
            os.waitpid(pid, 0)
        self.delStorage()

    def getStorage(self):
        self.__fs_base = tempfile.mktemp()
        return 'FileStorage', (self.__fs_base, '1')

    def delStorage(self):
        removefs(self.__fs_base)

class WindowsTests(GenericTests):

    """Add Windows-specific scaffolding to the generic test suite."""

    def setUp(self):
        zLOG.LOG("testZEO", zLOG.INFO, "setUp() %s" % self.id())
        args = self.getStorageInfo()
        name = args[0]
        args = args[1]
        zeo_addr, self.test_addr, self.test_pid = \
                  forker.start_zeo_server(name, args)
        storage = ClientStorage(zeo_addr, wait=1, min_disconnect_poll=0.1)
        self._storage = storage
        storage.registerDB(DummyDB(), None)

    def tearDown(self):
        self._storage.close()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(self.test_addr)
        s.close()
        # the connection should cause the storage server to die
        time.sleep(0.5)
        self.delStorage()

    def getStorageInfo(self):
        self.__fs_base = tempfile.mktemp()
        return 'FileStorage', (self.__fs_base, '1') # create=1

    def delStorage(self):
        removefs(self.__fs_base)

if os.name == "posix":
    test_classes = [UnixTests]
elif os.name == "nt":
    test_classes = [WindowsTests]
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
