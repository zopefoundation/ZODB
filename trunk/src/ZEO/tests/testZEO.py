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
import ZODB
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle


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
    # Locally defined (see above)
    MiscZEOTests
    ):

    """Combine tests from various origins in one class."""

    def open(self, read_only=0):
        # XXX Needed to support ReadOnlyStorage tests.  Ought to be a
        # cleaner way.
        addr = self._storage._addr
        self._storage.close()
        self._storage = ClientStorage(addr, read_only=read_only, wait=1)

    def checkWriteMethods(self):
        # ReadOnlyStorage defines checkWriteMethods.  The decision
        # about where to raise the read-only error was changed after
        # Zope 2.5 was released.  So this test needs to detect Zope
        # of the 2.5 vintage and skip the test.

        # The __version__ attribute was not present in Zope 2.5.
        if hasattr(ZODB, "__version__"):
            ReadOnlyStorage.ReadOnlyStorage.checkWriteMethods(self)


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
        filename = self.__fs_base = tempfile.mktemp()
        # Return a 1-tuple
        return """\
        <Storage>
            type FileStorage
            file_name %s
            create yes
        </Storage>
        """ % filename,

    def delStorage(self):
        from ZODB.FileStorage import cleanup
        cleanup(self.__fs_base)


class BDBTests(UnixTests):

    """Add Berkeley storage tests (not sure if these are Unix specific)."""

    def getStorage(self):
        self._envdir = tempfile.mktemp()
        # Return a 1-tuple
        return """\
        <Storage>
            type Full
            name %s
        </Storage>
        """ % self._envdir,

    def delStorage(self):
        from bsddb3Storage.BerkeleyBase import cleanup
        cleanup(self._envdir)


class WindowsTests(UnixTests):

    """Add Windows-specific scaffolding to the generic test suite."""

    def setUp(self):
        zLOG.LOG("testZEO", zLOG.INFO, "setUp() %s" % self.id())
        args = self.getStorage()
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


if os.name == "posix":
    test_classes = [UnixTests]
elif os.name == "nt":
    test_classes = [WindowsTests]
else:
    raise RuntimeError, "unsupported os: %s" % os.name

try:
    from bsddb3Storage.Full import Full
except ImportError:
    pass
else:
    test_classes.append(BDBTests)


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
