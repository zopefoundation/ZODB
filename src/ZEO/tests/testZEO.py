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

    def setUp(self):
        zLOG.LOG("testZEO", zLOG.INFO, "setUp() %s" % self.id())
        zeoport, adminaddr, pid = forker.start_zeo_server(self.getConfig())
        self._pids = [pid]
        self._servers = [adminaddr]
        self._storage = ClientStorage(zeoport, '1', cache_size=20000000,
                                      min_disconnect_poll=0.5, wait=1)
        self._storage.registerDB(DummyDB(), None)

    def tearDown(self):
        self._storage.close()
        for server in self._servers:
            forker.shutdown_zeo_server(server)
        if hasattr(os, 'waitpid'):
            # Not in Windows Python until 2.3
            for pid in self._pids:
                os.waitpid(pid, 0)

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


class FileStorageTests(GenericTests):
    """Test ZEO backed by a FileStorage."""
    level = 2

    def getConfig(self):
        filename = self.__fs_base = tempfile.mktemp()
        return """\
        <Storage>
            type FileStorage
            file_name %s
            create yes
        </Storage>
        """ % filename

    def checkPackVersionsInPast(self):
        # FileStorage can't cope with backpointers to objects
        # created in versions.  Should fix if we can figure out actually how
        # to fix it.
        pass


class BDBTests(FileStorageTests):
    """ZEO backed by a Berkeley full storage."""
    level = 2

    def getConfig(self):
        self._envdir = tempfile.mktemp()
        return """\
        <Storage>
            type BDBFullStorage
            name %s
        </Storage>
        """ % self._envdir

class MappingStorageTests(FileStorageTests):
    """ZEO backed by a Mapping storage."""

    def getConfig(self):
        self._envdir = tempfile.mktemp()
        return """\
        <Storage>
            type MappingStorage
            name %s
        </Storage>
        """ % self._envdir

    # Tests which MappingStorage can't possibly pass, because it doesn't
    # support versions or undo.
    def checkVersions(self): pass
    def checkVersionedStoreAndLoad(self): pass
    def checkVersionedLoadErrors(self): pass
    def checkVersionLock(self): pass
    def checkVersionEmpty(self): pass
    def checkUndoUnresolvable(self): pass
    def checkUndoInvalidation(self): pass
    def checkUndoInVersion(self): pass
    def checkUndoCreationBranch2(self): pass
    def checkUndoCreationBranch1(self): pass
    def checkUndoConflictResolution(self): pass
    def checkUndoCommitVersion(self): pass
    def checkUndoAbortVersion(self): pass
    def checkTwoObjectUndoAtOnce(self): pass
    def checkTwoObjectUndoAgain(self): pass
    def checkTwoObjectUndo(self): pass
    def checkTransactionalUndoAfterPackWithObjectUnlinkFromRoot(self): pass
    def checkTransactionalUndoAfterPack(self): pass
    def checkSimpleTransactionalUndo(self): pass
    def checkReadMethods(self): pass
    def checkPackAfterUndoDeletion(self): pass
    def checkPackAfterUndoManyTimes(self): pass
    def checkPackVersions(self): pass
    def checkPackUnlinkedFromRoot(self): pass
    def checkPackOnlyOneObject(self): pass
    def checkPackJustOldRevisions(self): pass
    def checkPackEmptyStorage(self): pass
    def checkPackAllRevisions(self): pass
    def checkPackVersionsInPast(self): pass
    def checkNotUndoable(self): pass
    def checkNewSerialOnCommitVersionToVersion(self): pass
    def checkModifyAfterAbortVersion(self): pass
    def checkLoadSerial(self): pass
    def checkCreateObjectInVersionWithAbort(self): pass
    def checkCommitVersionSerialno(self): pass
    def checkCommitVersionInvalidation(self): pass
    def checkCommitToOtherVersion(self): pass
    def checkCommitToNonVersion(self): pass
    def checkCommitLockUndoFinish(self): pass
    def checkCommitLockUndoClose(self): pass
    def checkCommitLockUndoAbort(self): pass
    def checkCommitEmptyVersionInvalidation(self): pass
    def checkAbortVersionSerialno(self): pass
    def checkAbortVersionInvalidation(self): pass
    def checkAbortVersionErrors(self): pass
    def checkAbortVersion(self): pass
    def checkAbortOneVersionCommitTheOther(self): pass
    def checkResolve(self): pass
    def check4ExtStorageThread(self): pass


test_classes = [FileStorageTests, MappingStorageTests]

import BDBStorage
if BDBStorage.is_available:
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
