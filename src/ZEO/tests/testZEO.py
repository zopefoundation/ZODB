##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Test suite for ZEO based on ZODB.tests."""

# System imports
import os
import random
import socket
import asyncore
import tempfile
import unittest
import logging
import shutil

# ZODB test support
import ZODB
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle

# ZODB test mixin classes
from ZODB.tests import StorageTestBase, BasicStorage, VersionStorage, \
     TransactionalUndoStorage, TransactionalUndoVersionStorage, \
     PackableStorage, Synchronization, ConflictResolution, RevisionStorage, \
     MTStorage, ReadOnlyStorage

from ZEO.ClientStorage import ClientStorage
from ZEO.tests import forker, Cache, CommitLockTests, ThreadTests

logger = logging.getLogger('ZEO.tests.testZEO')

class DummyDB:
    def invalidate(self, *args):
        pass

class OneTimeTests(unittest.TestCase):

    def checkZEOVersionNumber(self):
        import ZEO
        # Starting with ZODB 3.4, the ZODB and ZEO version numbers should
        # be identical.
        self.assertEqual(ZODB.__version__, ZEO.version)

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

def get_port():
    """Return a port that is not in use.

    Checks if a port is in use by trying to connect to it.  Assumes it
    is not in use if connect raises an exception.

    Raises RuntimeError after 10 tries.
    """
    for i in range(10):
        port = random.randrange(20000, 30000)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            try:
                s.connect(('localhost', port))
            except socket.error:
                # Perhaps we should check value of error too.
                return port
        finally:
            s.close()
    raise RuntimeError("Can't find port")

class GenericTests(
    # Base class for all ZODB tests
    StorageTestBase.StorageTestBase,
    # ZODB test mixin classes (in the same order as imported)
    BasicStorage.BasicStorage,
    PackableStorage.PackableStorage,
    Synchronization.SynchronizedStorage,
    MTStorage.MTStorage,
    ReadOnlyStorage.ReadOnlyStorage,
    # ZEO test mixin classes (in the same order as imported)
    CommitLockTests.CommitLockVoteTests,
    ThreadTests.ThreadTests,
    # Locally defined (see above)
    MiscZEOTests
    ):

    """Combine tests from various origins in one class."""

    def setUp(self):
        logger.info("setUp() %s", self.id())
        port = get_port()
        zconf = forker.ZEOConfig(('', port))
        zport, adminaddr, pid, path = forker.start_zeo_server(self.getConfig(),
                                                              zconf, port)
        self._pids = [pid]
        self._servers = [adminaddr]
        self._conf_path = path
        self.blob_cache_dir = tempfile.mkdtemp()  # This is the blob cache for ClientStorage
        self._storage = ClientStorage(zport, '1', cache_size=20000000,
                                      min_disconnect_poll=0.5, wait=1,
                                      wait_timeout=60, blob_dir=self.blob_cache_dir)
        self._storage.registerDB(DummyDB(), None)

    def tearDown(self):
        self._storage.close()
        os.remove(self._conf_path)
        shutil.rmtree(self.blob_cache_dir)
        for server in self._servers:
            forker.shutdown_zeo_server(server)
        if hasattr(os, 'waitpid'):
            # Not in Windows Python until 2.3
            for pid in self._pids:
                os.waitpid(pid, 0)

    def open(self, read_only=0):
        # Needed to support ReadOnlyStorage tests.  Ought to be a
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

    def checkSortKey(self):
        key = '%s:%s' % (self._storage._storage, self._storage._server_addr)
        self.assertEqual(self._storage.sortKey(), key)

class FullGenericTests(
    GenericTests,
    Cache.StorageWithCache,
    Cache.TransUndoStorageWithCache,
    CommitLockTests.CommitLockUndoTests,
    ConflictResolution.ConflictResolvingStorage,
    ConflictResolution.ConflictResolvingTransUndoStorage,
    PackableStorage.PackableUndoStorage,
    RevisionStorage.RevisionStorage,
    TransactionalUndoStorage.TransactionalUndoStorage,
    TransactionalUndoVersionStorage.TransactionalUndoVersionStorage,
    VersionStorage.VersionStorage,
    ):
    """Extend GenericTests with tests that MappingStorage can't pass."""

class FileStorageTests(FullGenericTests):
    """Test ZEO backed by a FileStorage."""
    level = 2

    def getConfig(self):
        filename = self.__fs_base = tempfile.mktemp()
        return """\
        <filestorage 1>
        path %s
        </filestorage>
        """ % filename

class MappingStorageTests(GenericTests):
    """ZEO backed by a Mapping storage."""

    def getConfig(self):
        return """<mappingstorage 1/>"""

class BlobAdaptedFileStorageTests(GenericTests):
    """ZEO backed by a BlobStorage-adapted FileStorage."""
    def setUp(self):
        self.blobdir = tempfile.mkdtemp()  # This is the blob directory on the ZEO server
        self.filestorage = tempfile.mktemp()
        super(BlobAdaptedFileStorageTests, self).setUp()
        
    def tearDown(self):
        shutil.rmtree(self.blobdir)
        os.unlink(self.filestorage)
        super(BlobAdaptedFileStorageTests, self).tearDown()

    def getConfig(self):
        return """
        <blobstorage 1>
          blob-dir %s
          <filestorage 2>
            path %s
          </filestorage>
        </blobstorage>
        """ % (self.blobdir, self.filestorage)

    def checkStoreBlob(self):
        from ZODB.utils import oid_repr, tid_repr
        from ZODB.Blobs.Blob import Blob
        from ZODB.Blobs.BlobStorage import BLOB_SUFFIX
        from ZODB.tests.StorageTestBase import zodb_pickle, ZERO, \
             handle_serials
        import transaction

        somedata = 'a' * 10

        blob = Blob()
        bd_fh = blob.open('w')
        bd_fh.write(somedata)
        bd_fh.close()
        tfname = bd_fh.name
        oid = self._storage.new_oid()
        data = zodb_pickle(blob)
        self.assert_(os.path.exists(tfname))

        t = transaction.Transaction()
        try:
            self._storage.tpc_begin(t)
            r1 = self._storage.storeBlob(oid, ZERO, data, tfname, '', t)
            r2 = self._storage.tpc_vote(t)
            revid = handle_serials(oid, r1, r2)
            self._storage.tpc_finish(t)
        except:
            self._storage.tpc_abort(t)
            raise
        self.assert_(not os.path.exists(tfname))
        filename = os.path.join(self.blobdir, oid_repr(oid),
                                tid_repr(revid) + BLOB_SUFFIX)
        self.assert_(os.path.exists(filename))
        self.assertEqual(somedata, open(filename).read())
        
    def checkLoadBlob(self):
        from ZODB.Blobs.Blob import Blob
        from ZODB.tests.StorageTestBase import zodb_pickle, ZERO, \
             handle_serials
        import transaction

        version = ''
        somedata = 'a' * 10

        blob = Blob()
        bd_fh = blob.open('w')
        bd_fh.write(somedata)
        bd_fh.close()
        tfname = bd_fh.name
        oid = self._storage.new_oid()
        data = zodb_pickle(blob)

        t = transaction.Transaction()
        try:
            self._storage.tpc_begin(t)
            r1 = self._storage.storeBlob(oid, ZERO, data, tfname, '', t)
            r2 = self._storage.tpc_vote(t)
            serial = handle_serials(oid, r1, r2)
            self._storage.tpc_finish(t)
        except:
            self._storage.tpc_abort(t)
            raise


        class Dummy:
            def __init__(self):
                self.acquired = 0
                self.released = 0
            def acquire(self):
                self.acquired += 1
            def release(self):
                self.released += 1

        class statusdict(dict):
            def __init__(self):
                self.added = []
                self.removed = []
                
            def __setitem__(self, k, v):
                self.added.append(k)
                super(statusdict, self).__setitem__(k, v)

            def __delitem__(self, k):
                self.removed.append(k)
                super(statusdict, self).__delitem__(k)

        # ensure that we do locking properly
        filename = self._storage.fshelper.getBlobFilename(oid, serial)
        thestatuslock = self._storage.blob_status_lock = Dummy()
        thebloblock = Dummy()

        def getBlobLock():
            return thebloblock

        # override getBlobLock to test that locking is performed
        self._storage.getBlobLock = getBlobLock
        thestatusdict = self._storage.blob_status = statusdict()

        filename = self._storage.loadBlob(oid, serial, version)

        self.assertEqual(thestatuslock.acquired, 2)
        self.assertEqual(thestatuslock.released, 2)
        
        self.assertEqual(thebloblock.acquired, 1)
        self.assertEqual(thebloblock.released, 1)

        self.assertEqual(thestatusdict.added, [(oid, serial)])
        self.assertEqual(thestatusdict.removed, [(oid, serial)])

test_classes = [FileStorageTests, MappingStorageTests,
                BlobAdaptedFileStorageTests]

def test_suite():
    suite = unittest.TestSuite()
    for klass in test_classes:
        sub = unittest.makeSuite(klass, "check")
        suite.addTest(sub)
    return suite


if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
