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
import asyncore
import doctest
import logging
import os
import random
import signal
import socket
import stat
import tempfile
import threading
import time
import unittest
import shutil

# ZODB test support
import ZODB
import ZODB.blob
import ZODB.tests.util
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle

# ZODB test mixin classes
from ZODB.tests import StorageTestBase, BasicStorage, VersionStorage, \
     TransactionalUndoStorage, TransactionalUndoVersionStorage, \
     PackableStorage, Synchronization, ConflictResolution, RevisionStorage, \
     MTStorage, ReadOnlyStorage

from ZODB.tests.testDemoStorage import DemoStorageWrappedBase

from ZEO.ClientStorage import ClientStorage

import ZEO.zrpc.connection

from ZEO.tests import forker, Cache, CommitLockTests, ThreadTests

import ZEO.tests.ConnectionTests

import ZEO.StorageServer

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

            # Now, storage 2 should eventually get the new data. It
            # will take some time, although hopefully not much.
            # We'll poll till we get it and whine if we time out:
            for n in range(30):
                time.sleep(.1)
                data, serial = storage2.load(oid, '')
                if (serial == revid2 and
                    zodb_unpickle(data) == MinPO('second')
                    ):
                    break
            else:
                raise AssertionError('Invalidation message was not sent!')
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

    shared_blob_dir = False
    blob_cache_dir = None

    def setUp(self):
        logger.info("setUp() %s", self.id())
        port = get_port()
        zconf = forker.ZEOConfig(('', port))
        zport, adminaddr, pid, path = forker.start_zeo_server(self.getConfig(),
                                                              zconf, port)
        self._pids = [pid]
        self._servers = [adminaddr]
        self._conf_path = path
        if not self.blob_cache_dir:
            # This is the blob cache for ClientStorage
            self.blob_cache_dir = tempfile.mkdtemp()
        self._storage = ClientStorage(
            zport, '1', cache_size=20000000,
            min_disconnect_poll=0.5, wait=1,
            wait_timeout=60, blob_dir=self.blob_cache_dir,
            shared_blob_dir=self.shared_blob_dir)
        self._storage.registerDB(DummyDB())

    def tearDown(self):
        self._storage.close()
        os.remove(self._conf_path)
        ZODB.blob.remove_committed_dir(self.blob_cache_dir)
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

class DemoStorageTests(
    GenericTests,
    Cache.StorageWithCache,
    VersionStorage.VersionStorage,
    ):

    def getConfig(self):
        return """
        <demostorage 1>
          <filestorage 1>
             path %s
          </filestorage>
        </demostorage>
        """ % tempfile.mktemp()

    def checkLoadBeforeVersion(self):
        # Doesn't implement loadBefore, except as a kind of place holder.
        pass
    
    # the next three pack tests depend on undo

    def checkPackVersionReachable(self):
        pass

    def checkPackVersions(self):
        pass

    def checkPackVersionsInPast(self):
        pass

class HeartbeatTests(ZEO.tests.ConnectionTests.CommonSetupTearDown):
    """Make sure a heartbeat is being sent and that it does no harm

    This is really hard to test properly because we can't see the data
    flow between the client and server and we can't really tell what's
    going on in the server very well. :(

    """

    def setUp(self):
        # Crank down the select frequency
        self.__old_client_timeout = ZEO.zrpc.connection.client_timeout
        ZEO.zrpc.connection.client_timeout = 0.1
        ZEO.zrpc.connection.client_trigger.pull_trigger()
        ZEO.tests.ConnectionTests.CommonSetupTearDown.setUp(self)

    def tearDown(self):
        ZEO.zrpc.connection.client_timeout = self.__old_client_timeout
        ZEO.zrpc.connection.client_trigger.pull_trigger()
        ZEO.tests.ConnectionTests.CommonSetupTearDown.tearDown(self)

    def getConfig(self, path, create, read_only):
        return """<mappingstorage 1/>"""

    def checkHeartbeatWithServerClose(self):
        # This is a minimal test that mainly tests that the heartbeat
        # function does no harm.
        client_timeout_count = ZEO.zrpc.connection.client_timeout_count
        self._storage = self.openClientStorage()
        time.sleep(1) # allow some time for the select loop to fire a few times
        self.assert_(ZEO.zrpc.connection.client_timeout_count
                     > client_timeout_count)
        self._dostore()

        if hasattr(os, 'kill'):
            # Kill server violently, in hopes of provoking problem
            os.kill(self._pids[0], signal.SIGKILL)
            self._servers[0] = None
        else:
            self.shutdownServer()

        for i in range(91):
            # wait for disconnection
            if not self._storage.is_connected():
                break
            time.sleep(0.1)
        else:
            raise AssertionError("Didn't detect server shutdown in 5 seconds")

    def checkHeartbeatWithClientClose(self):
        # This is a minimal test that mainly tests that the heartbeat
        # function does no harm.
        client_timeout_count = ZEO.zrpc.connection.client_timeout_count
        self._storage = self.openClientStorage()
        self._storage.close()
        time.sleep(1) # allow some time for the select loop to fire a few times
        self.assert_(ZEO.zrpc.connection.client_timeout_count
                     > client_timeout_count)


class CatastrophicClientLoopFailure(
    ZEO.tests.ConnectionTests.CommonSetupTearDown):
    """Test what happens when the client loop falls over
    """

    def getConfig(self, path, create, read_only):
        return """<mappingstorage 1/>"""

    def checkCatastrophicClientLoopFailure(self):
        self._storage = self.openClientStorage()

        class Evil:
            def writable(self):
                raise SystemError("I'm evil")

        log = []
        ZEO.zrpc.connection.client_logger.critical = (
            lambda m, *a, **kw: log.append((m % a, kw))
            )

        ZEO.zrpc.connection.client_map[None] = Evil()
        
        try:
            ZEO.zrpc.connection.client_trigger.pull_trigger()
        except DisconnectedError:
            pass

        time.sleep(.1)
        self.failIf(self._storage.is_connected())
        self.assertEqual(len(ZEO.zrpc.connection.client_map), 1)
        del ZEO.zrpc.connection.client_logger.critical
        self.assertEqual(log[0][0], 'The ZEO cient loop failed.')
        self.assert_('exc_info' in log[0][1])
        self.assertEqual(log[1][0], "Couldn't close a dispatcher.")
        self.assert_('exc_info' in log[1][1])

class ConnectionInvalidationOnReconnect(
    ZEO.tests.ConnectionTests.CommonSetupTearDown):
    """Test what happens when the client loop falls over
    """

    def getConfig(self, path, create, read_only):
        return """<mappingstorage 1/>"""

    def checkConnectionInvalidationOnReconnect(self):

        storage = ClientStorage(self.addr, wait=1, min_disconnect_poll=0.1)
        self._storage = storage

        # and we'll wait for the storage to be reconnected:
        for i in range(100):
            if storage.is_connected():
                break
            time.sleep(0.1)
        else:
            raise AssertionError("Couldn't connect to server")

        class DummyDB:
            _invalidatedCache = 0
            def invalidateCache(self):
                self._invalidatedCache += 1
            def invalidate(*a, **k):
                pass
                
        db = DummyDB()
        storage.registerDB(db)

        base = db._invalidatedCache

        # Now we'll force a disconnection and reconnection
        storage._connection.close()

        # and we'll wait for the storage to be reconnected:
        for i in range(100):
            if storage.is_connected():
                break
            time.sleep(0.1)
        else:
            raise AssertionError("Couldn't connect to server")

        # Now, the root object in the connection should have been invalidated:
        self.assertEqual(db._invalidatedCache, base+1)
    

class DemoStorageWrappedAroundClientStorage(DemoStorageWrappedBase):

    def getConfig(self):
        return """<mappingstorage 1/>"""

    def _makeBaseStorage(self):
        logger.info("setUp() %s", self.id())
        port = get_port()
        zconf = forker.ZEOConfig(('', port))
        zport, adminaddr, pid, path = forker.start_zeo_server(self.getConfig(),
                                                              zconf, port)
        self._pids = [pid]
        self._servers = [adminaddr]
        self._conf_path = path
        _base = ClientStorage(zport, '1', cache_size=20000000,
                                      min_disconnect_poll=0.5, wait=1,
                                      wait_timeout=60)
        _base.registerDB(DummyDB())
        return _base

    def tearDown(self):
        DemoStorageWrappedBase.tearDown(self)
        os.remove(self._conf_path)
        for server in self._servers:
            forker.shutdown_zeo_server(server)
        if hasattr(os, 'waitpid'):
            # Not in Windows Python until 2.3
            for pid in self._pids:
                os.waitpid(pid, 0)


test_classes = [OneTimeTests,
                FileStorageTests,
                MappingStorageTests,
                DemoStorageWrappedAroundClientStorage,
                HeartbeatTests,
                CatastrophicClientLoopFailure,
                ConnectionInvalidationOnReconnect,
               ]

class CommonBlobTests:

    def tearDown(self):
        super(BlobAdaptedFileStorageTests, self).tearDown()
        if os.path.exists(self.blobdir):
            # Might be gone already if the super() method deleted
            # the shared directory. Don't worry.
            shutil.rmtree(self.blobdir)

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
        from ZODB.blob import Blob, BLOB_SUFFIX
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

    def checkStoreBlob_wrong_partition(self):
        os_rename = os.rename
        try:
            def fail(*a):
                raise OSError
            os.rename = fail
            self.checkStoreBlob()
        finally:
            os.rename = os_rename

    def checkLoadBlob(self):
        from ZODB.blob import Blob
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

        filename = self._storage.loadBlob(oid, serial)
        self.assertEquals(somedata, open(filename, 'rb').read())
        self.assert_(not(os.stat(filename).st_mode & stat.S_IWRITE))
        self.assert_((os.stat(filename).st_mode & stat.S_IREAD))

    def checkTemporaryDirectory(self):
        self.assertEquals(self.blob_cache_dir,
                          self._storage.temporaryDirectory())

class BlobAdaptedFileStorageTests(GenericTests, CommonBlobTests):
    """ZEO backed by a BlobStorage-adapted FileStorage."""

    def setUp(self):
        self.blobdir = tempfile.mkdtemp()  # blob directory on ZEO server
        self.filestorage = tempfile.mktemp()
        super(BlobAdaptedFileStorageTests, self).setUp()

    def checkStoreAndLoadBlob(self):
        from ZODB.utils import oid_repr, tid_repr
        from ZODB.blob import Blob, BLOB_SUFFIX
        from ZODB.tests.StorageTestBase import zodb_pickle, ZERO, \
             handle_serials
        import transaction

        somedata_path = os.path.join(self.blob_cache_dir, 'somedata')
        somedata = open(somedata_path, 'w+b')
        for i in range(1000000):
            somedata.write("%s\n" % i)
        somedata.seek(0)
        
        blob = Blob()
        bd_fh = blob.open('w')
        ZODB.utils.cp(somedata, bd_fh)
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

        # The uncommitted data file should have been removed
        self.assert_(not os.path.exists(tfname))

        def check_data(path):
            self.assert_(os.path.exists(path))
            f = open(path, 'rb')
            somedata.seek(0) 
            d1 = d2 = 1
            while d1 or d2:
                d1 = f.read(8096)
                d2 = somedata.read(8096)
                self.assertEqual(d1, d2)
                
        
        # The file should have been copied to the server:
        filename = os.path.join(self.blobdir, oid_repr(oid),
                                tid_repr(revid) + BLOB_SUFFIX)
        check_data(filename)

        # It should also be in the cache:
        filename = os.path.join(self.blob_cache_dir, oid_repr(oid),
                                tid_repr(revid) + BLOB_SUFFIX)
        check_data(filename)

        # If we remove it from the cache and call loadBlob, it should
        # come back. We can do this in many threads.  We'll instrument
        # the method that is used to request data from teh server to
        # verify that it is only called once.

        sendBlob_org = ZEO.ServerStub.StorageServer.sendBlob
        calls = []
        def sendBlob(self, oid, serial):
            calls.append((oid, serial))
            sendBlob_org(self, oid, serial)

        ZODB.blob.remove_committed(filename)
        returns = []
        threads = [
            threading.Thread(
               target=lambda :
                      returns.append(self._storage.loadBlob(oid, revid))
               )
            for i in range(10)
            ]
        [thread.start() for thread in threads]
        [thread.join() for thread in threads]
        [self.assertEqual(r, filename) for r in returns]        
        check_data(filename)
        

class BlobWritableCacheTests(GenericTests, CommonBlobTests):

    def setUp(self):
        self.blobdir = self.blob_cache_dir = tempfile.mkdtemp()
        self.filestorage = tempfile.mktemp()
        self.shared_blob_dir = True
        super(BlobWritableCacheTests, self).setUp()


class StorageServerClientWrapper:

    def __init__(self):
        self.serials = []

    def serialnos(self, serials):
        self.serials.extend(serials)

    def info(self, info):
        pass

class StorageServerWrapper:

    def __init__(self, server, storage_id):
        self.storage_id = storage_id
        self.server = ZEO.StorageServer.ZEOStorage(server, server.read_only)
        self.server.register(storage_id, False)
        self.server._thunk = lambda : None
        self.server.client = StorageServerClientWrapper()

    def sortKey(self):
        return self.storage_id

    def __getattr__(self, name):
        return getattr(self.server, name)

    def registerDB(self, *args):
        pass

    def supportsUndo(self):
        return False

    def supportsVersions(self):
        return False

    def new_oid(self):
        return self.server.new_oids(1)[0]

    def tpc_begin(self, transaction):
        self.server.tpc_begin(id(transaction), '', '', {}, None, ' ')

    def tpc_vote(self, transaction):
        self.server._restart()
        self.server.vote(id(transaction))
        result = self.server.client.serials[:]
        del self.server.client.serials[:]
        return result

    def store(self, oid, serial, data, version, transaction):
        self.server.storea(oid, serial, data, version, id(transaction))

    def tpc_finish(self, transaction, func = lambda: None):
        self.server.tpc_finish(id(transaction))


def multiple_storages_invalidation_queue_is_not_insane():
    """
    >>> from ZEO.StorageServer import StorageServer, ZEOStorage
    >>> from ZODB.FileStorage import FileStorage
    >>> from ZODB.DB import DB
    >>> from persistent.dict import PersistentDict
    >>> from transaction import commit
    >>> fs1 = FileStorage('t1.fs')
    >>> fs2 = FileStorage('t2.fs')
    >>> server = StorageServer(('', get_port()), dict(fs1=fs1, fs2=fs2))

    >>> s1 = StorageServerWrapper(server, 'fs1')
    >>> s2 = StorageServerWrapper(server, 'fs2')

    >>> db1 = DB(s1); conn1 = db1.open()
    >>> db2 = DB(s2); conn2 = db2.open()

    >>> commit()
    >>> o1 = conn1.root()
    >>> for i in range(10):
    ...     o1.x = PersistentDict(); o1 = o1.x
    ...     commit()

    >>> last = fs1.lastTransaction()
    >>> for i in range(5):
    ...     o1.x = PersistentDict(); o1 = o1.x
    ...     commit()

    >>> o2 = conn2.root()
    >>> for i in range(20):
    ...     o2.x = PersistentDict(); o2 = o2.x
    ...     commit()
    
    >>> trans, oids = s1.getInvalidations(last)
    >>> from ZODB.utils import u64
    >>> sorted([int(u64(oid)) for (oid, v) in oids])
    [10, 11, 12, 13, 14]
    
    >>> server.close_server()
    """

def getInvalidationsAfterServerRestart():
    """

Clients were often forced to verify their caches after a server
restart even if there weren't many transactions between the server
restart and the client connect.

Let's create a file storage and stuff some data into it:

    >>> from ZEO.StorageServer import StorageServer, ZEOStorage
    >>> from ZODB.FileStorage import FileStorage
    >>> from ZODB.DB import DB
    >>> from persistent.dict import PersistentDict
    >>> fs = FileStorage('t.fs')
    >>> db = DB(fs)
    >>> conn = db.open()
    >>> from transaction import commit
    >>> last = []
    >>> for i in range(100):
    ...     conn.root()[i] = PersistentDict()
    ...     commit()
    ...     last.append(fs.lastTransaction())
    >>> db.close()

Now we'll open a storage server on the data, simulating a restart:
    
    >>> fs = FileStorage('t.fs')
    >>> sv = StorageServer(('', get_port()), dict(fs=fs))
    >>> s = ZEOStorage(sv, sv.read_only)
    >>> s.register('fs', False)

If we ask for the last transaction, we should get the last transaction
we saved:

    >>> s.lastTransaction() == last[-1]
    True

If a storage implements the method lastInvalidations, as FileStorage
does, then the stroage server will populate its invalidation data
structure using lastTransactions.


    >>> tid, oids = s.getInvalidations(last[-10])
    >>> tid == last[-1]
    True


    >>> from ZODB.utils import u64
    >>> sorted([int(u64(oid)) for (oid, version) in oids])
    [0, 92, 93, 94, 95, 96, 97, 98, 99, 100]

(Note that the fact that we get oids for 92-100 is actually an
artifact of the fact that the FileStorage lastInvalidations method
returns all OIDs written by transactions, even if the OIDs were
created and not modified. FileStorages don't record whether objects
were created rather than modified. Objects that are just created don't
need to be invalidated.  This means we'll invalidate objects that
dont' need to be invalidated, however, that's better than verifying
caches.)

    >>> sv.close_server()
    >>> fs.close()

If a storage doesn't implement lastInvalidations, a client can still
avoid verifying its cache if it was up to date when the server
restarted.  To illustrate this, we'll create a subclass of FileStorage
without this method:

    >>> class FS(FileStorage):
    ...     lastInvalidations = property()

    >>> fs = FS('t.fs')
    >>> sv = StorageServer(('', get_port()), dict(fs=fs))
    >>> st = StorageServerWrapper(sv, 'fs')
    >>> s = st.server
    
Now, if we ask fior the invalidations since the last committed
transaction, we'll get a result:

    >>> tid, oids = s.getInvalidations(last[-1])
    >>> tid == last[-1]
    True
    >>> oids
    []

    >>> db = DB(st); conn = db.open()
    >>> ob = conn.root()
    >>> for i in range(5):
    ...     ob.x = PersistentDict(); ob = ob.x
    ...     commit()
    ...     last.append(fs.lastTransaction())

    >>> ntid, oids = s.getInvalidations(tid)
    >>> ntid == last[-1]
    True

    >>> sorted([int(u64(oid)) for (oid, version) in oids])
    [0, 101, 102, 103, 104]

    """


test_classes = [FileStorageTests, MappingStorageTests, DemoStorageTests,
                BlobAdaptedFileStorageTests, BlobWritableCacheTests]

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(doctest.DocTestSuite(setUp=ZODB.tests.util.setUp,
                                       tearDown=ZODB.tests.util.tearDown))
    suite.addTest(doctest.DocFileSuite('registerDB.test'))
    suite.addTest(
        doctest.DocFileSuite('zeo-fan-out.test',
                             setUp=ZODB.tests.util.setUp,
                             tearDown=ZODB.tests.util.tearDown,
                             ),
        )
    for klass in test_classes:
        sub = unittest.makeSuite(klass, "check")
        suite.addTest(sub)
    return suite


if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
