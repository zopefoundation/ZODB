##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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

from ZEO.ClientStorage import ClientStorage
from ZEO.tests.forker import get_port
from ZEO.tests import forker, Cache, CommitLockTests, ThreadTests
from ZEO.tests import IterationTests
from ZEO.zrpc.error import DisconnectedError
from ZODB.tests import StorageTestBase, BasicStorage,  \
     TransactionalUndoStorage,  \
     PackableStorage, Synchronization, ConflictResolution, RevisionStorage, \
     MTStorage, ReadOnlyStorage, IteratorStorage, RecoveryStorage
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle
from ZODB.utils import p64, u64
import asyncore
import doctest
import logging
import os
import persistent
import shutil
import signal
import stat
import sys
import tempfile
import threading
import time
import transaction
import unittest
import ZEO.ServerStub
import ZEO.StorageServer
import ZEO.tests.ConnectionTests
import ZEO.zrpc.connection
import ZODB
import ZODB.blob
import ZODB.tests.testblob
import ZODB.tests.util
import ZODB.utils
import zope.testing.setupstack

logger = logging.getLogger('ZEO.tests.testZEO')

ZEO.zrpc.connection.start_client_thread()

class DummyDB:
    def invalidate(self, *args):
        pass
    def invalidateCache(*unused):
        pass


class CreativeGetState(persistent.Persistent):
    def __getstate__(self):
        self.name = 'me'
        return super(CreativeGetState, self).__getstate__()


class MiscZEOTests:
    """ZEO tests that don't fit in elsewhere."""

    def checkCreativeGetState(self):
        # This test covers persistent objects that provide their own
        # __getstate__ which modifies the state of the object.
        # For details see bug #98275

        db = ZODB.DB(self._storage)
        cn = db.open()
        rt = cn.root()
        m = CreativeGetState()
        m.attr = 'hi'
        rt['a'] = m

        # This commit used to fail because of the `Mine` object being put back
        # into `changed` state although it was already stored causing the ZEO
        # cache to bail out.
        transaction.commit()
        cn.close()

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

    def checkVolatileCacheWithImmediateLastTransaction(self):
        # Earlier, a ClientStorage would not have the last transaction id
        # available right after successful connection, this is required now.
        addr = self._storage._addr
        storage2 = ClientStorage(addr)
        self.assert_(storage2.is_connected())
        self.assertEquals(None, storage2.lastTransaction())
        storage2.close()

        self._dostore()
        storage3 = ClientStorage(addr)
        self.assert_(storage3.is_connected())
        self.assertEquals(8, len(storage3.lastTransaction()))
        self.assertNotEquals(ZODB.utils.z64, storage3.lastTransaction())
        storage3.close()

class ConfigurationTests(unittest.TestCase):

    def checkDropCacheRatherVerifyConfiguration(self):
        from ZODB.config import storageFromString
        # the default is to do verification and not drop the cache
        cs = storageFromString('''
        <zeoclient>
          server localhost:9090
          wait false
        </zeoclient>
        ''')
        self.assertEqual(cs._drop_cache_rather_verify, False)
        cs.close()
        # now for dropping
        cs = storageFromString('''
        <zeoclient>
          server localhost:9090
          wait false
          drop-cache-rather-verify true
        </zeoclient>
        ''')
        self.assertEqual(cs._drop_cache_rather_verify, True)
        cs.close()


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
    MiscZEOTests,
    ):

    """Combine tests from various origins in one class."""

    shared_blob_dir = False
    blob_cache_dir = None

    def setUp(self):
        StorageTestBase.StorageTestBase.setUp(self)
        logger.info("setUp() %s", self.id())
        port = get_port(self)
        zconf = forker.ZEOConfig(('', port))
        zport, adminaddr, pid, path = forker.start_zeo_server(self.getConfig(),
                                                              zconf, port)
        self._pids = [pid]
        self._servers = [adminaddr]
        self._conf_path = path
        if not self.blob_cache_dir:
            # This is the blob cache for ClientStorage
            self.blob_cache_dir = tempfile.mkdtemp(
                'blob_cache',
                dir=os.path.abspath(os.getcwd()))
        self._storage = ClientStorage(
            zport, '1', cache_size=20000000,
            min_disconnect_poll=0.5, wait=1,
            wait_timeout=60, blob_dir=self.blob_cache_dir,
            shared_blob_dir=self.shared_blob_dir)
        self._storage.registerDB(DummyDB())

    def tearDown(self):
        self._storage.close()
        for server in self._servers:
            forker.shutdown_zeo_server(server)
        if hasattr(os, 'waitpid'):
            # Not in Windows Python until 2.3
            for pid in self._pids:
                os.waitpid(pid, 0)
        StorageTestBase.StorageTestBase.tearDown(self)

    def runTest(self):
        try:
            super(GenericTests, self).runTest()
        except:
            self._failed = True
            raise
        else:
            self._failed = False

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
    Cache.TransUndoStorageWithCache,
    CommitLockTests.CommitLockUndoTests,
    ConflictResolution.ConflictResolvingStorage,
    ConflictResolution.ConflictResolvingTransUndoStorage,
    PackableStorage.PackableUndoStorage,
    RevisionStorage.RevisionStorage,
    TransactionalUndoStorage.TransactionalUndoStorage,
    IteratorStorage.IteratorStorage,
    IterationTests.IterationTests,
    ):
    """Extend GenericTests with tests that MappingStorage can't pass."""

class FileStorageRecoveryTests(StorageTestBase.StorageTestBase,
                               RecoveryStorage.RecoveryStorage):

    def getConfig(self):
        return """\
        <filestorage 1>
        path %s
        </filestorage>
        """ % tempfile.mktemp(dir='.')

    def _new_storage(self):
        port = get_port(self)
        zconf = forker.ZEOConfig(('', port))
        zport, adminaddr, pid, path = forker.start_zeo_server(self.getConfig(),
                                                              zconf, port)
        self._pids.append(pid)
        self._servers.append(adminaddr)

        blob_cache_dir = tempfile.mkdtemp(dir='.')

        storage = ClientStorage(
            zport, '1', cache_size=20000000,
            min_disconnect_poll=0.5, wait=1,
            wait_timeout=60, blob_dir=blob_cache_dir)
        storage.registerDB(DummyDB())
        return storage

    def setUp(self):
        StorageTestBase.StorageTestBase.setUp(self)
        self._pids = []
        self._servers = []

        self._storage = self._new_storage()
        self._dst = self._new_storage()

    def tearDown(self):
        self._storage.close()
        self._dst.close()

        for server in self._servers:
            forker.shutdown_zeo_server(server)
        if hasattr(os, 'waitpid'):
            # Not in Windows Python until 2.3
            for pid in self._pids:
                os.waitpid(pid, 0)
        StorageTestBase.StorageTestBase.tearDown(self)

    def new_dest(self):
        return self._new_storage()


class FileStorageTests(FullGenericTests):
    """Test ZEO backed by a FileStorage."""

    def getConfig(self):
        return """\
        <filestorage 1>
        path Data.fs
        </filestorage>
        """

    def checkInterfaceFromRemoteStorage(self):
        # ClientStorage itself doesn't implement IStorageIteration, but the
        # FileStorage on the other end does, and thus the ClientStorage
        # instance that is connected to it reflects this.
        self.failIf(ZODB.interfaces.IStorageIteration.implementedBy(
            ZEO.ClientStorage.ClientStorage))
        self.failUnless(ZODB.interfaces.IStorageIteration.providedBy(
            self._storage))
        # This is communicated using ClientStorage's _info object:
        self.assertEquals(
            (('ZODB.interfaces', 'IStorageRestoreable'),
             ('ZODB.interfaces', 'IStorageIteration'),
             ('ZODB.interfaces', 'IStorageUndoable'),
             ('ZODB.interfaces', 'IStorageCurrentRecordIteration'),
             ('ZODB.interfaces', 'IExternalGC'),
             ('ZODB.interfaces', 'IStorage'),
             ('zope.interface', 'Interface'),
             ),
            self._storage._info['interfaces']
            )


class MappingStorageTests(GenericTests):
    """ZEO backed by a Mapping storage."""

    def getConfig(self):
        return """<mappingstorage 1/>"""

    def checkSimpleIteration(self):
        # The test base class IteratorStorage assumes that we keep undo data
        # to construct our iterator, which we don't, so we disable this test.
        pass

    def checkUndoZombie(self):
        # The test base class IteratorStorage assumes that we keep undo data
        # to construct our iterator, which we don't, so we disable this test.
        pass

class DemoStorageTests(
    GenericTests,
    ):

    def getConfig(self):
        return """
        <demostorage 1>
          <filestorage 1>
             path Data.fs
          </filestorage>
        </demostorage>
        """

    def checkUndoZombie(self):
        # The test base class IteratorStorage assumes that we keep undo data
        # to construct our iterator, which we don't, so we disable this test.
        pass

    def checkPackWithMultiDatabaseReferences(self):
        pass # DemoStorage pack doesn't do gc
    checkPackAllRevisions = checkPackWithMultiDatabaseReferences

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


class ZRPCConnectionTests(ZEO.tests.ConnectionTests.CommonSetupTearDown):

    def getConfig(self, path, create, read_only):
        return """<mappingstorage 1/>"""

    def checkCatastrophicClientLoopFailure(self):
        # Test what happens when the client loop falls over
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
        self.assertEqual(log[0][0], 'The ZEO client loop failed.')
        self.assert_('exc_info' in log[0][1])
        self.assertEqual(log[1][0], "Couldn't close a dispatcher.")
        self.assert_('exc_info' in log[1][1])

    def checkExceptionLogsAtError(self):
        # Test the exceptions are logged at error
        self._storage = self.openClientStorage()
        conn = self._storage._connection
        # capture logging
        log = []
        conn.logger.log = (
            lambda l, m, *a, **kw: log.append((l,m % a, kw))
            )

        # This is a deliberately bogus call to get an exception
        # logged
        self._storage._connection.handle_request('foo',0,'history',(1,2,3,4))
        # test logging
        if __debug__:
            level,message,kw = log[1]
        else:
            level,message,kw = log[0]
        self.assertEqual(level,logging.ERROR)
        self.failUnless(message.endswith(
                ') history() raised exception: history() takes at'
                ' most 3 arguments (5 given)'
                ))
        self.assertEqual(kw,{'exc_info':True})
        
        # cleanup
        del conn.logger.log

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


class CommonBlobTests:

    def getConfig(self):
        return """
        <blobstorage 1>
          blob-dir blobs
          <filestorage 2>
            path Data.fs
          </filestorage>
        </blobstorage>
        """

    blobdir = 'blobs'
    blob_cache_dir = 'blob_cache'

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
        filename = self._storage.fshelper.getBlobFilename(oid, revid)
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
        self.assertEquals(os.path.join(self.blob_cache_dir, 'tmp'),
                          self._storage.temporaryDirectory())

    def checkTransactionBufferCleanup(self):
        oid = self._storage.new_oid()
        open('blob_file', 'w').write('I am a happy blob.')
        t = transaction.Transaction()
        self._storage.tpc_begin(t)
        self._storage.storeBlob(
          oid, ZODB.utils.z64, 'foo', 'blob_file', '', t)
        self._storage.close()


class BlobAdaptedFileStorageTests(FullGenericTests, CommonBlobTests):
    """ZEO backed by a BlobStorage-adapted FileStorage."""

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

        # The file should be in the cache ...
        filename = self._storage.fshelper.getBlobFilename(oid, revid)
        check_data(filename)

        # ... and on the server
        server_filename = os.path.join(
            self.blobdir,
            ZODB.blob.BushyLayout().getBlobFilePath(oid, revid),
            )

        self.assert_(server_filename.startswith(self.blobdir))
        check_data(server_filename)

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


class BlobWritableCacheTests(FullGenericTests, CommonBlobTests):

    blob_cache_dir = 'blobs'
    shared_blob_dir = True

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

    def store(self, oid, serial, data, version_ignored, transaction):
        self.server.storea(oid, serial, data, id(transaction))

    def tpc_finish(self, transaction, func = lambda: None):
        self.server.tpc_finish(id(transaction))


def multiple_storages_invalidation_queue_is_not_insane():
    """
    >>> from ZEO.StorageServer import StorageServer, ZEOStorage
    >>> from ZODB.FileStorage import FileStorage
    >>> from ZODB.DB import DB
    >>> from persistent.mapping import PersistentMapping
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
    ...     o1.x = PersistentMapping(); o1 = o1.x
    ...     commit()

    >>> last = fs1.lastTransaction()
    >>> for i in range(5):
    ...     o1.x = PersistentMapping(); o1 = o1.x
    ...     commit()

    >>> o2 = conn2.root()
    >>> for i in range(20):
    ...     o2.x = PersistentMapping(); o2 = o2.x
    ...     commit()

    >>> trans, oids = s1.getInvalidations(last)
    >>> from ZODB.utils import u64
    >>> sorted([int(u64(oid)) for oid in oids])
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
    >>> from persistent.mapping import PersistentMapping
    >>> fs = FileStorage('t.fs')
    >>> db = DB(fs)
    >>> conn = db.open()
    >>> from transaction import commit
    >>> last = []
    >>> for i in range(100):
    ...     conn.root()[i] = PersistentMapping()
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
    >>> sorted([int(u64(oid)) for oid in oids])
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

Now, if we ask for the invalidations since the last committed
transaction, we'll get a result:

    >>> tid, oids = s.getInvalidations(last[-1])
    >>> tid == last[-1]
    True
    >>> oids
    []

    >>> db = DB(st); conn = db.open()
    >>> ob = conn.root()
    >>> for i in range(5):
    ...     ob.x = PersistentMapping(); ob = ob.x
    ...     commit()
    ...     last.append(fs.lastTransaction())

    >>> ntid, oids = s.getInvalidations(tid)
    >>> ntid == last[-1]
    True

    >>> sorted([int(u64(oid)) for oid in oids])
    [0, 101, 102, 103, 104]

    >>> fs.close()
    """

def tpc_finish_error():
    r"""Server errors in tpc_finish weren't handled properly.

    >>> import ZEO.ClientStorage, ZEO.zrpc.connection

    >>> class Connection:
    ...     peer_protocol_version = (
    ...         ZEO.zrpc.connection.Connection.current_protocol)
    ...     def __init__(self, client):
    ...         self.client = client
    ...     def get_addr(self):
    ...         return 'server'
    ...     def is_async(self):
    ...         return True
    ...     def register_object(self, ob):
    ...         pass
    ...     def close(self):
    ...         print 'connection closed'

    >>> class ConnectionManager:
    ...     def __init__(self, addr, client, tmin, tmax):
    ...         self.client = client
    ...     def connect(self, sync=1):
    ...         self.client.notifyConnected(Connection(self.client))
    ...     def close(self):
    ...         pass

    >>> class StorageServer:
    ...     should_fail = True
    ...     def __init__(self, conn):
    ...         self.conn = conn
    ...         self.t = None
    ...     def get_info(self):
    ...         return {}
    ...     def endZeoVerify(self):
    ...         self.conn.client.endVerify()
    ...     def lastTransaction(self):
    ...         return '\0'*8
    ...     def tpc_begin(self, t, *args):
    ...         if self.t is not None:
    ...             raise TypeError('already trans')
    ...         self.t = t
    ...         print 'begin', args
    ...     def vote(self, t):
    ...         if self.t != t:
    ...             raise TypeError('bad trans')
    ...         print 'vote'
    ...     def tpc_finish(self, *args):
    ...         if self.should_fail:
    ...             raise TypeError()
    ...         print 'finish'
    ...     def tpc_abort(self, t):
    ...         if self.t != t:
    ...             raise TypeError('bad trans')
    ...         self.t = None
    ...         print 'abort'
    ...     def iterator_gc(*args):
    ...         pass

    >>> class ClientStorage(ZEO.ClientStorage.ClientStorage):
    ...     ConnectionManagerClass = ConnectionManager
    ...     StorageServerStubClass = StorageServer

    >>> class Transaction:
    ...     user = 'test'
    ...     description = ''
    ...     _extension = {}

    >>> cs = ClientStorage(('', ''))
    >>> t1 = Transaction()
    >>> cs.tpc_begin(t1)
    begin ('test', '', {}, None, ' ')

    >>> cs.tpc_vote(t1)
    vote

    >>> cs.tpc_finish(t1)
    Traceback (most recent call last):
    ...
    TypeError

    >>> cs.tpc_abort(t1)
    abort

    >>> t2 = Transaction()
    >>> cs.tpc_begin(t2)
    begin ('test', '', {}, None, ' ')
    >>> cs.tpc_vote(t2)
    vote

    If client storage has an internal error after the storage finish
    succeeeds, it will close the connection, which will force a
    restart and reverification.

    >>> StorageServer.should_fail = False
    >>> cs._update_cache = lambda : None
    >>> try: cs.tpc_finish(t2)
    ... except: pass
    ... else: print "Should have failed"
    finish
    connection closed

    >>> cs.close()
    connection closed
    """

def client_has_newer_data_than_server():
    """It is bad if a client has newer data than the server.

    >>> db = ZODB.DB('Data.fs')
    >>> db.close()
    >>> shutil.copyfile('Data.fs', 'Data.save')
    >>> addr, admin = start_server(keep=1)
    >>> db = ZEO.DB(addr, name='client', max_disconnect_poll=.01)
    >>> wait_connected(db.storage)
    >>> conn = db.open()
    >>> conn.root().x = 1
    >>> transaction.commit()

    OK, we've added some data to the storage and the client cache has
    the new data. Now, we'll stop the server, put back the old data, and
    see what happens. :)

    >>> stop_server(admin)
    >>> shutil.copyfile('Data.save', 'Data.fs')

    >>> import zope.testing.loggingsupport
    >>> handler = zope.testing.loggingsupport.InstalledHandler(
    ...     'ZEO', level=logging.ERROR)
    >>> formatter = logging.Formatter('%(name)s %(levelname)s %(message)s')

    >>> _, admin = start_server(addr=addr)

    >>> for i in range(1000):
    ...     while len(handler.records) < 5:
    ...           time.sleep(.01)

    >>> db.close()
    >>> for record in handler.records[:5]:
    ...     print formatter.format(record)
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    ZEO.ClientStorage CRITICAL client
    Client has seen newer transactions than server!
    ZEO.zrpc ERROR (...) CW: error in notifyConnected (('localhost', ...))
    Traceback (most recent call last):
    ...
    ClientStorageError: client Client has seen newer transactions than server!
    ZEO.ClientStorage CRITICAL client
    Client has seen newer transactions than server!
    ZEO.zrpc ERROR (...) CW: error in notifyConnected (('localhost', ...))
    Traceback (most recent call last):
    ...
    ClientStorageError: client Client has seen newer transactions than server!
    ...

    Note that the errors repeat because the client keeps on trying to connect.

    >>> handler.uninstall()
    >>> stop_server(admin)

    """

def history_over_zeo():
    """
    >>> addr, _ = start_server()
    >>> db = ZEO.DB(addr)
    >>> wait_connected(db.storage)
    >>> conn = db.open()
    >>> conn.root().x = 0
    >>> transaction.commit()
    >>> len(db.history(conn.root()._p_oid, 99))
    2

    >>> db.close()
    """

def dont_log_poskeyerrors_on_server():
    """
    >>> addr, admin = start_server()
    >>> cs = ClientStorage(addr)
    >>> cs.load(ZODB.utils.p64(1))
    Traceback (most recent call last):
    ...
    POSKeyError: 0x01

    >>> cs.close()
    >>> stop_server(admin)
    >>> 'POSKeyError' in open('server-%s.log' % addr[1]).read()
    False
    """

def delete_object_multiple_clients():
    """If we delete on one client, the delete should be reflected on the other.

    First, we'll create an object:

    >>> addr, _ = start_server()
    >>> db = ZEO.DB(addr)
    >>> conn = db.open()
    >>> conn.root()[0] = conn.root().__class__()
    >>> transaction.commit()
    >>> oid = conn.root()[0]._p_oid

    We verify that we can read it in another client, which also loads
    it into the client cache.

    >>> cs = ClientStorage(addr)
    >>> p, s = cs.load(oid)

    Now, we'll remove the object:

    >>> txn = transaction.begin()
    >>> db.storage.tpc_begin(txn)
    >>> db.storage.deleteObject(oid, s, txn)
    >>> db.storage.tpc_vote(txn)
    >>> db.storage.tpc_finish(txn)

    And we'll get a POSKeyError if we try to access it:

    >>> db.storage.load(oid) # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    POSKeyError: ...

    We'll wait for our other storage to get the invalidation and then
    try to access the object. We'll get a POSKeyError there too:

    >>> tid = db.storage.lastTransaction()
    >>> forker.wait_until(
    ...    'cs has caught up',
    ...    lambda : cs.lastTransaction() == tid)
    >>> cs.load(oid) # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    POSKeyError: ...

    >>> db.close()
    >>> cs.close()
    """


def open_convenience():
    """Often, we just want to open a single connection.

    >>> addr, _ = start_server(path='data.fs')
    >>> conn = ZEO.connection(addr)
    >>> conn.root()
    {}

    >>> conn.root()['x'] = 1
    >>> transaction.commit()
    >>> conn.close()

    Let's make sure the database was cloased when we closed the
    connection, and that the data is there.

    >>> db = ZEO.DB(addr)
    >>> conn = db.open()
    >>> conn.root()
    {'x': 1}
    >>> db.close()
    """

def client_asyncore_thread_has_name():
    """
    >>> len([t for t in threading.enumerate()
    ...      if t.getName() == 'ZEO.zrpc.connection'])
    1
    """

def runzeo_without_configfile():
    """
    >>> open('runzeo', 'w').write('''
    ... import sys
    ... sys.path[:] = %r
    ... import ZEO.runzeo
    ... ZEO.runzeo.main(sys.argv[1:])
    ... ''' % sys.path)

    >>> import subprocess, re
    >>> print re.sub('\d\d+|[:]', '', subprocess.Popen(
    ...     [sys.executable, 'runzeo', '-a:%s' % get_port(), '-ft', '--test'],
    ...     stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    ...     ).stdout.read()), # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    ------
    --T INFO ZEO.runzeo () opening storage '1' using FileStorage
    ------
    --T INFO ZEO.StorageServer () StorageServer created RW with storages 1RWt
    ------
    --T INFO ZEO.zrpc () listening on ...
    ------
    --T INFO ZEO.runzeo () closing storage '1'
    testing exit immediately
    """

def invalidate_client_cache_entry_on_server_commit_error():
    """

When the serials returned during commit includes an error, typically a
conflict error, invalidate the cache entry.  This is important when
the cache is messed up.

    >>> addr, _ = start_server()
    >>> conn1 = ZEO.connection(addr)
    >>> conn1.root.x = conn1.root().__class__()
    >>> transaction.commit()
    >>> conn1.root.x
    {}

    >>> cs = ZEO.ClientStorage.ClientStorage(addr, client='cache')
    >>> conn2 = ZODB.connection(cs)
    >>> conn2.root.x
    {}

    >>> conn2.close()
    >>> cs.close()

    >>> conn1.root.x['x'] = 1
    >>> transaction.commit()
    >>> conn1.root.x
    {'x': 1}

Now, let's screw up the cache by making it have a last tid that is later than
the root serial.

    >>> import ZEO.cache
    >>> cache = ZEO.cache.ClientCache('cache-1.zec')
    >>> cache.setLastTid(p64(u64(conn1.root.x._p_serial)+1))
    >>> cache.close()

We'll also update the server so that it's last tid is newer than the cache's:

    >>> conn1.root.y = 1
    >>> transaction.commit()
    >>> conn1.root.y = 2
    >>> transaction.commit()

Now, if we reopen the client storage, we'll get the wrong root:

    >>> cs = ZEO.ClientStorage.ClientStorage(addr, client='cache')
    >>> conn2 = ZODB.connection(cs)
    >>> conn2.root.x
    {}

And, we'll get a conflict error if we try to modify it:

    >>> conn2.root.x['y'] = 1
    >>> transaction.commit() # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    ConflictError: ...

But, if we abort, we'll get up to date data and we'll see the changes.

    >>> transaction.abort()
    >>> conn2.root.x
    {'x': 1}
    >>> conn2.root.x['y'] = 1
    >>> transaction.commit()
    >>> sorted(conn2.root.x.items())
    [('x', 1), ('y', 1)]

    >>> cs.close()
    >>> conn1.close()

    """

script_template = """
import sys
sys.path[:] = %(path)r

%(src)s

"""

def generate_script(name, src):
    open(name, 'w').write(script_template % dict(
        exe=sys.executable,
        path=sys.path,
        src=src,
        ))

def runzeo_logrotate_on_sigusr2():
    """
    >>> port = get_port()
    >>> open('c', 'w').write('''
    ... <zeo>
    ...    address %s
    ... </zeo>
    ... <mappingstorage>
    ... </mappingstorage>
    ... <eventlog>
    ...    <logfile>
    ...       path l
    ...    </logfile>
    ... </eventlog>
    ... ''' % port)
    >>> generate_script('s', '''
    ... import ZEO.runzeo
    ... ZEO.runzeo.main()
    ... ''')
    >>> import subprocess, signal
    >>> p = subprocess.Popen([sys.executable, 's', '-Cc'], close_fds=True)
    >>> forker.wait_until('started',
    ...   lambda : os.path.exists('l') and ('listening on' in open('l').read())
    ...   )

    >>> oldlog = open('l').read()
    >>> os.rename('l', 'o')
    >>> os.kill(p.pid, signal.SIGUSR2)

    >>> forker.wait_until('new file', lambda : os.path.exists('l'))
    >>> s = ClientStorage(('', port))
    >>> s.close()
    >>> forker.wait_until('See logging',
    ...                   lambda : ('Log files ' in open('l').read()))
    >>> open('o').read() == oldlog # No new data in old log
    True

    # Cleanup:

    >>> os.kill(p.pid, signal.SIGKILL)
    >>> _ = p.wait()
    """

if sys.platform.startswith('win'):
    del runzeo_logrotate_on_sigusr2

def quick_close_doesnt_kill_server():
    r"""

    Start a server:

    >>> addr, _ = start_server()

    Now connect and immediately disconnect. This caused the server to
    die in the past:

    >>> import socket, struct
    >>> for i in range(5):
    ...     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ...     s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
    ...                  struct.pack('ii', 1, 0))
    ...     s.connect(addr)
    ...     s.close()

    Now we should be able to connect as normal:

    >>> db = ZEO.DB(addr)
    >>> db.storage.is_connected()
    True

    >>> db.close()

    """
def sync_connect_doesnt_hang():
    r"""
    >>> import threading
    >>> import ZEO.zrpc.client
    >>> ConnectThread = ZEO.zrpc.client.ConnectThread
    >>> ZEO.zrpc.client.ConnectThread = lambda *a, **kw: threading.Thread()

    >>> class CM(ZEO.zrpc.client.ConnectionManager):
    ...     sync_wait = 1
    ...     _start_asyncore_loop = lambda self: None
    >>> cm = CM(('', 0), object())

    Calling connect results in an exception being raised, instead of hanging
    indefinitely when the thread dies without setting up the connection.

    >>> cm.connect(sync=1)
    Traceback (most recent call last):
    ...
    AssertionError

    >>> cm.thread.isAlive()
    False
    >>> ZEO.zrpc.client.ConnectThread = ConnectThread

    """

def lp143344_extension_methods_not_lost_on_server_restart():
    r"""
Make sure we don't lose exension methods on server restart.

    >>> addr, adminaddr = start_server(keep=True)
    >>> conn = ZEO.connection(addr)
    >>> conn.root.x = 1
    >>> transaction.commit()
    >>> conn.db().storage.answer_to_the_ultimate_question()
    42

    >>> stop_server(adminaddr)
    >>> forker.wait_until('not connected',
    ...            lambda : not conn.db().storage.is_connected())
    >>> _ = start_server(addr=addr)
    >>> forker.wait_until('connected', conn.db().storage.is_connected)

    >>> conn.root.x
    1
    >>> conn.db().storage.answer_to_the_ultimate_question()
    42

    >>> conn.close()
    """

slow_test_classes = [
    BlobAdaptedFileStorageTests, BlobWritableCacheTests,
    DemoStorageTests, FileStorageTests, MappingStorageTests,
    ]

quick_test_classes = [
    FileStorageRecoveryTests, ConfigurationTests, HeartbeatTests,
    ZRPCConnectionTests,
    ]

class ServerManagingClientStorage(ClientStorage):

    class StorageServerStubClass(ZEO.ServerStub.StorageServer):

        # Wait for abort for the benefit of blob_transaction.txt
        def tpc_abort(self, id):
            self.rpc.call('tpc_abort', id)

    def __init__(self, name, blob_dir, shared=False, extrafsoptions=''):
        if shared:
            server_blob_dir = blob_dir
        else:
            server_blob_dir = 'server-'+blob_dir
        self.globs = {}
        port = forker.get_port2(self)
        addr, admin, pid, config = forker.start_zeo_server(
            """
            <blobstorage>
                blob-dir %s
                <filestorage>
                   path %s
                   %s
                </filestorage>
            </blobstorage>
            """ % (server_blob_dir, name+'.fs', extrafsoptions),
            port=port,
            )
        os.remove(config)
        zope.testing.setupstack.register(self, os.waitpid, pid, 0)
        zope.testing.setupstack.register(
            self, forker.shutdown_zeo_server, admin)
        if shared:
            ClientStorage.__init__(self, addr, blob_dir=blob_dir,
                                   shared_blob_dir=True)
        else:
            ClientStorage.__init__(self, addr, blob_dir=blob_dir)

    def close(self):
        ClientStorage.close(self)
        zope.testing.setupstack.tearDown(self)

def create_storage_shared(name, blob_dir):
    return ServerManagingClientStorage(name, blob_dir, True)

class ServerManagingClientStorageForIExternalGCTest(
    ServerManagingClientStorage):

    def pack(self, t=None, referencesf=None):
        ServerManagingClientStorage.pack(self, t, referencesf, wait=True)
        # Packing doesn't clear old versions out of zeo client caches,
        # so we'll clear the caches.
        self._cache.clear()
        ZEO.ClientStorage._check_blob_cache_size(self.blob_dir, 0)

def test_suite():
    suite = unittest.TestSuite()

    # Collect misc tests into their own layer to educe size of
    # unit test layer
    zeo = unittest.TestSuite()
    zeo.addTest(unittest.makeSuite(ZODB.tests.util.AAAA_Test_Runner_Hack))
    zeo.addTest(doctest.DocTestSuite(
        setUp=forker.setUp, tearDown=zope.testing.setupstack.tearDown))
    zeo.addTest(doctest.DocTestSuite(ZEO.tests.IterationTests,
        setUp=forker.setUp, tearDown=zope.testing.setupstack.tearDown))
    zeo.addTest(doctest.DocFileSuite('registerDB.test'))
    zeo.addTest(
        doctest.DocFileSuite(
            'zeo-fan-out.test', 'zdoptions.test',
            'drop_cache_rather_than_verify.txt', 'client-config.test',
            'protocols.test', 'zeo_blob_cache.test', 'invalidation-age.txt',
            setUp=forker.setUp, tearDown=zope.testing.setupstack.tearDown,
            ),
        )
    zeo.addTest(PackableStorage.IExternalGC_suite(
        lambda :
        ServerManagingClientStorageForIExternalGCTest(
            'data.fs', 'blobs', extrafsoptions='pack-gc false')
        ))
    for klass in quick_test_classes:
        zeo.addTest(unittest.makeSuite(klass, "check"))
    zeo.layer = ZODB.tests.util.MininalTestLayer('testZeo-misc')
    suite.addTest(zeo)

    # Put the heavyweights in their own layers
    for klass in slow_test_classes:
        sub = unittest.makeSuite(klass, "check")
        sub.layer = ZODB.tests.util.MininalTestLayer(klass.__name__)
        suite.addTest(sub)

    suite.addTest(ZODB.tests.testblob.storage_reusable_suite(
        'ClientStorageNonSharedBlobs', ServerManagingClientStorage))
    suite.addTest(ZODB.tests.testblob.storage_reusable_suite(
        'ClientStorageSharedBlobs', create_storage_shared))

    return suite


if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
