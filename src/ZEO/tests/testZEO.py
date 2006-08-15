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
import logging
import os
import random
import signal
import socket
import tempfile
import time
import unittest

# ZODB test support
import ZODB
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

    def setUp(self):
        logger.info("setUp() %s", self.id())
        port = get_port()
        zconf = forker.ZEOConfig(('', port))
        zport, adminaddr, pid, path = forker.start_zeo_server(self.getConfig(),
                                                              zconf, port)
        self._pids = [pid]
        self._servers = [adminaddr]
        self._conf_path = path
        self._storage = ClientStorage(zport, '1', cache_size=20000000,
                                      min_disconnect_poll=0.5, wait=1,
                                      wait_timeout=60)
        self._storage.registerDB(DummyDB(), None)

    def tearDown(self):
        self._storage.close()
        os.remove(self._conf_path)
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
        storage.registerDB(db, None)

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
        _base.registerDB(DummyDB(), None)
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

def test_suite():
    suite = unittest.TestSuite()
    for klass in test_classes:
        sub = unittest.makeSuite(klass, "check")
        suite.addTest(sub)
    return suite


if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
