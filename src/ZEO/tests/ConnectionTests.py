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

import os
import sys
import time
import random
import asyncore
import threading
import logging

import ZEO.ServerStub
from ZEO.ClientStorage import ClientStorage
from ZEO.Exceptions import ClientDisconnected
from ZEO.zrpc.marshal import Marshaller
from ZEO.tests import forker

from ZODB.DB import DB
from ZODB.POSException import ReadOnlyError, ConflictError
from ZODB.tests.StorageTestBase import StorageTestBase
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase \
     import zodb_pickle, zodb_unpickle, handle_all_serials, handle_serials
import ZODB.tests.util

import transaction
from transaction import Transaction

logger = logging.getLogger('ZEO.tests.ConnectionTests')

ZERO = '\0'*8

class TestServerStub(ZEO.ServerStub.StorageServer):
    __super_getInvalidations = ZEO.ServerStub.StorageServer.getInvalidations

    def getInvalidations(self, tid):
        # squirrel the results away for inspection by test case
        self._last_invals = self.__super_getInvalidations(tid)
        return self._last_invals

class TestClientStorage(ClientStorage):

    test_connection = False

    StorageServerStubClass = TestServerStub

    connection_count_for_tests = 0

    def notifyConnected(self, conn):
        ClientStorage.notifyConnected(self, conn)
        self.connection_count_for_tests += 1

    def verify_cache(self, stub):
        self.end_verify = threading.Event()
        self.verify_result = ClientStorage.verify_cache(self, stub)

    def endVerify(self):
        ClientStorage.endVerify(self)
        self.end_verify.set()

    def testConnection(self, conn):
        try:
            return ClientStorage.testConnection(self, conn)
        finally:
            self.test_connection = True

class DummyDB:
    def invalidate(self, *args, **kwargs):
        pass

    def invalidateCache(self):
        pass

class CommonSetupTearDown(StorageTestBase):
    """Common boilerplate"""

    __super_setUp = StorageTestBase.setUp
    __super_tearDown = StorageTestBase.tearDown
    keep = 0
    invq = None
    timeout = None
    monitor = 0
    db_class = DummyDB

    def setUp(self, before=None):
        """Test setup for connection tests.

        This starts only one server; a test may start more servers by
        calling self._newAddr() and then self.startServer(index=i)
        for i in 1, 2, ...
        """
        self.__super_setUp()
        logging.info("setUp() %s", self.id())
        self.file = 'storage_conf'
        self.addr = []
        self._pids = []
        self._servers = []
        self.conf_paths = []
        self.caches = []
        self._newAddr()
        self.startServer()

#         self._old_log_level = logging.getLogger().getEffectiveLevel()
#         logging.getLogger().setLevel(logging.WARNING)
#         self._log_handler = logging.StreamHandler()
#         logging.getLogger().addHandler(self._log_handler)

    def tearDown(self):
        """Try to cause the tests to halt"""
#         logging.getLogger().setLevel(self._old_log_level)
#         logging.getLogger().removeHandler(self._log_handler)
#         logging.info("tearDown() %s" % self.id())

        for p in self.conf_paths:
            os.remove(p)
        if getattr(self, '_storage', None) is not None:
            self._storage.close()
            if hasattr(self._storage, 'cleanup'):
                logging.debug("cleanup storage %s" %
                         self._storage.__name__)
                self._storage.cleanup()
        for adminaddr in self._servers:
            if adminaddr is not None:
                forker.shutdown_zeo_server(adminaddr)
        for pid in self._pids:
            try:
                os.waitpid(pid, 0)
            except OSError:
                pass # The subprocess module may already have waited
                    
        for c in self.caches:
            for i in 0, 1:
                for ext in "", ".trace", ".lock":
                    path = "%s-%s.zec%s" % (c, "1", ext)
                    # On Windows before 2.3, we don't have a way to wait for
                    # the spawned server(s) to close, and they inherited
                    # file descriptors for our open files.  So long as those
                    # processes are alive, we can't delete the files.  Try
                    # a few times then give up.
                    need_to_delete = False
                    if os.path.exists(path):
                        need_to_delete = True
                        for dummy in range(5):
                            try:
                                os.unlink(path)
                            except:
                                time.sleep(0.5)
                            else:
                                need_to_delete = False
                                break
                    if need_to_delete:
                        os.unlink(path)  # sometimes this is just gonna fail
        self.__super_tearDown()

    def _newAddr(self):
        self.addr.append(self._getAddr())

    def _getAddr(self):
        return 'localhost', forker.get_port(self)

    def getConfig(self, path, create, read_only):
        raise NotImplementedError

    cache_id = 1

    def openClientStorage(self, cache=None, cache_size=200000, wait=1,
                          read_only=0, read_only_fallback=0,
                          username=None, password=None, realm=None):
        if cache is None:
            cache = str(self.__class__.cache_id)
            self.__class__.cache_id += 1
        self.caches.append(cache)
        storage = TestClientStorage(self.addr,
                                    client=cache,
                                    var='.',
                                    cache_size=cache_size,
                                    wait=wait,
                                    min_disconnect_poll=0.1,
                                    read_only=read_only,
                                    read_only_fallback=read_only_fallback,
                                    username=username,
                                    password=password,
                                    realm=realm)
        storage.registerDB(DummyDB())
        return storage

    def getServerConfig(self, addr, ro_svr):
        zconf = forker.ZEOConfig(addr)
        if ro_svr:
            zconf.read_only = 1
        if self.monitor:
            zconf.monitor_address = ("", 42000)
        if self.invq:
            zconf.invalidation_queue_size = self.invq
        if self.timeout:
            zconf.transaction_timeout = self.timeout
        return zconf

    def startServer(self, create=1, index=0, read_only=0, ro_svr=0, keep=None,
                    path=None):
        addr = self.addr[index]
        logging.info("startServer(create=%d, index=%d, read_only=%d) @ %s" %
                     (create, index, read_only, addr))
        if path is None:
            path = "%s.%d" % (self.file, index)
        sconf = self.getConfig(path, create, read_only)
        zconf = self.getServerConfig(addr, ro_svr)
        if keep is None:
            keep = self.keep
        zeoport, adminaddr, pid, path = forker.start_zeo_server(
            sconf, zconf, addr[1], keep)
        self.conf_paths.append(path)
        self._pids.append(pid)
        self._servers.append(adminaddr)

    def shutdownServer(self, index=0):
        logging.info("shutdownServer(index=%d) @ %s" %
                     (index, self._servers[index]))
        adminaddr = self._servers[index]
        if adminaddr is not None:
            forker.shutdown_zeo_server(adminaddr)
            self._servers[index] = None

    def pollUp(self, timeout=30.0, storage=None):
        if storage is None:
            storage = self._storage
        # Poll until we're connected.
        now = time.time()
        giveup = now + timeout
        while not storage.is_connected():
            asyncore.poll(0.1)
            now = time.time()
            if now > giveup:
                self.fail("timed out waiting for storage to connect")
            # When the socket map is empty, poll() returns immediately,
            # and this is a pure busy-loop then.  At least on some Linux
            # flavors, that can starve the thread trying to connect,
            # leading to grossly increased runtime (typical) or bogus
            # "timed out" failures.  A little sleep here cures both.
            time.sleep(0.1)

    def pollDown(self, timeout=30.0):
        # Poll until we're disconnected.
        now = time.time()
        giveup = now + timeout
        while self._storage.is_connected():
            asyncore.poll(0.1)
            now = time.time()
            if now > giveup:
                self.fail("timed out waiting for storage to disconnect")
            # See pollUp() for why we sleep a little here.
            time.sleep(0.1)


class ConnectionTests(CommonSetupTearDown):
    """Tests that explicitly manage the server process.

    To test the cache or re-connection, these test cases explicit
    start and stop a ZEO storage server.
    """

    def checkMultipleAddresses(self):
        for i in range(4):
            self._newAddr()
        self._storage = self.openClientStorage('test', 100000)
        oid = self._storage.new_oid()
        obj = MinPO(12)
        self._dostore(oid, data=obj)
        self._storage.close()

    def checkReadOnlyClient(self):
        # Open a read-only client to a read-write server; stores fail

        # Start a read-only client for a read-write server
        self._storage = self.openClientStorage(read_only=1)
        # Stores should fail here
        self.assertRaises(ReadOnlyError, self._dostore)
        self._storage.close()

    def checkReadOnlyServer(self):
        # Open a read-only client to a read-only *server*; stores fail

        # We don't want the read-write server created by setUp()
        self.shutdownServer()
        self._servers = []
        # Start a read-only server
        self.startServer(create=0, index=0, ro_svr=1)
        # Start a read-only client
        self._storage = self.openClientStorage(read_only=1)
        # Stores should fail here
        self.assertRaises(ReadOnlyError, self._dostore)
        self._storage.close()
        # Get rid of the 'test left new threads behind' warning
        time.sleep(0.1)

    def checkReadOnlyFallbackWritable(self):
        # Open a fallback client to a read-write server; stores succeed

        # Start a read-only-fallback client for a read-write server
        self._storage = self.openClientStorage(read_only_fallback=1)
        # Stores should succeed here
        self._dostore()
        self._storage.close()

    def checkReadOnlyFallbackReadOnlyServer(self):
        # Open a fallback client to a read-only *server*; stores fail

        # We don't want the read-write server created by setUp()
        self.shutdownServer()
        self._servers = []
        # Start a read-only server
        self.startServer(create=0, index=0, ro_svr=1)
        # Start a read-only-fallback client
        self._storage = self.openClientStorage(read_only_fallback=1)
        self.assert_(self._storage.isReadOnly())
        # Stores should fail here
        self.assertRaises(ReadOnlyError, self._dostore)
        self._storage.close()


    def checkDisconnectionError(self):
        # Make sure we get a ClientDisconnected when we try to read an
        # object when we're not connected to a storage server and the
        # object is not in the cache.
        self.shutdownServer()
        self._storage = self.openClientStorage('test', 1000, wait=0)
        self.assertRaises(ClientDisconnected,
                          self._storage.load, 'fredwash', '')
        self._storage.close()

    def checkBasicPersistence(self):
        # Verify cached data persists across client storage instances.

        # To verify that the cache is being used, the test closes the
        # server and then starts a new client with the server down.
        # When the server is down, a load() gets the data from its cache.

        self._storage = self.openClientStorage('test', 100000)
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

    def checkDisconnectedCacheWorks(self):
        # Check that the cache works when the client is disconnected.
        self._storage = self.openClientStorage('test')
        oid1 = self._storage.new_oid()
        obj1 = MinPO("1" * 500)
        self._dostore(oid1, data=obj1)
        oid2 = self._storage.new_oid()
        obj2 = MinPO("2" * 500)
        self._dostore(oid2, data=obj2)
        expected1 = self._storage.load(oid1, '')
        expected2 = self._storage.load(oid2, '')

        # Shut it all down, and try loading from the persistent cache file
        # without a server present.
        self._storage.close()
        self.shutdownServer()
        self._storage = self.openClientStorage('test', wait=False)
        self.assertEqual(expected1, self._storage.load(oid1, ''))
        self.assertEqual(expected2, self._storage.load(oid2, ''))
        self._storage.close()

    def checkDisconnectedCacheFails(self):
        # Like checkDisconnectedCacheWorks above, except the cache
        # file is so small that only one object can be remembered.
        self._storage = self.openClientStorage('test', cache_size=900)
        oid1 = self._storage.new_oid()
        obj1 = MinPO("1" * 500)
        self._dostore(oid1, data=obj1)
        oid2 = self._storage.new_oid()
        obj2 = MinPO("2" * 500)
        # The cache file is so small that adding oid2 will evict oid1.
        self._dostore(oid2, data=obj2)
        expected2 = self._storage.load(oid2, '')

        # Shut it all down, and try loading from the persistent cache file
        # without a server present.
        self._storage.close()
        self.shutdownServer()
        self._storage = self.openClientStorage('test', cache_size=900,
                                               wait=False)
        # oid2 should still be in cache.
        self.assertEqual(expected2, self._storage.load(oid2, ''))
        # But oid1 should have been purged, so that trying to load it will
        # try to fetch it from the (non-existent) ZEO server.
        self.assertRaises(ClientDisconnected, self._storage.load, oid1, '')
        self._storage.close()

    def checkVerificationInvalidationPersists(self):
        # This tests a subtle invalidation bug from ZODB 3.3:
        # invalidations processed as part of ZEO cache verification acted
        # kinda OK wrt the in-memory cache structures, but had no effect
        # on the cache file.  So opening the file cache again could
        # incorrectly believe that a previously invalidated object was
        # still current.  This takes some effort to set up.

        # First, using a persistent cache ('test'), create an object
        # MinPO(13).  We used to see this again at the end of this test,
        # despite that we modify it, and despite that it gets invalidated
        # in 'test', before the end.
        self._storage = self.openClientStorage('test')
        oid = self._storage.new_oid()
        obj = MinPO(13)
        self._dostore(oid, data=obj)
        self._storage.close()

        # Now modify obj via a temp connection.  `test` won't learn about
        # this until we open a connection using `test` again.
        self._storage = self.openClientStorage()
        pickle, rev = self._storage.load(oid, '')
        newobj = zodb_unpickle(pickle)
        self.assertEqual(newobj, obj)
        newobj.value = 42 # .value *should* be 42 forever after now, not 13
        self._dostore(oid, data=newobj, revid=rev)
        self._storage.close()

        # Open 'test' again.  `oid` in this cache should be (and is)
        # invalidated during cache verification.  The bug was that it
        # got invalidated (kinda) in memory, but not in the cache file.
        self._storage = self.openClientStorage('test')

        # The invalidation happened already.  Now create and store a new
        # object before closing this storage:  this is so `test` believes
        # it's seen transactions beyond the one that invalidated `oid`, so
        # that the *next* time we open `test` it doesn't process another
        # invalidation for `oid`.  It's also important that we not try to
        # load `oid` now:  because it's been (kinda) invalidated in the
        # cache's memory structures, loading it now would fetch the
        # current revision from the server, thus hiding the bug.
        obj2 = MinPO(666)
        oid2 = self._storage.new_oid()
        self._dostore(oid2, data=obj2)
        self._storage.close()

        # Finally, open `test` again and load `oid`.  `test` believes
        # it's beyond the transaction that modified `oid`, so its view
        # of whether it has an up-to-date `oid` comes solely from the disk
        # file, unaffected by cache verification.
        self._storage = self.openClientStorage('test')
        pickle, rev = self._storage.load(oid, '')
        newobj_copy = zodb_unpickle(pickle)
        # This used to fail, with
        #     AssertionError: MinPO(13) != MinPO(42)
        # That is, `test` retained a stale revision of the object on disk.
        self.assertEqual(newobj_copy, newobj)
        self._storage.close()

    def checkBadMessage1(self):
        # not even close to a real message
        self._bad_message("salty")

    def checkBadMessage2(self):
        # just like a real message, but with an unpicklable argument
        global Hack
        class Hack:
            pass

        msg = Marshaller().encode(1, 0, "foo", (Hack(),))
        self._bad_message(msg)
        del Hack

    def _bad_message(self, msg):
        # Establish a connection, then send the server an ill-formatted
        # request.  Verify that the connection is closed and that it is
        # possible to establish a new connection.

        self._storage = self.openClientStorage()
        self._dostore()

        # break into the internals to send a bogus message
        zrpc_conn = self._storage._server.rpc
        zrpc_conn.message_output(msg)

        try:
            self._dostore()
        except ClientDisconnected:
            pass
        else:
            self._storage.close()
            self.fail("Server did not disconnect after bogus message")
        self._storage.close()

        self._storage = self.openClientStorage()
        self._dostore()
        self._storage.close()

    # Test case for multiple storages participating in a single
    # transaction.  This is not really a connection test, but it needs
    # about the same infrastructure (several storage servers).

    # TODO: with the current ZEO code, this occasionally fails.
    # That's the point of this test. :-)

    def NOcheckMultiStorageTransaction(self):
        # Configuration parameters (larger values mean more likely deadlocks)
        N = 2
        # These don't *have* to be all the same, but it's convenient this way
        self.nservers = N
        self.nthreads = N
        self.ntrans = N
        self.nobj = N

        # Start extra servers
        for i in range(1, self.nservers):
            self._newAddr()
            self.startServer(index=i)

        # Spawn threads that each do some transactions on all storages
        threads = []
        try:
            for i in range(self.nthreads):
                t = MSTThread(self, "T%d" % i)
                threads.append(t)
                t.start()
            # Wait for all threads to finish
            for t in threads:
                t.join(60)
                self.failIf(t.isAlive(), "%s didn't die" % t.getName())
        finally:
            for t in threads:
                t.closeclients()

    def checkCrossDBInvalidations(self):
        db1 = DB(self.openClientStorage())
        c1 = db1.open()
        r1 = c1.root()

        r1["a"] = MinPO("a")
        transaction.commit()
        self.assertEqual(r1._p_state, 0) # up-to-date

        db2 = DB(self.openClientStorage())
        r2 = db2.open().root()

        self.assertEqual(r2["a"].value, "a")

        r2["b"] = MinPO("b")
        transaction.commit()

        # Make sure the invalidation is received in the other client.
        # We've had problems with this timing out on "slow" and/or "very
        # busy" machines, so we increase the sleep time on each trip, and
        # are willing to wait quite a long time.
        for i in range(20):
            c1.sync()
            if r1._p_state == -1:
                break
            time.sleep(i / 10.0)
        self.assertEqual(r1._p_state, -1) # ghost

        r1.keys() # unghostify
        self.assertEqual(r1._p_serial, r2._p_serial)
        self.assertEqual(r1["b"].value, "b")

        db2.close()
        db1.close()

    def checkCheckForOutOfDateServer(self):
        # We don't want to connect a client to a server if the client
        # has seen newer transactions.
        self._storage = self.openClientStorage()
        self._dostore()
        self.shutdownServer()
        self.assertRaises(ClientDisconnected, self._storage.load, '\0'*8, '')

        self.startServer()

        # No matter how long we wait, the client won't reconnect:
        time.sleep(2)
        self.assertRaises(ClientDisconnected, self._storage.load, '\0'*8, '')

class InvqTests(CommonSetupTearDown):
    invq = 3

    def checkQuickVerificationWith2Clients(self):
        perstorage = self.openClientStorage(cache="test", cache_size=4000)
        self.assertEqual(perstorage.verify_result, "empty cache")

        self._storage = self.openClientStorage()
        oid = self._storage.new_oid()
        oid2 = self._storage.new_oid()
        # When we create a new storage, it should always do a full
        # verification
        self.assertEqual(self._storage.verify_result, "empty cache")
        # do two storages of the object to make sure an invalidation
        # message is generated
        revid = self._dostore(oid)
        revid = self._dostore(oid, revid)
        # Create a second object and revision to guarantee it doesn't
        # show up in the list of invalidations sent when perstore restarts.
        revid2 = self._dostore(oid2)
        revid2 = self._dostore(oid2, revid2)

        perstorage.load(oid, '')
        perstorage.close()

        forker.wait_until(lambda : os.path.exists('test-1.zec'))

        revid = self._dostore(oid, revid)

        perstorage = self.openClientStorage(cache="test")

        forker.wait_until(
            (lambda : perstorage.verify_result == "quick verification"),
            onfail=(lambda : None))

        self.assertEqual(perstorage.verify_result, "quick verification")
        self.assertEqual(perstorage._server._last_invals,
                         (revid, [oid]))

        self.assertEqual(perstorage.load(oid, ''),
                         self._storage.load(oid, ''))
        perstorage.close()

    def checkVerificationWith2ClientsInvqOverflow(self):
        perstorage = self.openClientStorage(cache="test")
        self.assertEqual(perstorage.verify_result, "empty cache")

        self._storage = self.openClientStorage()
        oid = self._storage.new_oid()
        # When we create a new storage, it should always do a full
        # verification
        self.assertEqual(self._storage.verify_result, "empty cache")
        # do two storages of the object to make sure an invalidation
        # message is generated
        revid = self._dostore(oid)
        revid = self._dostore(oid, revid)
        forker.wait_until(
            "Client has seen all of the transactions from the server",
            lambda :
            perstorage.lastTransaction() == self._storage.lastTransaction()
            )
        perstorage.load(oid, '')
        perstorage.close()

        # the test code sets invq bound to 2
        for i in range(5):
            revid = self._dostore(oid, revid)

        perstorage = self.openClientStorage(cache="test")
        self.assertEqual(perstorage.verify_result, "full verification")
        t = time.time() + 30
        while not perstorage.end_verify.isSet():
            perstorage.sync()
            if time.time() > t:
                self.fail("timed out waiting for endVerify")

        self.assertEqual(self._storage.load(oid, '')[1], revid)
        self.assertEqual(perstorage.load(oid, ''),
                         self._storage.load(oid, ''))

        perstorage.close()

class ReconnectionTests(CommonSetupTearDown):
    # The setUp() starts a server automatically.  In order for its
    # state to persist, we set the class variable keep to 1.  In
    # order for its state to be cleaned up, the last startServer()
    # call in the test must pass keep=0.
    keep = 1
    invq = 2

    def checkReadOnlyStorage(self):
        # Open a read-only client to a read-only *storage*; stores fail

        # We don't want the read-write server created by setUp()
        self.shutdownServer()
        self._servers = []
        # Start a read-only server
        self.startServer(create=0, index=0, read_only=1, keep=0)
        # Start a read-only client
        self._storage = self.openClientStorage(read_only=1)
        # Stores should fail here
        self.assertRaises(ReadOnlyError, self._dostore)

    def checkReadOnlyFallbackReadOnlyStorage(self):
        # Open a fallback client to a read-only *storage*; stores fail

        # We don't want the read-write server created by setUp()
        self.shutdownServer()
        self._servers = []
        # Start a read-only server
        self.startServer(create=0, index=0, read_only=1, keep=0)
        # Start a read-only-fallback client
        self._storage = self.openClientStorage(read_only_fallback=1)
        # Stores should fail here
        self.assertRaises(ReadOnlyError, self._dostore)

    # TODO:  Compare checkReconnectXXX() here to checkReconnection()
    # further down.  Is the code here hopelessly naive, or is
    # checkReconnection() overwrought?

    def checkReconnectWritable(self):
        # A read-write client reconnects to a read-write server

        # Start a client
        self._storage = self.openClientStorage()
        # Stores should succeed here
        self._dostore()

        # Shut down the server
        self.shutdownServer()
        self._servers = []
        # Poll until the client disconnects
        self.pollDown()
        # Stores should fail now
        self.assertRaises(ClientDisconnected, self._dostore)

        # Restart the server
        self.startServer(create=0)
        # Poll until the client connects
        self.pollUp()
        # Stores should succeed here
        self._dostore()
        self._storage.close()

    def checkReconnectReadOnly(self):
        # A read-only client reconnects from a read-write to a
        # read-only server

        # Start a client
        self._storage = self.openClientStorage(read_only=1)
        # Stores should fail here
        self.assertRaises(ReadOnlyError, self._dostore)

        # Shut down the server
        self.shutdownServer()
        self._servers = []
        # Poll until the client disconnects
        self.pollDown()
        # Stores should still fail
        self.assertRaises(ReadOnlyError, self._dostore)

        # Restart the server
        self.startServer(create=0, read_only=1, keep=0)
        # Poll until the client connects
        self.pollUp()
        # Stores should still fail
        self.assertRaises(ReadOnlyError, self._dostore)

    def checkReconnectFallback(self):
        # A fallback client reconnects from a read-write to a
        # read-only server

        # Start a client in fallback mode
        self._storage = self.openClientStorage(read_only_fallback=1)
        # Stores should succeed here
        self._dostore()

        # Shut down the server
        self.shutdownServer()
        self._servers = []
        # Poll until the client disconnects
        self.pollDown()
        # Stores should fail now
        self.assertRaises(ClientDisconnected, self._dostore)

        # Restart the server
        self.startServer(create=0, read_only=1, keep=0)
        # Poll until the client connects
        self.pollUp()
        # Stores should fail here
        self.assertRaises(ReadOnlyError, self._dostore)

    def checkReconnectUpgrade(self):
        # A fallback client reconnects from a read-only to a
        # read-write server

        # We don't want the read-write server created by setUp()
        self.shutdownServer()
        self._servers = []
        # Start a read-only server
        self.startServer(create=0, read_only=1)
        # Start a client in fallback mode
        self._storage = self.openClientStorage(read_only_fallback=1)
        # Stores should fail here
        self.assertRaises(ReadOnlyError, self._dostore)

        # Shut down the server
        self.shutdownServer()
        self._servers = []
        # Poll until the client disconnects
        self.pollDown()
        # Stores should fail now
        self.assertRaises(ClientDisconnected, self._dostore)

        # Restart the server, this time read-write
        self.startServer(create=0, keep=0)
        # Poll until the client sconnects
        self.pollUp()
        # Stores should now succeed
        self._dostore()

    def checkReconnectSwitch(self):
        # A fallback client initially connects to a read-only server,
        # then discovers a read-write server and switches to that

        # We don't want the read-write server created by setUp()
        self.shutdownServer()
        self._servers = []
        # Allocate a second address (for the second server)
        self._newAddr()

        # Start a read-only server
        self.startServer(create=0, index=0, read_only=1, keep=0)
        # Start a client in fallback mode
        self._storage = self.openClientStorage(read_only_fallback=1)
        # Stores should fail here
        self.assertRaises(ReadOnlyError, self._dostore)

        # Start a read-write server
        self.startServer(index=1, read_only=0, keep=0)
        # After a while, stores should work
        for i in range(300): # Try for 30 seconds
            try:
                self._dostore()
                break
            except (ClientDisconnected, ReadOnlyError):
                # If the client isn't connected at all, sync() returns
                # quickly and the test fails because it doesn't wait
                # long enough for the client.
                time.sleep(0.1)
        else:
            self.fail("Couldn't store after starting a read-write server")

    def checkNoVerificationOnServerRestart(self):
        self._storage = self.openClientStorage()
        # When we create a new storage, it should always do a full
        # verification
        self.assertEqual(self._storage.verify_result, "empty cache")
        self._dostore()
        self.shutdownServer()
        self.pollDown()
        self._storage.verify_result = None
        self.startServer(create=0, keep=0)
        self.pollUp()
        # There were no transactions committed, so no verification
        # should be needed.
        self.assertEqual(self._storage.verify_result, "no verification")

    def checkNoVerificationOnServerRestartWith2Clients(self):
        perstorage = self.openClientStorage(cache="test")
        self.assertEqual(perstorage.verify_result, "empty cache")

        self._storage = self.openClientStorage()
        oid = self._storage.new_oid()
        # When we create a new storage, it should always do a full
        # verification
        self.assertEqual(self._storage.verify_result, "empty cache")
        # do two storages of the object to make sure an invalidation
        # message is generated
        revid = self._dostore(oid)
        revid = self._dostore(oid, revid)
        forker.wait_until(
            "Client has seen all of the transactions from the server",
            lambda :
            perstorage.lastTransaction() == self._storage.lastTransaction()
            )
        perstorage.load(oid, '')
        self.shutdownServer()
        self.pollDown()
        self._storage.verify_result = None
        perstorage.verify_result = None
        logging.info('2ALLBEEF')
        self.startServer(create=0, keep=0)
        self.pollUp()
        self.pollUp(storage=perstorage)
        # There were no transactions committed, so no verification
        # should be needed.
        self.assertEqual(self._storage.verify_result, "no verification")
        self.assertEqual(perstorage.verify_result, "no verification")
        perstorage.close()
        self._storage.close()

    def checkDisconnectedAbort(self):
        self._storage = self.openClientStorage()
        self._dostore()
        oids = [self._storage.new_oid() for i in range(5)]
        txn = Transaction()
        self._storage.tpc_begin(txn)
        for oid in oids:
            data = zodb_pickle(MinPO(oid))
            self._storage.store(oid, None, data, '', txn)
        self.shutdownServer()
        self.assertRaises(ClientDisconnected, self._storage.tpc_vote, txn)
        self._storage.tpc_abort(txn)
        self.startServer(create=0)
        self._storage._wait()
        self._dostore()

        # This test is supposed to cover the following error, although
        # I don't have much confidence that it does.  The likely
        # explanation for the error is that the _tbuf contained
        # objects that weren't in the _seriald, because the client was
        # interrupted waiting for tpc_vote() to return.  When the next
        # transaction committed, it tried to do something with the
        # bogus _tbuf entries.  The explanation is wrong/incomplete,
        # because tpc_begin() should clear the _tbuf.

        # 2003-01-15T15:44:19 ERROR(200) ZODB A storage error occurred
        # in the last phase of a two-phase commit.  This shouldn't happen.

        # Traceback (innermost last):
        # Module ZODB.Transaction, line 359, in _finish_one
        # Module ZODB.Connection, line 691, in tpc_finish
        # Module ZEO.ClientStorage, line 679, in tpc_finish
        # Module ZEO.ClientStorage, line 709, in _update_cache
        # KeyError: ...


    def checkReconnection(self):
        # Check that the client reconnects when a server restarts.

        self._storage = self.openClientStorage()
        oid = self._storage.new_oid()
        obj = MinPO(12)
        self._dostore(oid, data=obj)
        logging.info("checkReconnection(): About to shutdown server")
        self.shutdownServer()
        logging.info("checkReconnection(): About to restart server")
        self.startServer(create=0)
        forker.wait_until('reconnect', self._storage.is_connected)
        oid = self._storage.new_oid()
        obj = MinPO(12)
        while 1:
            try:
                self._dostore(oid, data=obj)
                break
            except ClientDisconnected:
                # Maybe the exception mess is better now
                logging.info("checkReconnection(): Error after"
                             " server restart; retrying.", exc_info=True)
                transaction.abort()
            # Give the other thread a chance to run.
            time.sleep(0.1)
        logging.info("checkReconnection(): finished")
        self._storage.close()

    def checkMultipleServers(self):
        # Crude test-- just start two servers and do a commit at each one.

        self._newAddr()
        self._storage = self.openClientStorage('test', 100000)
        self._dostore()
        self.shutdownServer(index=0)

        # When we start the second server, we use file data file from
        # the original server so tha the new server is a replica of
        # the original.  We need this becaise ClientStorage won't use
        # a server if the server's last transaction is earlier than
        # what the client has seen.
        self.startServer(index=1, path=self.file+'.0', create=False)

        # If we can still store after shutting down one of the
        # servers, we must be reconnecting to the other server.

        did_a_store = 0
        for i in range(10):
            try:
                self._dostore()
                did_a_store = 1
                break
            except ClientDisconnected:
                time.sleep(0.5)
        self.assert_(did_a_store)
        self._storage.close()

class TimeoutTests(CommonSetupTearDown):
    timeout = 1

    def checkTimeout(self):
        storage = self.openClientStorage()
        txn = Transaction()
        storage.tpc_begin(txn)
        storage.tpc_vote(txn)
        time.sleep(2)
        self.assertRaises(ClientDisconnected, storage.tpc_finish, txn)
        storage.close()

    def checkTimeoutOnAbort(self):
        storage = self.openClientStorage()
        txn = Transaction()
        storage.tpc_begin(txn)
        storage.tpc_vote(txn)
        storage.tpc_abort(txn)
        storage.close()

    def checkTimeoutOnAbortNoLock(self):
        storage = self.openClientStorage()
        txn = Transaction()
        storage.tpc_begin(txn)
        storage.tpc_abort(txn)
        storage.close()

    def checkTimeoutAfterVote(self):
        self._storage = storage = self.openClientStorage()
        # Assert that the zeo cache is empty
        self.assert_(not list(storage._cache.contents()))
        # Create the object
        oid = storage.new_oid()
        obj = MinPO(7)
        # Now do a store, sleeping before the finish so as to cause a timeout
        t = Transaction()
        old_connection_count = storage.connection_count_for_tests
        storage.tpc_begin(t)
        revid1 = storage.store(oid, ZERO, zodb_pickle(obj), '', t)
        storage.tpc_vote(t)
        # Now sleep long enough for the storage to time out
        time.sleep(3)
        self.assert_(
            (not storage.is_connected())
            or
            (storage.connection_count_for_tests > old_connection_count)
            )
        storage._wait()
        self.assert_(storage.is_connected())
        # We expect finish to fail
        self.assertRaises(ClientDisconnected, storage.tpc_finish, t)
        # The cache should still be empty
        self.assert_(not list(storage._cache.contents()))
        # Load should fail since the object should not be in either the cache
        # or the server.
        self.assertRaises(KeyError, storage.load, oid, '')

    def checkTimeoutProvokingConflicts(self):
        self._storage = storage = self.openClientStorage()
        # Assert that the zeo cache is empty.
        self.assert_(not list(storage._cache.contents()))
        # Create the object
        oid = storage.new_oid()
        obj = MinPO(7)
        # We need to successfully commit an object now so we have something to
        # conflict about.
        t = Transaction()
        storage.tpc_begin(t)
        revid1a = storage.store(oid, ZERO, zodb_pickle(obj), '', t)
        revid1b = storage.tpc_vote(t)
        revid1 = handle_serials(oid, revid1a, revid1b)
        storage.tpc_finish(t)
        # Now do a store, sleeping before the finish so as to cause a timeout.
        obj.value = 8
        t = Transaction()
        old_connection_count = storage.connection_count_for_tests
        storage.tpc_begin(t)
        revid2a = storage.store(oid, revid1, zodb_pickle(obj), '', t)
        revid2b = storage.tpc_vote(t)
        revid2 = handle_serials(oid, revid2a, revid2b)

        # Now sleep long enough for the storage to time out.
        # This used to sleep for 3 seconds, and sometimes (but very rarely)
        # failed then.  Now we try for a minute.  It typically succeeds
        # on the second time thru the loop, and, since self.timeout is 1,
        # it's typically faster now (2/1.8 ~= 1.11 seconds sleeping instead
        # of 3).
        deadline = time.time() + 60 # wait up to a minute
        while time.time() < deadline:
            if (storage.is_connected() and
                (storage.connection_count_for_tests == old_connection_count)
                ):
                time.sleep(self.timeout / 1.8)
            else:
                break
        self.assert_(
            (not storage.is_connected())
            or
            (storage.connection_count_for_tests > old_connection_count)
            )
        storage._wait()
        self.assert_(storage.is_connected())
        # We expect finish to fail.
        self.assertRaises(ClientDisconnected, storage.tpc_finish, t)
        storage.tpc_abort(t)

        # Now we think we've committed the second transaction, but we really
        # haven't.  A third one should produce a POSKeyError on the server,
        # which manifests as a ConflictError on the client.
        obj.value = 9
        t = Transaction()
        storage.tpc_begin(t)
        storage.store(oid, revid2, zodb_pickle(obj), '', t)
        self.assertRaises(ConflictError, storage.tpc_vote, t)
        # Even aborting won't help.
        storage.tpc_abort(t)
        self.assertRaises(ZODB.POSException.StorageTransactionError,
                          storage.tpc_finish, t)
        # Try again.
        obj.value = 10
        t = Transaction()
        storage.tpc_begin(t)
        storage.store(oid, revid2, zodb_pickle(obj), '', t)
        # Even aborting won't help.
        self.assertRaises(ConflictError, storage.tpc_vote, t)
        # Abort this one and try a transaction that should succeed.
        storage.tpc_abort(t)
        
        # Now do a store.
        obj.value = 11
        t = Transaction()
        storage.tpc_begin(t)
        revid2a = storage.store(oid, revid1, zodb_pickle(obj), '', t)
        revid2b = storage.tpc_vote(t)
        revid2 = handle_serials(oid, revid2a, revid2b)
        storage.tpc_finish(t)
        # Now load the object and verify that it has a value of 11.
        data, revid = storage.load(oid, '')
        self.assertEqual(zodb_unpickle(data), MinPO(11))
        self.assertEqual(revid, revid2)

class MSTThread(threading.Thread):

    __super_init = threading.Thread.__init__

    def __init__(self, testcase, name):
        self.__super_init(name=name)
        self.testcase = testcase
        self.clients = []

    def run(self):
        tname = self.getName()
        testcase = self.testcase

        # Create client connections to each server
        clients = self.clients
        for i in range(len(testcase.addr)):
            c = testcase.openClientStorage(addr=testcase.addr[i])
            c.__name = "C%d" % i
            clients.append(c)

        for i in range(testcase.ntrans):
            # Because we want a transaction spanning all storages,
            # we can't use _dostore().  This is several _dostore() calls
            # expanded in-line (mostly).

            # Create oid->serial mappings
            for c in clients:
                c.__oids = []
                c.__serials = {}

            # Begin a transaction
            t = Transaction()
            for c in clients:
                #print "%s.%s.%s begin\n" % (tname, c.__name, i),
                c.tpc_begin(t)

            for j in range(testcase.nobj):
                for c in clients:
                    # Create and store a new object on each server
                    oid = c.new_oid()
                    c.__oids.append(oid)
                    data = MinPO("%s.%s.t%d.o%d" % (tname, c.__name, i, j))
                    #print data.value
                    data = zodb_pickle(data)
                    s = c.store(oid, ZERO, data, '', t)
                    c.__serials.update(handle_all_serials(oid, s))

            # Vote on all servers and handle serials
            for c in clients:
                #print "%s.%s.%s vote\n" % (tname, c.__name, i),
                s = c.tpc_vote(t)
                c.__serials.update(handle_all_serials(None, s))

            # Finish on all servers
            for c in clients:
                #print "%s.%s.%s finish\n" % (tname, c.__name, i),
                c.tpc_finish(t)

            for c in clients:
                # Check that we got serials for all oids
                for oid in c.__oids:
                    testcase.failUnless(c.__serials.has_key(oid))
                # Check that we got serials for no other oids
                for oid in c.__serials.keys():
                    testcase.failUnless(oid in c.__oids)

    def closeclients(self):
        # Close clients opened by run()
        for c in self.clients:
            try:
                c.close()
            except:
                pass
