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

import os
import sys
import time
import random
import select
import socket
import asyncore
import tempfile
import thread # XXX do we really need to catch thread.error
import threading
import time

import zLOG

from ZEO.ClientStorage import ClientStorage
from ZEO.Exceptions import Disconnected
from ZEO.zrpc.marshal import Marshaller
from ZEO.tests import forker

from ZODB.Transaction import get_transaction, Transaction
from ZODB.POSException import ReadOnlyError
from ZODB.tests.StorageTestBase import StorageTestBase
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_pickle, zodb_unpickle
from ZODB.tests.StorageTestBase import handle_all_serials, ZERO

class TestClientStorage(ClientStorage):

    def verify_cache(self, stub):
        self.end_verify = threading.Event()
        self.verify_result = ClientStorage.verify_cache(self, stub)

    def endVerify(self):
        ClientStorage.endVerify(self)
        self.end_verify.set()

class DummyDB:
    def invalidate(self, *args, **kwargs):
        pass


class CommonSetupTearDown(StorageTestBase):
    """Common boilerplate"""

    __super_setUp = StorageTestBase.setUp
    __super_tearDown = StorageTestBase.tearDown
    keep = 0
    invq = None
    timeout = None
    monitor = 0

    def setUp(self):
        """Test setup for connection tests.

        This starts only one server; a test may start more servers by
        calling self._newAddr() and then self.startServer(index=i)
        for i in 1, 2, ...
        """
        self.__super_setUp()
        zLOG.LOG("testZEO", zLOG.INFO, "setUp() %s" % self.id())
        self.file = tempfile.mktemp()
        self.addr = []
        self._pids = []
        self._servers = []
        self._newAddr()
        self.startServer()

    def tearDown(self):
        """Try to cause the tests to halt"""
        zLOG.LOG("testZEO", zLOG.INFO, "tearDown() %s" % self.id())
        if getattr(self, '_storage', None) is not None:
            self._storage.close()
            if hasattr(self._storage, 'cleanup'):
                self._storage.cleanup()
        for adminaddr in self._servers:
            if adminaddr is not None:
                forker.shutdown_zeo_server(adminaddr)
        if hasattr(os, 'waitpid'):
            # Not in Windows Python until 2.3
            for pid in self._pids:
                os.waitpid(pid, 0)
        for i in 0, 1:
            path = "c1-test-%d.zec" % i
            if os.path.exists(path):
                try:
                    os.unlink(path)
                except os.error:
                    pass
        self.__super_tearDown()

    def _newAddr(self):
        self.addr.append(self._getAddr())

    def _getAddr(self):
        # port+1 is also used, so only draw even port numbers
        return 'localhost', random.randrange(25000, 30000, 2)

    def getConfig(self):
        raise NotImplementedError

    def openClientStorage(self, cache='', cache_size=200000, wait=1,
                          read_only=0, read_only_fallback=0):
        base = TestClientStorage(self.addr,
                                 client=cache,
                                 cache_size=cache_size,
                                 wait=wait,
                                 min_disconnect_poll=0.1,
                                 read_only=read_only,
                                 read_only_fallback=read_only_fallback)
        storage = base
        storage.registerDB(DummyDB(), None)
        return storage

    def startServer(self, create=1, index=0, read_only=0, ro_svr=0):
        addr = self.addr[index]
        zLOG.LOG("testZEO", zLOG.INFO,
                 "startServer(create=%d, index=%d, read_only=%d) @ %s" %
                 (create, index, read_only, addr))
        path = "%s.%d" % (self.file, index)
        conf = self.getConfig(path, create, read_only)
        zeoport, adminaddr, pid = forker.start_zeo_server(
            conf, addr, ro_svr,
            self.monitor, self.keep, self.invq, self.timeout)
        self._pids.append(pid)
        self._servers.append(adminaddr)

    def shutdownServer(self, index=0):
        zLOG.LOG("testZEO", zLOG.INFO, "shutdownServer(index=%d) @ %s" %
                 (index, self._servers[index]))
        adminaddr = self._servers[index]
        if adminaddr is not None:
            forker.shutdown_zeo_server(adminaddr)
            self._servers[index] = None

    def pollUp(self, timeout=30.0):
        # Poll until we're connected
        now = time.time()
        giveup = now + timeout
        while not self._storage.is_connected():
            asyncore.poll(0.1)
            now = time.time()
            if now > giveup:
                self.fail("timed out waiting for storage to connect")

    def pollDown(self, timeout=30.0):
        # Poll until we're disconnected
        now = time.time()
        giveup = now + timeout
        while self._storage.is_connected():
            asyncore.poll(0.1)
            now = time.time()
            if now > giveup:
                self.fail("timed out waiting for storage to disconnect")


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

    def checkMultipleServers(self):
        # XXX crude test at first -- just start two servers and do a
        # commit at each one.

        self._newAddr()
        self._storage = self.openClientStorage('test', 100000)
        self._dostore()

        self.shutdownServer(index=0)
        self.startServer(index=1)

        # If we can still store after shutting down one of the
        # servers, we must be reconnecting to the other server.

        for i in range(10):
            try:
                self._dostore()
                break
            except Disconnected:
                self._storage.sync()
                time.sleep(0.5)

    def checkReadOnlyClient(self):
        # Open a read-only client to a read-write server; stores fail

        # Start a read-only client for a read-write server
        self._storage = self.openClientStorage(read_only=1)
        # Stores should fail here
        self.assertRaises(ReadOnlyError, self._dostore)

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

    def checkReadOnlyFallbackWritable(self):
        # Open a fallback client to a read-write server; stores succeed

        # Start a read-only-fallback client for a read-write server
        self._storage = self.openClientStorage(read_only_fallback=1)
        # Stores should succeed here
        self._dostore()

    def checkReadOnlyFallbackReadOnlyServer(self):
        # Open a fallback client to a read-only *server*; stores fail

        # We don't want the read-write server created by setUp()
        self.shutdownServer()
        self._servers = []
        # Start a read-only server
        self.startServer(create=0, index=0, ro_svr=1)
        # Start a read-only-fallback client
        self._storage = self.openClientStorage(read_only_fallback=1)
        # Stores should fail here
        self.assertRaises(ReadOnlyError, self._dostore)

    # XXX Compare checkReconnectXXX() here to checkReconnection()
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
        self.assertRaises(Disconnected, self._dostore)

        # Restart the server
        self.startServer(create=0)
        # Poll until the client connects
        self.pollUp()
        # Stores should succeed here
        self._dostore()

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

    def checkRollover(self):
        # Check that the cache works when the files are swapped.

        # In this case, only one object fits in a cache file.  When the
        # cache files swap, the first object is effectively uncached.

        self._storage = self.openClientStorage('test', 1000)
        oid1 = self._storage.new_oid()
        obj1 = MinPO("1" * 500)
        self._dostore(oid1, data=obj1)
        oid2 = self._storage.new_oid()
        obj2 = MinPO("2" * 500)
        self._dostore(oid2, data=obj2)
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
        self._dostore(oid, data=obj)
        zLOG.LOG("checkReconnection", zLOG.INFO,
                 "About to shutdown server")
        self.shutdownServer()
        zLOG.LOG("checkReconnection", zLOG.INFO,
                 "About to restart server")
        self.startServer(create=0)
        oid = self._storage.new_oid()
        obj = MinPO(12)
        while 1:
            try:
                self._dostore(oid, data=obj)
                break
            except Disconnected:
                # Maybe the exception mess is better now
##            except (Disconnected, select.error,
##                    threading.ThreadError, socket.error):
                zLOG.LOG("checkReconnection", zLOG.INFO,
                         "Error after server restart; retrying.",
                         error=sys.exc_info())
                get_transaction().abort()
                self._storage.sync()
            # XXX This is a bloody pain.  We're placing a heavy burden
            # on users to catch a plethora of exceptions in order to
            # write robust code.  Need to think about implementing
            # John Heintz's suggestion to make sure all exceptions
            # inherit from POSException.
        zLOG.LOG("checkReconnection", zLOG.INFO, "finished")

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
        except Disconnected:
            pass
        else:
            self._storage.close()
            self.fail("Server did not disconnect after bogus message")
        self._storage.close()

        self._storage = self.openClientStorage()
        self._dostore()

    # Test case for multiple storages participating in a single
    # transaction.  This is not really a connection test, but it needs
    # about the same infrastructure (several storage servers).

    # XXX WARNING: with the current ZEO code, this occasionally fails.
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

class ReconnectionTests(CommonSetupTearDown):
    keep = 1
    invq = 2

    def checkReadOnlyStorage(self):
        # Open a read-only client to a read-only *storage*; stores fail

        # We don't want the read-write server created by setUp()
        self.shutdownServer()
        self._servers = []
        # Start a read-only server
        self.startServer(create=0, index=0, read_only=1)
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
        self.startServer(create=0, index=0, read_only=1)
        # Start a read-only-fallback client
        self._storage = self.openClientStorage(read_only_fallback=1)
        # Stores should fail here
        self.assertRaises(ReadOnlyError, self._dostore)

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
        self.startServer(create=0, read_only=1)
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
        self.assertRaises(Disconnected, self._dostore)

        # Restart the server
        self.startServer(create=0, read_only=1)
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
        self.assertRaises(Disconnected, self._dostore)

        # Restart the server, this time read-write
        self.startServer(create=0)
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
        self.startServer(create=0, index=0, read_only=1)
        # Start a client in fallback mode
        self._storage = self.openClientStorage(read_only_fallback=1)
        # Stores should fail here
        self.assertRaises(ReadOnlyError, self._dostore)

        # Start a read-write server
        self.startServer(index=1, read_only=0)
        # After a while, stores should work
        for i in range(300): # Try for 30 seconds
            try:
                self._dostore()
                break
            except (Disconnected, ReadOnlyError):
                self._storage.sync()
        else:
            self.fail("Couldn't store after starting a read-write server")

    def checkNoVerificationOnServerRestart(self):
        self._storage = self.openClientStorage()
        # When we create a new storage, it should always do a full
        # verification
        self.assertEqual(self._storage.verify_result, "full verification")
        self._dostore()
        self.shutdownServer()
        self.pollDown()
        self._storage.verify_result = None
        self.startServer(create=0)
        self.pollUp()
        # There were no transactions committed, so no verification
        # should be needed.
        self.assertEqual(self._storage.verify_result, "no verification")
        
    def checkNoVerificationOnServerRestartWith2Clients(self):
        perstorage = self.openClientStorage(cache="test")
        self.assertEqual(perstorage.verify_result, "full verification")
        
        self._storage = self.openClientStorage()
        oid = self._storage.new_oid()
        # When we create a new storage, it should always do a full
        # verification
        self.assertEqual(self._storage.verify_result, "full verification")
        # do two storages of the object to make sure an invalidation
        # message is generated
        revid = self._dostore(oid)
        self._dostore(oid, revid)

        perstorage.load(oid, '')

        self.shutdownServer()

        self.pollDown()
        self._storage.verify_result = None
        perstorage.verify_result = None
        self.startServer(create=0)
        self.pollUp()
        # There were no transactions committed, so no verification
        # should be needed.
        self.assertEqual(self._storage.verify_result, "no verification")

        perstorage.close()
        self.assertEqual(perstorage.verify_result, "no verification")

    def checkQuickVerificationWith2Clients(self):
        perstorage = self.openClientStorage(cache="test")
        self.assertEqual(perstorage.verify_result, "full verification")
        
        self._storage = self.openClientStorage()
        oid = self._storage.new_oid()
        # When we create a new storage, it should always do a full
        # verification
        self.assertEqual(self._storage.verify_result, "full verification")
        # do two storages of the object to make sure an invalidation
        # message is generated
        revid = self._dostore(oid)
        revid = self._dostore(oid, revid)

        perstorage.load(oid, '')
        perstorage.close()
        
        revid = self._dostore(oid, revid)

        perstorage = self.openClientStorage(cache="test")
        self.assertEqual(perstorage.verify_result, "quick verification")

        self.assertEqual(perstorage.load(oid, ''),
                         self._storage.load(oid, ''))



    def checkVerificationWith2ClientsInvqOverflow(self):
        perstorage = self.openClientStorage(cache="test")
        self.assertEqual(perstorage.verify_result, "full verification")
        
        self._storage = self.openClientStorage()
        oid = self._storage.new_oid()
        # When we create a new storage, it should always do a full
        # verification
        self.assertEqual(self._storage.verify_result, "full verification")
        # do two storages of the object to make sure an invalidation
        # message is generated
        revid = self._dostore(oid)
        revid = self._dostore(oid, revid)

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

class TimeoutTests(CommonSetupTearDown):
    timeout = 1

    def checkTimeout(self):
        storage = self.openClientStorage()
        txn = Transaction()
        storage.tpc_begin(txn)
        storage.tpc_vote(txn)
        time.sleep(2)
        self.assertRaises(Disconnected, storage.tpc_finish, txn)

    def checkTimeoutOnAbort(self):
        storage = self.openClientStorage()
        txn = Transaction()
        storage.tpc_begin(txn)
        storage.tpc_vote(txn)
        storage.tpc_abort(txn)

    def checkTimeoutOnAbortNoLock(self):
        storage = self.openClientStorage()
        txn = Transaction()
        storage.tpc_begin(txn)
        storage.tpc_abort(txn)

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
