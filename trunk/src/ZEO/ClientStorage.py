##############################################################################
#
# Copyright (c) 2001, 2002, 2003 Zope Corporation and Contributors.
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
"""The ClientStorage class and the exceptions that it may raise.

Public contents of this module:

ClientStorage -- the main class, implementing the Storage API
"""

import cPickle
import os
import socket
import tempfile
import threading
import time
import types

from ZEO import ClientCache, ServerStub
from ZEO.TransactionBuffer import TransactionBuffer
from ZEO.Exceptions import ClientStorageError, UnrecognizedResult, \
     ClientDisconnected, AuthError
from ZEO.auth import get_module
from ZEO.zrpc.client import ConnectionManager

from ZODB import POSException
from ZODB.TimeStamp import TimeStamp
from zLOG import LOG, PROBLEM, INFO, BLATHER, ERROR

def log2(type, msg, subsys="ZCS:%d" % os.getpid()):
    LOG(subsys, type, msg)

try:
    from ZODB.ConflictResolution import ResolvedSerial
except ImportError:
    ResolvedSerial = 'rs'

def tid2time(tid):
    return str(TimeStamp(tid))

def get_timestamp(prev_ts=None):
    """Internal helper to return a unique TimeStamp instance.

    If the optional argument is not None, it must be a TimeStamp; the
    return value is then guaranteed to be at least 1 microsecond later
    the argument.
    """
    t = time.time()
    t = apply(TimeStamp, (time.gmtime(t)[:5] + (t % 60,)))
    if prev_ts is not None:
        t = t.laterThan(prev_ts)
    return t

class DisconnectedServerStub:
    """Internal helper class used as a faux RPC stub when disconnected.

    This raises ClientDisconnected on all attribute accesses.

    This is a singleton class -- there should be only one instance,
    the global disconnected_stub, os it can be tested by identity.
    """

    def __getattr__(self, attr):
        raise ClientDisconnected()

# Singleton instance of DisconnectedServerStub
disconnected_stub = DisconnectedServerStub()

MB = 1024**2

class ClientStorage:

    """A Storage class that is a network client to a remote storage.

    This is a faithful implementation of the Storage API.

    This class is thread-safe; transactions are serialized in
    tpc_begin().
    """

    # Classes we instantiate.  A subclass might override.

    TransactionBufferClass = TransactionBuffer
    ClientCacheClass = ClientCache.ClientCache
    ConnectionManagerClass = ConnectionManager
    StorageServerStubClass = ServerStub.StorageServer

    def __init__(self, addr, storage='1', cache_size=20 * MB,
                 name='', client=None, debug=0, var=None,
                 min_disconnect_poll=5, max_disconnect_poll=300,
                 wait_for_server_on_startup=None, # deprecated alias for wait
                 wait=None, # defaults to 1
                 read_only=0, read_only_fallback=0,
                 username='', password='', realm=None):
        """ClientStorage constructor.

        This is typically invoked from a custom_zodb.py file.

        All arguments except addr should be keyword arguments.
        Arguments:

        addr -- The server address(es).  This is either a list of
            addresses or a single address.  Each address can be a
            (hostname, port) tuple to signify a TCP/IP connection or
            a pathname string to signify a Unix domain socket
            connection.  A hostname may be a DNS name or a dotted IP
            address.  Required.

        storage -- The storage name, defaulting to '1'.  The name must
            match one of the storage names supported by the server(s)
            specified by the addr argument.  The storage name is
            displayed in the Zope control panel.

        cache_size -- The disk cache size, defaulting to 20 megabytes.
            This is passed to the ClientCache constructor.

        name -- The storage name, defaulting to ''.  If this is false,
            str(addr) is used as the storage name.

        client -- A name used to construct persistent cache filenames.
            Defaults to None, in which case the cache is not persistent.

        debug -- Ignored.  This is present only for backwards
            compatibility with ZEO 1.

        var -- When client is not None, this specifies the directory
            where the persistent cache files are created.  It defaults
            to None, in whichcase the current directory is used.

        min_disconnect_poll -- The minimum delay in seconds between
            attempts to connect to the server, in seconds.  Defaults
            to 5 seconds.

        max_disconnect_poll -- The maximum delay in seconds between
            attempts to connect to the server, in seconds.  Defaults
            to 300 seconds.

        wait_for_server_on_startup -- A backwards compatible alias for
            the wait argument.

        wait -- A flag indicating whether to wait until a connection
            with a server is made, defaulting to true.

        read_only -- A flag indicating whether this should be a
            read-only storage, defaulting to false (i.e. writing is
            allowed by default).

        read_only_fallback -- A flag indicating whether a read-only
            remote storage should be acceptable as a fallback when no
            writable storages are available.  Defaults to false.  At
            most one of read_only and read_only_fallback should be
            true.

        username -- string with username to be used when authenticating.
            These only need to be provided if you are connecting to an
            authenticated server storage.
 
        password -- string with plaintext password to be used
            when authenticated.

        Note that the authentication protocol is defined by the server
        and is detected by the ClientStorage upon connecting (see
        testConnection() and doAuth() for details).
        """

        log2(INFO, "%s (pid=%d) created %s/%s for storage: %r" %
             (self.__class__.__name__,
              os.getpid(),
              read_only and "RO" or "RW",
              read_only_fallback and "fallback" or "normal",
              storage))

        if debug:
            log2(INFO, "ClientStorage(): debug argument is no longer used")

        # wait defaults to True, but wait_for_server_on_startup overrides
        # if not None
        if wait_for_server_on_startup is not None:
            if wait is not None and wait != wait_for_server_on_startup:
                log2(PROBLEM,
                     "ClientStorage(): conflicting values for wait and "
                     "wait_for_server_on_startup; wait prevails")
            else:
                log2(INFO,
                     "ClientStorage(): wait_for_server_on_startup "
                     "is deprecated; please use wait instead")
                wait = wait_for_server_on_startup
        elif wait is None:
            wait = 1

        self._addr = addr # For tests

        # A ZEO client can run in disconnected mode, using data from
        # its cache, or in connected mode.  Several instance variables
        # are related to whether the client is connected.

        # _server: All method calls are invoked through the server
        #    stub.  When not connect, set to disconnected_stub an
        #    object that raises ClientDisconnected errors.

        # _ready: A threading Event that is set only if _server
        #    is set to a real stub.

        # _connection: The current zrpc connection or None.

        # _connection is set as soon as a connection is established,
        # but _server is set only after cache verification has finished
        # and clients can safely use the server.  _pending_server holds
        # a server stub while it is being verified.
        
        self._server = disconnected_stub
        self._connection = None
        self._pending_server = None
        self._ready = threading.Event()

        # _is_read_only stores the constructor argument
        self._is_read_only = read_only
        # _conn_is_read_only stores the status of the current connection
        self._conn_is_read_only = 0
        self._storage = storage
        self._read_only_fallback = read_only_fallback
        self._username = username
        self._password = password
        self._realm = realm
        # _server_addr is used by sortKey()
        self._server_addr = None
        self._tfile = None
        self._pickler = None

        self._info = {'length': 0, 'size': 0, 'name': 'ZEO Client',
                      'supportsUndo':0, 'supportsVersions': 0,
                      'supportsTransactionalUndo': 0}

        self._tbuf = self.TransactionBufferClass()
        self._db = None

        # _serials: stores (oid, serialno) as returned by server
        # _seriald: _check_serials() moves from _serials to _seriald,
        #           which maps oid to serialno
        self._serials = []
        self._seriald = {}

        self.__name__ = name or str(addr) # Standard convention for storages

        # A ClientStorage only allows one thread to commit at a time.
        # Mutual exclusion is achieved using _tpc_cond, which
        # protects _transaction.  A thread that wants to assign to
        # self._transaction must acquire _tpc_cond first.  A thread
        # that decides it's done with a transaction (whether via success
        # or failure) must set _transaction to None and do
        # _tpc_cond.notify() before releasing _tpc_cond.
        self._tpc_cond = threading.Condition()
        self._transaction = None

        # Prevent multiple new_oid calls from going out.  The _oids
        # variable should only be modified while holding the
        # _oid_lock.
        self._oid_lock = threading.Lock()
        self._oids = [] # Object ids retrieved from new_oids()

        # Can't read data in one thread while writing data
        # (tpc_finish) in another thread.  In general, the lock
        # must prevent access to the cache while _update_cache
        # is executing.
        self._lock = threading.Lock()

        t = self._ts = get_timestamp()
        self._serial = `t`
        self._oid = '\0\0\0\0\0\0\0\0'

        # Decide whether to use non-temporary files
        client = client
        self._cache = self.ClientCacheClass(storage, cache_size,
                                            client=client, var=var)

        self._rpc_mgr = self.ConnectionManagerClass(addr, self,
                                                    tmin=min_disconnect_poll,
                                                    tmax=max_disconnect_poll)

        if wait:
            self._wait()
        else:
            # attempt_connect() will make an attempt that doesn't block
            # "too long," for a very vague notion of too long.  If that
            # doesn't succeed, call connect() to start a thread.
            if not self._rpc_mgr.attempt_connect():
                self._rpc_mgr.connect()
            # If the connect hasn't occurred, run with cached data.
            if not self._ready.isSet():
                self._cache.open()

    def _wait(self):
        # Wait for a connection to be established.
        self._rpc_mgr.connect(sync=1)
        # When a synchronous connect() call returns, there is
        # a valid _connection object but cache validation may
        # still be going on.  This code must wait until validation
        # finishes, but if the connection isn't a zrpc async
        # connection it also needs to poll for input.
        if self._connection.is_async():
            while 1:
                self._ready.wait(30)
                if self._ready.isSet():
                    break
                log2(INFO, "Wait for cache verification to finish")
        else:
            self._wait_sync()

    def _wait_sync(self):
        # If there is no mainloop running, this code needs
        # to call poll() to cause asyncore to handle events.
        while 1:
            if self._ready.isSet():
                break
            log2(INFO, "Wait for cache verification to finish")
            if self._connection is None:
                # If the connection was closed while we were
                # waiting for it to become ready, start over.
                return self._wait()
            else:
                self._connection.pending(30)

    def close(self):
        """Storage API: finalize the storage, releasing external resources."""
        self._tbuf.close()
        if self._cache is not None:
            self._cache.close()
            self._cache = None
        if self._rpc_mgr is not None:
            self._rpc_mgr.close()
            self._rpc_mgr = None

    def registerDB(self, db, limit):
        """Storage API: register a database for invalidation messages.

        This is called by ZODB.DB (and by some tests).

        The storage isn't really ready to use until after this call.
        """
        self._db = db

    def is_connected(self):
        """Return whether the storage is currently connected to a server."""
        # This function is used by clients, so we only report that a
        # connection exists when the connection is ready to use.
        return self._ready.isSet()

    def sync(self):
        """Handle any pending invalidation messages.

        This is called by the sync method in ZODB.Connection.
        """
        # If there is no connection, return immediately.  Technically,
        # there are no pending invalidations so they are all handled.
        # There doesn't seem to be much benefit to raising an exception.
        
        cn = self._connection
        if cn is not None:
            cn.pending()

    def doAuth(self, protocol, stub):
        if not (self._username and self._password):
            raise AuthError, "empty username or password"

        module = get_module(protocol)
        if not module:
            log2(PROBLEM, "%s: no such an auth protocol: %s" %
                 (self.__class__.__name__, protocol))
            return

        storage_class, client, db_class = module

        if not client:
            log2(PROBLEM,
                 "%s: %s isn't a valid protocol, must have a Client class" %
                 (self.__class__.__name__, protocol))
            raise AuthError, "invalid protocol"
        
        c = client(stub)
        
        # Initiate authentication, returns boolean specifying whether OK
        return c.start(self._username, self._realm, self._password)
        
    def testConnection(self, conn):
        """Internal: test the given connection.

        This returns:   1 if the connection is an optimal match,
                        0 if it is a suboptimal but acceptable match.
        It can also raise DisconnectedError or ReadOnlyError.

        This is called by ZEO.zrpc.ConnectionManager to decide which
        connection to use in case there are multiple, and some are
        read-only and others are read-write.

        This works by calling register() on the server.  In read-only
        mode, register() is called with the read_only flag set.  In
        writable mode and in read-only fallback mode, register() is
        called with the read_only flag cleared.  In read-only fallback
        mode only, if the register() call raises ReadOnlyError, it is
        retried with the read-only flag set, and if this succeeds,
        this is deemed a suboptimal match.  In all other cases, a
        succeeding register() call is deemed an optimal match, and any
        exception raised by register() is passed through.
        """
        log2(INFO, "Testing connection %r" % conn)
        # XXX Check the protocol version here?
        self._conn_is_read_only = 0
        stub = self.StorageServerStubClass(conn)

        auth = stub.getAuthProtocol()
        log2(INFO, "Server authentication protocol %r" % auth)
        if auth:
            if self.doAuth(auth, stub):
                log2(INFO, "Client authentication successful")
            else:
                log2(ERROR, "Authentication failed")
                raise AuthError, "Authentication failed"
        
        try:
            stub.register(str(self._storage), self._is_read_only)
            return 1
        except POSException.ReadOnlyError:
            if not self._read_only_fallback:
                raise
            log2(INFO, "Got ReadOnlyError; trying again with read_only=1")
            stub.register(str(self._storage), read_only=1)
            self._conn_is_read_only = 1
            return 0

    def notifyConnected(self, conn):
        """Internal: start using the given connection.

        This is called by ConnectionManager after it has decided which
        connection should be used.
        """
        if self._cache is None:
            # the storage was closed, but the connect thread called
            # this method before it was stopped.
            return

        # XXX would like to report whether we get a read-only connection
        if self._connection is not None:
            reconnect = 1
        else:
            reconnect = 0
        self.set_server_addr(conn.get_addr())

        # If we are upgrading from a read-only fallback connection,
        # we must close the old connection to prevent it from being
        # used while the cache is verified against the new connection.
        if self._connection is not None:
            self._connection.close()
        self._connection = conn

        if reconnect:
            log2(INFO, "Reconnected to storage: %s" % self._server_addr)
        else:
            log2(INFO, "Connected to storage: %s" % self._server_addr)

        stub = self.StorageServerStubClass(conn)
        self._oids = []
        self._info.update(stub.get_info())
        self.verify_cache(stub)
        if not conn.is_async():
            log2(INFO, "Waiting for cache verification to finish")
            self._wait_sync()
        self._handle_extensions()

    def _handle_extensions(self):
        for name in self.getExtensionMethods().keys():
            if not hasattr(self, name):
                setattr(self, name, self._server.extensionMethod(name))

    def set_server_addr(self, addr):
        # Normalize server address and convert to string
        if isinstance(addr, types.StringType):
            self._server_addr = addr
        else:
            assert isinstance(addr, types.TupleType)
            # If the server is on a remote host, we need to guarantee
            # that all clients used the same name for the server.  If
            # they don't, the sortKey() may be different for each client.
            # The best solution seems to be the official name reported
            # by gethostbyaddr().
            host = addr[0]
            try:
                canonical, aliases, addrs = socket.gethostbyaddr(host)
            except socket.error, err:
                log2(BLATHER, "Error resolving host: %s (%s)" % (host, err))
                canonical = host
            self._server_addr = str((canonical, addr[1]))

    def sortKey(self):
        # If the client isn't connected to anything, it can't have a
        # valid sortKey().  Raise an error to stop the transaction early.
        if self._server_addr is None:
            raise ClientDisconnected
        else:
            return self._server_addr

    def verify_cache(self, server):
        """Internal routine called to verify the cache.

        The return value (indicating which path we took) is used by
        the test suite.
        """

        # If verify_cache() finishes the cache verification process,
        # it should set self._server.  If it goes through full cache
        # verification, then endVerify() should self._server.
        
        last_inval_tid = self._cache.getLastTid()
        if last_inval_tid is not None:
            ltid = server.lastTransaction()
            if ltid == last_inval_tid:
                log2(INFO, "No verification necessary "
                     "(last_inval_tid up-to-date)")
                self._cache.open()
                self._server = server
                self._ready.set()
                return "no verification"

            # log some hints about last transaction
            log2(INFO, "last inval tid: %r %s\n"
                 % (last_inval_tid, tid2time(last_inval_tid)))
            log2(INFO, "last transaction: %r %s" %
                 (ltid, ltid and tid2time(ltid)))

            pair = server.getInvalidations(last_inval_tid)
            if pair is not None:
                log2(INFO, "Recovering %d invalidations" % len(pair[1]))
                self._cache.open()
                self.invalidateTransaction(*pair)
                self._server = server
                self._ready.set()
                return "quick verification"
            
        log2(INFO, "Verifying cache")
        # setup tempfile to hold zeoVerify results
        self._tfile = tempfile.TemporaryFile(suffix=".inv")
        self._pickler = cPickle.Pickler(self._tfile, 1)
        self._pickler.fast = 1 # Don't use the memo

        self._cache.verify(server.zeoVerify)
        self._pending_server = server
        server.endZeoVerify()
        return "full verification"

    ### Is there a race condition between notifyConnected and
    ### notifyDisconnected? In Particular, what if we get
    ### notifyDisconnected in the middle of notifyConnected?
    ### The danger is that we'll proceed as if we were connected
    ### without worrying if we were, but this would happen any way if
    ### notifyDisconnected had to get the instance lock.  There's
    ### nothing to gain by getting the instance lock.

    def notifyDisconnected(self):
        """Internal: notify that the server connection was terminated.

        This is called by ConnectionManager when the connection is
        closed or when certain problems with the connection occur.
        """
        log2(PROBLEM, "Disconnected from storage: %s"
             % repr(self._server_addr))
        self._connection = None
        self._ready.clear()
        self._server = disconnected_stub

    def __len__(self):
        """Return the size of the storage."""
        # XXX Where is this used?
        return self._info['length']

    def getName(self):
        """Storage API: return the storage name as a string.

        The return value consists of two parts: the name as determined
        by the name and addr argments to the ClientStorage
        constructor, and the string 'connected' or 'disconnected' in
        parentheses indicating whether the storage is (currently)
        connected.
        """
        return "%s (%s)" % (
            self.__name__,
            self.is_connected() and "connected" or "disconnected")

    def getSize(self):
        """Storage API: an approximate size of the database, in bytes."""
        return self._info['size']

    def getExtensionMethods(self):
        """getExtensionMethods

        This returns a dictionary whose keys are names of extra methods
        provided by this storage. Storage proxies (such as ZEO) should
        call this method to determine the extra methods that they need
        to proxy in addition to the standard storage methods.
        Dictionary values should be None; this will be a handy place
        for extra marshalling information, should we need it
        """
        return self._info.get('extensionMethods', {})

    def supportsUndo(self):
        """Storage API: return whether we support undo."""
        return self._info['supportsUndo']

    def supportsVersions(self):
        """Storage API: return whether we support versions."""
        return self._info['supportsVersions']

    def supportsTransactionalUndo(self):
        """Storage API: return whether we support transactional undo."""
        return self._info['supportsTransactionalUndo']

    def isReadOnly(self):
        """Storage API: return whether we are in read-only mode."""
        if self._is_read_only:
            return 1
        else:
            # If the client is configured for a read-write connection
            # but has a read-only fallback connection, _conn_is_read_only
            # will be True.
            return self._conn_is_read_only

    def _check_trans(self, trans):
        """Internal helper to check a transaction argument for sanity."""
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        if self._transaction is not trans:
            raise POSException.StorageTransactionError(self._transaction,
                                                       trans)

    def abortVersion(self, version, transaction):
        """Storage API: clear any changes made by the given version."""
        self._check_trans(transaction)
        oids = self._server.abortVersion(version, self._serial)
        # When a version aborts, invalidate the version and
        # non-version data.  The non-version data should still be
        # valid, but older versions of ZODB will change the
        # non-version serialno on an abort version.  With those
        # versions of ZODB, you'd get a conflict error if you tried to
        # commit a transaction with the cached data.

        # XXX If we could guarantee that ZODB gave the right answer,
        # we could just invalidate the version data.
        for oid in oids:
            self._tbuf.invalidate(oid, '')
        return oids

    def commitVersion(self, source, destination, transaction):
        """Storage API: commit the source version in the destination."""
        self._check_trans(transaction)
        oids = self._server.commitVersion(source, destination, self._serial)
        if destination:
            # just invalidate our version data
            for oid in oids:
                self._tbuf.invalidate(oid, source)
        else:
            # destination is '', so invalidate version and non-version
            for oid in oids:
                self._tbuf.invalidate(oid, destination)
        return oids

    def history(self, oid, version, length=1):
        """Storage API: return a sequence of HistoryEntry objects.

        This does not support the optional filter argument defined by
        the Storage API.
        """
        return self._server.history(oid, version, length)

    def getSerial(self, oid):
        """Storage API: return current serial number for oid."""
        return self._server.getSerial(oid)

    def loadSerial(self, oid, serial):
        """Storage API: load a historical revision of an object."""
        return self._server.loadSerial(oid, serial)

    def load(self, oid, version):
        """Storage API: return the data for a given object.

        This returns the pickle data and serial number for the object
        specified by the given object id and version, if they exist;
        otherwise a KeyError is raised.
        """
        self._lock.acquire()    # for atomic processing of invalidations
        try:
            p = self._cache.load(oid, version)
            if p:
                return p
        finally:
            self._lock.release()
            
        if self._server is None:
            raise ClientDisconnected()
        
        # If an invalidation for oid comes in during zeoLoad, that's OK
        # because we'll get oid's new state.
        p, s, v, pv, sv = self._server.zeoLoad(oid)
        self._cache.checkSize(0)
        self._cache.store(oid, p, s, v, pv, sv)
        if v and version and v == version:
            return pv, sv
        else:
            if s:
                return p, s
            raise KeyError, oid # no non-version data for this

    def modifiedInVersion(self, oid):
        """Storage API: return the version, if any, that modfied an object.

        If no version modified the object, return an empty string.
        """
        self._lock.acquire()
        try:
            v = self._cache.modifiedInVersion(oid)
            if v is not None:
                return v
        finally:
            self._lock.release()
        return self._server.modifiedInVersion(oid)

    def new_oid(self):
        """Storage API: return a new object identifier."""
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        # avoid multiple oid requests to server at the same time
        self._oid_lock.acquire()
        try:
            if not self._oids:
                self._oids = self._server.new_oids()
                self._oids.reverse()
            return self._oids.pop()
        finally:
            self._oid_lock.release()

    def pack(self, t=None, referencesf=None, wait=1, days=0):
        """Storage API: pack the storage.

        Deviations from the Storage API: the referencesf argument is
        ignored; two additional optional arguments wait and days are
        provided:

        wait -- a flag indicating whether to wait for the pack to
            complete; defaults to true.

        days -- a number of days to subtract from the pack time;
            defaults to zero.
        """
        # XXX Is it okay that read-only connections allow pack()?
        # rf argument ignored; server will provide it's own implementation
        if t is None:
            t = time.time()
        t = t - (days * 86400)
        return self._server.pack(t, wait)

    def _check_serials(self):
        """Internal helper to move data from _serials to _seriald."""
        if self._serials:
            l = len(self._serials)
            r = self._serials[:l]
            del self._serials[:l]
            for oid, s in r:
                if isinstance(s, Exception):
                    raise s
                self._seriald[oid] = s
            return r

    def store(self, oid, serial, data, version, transaction):
        """Storage API: store data for an object."""
        self._check_trans(transaction)
        self._server.storea(oid, serial, data, version, self._serial)
        self._tbuf.store(oid, version, data)
        return self._check_serials()

    def tpc_vote(self, transaction):
        """Storage API: vote on a transaction."""
        if transaction is not self._transaction:
            return
        self._server.vote(self._serial)
        return self._check_serials()

    def tpc_begin(self, txn, tid=None, status=' '):
        """Storage API: begin a transaction."""
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self._tpc_cond.acquire()
        while self._transaction is not None:
            # It is allowable for a client to call two tpc_begins in a
            # row with the same transaction, and the second of these
            # must be ignored.
            if self._transaction == txn:
                self._tpc_cond.release()
                return
            self._tpc_cond.wait(30)
        self._transaction = txn
        self._tpc_cond.release()

        if tid is None:
            self._ts = get_timestamp(self._ts)
            id = `self._ts`
        else:
            self._ts = TimeStamp(tid)
            id = tid

        try:
            self._server.tpc_begin(id, txn.user, txn.description,
                                   txn._extension, tid, status)
        except:
            # Client may have disconnected during the tpc_begin().
            if self._server is not disconnected_stub:
                self.end_transaction()
            raise

        self._serial = id
        self._tbuf.clear()
        self._seriald.clear()
        del self._serials[:]

    def end_transaction(self):
        """Internal helper to end a transaction."""
        # the right way to set self._transaction to None
        # calls notify() on _tpc_cond in case there are waiting threads
        self._ltid = self._serial
        self._tpc_cond.acquire()
        self._transaction = None
        self._tpc_cond.notify()
        self._tpc_cond.release()

    def lastTransaction(self):
        return self._ltid

    def tpc_abort(self, transaction):
        """Storage API: abort a transaction."""
        if transaction is not self._transaction:
            return
        try:
            # XXX Are there any transactions that should prevent an
            # abort from occurring?  It seems wrong to swallow them
            # all, yet you want to be sure that other abort logic is
            # executed regardless.
            try:
                self._server.tpc_abort(self._serial)
            except ClientDisconnected:
                log2(BLATHER, 'ClientDisconnected in tpc_abort() ignored')
        finally:
            self._tbuf.clear()
            self._seriald.clear()
            del self._serials[:]
            self.end_transaction()

    def tpc_finish(self, transaction, f=None):
        """Storage API: finish a transaction."""
        if transaction is not self._transaction:
            return
        try:
            self._lock.acquire()  # for atomic processing of invalidations
            try:
                self._update_cache()
            finally:
                self._lock.release()
                
            if f is not None:
                f()

            tid = self._server.tpc_finish(self._serial)
            self._cache.setLastTid(tid)

            r = self._check_serials()
            assert r is None or len(r) == 0, "unhandled serialnos: %s" % r
        finally:
            self.end_transaction()

    def _update_cache(self):
        """Internal helper to handle objects modified by a transaction.

        This iterates over the objects in the transaction buffer and
        update or invalidate the cache.
        """
        # Must be called with _lock already acquired.
        
        self._cache.checkSize(self._tbuf.get_size())
        try:
            self._tbuf.begin_iterate()
        except ValueError, msg:
            raise ClientStorageError, (
                "Unexpected error reading temporary file in "
                "client storage: %s" % msg)
        while 1:
            try:
                t = self._tbuf.next()
            except ValueError, msg:
                raise ClientStorageError, (
                    "Unexpected error reading temporary file in "
                    "client storage: %s" % msg)
            if t is None:
                break
            oid, v, p = t
            if p is None: # an invalidation
                s = None
            else:
                s = self._seriald[oid]
            if s == ResolvedSerial or s is None:
                self._cache.invalidate(oid, v)
            else:
                self._cache.update(oid, s, v, p)
        self._tbuf.clear()

    def transactionalUndo(self, trans_id, trans):
        """Storage API: undo a transaction.

        This is executed in a transactional context.  It has no effect
        until the transaction is committed.  It can be undone itself.

        Zope uses this to implement undo unless it is not supported by
        a storage.
        """
        self._check_trans(trans)
        oids = self._server.transactionalUndo(trans_id, self._serial)
        for oid in oids:
            self._tbuf.invalidate(oid, '')
        return oids

    def undo(self, transaction_id):
        """Storage API: undo a transaction, writing directly to the storage."""
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        oids = self._server.undo(transaction_id)
        self._lock.acquire()
        try:
            for oid in oids:
                self._cache.invalidate(oid, '')
        finally:
            self._lock.release()
        return oids

    def undoInfo(self, first=0, last=-20, specification=None):
        """Storage API: return undo information."""
        return self._server.undoInfo(first, last, specification)

    def undoLog(self, first=0, last=-20, filter=None):
        """Storage API: return a sequence of TransactionDescription objects.

        The filter argument should be None or left unspecified, since
        it is impossible to pass the filter function to the server to
        be executed there.  If filter is not None, an empty sequence
        is returned.
        """
        if filter is not None:
            return []
        return self._server.undoLog(first, last)

    def versionEmpty(self, version):
        """Storage API: return whether the version has no transactions."""
        return self._server.versionEmpty(version)

    def versions(self, max=None):
        """Storage API: return a sequence of versions in the storage."""
        return self._server.versions(max)

    # Below are methods invoked by the StorageServer

    def serialnos(self, args):
        """Server callback to pass a list of changed (oid, serial) pairs."""
        self._serials.extend(args)

    def info(self, dict):
        """Server callback to update the info dictionary."""
        self._info.update(dict)

    def invalidateVerify(self, args):
        """Server callback to invalidate an (oid, version) pair.

        This is called as part of cache validation.
        """
        # Invalidation as result of verify_cache().
        # Queue an invalidate for the end the verification procedure.
        if self._pickler is None:
            # XXX This should never happen
            return
        self._pickler.dump(args)

    def _process_invalidations(self, invs):
        # Invalidations are sent by the ZEO server as a sequence of
        # oid, version pairs.  The DB's invalidate() method expects a
        # dictionary of oids.
        
        self._lock.acquire()
        try:
            # versions maps version names to dictionary of invalidations
            versions = {}
            for oid, version in invs:
                d = versions.setdefault(version, {})
                self._cache.invalidate(oid, version=version)
                d[oid] = 1
            if self._db is not None:
                for v, d in versions.items():
                    self._db.invalidate(d, version=v)
        finally:
            self._lock.release()

    def endVerify(self):
        """Server callback to signal end of cache validation."""
        if self._pickler is None:
            return
        # write end-of-data marker
        self._pickler.dump((None, None))
        self._pickler = None
        self._tfile.seek(0)
        f = self._tfile
        self._tfile = None
        self._process_invalidations(InvalidationLogIterator(f))
        f.close()

        log2(INFO, "endVerify finishing")
        self._server = self._pending_server
        self._ready.set()
        self._pending_conn = None
        log2(INFO, "endVerify finished")

    def invalidateTransaction(self, tid, args):
        """Invalidate objects modified by tid."""
        self._cache.setLastTid(tid)
        if self._pickler is not None:
            self.log("Transactional invalidation during cache verification",
                     level=zLOG.BLATHER)
            for t in args:
                self.self._pickler.dump(t)
            return
        self._process_invalidations(args)

    # The following are for compatibility with protocol version 2.0.0

    def invalidateTrans(self, args):
        return self.invalidateTransaction(None, args)

    invalidate = invalidateVerify
    end = endVerify
    Invalidate = invalidateTrans

try:
    StopIteration
except NameError:
    class StopIteration(Exception):
        pass

class InvalidationLogIterator:
    """Helper class for reading invalidations in endVerify."""

    def __init__(self, fileobj):
        self._unpickler = cPickle.Unpickler(fileobj)
        self.getitem_i = 0

    def __iter__(self):
        return self

    def next(self):
        oid, version = self._unpickler.load()
        if oid is None:
            raise StopIteration
        return oid, version

    # The __getitem__() method is needed to support iteration
    # in Python 2.1.

    def __getitem__(self, i):
        assert i == self.getitem_i
        try:
            obj = self.next()
        except StopIteration:
            raise IndexError, i
        self.getitem_i += 1
        return obj
