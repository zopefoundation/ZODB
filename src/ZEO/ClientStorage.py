##############################################################################
#
# Copyright (c) 2001, 2002, 2003 Zope Corporation and Contributors.
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
import logging

from zope.interface import implements
from ZEO import ServerStub
from ZEO.cache import ClientCache
from ZEO.TransactionBuffer import TransactionBuffer
from ZEO.Exceptions import ClientStorageError, ClientDisconnected, AuthError
from ZEO.auth import get_module
from ZEO.zrpc.client import ConnectionManager

from ZODB import POSException
from ZODB import utils
from ZODB.loglevels import BLATHER
from ZODB.Blobs.interfaces import IBlobStorage
from ZODB.Blobs.Blob import FilesystemHelper
from persistent.TimeStamp import TimeStamp

logger = logging.getLogger('ZEO.ClientStorage')
_pid = str(os.getpid())

def log2(msg, level=logging.INFO, subsys=_pid, exc_info=False):
    message = "(%s) %s" % (subsys, msg)
    logger.log(level, message, exc_info=exc_info)

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
    t = TimeStamp(*time.gmtime(t)[:5] + (t % 60,))
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

class ClientStorage(object):

    """A Storage class that is a network client to a remote storage.

    This is a faithful implementation of the Storage API.

    This class is thread-safe; transactions are serialized in
    tpc_begin().
    """

    implements(IBlobStorage)
    # Classes we instantiate.  A subclass might override.

    TransactionBufferClass = TransactionBuffer
    ClientCacheClass = ClientCache
    ConnectionManagerClass = ConnectionManager
    StorageServerStubClass = ServerStub.StorageServer

    def __init__(self, addr, storage='1', cache_size=20 * MB,
                 name='', client=None, debug=0, var=None,
                 min_disconnect_poll=5, max_disconnect_poll=300,
                 wait_for_server_on_startup=None, # deprecated alias for wait
                 wait=None, wait_timeout=None,
                 read_only=0, read_only_fallback=0,
                 username='', password='', realm=None,
                 blob_dir=None, blob_cache_writable=False):
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
            See ClientCache for more info.

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

        wait_timeout -- Maximum time to wait for a connection before
            giving up.  Only meaningful if wait is True.

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

        realm -- not documented.

        blob_dir -- directory path for blob data.  'blob data' is data that
            is retrieved via the loadBlob API.

        blob_cache_writable -- Flag whether the blob_dir is a writable shared
        filesystem that should be used instead of transferring blob data over
        zrpc.

        Note that the authentication protocol is defined by the server
        and is detected by the ClientStorage upon connecting (see
        testConnection() and doAuth() for details).
        """

        log2("%s (pid=%d) created %s/%s for storage: %r" %
             (self.__class__.__name__,
              os.getpid(),
              read_only and "RO" or "RW",
              read_only_fallback and "fallback" or "normal",
              storage))

        if debug:
            log2("ClientStorage(): debug argument is no longer used")

        # wait defaults to True, but wait_for_server_on_startup overrides
        # if not None
        if wait_for_server_on_startup is not None:
            if wait is not None and wait != wait_for_server_on_startup:
                log2("ClientStorage(): conflicting values for wait and "
                     "wait_for_server_on_startup; wait prevails",
                     level=logging.WARNING)
            else:
                log2("ClientStorage(): wait_for_server_on_startup "
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

        # Flag tracking disconnections in the middle of a transaction.  This
        # is reset in tpc_begin() and set in notifyDisconnected().
        self._midtxn_disconnect = 0

        # _server_addr is used by sortKey()
        self._server_addr = None
        self._tfile = None
        self._pickler = None

        self._info = {'length': 0, 'size': 0, 'name': 'ZEO Client',
                      'supportsUndo':0, 'supportsVersions': 0}

        self._tbuf = self.TransactionBufferClass()
        self._db = None
        self._ltid = None # the last committed transaction

        # _serials: stores (oid, serialno) as returned by server
        # _seriald: _check_serials() moves from _serials to _seriald,
        #           which maps oid to serialno

        # TODO:  If serial number matches transaction id, then there is
        # no need to have all this extra infrastructure for handling
        # serial numbers.  The vote call can just return the tid.
        # If there is a conflict error, we can't have a special method
        # called just to propagate the error.
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

        # load() and tpc_finish() must be serialized to guarantee
        # that cache modifications from each occur atomically.
        # It also prevents multiple load calls occuring simultaneously,
        # which simplifies the cache logic.
        self._load_lock = threading.Lock()
        # _load_oid and _load_status are protected by _lock
        self._load_oid = None
        self._load_status = None

        # Can't read data in one thread while writing data
        # (tpc_finish) in another thread.  In general, the lock
        # must prevent access to the cache while _update_cache
        # is executing.
        self._lock = threading.Lock()

        # XXX need to check for POSIX-ness here
        self.blob_dir = blob_dir
        self.blob_cache_writable = blob_cache_writable
        if blob_dir is not None:
            self.fshelper = FilesystemHelper(blob_dir)
            self.fshelper.create()
            self.fshelper.checkSecure()
        else:
            self.fshelper = None

        # Initialize locks
        self.blob_status_lock = threading.Lock()
        self.blob_status = {}

        # Decide whether to use non-temporary files
        if client is not None:
            dir = var or os.getcwd()
            cache_path = os.path.join(dir, "%s-%s.zec" % (client, storage))
        else:
            cache_path = None
        self._cache = self.ClientCacheClass(cache_path, size=cache_size)
        # TODO:  maybe there's a better time to open the cache?  Unclear.
        self._cache.open()

        self._rpc_mgr = self.ConnectionManagerClass(addr, self,
                                                    tmin=min_disconnect_poll,
                                                    tmax=max_disconnect_poll)

        if wait:
            self._wait(wait_timeout)
        else:
            # attempt_connect() will make an attempt that doesn't block
            # "too long," for a very vague notion of too long.  If that
            # doesn't succeed, call connect() to start a thread.
            if not self._rpc_mgr.attempt_connect():
                self._rpc_mgr.connect()

    def _wait(self, timeout=None):
        if timeout is not None:
            deadline = time.time() + timeout
            log2("Setting deadline to %f" % deadline, level=BLATHER)
        else:
            deadline = None
        # Wait for a connection to be established.
        self._rpc_mgr.connect(sync=1)
        # When a synchronous connect() call returns, there is
        # a valid _connection object but cache validation may
        # still be going on.  This code must wait until validation
        # finishes, but if the connection isn't a zrpc async
        # connection it also needs to poll for input.
        assert self._connection.is_async()
        while 1:
            self._ready.wait(30)
            if self._ready.isSet():
                break
            if timeout and time.time() > deadline:
                log2("Timed out waiting for connection",
                     level=logging.WARNING)
                break
            log2("Waiting for cache verification to finish")

    def close(self):
        """Storage API: finalize the storage, releasing external resources."""
        self._tbuf.close()
        if self._cache is not None:
            self._cache.close()
            self._cache = None
        if self._rpc_mgr is not None:
            self._rpc_mgr.close()
            self._rpc_mgr = None

    def registerDB(self, db):
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
        # The separate async thread should keep us up to date
        pass

    def doAuth(self, protocol, stub):
        if not (self._username and self._password):
            raise AuthError("empty username or password")

        module = get_module(protocol)
        if not module:
            log2("%s: no such an auth protocol: %s" %
                 (self.__class__.__name__, protocol), level=logging.WARNING)
            return

        storage_class, client, db_class = module

        if not client:
            log2("%s: %s isn't a valid protocol, must have a Client class" %
                 (self.__class__.__name__, protocol), level=logging.WARNING)
            raise AuthError("invalid protocol")

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
        log2("Testing connection %r" % conn)
        # TODO:  Should we check the protocol version here?
        self._conn_is_read_only = 0
        stub = self.StorageServerStubClass(conn)

        auth = stub.getAuthProtocol()
        log2("Server authentication protocol %r" % auth)
        if auth:
            skey = self.doAuth(auth, stub)
            if skey:
                log2("Client authentication successful")
                conn.setSessionKey(skey)
            else:
                log2("Authentication failed")
                raise AuthError("Authentication failed")

        try:
            stub.register(str(self._storage), self._is_read_only)
            return 1
        except POSException.ReadOnlyError:
            if not self._read_only_fallback:
                raise
            log2("Got ReadOnlyError; trying again with read_only=1")
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

        # invalidate our db cache
        if self._db is not None:
            self._db.invalidateCache()

        # TODO:  report whether we get a read-only connection.
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
            log2("Reconnected to storage: %s" % self._server_addr)
        else:
            log2("Connected to storage: %s" % self._server_addr)

        stub = self.StorageServerStubClass(conn)
        self._oids = []
        self.verify_cache(stub)

        # It's important to call get_info after calling verify_cache.
        # If we end up doing a full-verification, we need to wait till
        # it's done.  By doing a synchonous call, we are guarenteed
        # that the verification will be done because operations are
        # handled in order.        
        self._info.update(stub.get_info())

        assert conn.is_async()

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
                log2("Error resolving host: %s (%s)" % (host, err),
                     level=BLATHER)
                canonical = host
            self._server_addr = str((canonical, addr[1]))

    def sortKey(self):
        # If the client isn't connected to anything, it can't have a
        # valid sortKey().  Raise an error to stop the transaction early.
        if self._server_addr is None:
            raise ClientDisconnected
        else:
            return '%s:%s' % (self._storage, self._server_addr)

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
                log2("No verification necessary (last_inval_tid up-to-date)")
                self._server = server
                self._ready.set()
                return "no verification"

            # log some hints about last transaction
            log2("last inval tid: %r %s\n"
                 % (last_inval_tid, tid2time(last_inval_tid)))
            log2("last transaction: %r %s" %
                 (ltid, ltid and tid2time(ltid)))

            pair = server.getInvalidations(last_inval_tid)
            if pair is not None:
                log2("Recovering %d invalidations" % len(pair[1]))
                self.invalidateTransaction(*pair)
                self._server = server
                self._ready.set()
                return "quick verification"

        log2("Verifying cache")
        # setup tempfile to hold zeoVerify results
        self._tfile = tempfile.TemporaryFile(suffix=".inv")
        self._pickler = cPickle.Pickler(self._tfile, 1)
        self._pickler.fast = 1 # Don't use the memo

        # TODO:  should batch these operations for efficiency; would need
        # to acquire lock ...
        for oid, tid, version in self._cache.contents():
            server.verify(oid, version, tid)
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
        log2("Disconnected from storage: %s" % repr(self._server_addr))
        self._connection = None
        self._ready.clear()
        self._server = disconnected_stub
        self._midtxn_disconnect = 1

    def __len__(self):
        """Return the size of the storage."""
        # TODO:  Is this method used?
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

    def abortVersion(self, version, txn):
        """Storage API: clear any changes made by the given version."""
        self._check_trans(txn)
        tid, oids = self._server.abortVersion(version, id(txn))
        # When a version aborts, invalidate the version and
        # non-version data.  The non-version data should still be
        # valid, but older versions of ZODB will change the
        # non-version serialno on an abort version.  With those
        # versions of ZODB, you'd get a conflict error if you tried to
        # commit a transaction with the cached data.

        # If we could guarantee that ZODB gave the right answer,
        # we could just invalidate the version data.
        for oid in oids:
            self._tbuf.invalidate(oid, '')
        return tid, oids

    def commitVersion(self, source, destination, txn):
        """Storage API: commit the source version in the destination."""
        self._check_trans(txn)
        tid, oids = self._server.commitVersion(source, destination, id(txn))
        if destination:
            # just invalidate our version data
            for oid in oids:
                self._tbuf.invalidate(oid, source)
        else:
            # destination is "", so invalidate version and non-version
            for oid in oids:
                self._tbuf.invalidate(oid, "")
        return tid, oids

    def history(self, oid, version, length=1):
        """Storage API: return a sequence of HistoryEntry objects.

        This does not support the optional filter argument defined by
        the Storage API.
        """
        return self._server.history(oid, version, length)

    def record_iternext(self, next=None):
        """Storage API: get the next database record.

        This is part of the conversion-support API.
        """
        return self._server.record_iternext(next)

    def getTid(self, oid):
        """Storage API: return current serial number for oid."""
        return self._server.getTid(oid)

    def loadSerial(self, oid, serial):
        """Storage API: load a historical revision of an object."""
        return self._server.loadSerial(oid, serial)

    def load(self, oid, version):
        """Storage API: return the data for a given object.

        This returns the pickle data and serial number for the object
        specified by the given object id and version, if they exist;
        otherwise a KeyError is raised.
        """
        return self.loadEx(oid, version)[:2]

    def loadEx(self, oid, version):
        self._lock.acquire()    # for atomic processing of invalidations
        try:
            t = self._cache.load(oid, version)
            if t:
                return t
        finally:
            self._lock.release()

        if self._server is None:
            raise ClientDisconnected()

        self._load_lock.acquire()
        try:
            self._lock.acquire()
            try:
                self._load_oid = oid
                self._load_status = 1
            finally:
                self._lock.release()

            data, tid, ver = self._server.loadEx(oid, version)

            self._lock.acquire()    # for atomic processing of invalidations
            try:
                if self._load_status:
                    self._cache.store(oid, ver, tid, None, data)
                self._load_oid = None
            finally:
                self._lock.release()
        finally:
            self._load_lock.release()

        return data, tid, ver

    def loadBefore(self, oid, tid):
        self._lock.acquire()
        try:
            t = self._cache.loadBefore(oid, tid)
            if t is not None:
                return t
        finally:
            self._lock.release()

        t = self._server.loadBefore(oid, tid)
        if t is None:
            return None
        data, start, end = t
        if end is None:
            # This method should not be used to get current data.  It
            # doesn't use the _load_lock, so it is possble to overlap
            # this load with an invalidation for the same object.

            # If we call again, we're guaranteed to get the
            # post-invalidation data.  But if the data is still
            # current, we'll still get end == None.

            # Maybe the best thing to do is to re-run the test with
            # the load lock in the case.  That's slow performance, but
            # I don't think real application code will ever care about
            # it.

            return data, start, end
        self._lock.acquire()
        try:
            self._cache.store(oid, "", start, end, data)
        finally:
            self._lock.release()

        return data, start, end

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
        # TODO: Is it okay that read-only connections allow pack()?
        # rf argument ignored; server will provide its own implementation
        if t is None:
            t = time.time()
        t = t - (days * 86400)
        return self._server.pack(t, wait)

    def _check_serials(self):
        """Internal helper to move data from _serials to _seriald."""
        # serials are always going to be the same, the only
        # question is whether an exception has been raised.
        if self._serials:
            l = len(self._serials)
            r = self._serials[:l]
            del self._serials[:l]
            for oid, s in r:
                if isinstance(s, Exception):
                    raise s
                self._seriald[oid] = s
            return r

    def store(self, oid, serial, data, version, txn):
        """Storage API: store data for an object."""
        self._check_trans(txn)
        self._server.storea(oid, serial, data, version, id(txn))
        self._tbuf.store(oid, version, data)
        return self._check_serials()

    def storeBlob(self, oid, serial, data, blobfilename, version, txn):
        """Storage API: store a blob object."""
        serials = self.store(oid, serial, data, version, txn)
        if self.blob_cache_writable:
            self._storeBlob_shared(oid, serial, data, blobfilename, version, txn)
        else:
            self._storeBlob_copy(oid, serial, data, blobfilename, version, txn)
        return serials

    def _storeBlob_shared(self, oid, serial, data, filename, version, txn):
        # First, move the blob into the blob directory
        dir = self.fshelper.getPathForOID(oid)
        if not os.path.exists(dir):
            os.mkdir(dir)
        fd, target = self.fshelper.blob_mkstemp(oid, serial)
        os.close(fd)
        os.rename(filename, target)
        # Now tell the server where we put it
        self._server.storeBlobShared(oid, serial, data,
                                     os.path.basename(target), version, id(txn))

    def _storeBlob_copy(self, oid, serial, data, blobfilename, version, txn):
        """Version of storeBlob() that copies the data over the ZEO protocol."""
        blobfile = open(blobfilename, "rb")
        while True:
            chunk = blobfile.read(1<<16)
            # even if the blobfile is completely empty, we need to call
            # storeBlob at least once in order to be able to call
            # storeBlobEnd successfully.
            self._server.storeBlob(oid, serial, chunk, version, id(txn))
            if not chunk:
                self._server.storeBlobEnd(oid, serial, data, version, id(txn))
                break
        blobfile.close()
        os.unlink(blobfilename)

    def _do_load_blob(self, oid, serial, version):
        """Do the actual loading from the RPC server."""
        blob_filename = self.fshelper.getBlobFilename(oid, serial)
        if self._server is None:
            raise ClientDisconnected()

        targetpath = self.fshelper.getPathForOID(oid)
        if not os.path.exists(targetpath):
            os.makedirs(targetpath, 0700)

        # We write to a temporary file first, so we do not accidentally 
        # allow half-baked copies of this blob be loaded
        tempfd, tempfilename = self.fshelper.blob_mkstemp(oid, serial)
        tempfile = os.fdopen(tempfd, 'wb')

        offset = 0
        while True:
            chunk = self._server.loadBlob(oid, serial, version, offset)
            if not chunk:
                break
            offset += len(chunk)
            tempfile.write(chunk)

        tempfile.close()
        # XXX will fail on Windows if file is open
        os.rename(tempfilename, blob_filename)
        return blob_filename

    def loadBlob(self, oid, serial, version):
        """Loading a blob has to know about loading the same blob
           from another thread as the same time.

            1. Check if the blob is downloaded already
            2. Check whether it is currently beeing downloaded
            2a. Wait for other download to finish, return 
            3. If not beeing downloaded, start download
        """
        if self.fshelper is None:
            raise POSException.Unsupported("No blob cache directory is "
                                           "configured.")

        blob_filename = self.fshelper.getBlobFilename(oid, serial)
        # Case 1: Blob is available already, just use it
        if os.path.exists(blob_filename):
            log2("Found blob %s/%s in cache." % (utils.oid_repr(oid),
                utils.tid_repr(serial)), level=BLATHER)
            return blob_filename

        # Case 2,3: Blob might still be downloading or not there yet

        # Try to get or create a lock for the downloading of this blob, 
        # identified by it's oid and serial
        lock_key = (oid, serial)
        
        # We need to make the check for an existing lock and the possible
        # creation of a new one atomic, so there is another lock:
        self.blob_status_lock.acquire()
        try:
            if not self.blob_status.has_key(oid):
                self.blob_status[lock_key] = self.getBlobLock()
            lock = self.blob_status[lock_key]
        finally:
            self.blob_status_lock.release()

        # We acquire the lock to either start downloading, or wait
        # for another download to finish
        lock.acquire()
        try:
            # If there was another download that is finished by now,
            # we just take the result.
            if os.path.exists(blob_filename):
                log2("Found blob %s/%s in cache after it was downloaded "
                     "from another thread." % (utils.oid_repr(oid),
                     utils.tid_repr(serial)), level=BLATHER)
                return blob_filename

            # Otherwise we download and use that
            return self._do_load_blob(oid, serial, version)
        finally:
            # When done we remove the download lock ...
            lock.release()

            # And the status information isn't needed as well,
            # but we have to use the second lock here as well, to avoid
            # making the creation of this status lock non-atomic (see above)
            self.blob_status_lock.acquire()
            try:
                del self.blob_status[lock_key]
            finally:
                self.blob_status_lock.release()

    def getBlobLock(self):
        # indirection to support unit testing
        return threading.Lock()

    def tpc_vote(self, txn):
        """Storage API: vote on a transaction."""
        if txn is not self._transaction:
            return
        self._server.vote(id(txn))
        return self._check_serials()

    def tpc_begin(self, txn, tid=None, status=' '):
        """Storage API: begin a transaction."""
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self._tpc_cond.acquire()
        self._midtxn_disconnect = 0
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

        try:
            self._server.tpc_begin(id(txn), txn.user, txn.description,
                                   txn._extension, tid, status)
        except:
            # Client may have disconnected during the tpc_begin().
            if self._server is not disconnected_stub:
                self.end_transaction()
            raise

        self._tbuf.clear()
        self._seriald.clear()
        del self._serials[:]

    def end_transaction(self):
        """Internal helper to end a transaction."""
        # the right way to set self._transaction to None
        # calls notify() on _tpc_cond in case there are waiting threads
        self._tpc_cond.acquire()
        self._transaction = None
        self._tpc_cond.notify()
        self._tpc_cond.release()

    def lastTransaction(self):
        return self._cache.getLastTid()

    def tpc_abort(self, txn):
        """Storage API: abort a transaction."""
        if txn is not self._transaction:
            return
        try:
            # Caution:  Are there any exceptions that should prevent an
            # abort from occurring?  It seems wrong to swallow them
            # all, yet you want to be sure that other abort logic is
            # executed regardless.
            try:
                self._server.tpc_abort(id(txn))
            except ClientDisconnected:
                log2("ClientDisconnected in tpc_abort() ignored",
                     level=BLATHER)
        finally:
            self._tbuf.clear()
            self._seriald.clear()
            del self._serials[:]
            self.end_transaction()

    def tpc_finish(self, txn, f=None):
        """Storage API: finish a transaction."""
        if txn is not self._transaction:
            return
        self._load_lock.acquire()
        try:
            if self._midtxn_disconnect:
                raise ClientDisconnected(
                       'Calling tpc_finish() on a disconnected transaction')

            # The calls to tpc_finish() and _update_cache() should
            # never run currently with another thread, because the
            # tpc_cond condition variable prevents more than one
            # thread from calling tpc_finish() at a time.
            tid = self._server.tpc_finish(id(txn))
            self._lock.acquire()  # for atomic processing of invalidations
            try:
                self._update_cache(tid)
                if f is not None:
                    f(tid)
            finally:
                self._lock.release()

            r = self._check_serials()
            assert r is None or len(r) == 0, "unhandled serialnos: %s" % r
        finally:
            self._load_lock.release()
            self.end_transaction()

    def _update_cache(self, tid):
        """Internal helper to handle objects modified by a transaction.

        This iterates over the objects in the transaction buffer and
        update or invalidate the cache.
        """
        # Must be called with _lock already acquired.

        # Not sure why _update_cache() would be called on a closed storage.
        if self._cache is None:
            return

        for oid, version, data in self._tbuf:
            self._cache.invalidate(oid, version, tid)
            # If data is None, we just invalidate.
            if data is not None:
                s = self._seriald[oid]
                if s != ResolvedSerial:
                    assert s == tid, (s, tid)
                    self._cache.store(oid, version, s, None, data)
        self._tbuf.clear()

    def undo(self, trans_id, txn):
        """Storage API: undo a transaction.

        This is executed in a transactional context.  It has no effect
        until the transaction is committed.  It can be undone itself.

        Zope uses this to implement undo unless it is not supported by
        a storage.
        """
        self._check_trans(txn)
        tid, oids = self._server.undo(trans_id, id(txn))
        for oid in oids:
            self._tbuf.invalidate(oid, '')
        return tid, oids

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
            # This should never happen.  TODO:  assert it doesn't, or log
            # if it does.
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
            for oid, version, tid in invs:
                if oid == self._load_oid:
                    self._load_status = 0
                self._cache.invalidate(oid, version, tid)
                oids = versions.get((version, tid))
                if not oids:
                    versions[(version, tid)] = [oid]
                else:
                    oids.append(oid)

            if self._db is not None:
                for (version, tid), d in versions.items():
                    self._db.invalidate(tid, d, version=version)
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

        log2("endVerify finishing")
        self._server = self._pending_server
        self._ready.set()
        self._pending_conn = None
        log2("endVerify finished")

    def invalidateTransaction(self, tid, args):
        """Invalidate objects modified by tid."""
        self._lock.acquire()
        try:
            self._cache.setLastTid(tid)
        finally:
            self._lock.release()
        if self._pickler is not None:
            log2("Transactional invalidation during cache verification",
                 level=BLATHER)
            for t in args:
                self._pickler.dump(t)
            return
        self._process_invalidations([(oid, version, tid)
                                     for oid, version in args])

    # The following are for compatibility with protocol version 2.0.0

    def invalidateTrans(self, args):
        return self.invalidateTransaction(None, args)

    invalidate = invalidateVerify
    end = endVerify
    Invalidate = invalidateTrans

def InvalidationLogIterator(fileobj):
    unpickler = cPickle.Unpickler(fileobj)
    while 1:
        oid, version = unpickler.load()
        if oid is None:
            break
        yield oid, version, None
