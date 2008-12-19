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

from persistent.TimeStamp import TimeStamp
from ZEO.auth import get_module
from ZEO.cache import ClientCache
from ZEO.Exceptions import ClientStorageError, ClientDisconnected, AuthError
from ZEO import ServerStub
from ZEO.TransactionBuffer import TransactionBuffer
from ZEO.zrpc.client import ConnectionManager
from ZODB import POSException
from ZODB import utils
from ZODB.loglevels import BLATHER
import BTrees.IOBTree
import cPickle
import logging
import os
import re
import socket
import stat
import sys
import tempfile
import threading
import time
import types
import weakref
import zc.lockfile
import ZEO.interfaces
import ZODB
import ZODB.BaseStorage
import ZODB.interfaces
import zope.event
import zope.interface

logger = logging.getLogger(__name__)

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
    the global disconnected_stub, so it can be tested by identity.

    """

    def __getattr__(self, attr):
        raise ClientDisconnected()

# Singleton instance of DisconnectedServerStub
disconnected_stub = DisconnectedServerStub()

MB = 1024**2

class ClientStorage(object):
    """A storage class that is a network client to a remote storage.

    This is a faithful implementation of the Storage API.

    This class is thread-safe; transactions are serialized in
    tpc_begin().

    """

    # ClientStorage does not declare any interfaces here. Interfaces are
    # declared according to the server's storage once a connection is
    # established.


    # Classes we instantiate.  A subclass might override.
    TransactionBufferClass = TransactionBuffer
    ClientCacheClass = ClientCache
    ConnectionManagerClass = ConnectionManager
    StorageServerStubClass = ServerStub.stub

    def __init__(self, addr, storage='1', cache_size=20 * MB,
                 name='', client=None, var=None,
                 min_disconnect_poll=1, max_disconnect_poll=30,
                 wait_for_server_on_startup=None, # deprecated alias for wait
                 wait=None, wait_timeout=None,
                 read_only=0, read_only_fallback=0,
                 drop_cache_rather_verify=False,
                 username='', password='', realm=None,
                 blob_dir=None, shared_blob_dir=False,
                 blob_cache_size=None, blob_cache_size_check=10,
                 ):
        """ClientStorage constructor.

        This is typically invoked from a custom_zodb.py file.

        All arguments except addr should be keyword arguments.
        Arguments:

        addr
            The server address(es).  This is either a list of
            addresses or a single address.  Each address can be a
            (hostname, port) tuple to signify a TCP/IP connection or
            a pathname string to signify a Unix domain socket
            connection.  A hostname may be a DNS name or a dotted IP
            address.  Required.

        storage
            The storage name, defaulting to '1'.  The name must
            match one of the storage names supported by the server(s)
            specified by the addr argument.  The storage name is
            displayed in the Zope control panel.

        cache_size
            The disk cache size, defaulting to 20 megabytes.
            This is passed to the ClientCache constructor.

        name
            The storage name, defaulting to ''.  If this is false,
            str(addr) is used as the storage name.

        client
            A name used to construct persistent cache filenames.
            Defaults to None, in which case the cache is not persistent.
            See ClientCache for more info.

        var
            When client is not None, this specifies the directory
            where the persistent cache files are created.  It defaults
            to None, in whichcase the current directory is used.

        min_disconnect_poll
            The minimum delay in seconds between
            attempts to connect to the server, in seconds.  Defaults
            to 5 seconds.

        max_disconnect_poll
            The maximum delay in seconds between
            attempts to connect to the server, in seconds.  Defaults
            to 300 seconds.

        wait_for_server_on_startup
            A backwards compatible alias for
            the wait argument.

        wait
            A flag indicating whether to wait until a connection
            with a server is made, defaulting to true.

        wait_timeout
            Maximum time to wait for a connection before
            giving up.  Only meaningful if wait is True.

        read_only
            A flag indicating whether this should be a
            read-only storage, defaulting to false (i.e. writing is
            allowed by default).

        read_only_fallback
            A flag indicating whether a read-only
            remote storage should be acceptable as a fallback when no
            writable storages are available.  Defaults to false.  At
            most one of read_only and read_only_fallback should be
            true.

        username
            string with username to be used when authenticating.
            These only need to be provided if you are connecting to an
            authenticated server storage.

        password
            string with plaintext password to be used when authenticated.

        realm
            not documented.

        drop_cache_rather_verify
            a flag indicating that the cache should be dropped rather
            than expensively verified.

        blob_dir
            directory path for blob data.  'blob data' is data that
            is retrieved via the loadBlob API.

        shared_blob_dir
            Flag whether the blob_dir is a server-shared filesystem
            that should be used instead of transferring blob data over
            zrpc.

        blob_cache_size
            Maximum size of the ZEO blob cache, in bytes.  If not set, then
            the cache size isn't checked and the blob directory will
            grow without bound.
            
            This option is ignored if shared_blob_dir is true.

        blob_cache_size_check
            ZEO check size as percent of blob_cache_size.  The ZEO
            cache size will be checked when this many bytes have been
            loaded into the cache. Defaults to 10% of the blob cache
            size.   This option is ignored if shared_blob_dir is true.

        Note that the authentication protocol is defined by the server
        and is detected by the ClientStorage upon connecting (see
        testConnection() and doAuth() for details).

        """

        self.__name__ = name or str(addr) # Standard convention for storages
        
        logger.info(
            "%s %s (pid=%d) created %s/%s for storage: %r",
            self.__name__,
            self.__class__.__name__,
            os.getpid(),
            read_only and "RO" or "RW",
            read_only_fallback and "fallback" or "normal",
            storage,
            )

        self._drop_cache_rather_verify = drop_cache_rather_verify

        # wait defaults to True, but wait_for_server_on_startup overrides
        # if not None
        if wait_for_server_on_startup is not None:
            if wait is not None and wait != wait_for_server_on_startup:
                logger.warning(
                    "%s ClientStorage(): conflicting values for wait and "
                    "wait_for_server_on_startup; wait prevails",
                    self.__name__)
            else:
                logger.info(
                     "%s ClientStorage(): wait_for_server_on_startup "
                     "is deprecated; please use wait instead",
                    self.__name__)
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
        self._storage = storage
        self._read_only_fallback = read_only_fallback
        self._username = username
        self._password = password
        self._realm = realm

        self._iterators = weakref.WeakValueDictionary()
        self._iterator_ids = set()

        # Flag tracking disconnections in the middle of a transaction.  This
        # is reset in tpc_begin() and set in notifyDisconnected().
        self._midtxn_disconnect = 0

        # _server_addr is used by sortKey()
        self._server_addr = None

        self._pickler = self._tfile = None
        
        self._info = {'length': 0, 'size': 0, 'name': 'ZEO Client',
                      'supportsUndo': 0, 'interfaces': ()}

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
        self.shared_blob_dir = shared_blob_dir
        
        if blob_dir is not None:
            # Avoid doing this import unless we need it, as it
            # currently requires pywin32 on Windows.
            import ZODB.blob
            if shared_blob_dir:
                self.fshelper = ZODB.blob.FilesystemHelper(blob_dir)
            else:
                if 'zeocache' not in ZODB.blob.LAYOUTS:
                    ZODB.blob.LAYOUTS['zeocache'] = BlobCacheLayout()
                self.fshelper = ZODB.blob.FilesystemHelper(
                    blob_dir, layout_name='zeocache')
                self.fshelper.create()
            self.fshelper.checkSecure()
        else:
            self.fshelper = None

        if client is not None:
            dir = var or os.getcwd()
            cache_path = os.path.join(dir, "%s-%s.zec" % (client, storage))
        else:
            cache_path = None

        self._cache = self.ClientCacheClass(cache_path, size=cache_size)


        self._blob_cache_size = blob_cache_size
        self._blob_data_bytes_loaded = 0
        if blob_cache_size is not None:
            self._blob_cache_size_check = (
                blob_cache_size * blob_cache_size_check / 100)
            self._check_blob_size()

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
            logger.debug("%s Setting deadline to %f", self.__name__, deadline)
        else:
            deadline = None
        # Wait for a connection to be established.
        self._rpc_mgr.connect(sync=1)
        # When a synchronous connect() call returns, there is
        # a valid _connection object but cache validation may
        # still be going on.  This code must wait until validation
        # finishes, but if the connection isn't a zrpc async
        # connection it also needs to poll for input.
        while 1:
            self._ready.wait(30)
            if self._ready.isSet():
                break
            if timeout and time.time() > deadline:
                logger.warning("%s Timed out waiting for connection",
                               self.__name__)
                break
            logger.info("%s Waiting for cache verification to finish",
                        self.__name__)

    def close(self):
        """Storage API: finalize the storage, releasing external resources."""
        if self._rpc_mgr is not None:
            self._rpc_mgr.close()
            self._rpc_mgr = None
        if self._connection is not None:
            self._connection.register_object(None) # Don't call me!
            self._connection.close()
            self._connection = None

        self._tbuf.close()
        if self._cache is not None:
            self._cache.close()
            self._cache = None
        if self._tfile is not None:
            self._tfile.close()

        if self._check_blob_size_thread is not None:
            self._check_blob_size_thread.join()

    _check_blob_size_thread = None
    def _check_blob_size(self, bytes=None):
        if self._blob_cache_size is None:
            return
        if self.shared_blob_dir or not self.blob_dir:
            return

        if (bytes is not None) and (bytes < self._blob_cache_size_check):
            return
        
        self._blob_data_bytes_loaded = 0

        target = max(self._blob_cache_size - self._blob_cache_size_check, 0)
        
        check_blob_size_thread = threading.Thread(
            target=_check_blob_cache_size,
            args=(self.blob_dir, self._blob_cache_size),
            )
        check_blob_size_thread.setDaemon(True)
        check_blob_size_thread.start()
        self._check_blob_size_thread = check_blob_size_thread

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
            logger.error("%s %s: no such an auth protocol: %s",
                         self.__name__, self.__class__.__name__, protocol)
            return

        storage_class, client, db_class = module

        if not client:
            logger.error(
                "%s %s: %s isn't a valid protocol, must have a Client class",
                self.__name__, self.__class__.__name__, protocol)
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
        logger.info("%s Testing connection %r", self.__name__, conn)
        # TODO:  Should we check the protocol version here?
        conn._is_read_only = self._is_read_only
        stub = self.StorageServerStubClass(conn)

        auth = stub.getAuthProtocol()
        logger.info("%s Server authentication protocol %r", self.__name__, auth)
        if auth:
            skey = self.doAuth(auth, stub)
            if skey:
                logger.info("%s Client authentication successful",
                            self.__name__)
                conn.setSessionKey(skey)
            else:
                logger.info("%s Authentication failed",
                            self.__name__)
                raise AuthError("Authentication failed")

        try:
            stub.register(str(self._storage), self._is_read_only)
            return 1
        except POSException.ReadOnlyError:
            if not self._read_only_fallback:
                raise
            logger.info("%s Got ReadOnlyError; trying again with read_only=1",
                        self.__name__)
            stub.register(str(self._storage), read_only=1)
            conn._is_read_only = True
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


        if self._connection is not None:
            # If we are upgrading from a read-only fallback connection,
            # we must close the old connection to prevent it from being
            # used while the cache is verified against the new connection.
            self._connection.register_object(None) # Don't call me!
            self._connection.close()
            self._connection = None
            self._ready.clear()
            reconnect = 1
        else:
            reconnect = 0

        self.set_server_addr(conn.get_addr())
        self._connection = conn

        # invalidate our db cache
        if self._db is not None:
            self._db.invalidateCache()

        if reconnect:
            logger.info("%s Reconnected to storage: %s",
                        self.__name__, self._server_addr)
        else:
            logger.info("%s Connected to storage: %s",
                        self.__name__, self._server_addr)

        stub = self.StorageServerStubClass(conn)
        self._oids = []
        self.verify_cache(stub)

        # It's important to call get_info after calling verify_cache.
        # If we end up doing a full-verification, we need to wait till
        # it's done.  By doing a synchonous call, we are guarenteed
        # that the verification will be done because operations are
        # handled in order.        
        self._info.update(stub.get_info())

        self._handle_extensions()

        for iface in (
            ZODB.interfaces.IStorageRestoreable,
            ZODB.interfaces.IStorageIteration,
            ZODB.interfaces.IStorageUndoable,
            ZODB.interfaces.IStorageCurrentRecordIteration,
            ZODB.interfaces.IBlobStorage,
            ZODB.interfaces.IExternalGC,
            ):
            if (iface.__module__, iface.__name__) in self._info.get(
                'interfaces', ()):
                zope.interface.alsoProvides(self, iface)

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
                logger.debug("%s Error resolving host: %s (%s)",
                             self.__name__, host, err)
                canonical = host
            self._server_addr = str((canonical, addr[1]))

    def sortKey(self):
        # If the client isn't connected to anything, it can't have a
        # valid sortKey().  Raise an error to stop the transaction early.
        if self._server_addr is None:
            raise ClientDisconnected
        else:
            return '%s:%s' % (self._storage, self._server_addr)

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
        logger.info("%s Disconnected from storage: %r",
                    self.__name__, self._server_addr)
        self._connection = None
        self._ready.clear()
        self._server = disconnected_stub
        self._midtxn_disconnect = 1
        self._iterator_gc(True)

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

    def isReadOnly(self):
        """Storage API: return whether we are in read-only mode."""
        if self._is_read_only:
            return True
        else:
            # If the client is configured for a read-write connection
            # but has a read-only fallback connection, conn._is_read_only
            # will be True.  If self._connection is None, we'll behave as
            # read_only
            try:
                return self._connection._is_read_only
            except AttributeError:
                return True

    def _check_trans(self, trans):
        """Internal helper to check a transaction argument for sanity."""
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        if self._transaction is not trans:
            raise POSException.StorageTransactionError(self._transaction,
                                                       trans)

    def history(self, oid, size=1):
        """Storage API: return a sequence of HistoryEntry objects.
        """
        return self._server.history(oid, size)

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

    def load(self, oid, version=''):
        """Storage API: return the data for a given object.

        This returns the pickle data and serial number for the object
        specified by the given object id, if they exist;
        otherwise a KeyError is raised.

        """
        self._lock.acquire()    # for atomic processing of invalidations
        try:
            t = self._cache.load(oid)
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

            data, tid = self._server.loadEx(oid)

            self._lock.acquire()    # for atomic processing of invalidations
            try:
                if self._load_status:
                    self._cache.store(oid, tid, None, data)
                self._load_oid = None
            finally:
                self._lock.release()
        finally:
            self._load_lock.release()

        return data, tid

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
            self._cache.store(oid, start, end, data)
        finally:
            self._lock.release()

        return data, start, end

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
        assert not version

        self._check_trans(txn)
        self._server.storea(oid, serial, data, id(txn))
        self._tbuf.store(oid, data)
        return self._check_serials()

    def storeBlob(self, oid, serial, data, blobfilename, version, txn):
        """Storage API: store a blob object."""
        assert not version
        serials = self.store(oid, serial, data, '', txn)
        if self.shared_blob_dir:
            self._storeBlob_shared(oid, serial, data, blobfilename, txn)
        else:
            self._server.storeBlob(oid, serial, data, blobfilename, txn)
            self._tbuf.storeBlob(oid, blobfilename)
        return serials

    def _storeBlob_shared(self, oid, serial, data, filename, txn):
        # First, move the blob into the blob directory
        self.fshelper.getPathForOID(oid, create=True)
        fd, target = self.fshelper.blob_mkstemp(oid, serial)
        os.close(fd)

        if sys.platform == 'win32':
            # On windows, we can't rename to an existing file.  We'll
            # use a slightly different file name. We keep the old one
            # until we're done to avoid conflicts. Then remove the old name.
            target += 'w'
            ZODB.blob.rename_or_copy_blob(filename, target)
            os.remove(target[:-1])
        else:
            ZODB.blob.rename_or_copy_blob(filename, target)

        # Now tell the server where we put it
        self._server.storeBlobShared(
            oid, serial, data, os.path.basename(target), id(txn))

    def receiveBlobStart(self, oid, serial):
        blob_filename = self.fshelper.getBlobFilename(oid, serial)
        assert not os.path.exists(blob_filename)
        lockfilename = os.path.join(os.path.dirname(blob_filename), '.lock')
        assert os.path.exists(lockfilename)
        blob_filename += '.dl'
        assert not os.path.exists(blob_filename)
        f = open(blob_filename, 'wb')
        f.close()

    def receiveBlobChunk(self, oid, serial, chunk):
        blob_filename = self.fshelper.getBlobFilename(oid, serial)+'.dl'
        assert os.path.exists(blob_filename)
        f = open(blob_filename, 'r+b')
        f.seek(0, 2)
        f.write(chunk)
        f.close()
        self._blob_data_bytes_loaded += len(chunk)
        self._check_blob_size(self._blob_data_bytes_loaded)

    def receiveBlobStop(self, oid, serial):
        blob_filename = self.fshelper.getBlobFilename(oid, serial)
        os.rename(blob_filename+'.dl', blob_filename)
        os.chmod(blob_filename, stat.S_IREAD)

    def deleteObject(self, oid, serial, txn):
        self._check_trans(txn)
        self._server.deleteObject(oid, serial, id(txn))
        self._tbuf.store(oid, None)

    def loadBlob(self, oid, serial):
        # Load a blob.  If it isn't present and we have a shared blob
        # directory, then assume that it doesn't exist on the server
        # and return None.

        if self.fshelper is None:
            raise POSException.Unsupported("No blob cache directory is "
                                           "configured.")

        blob_filename = self.fshelper.getBlobFilename(oid, serial)
        if self.shared_blob_dir:
            if os.path.exists(blob_filename):
                return blob_filename
            else:
                # We're using a server shared cache.  If the file isn't
                # here, it's not anywhere.
                raise POSException.POSKeyError("No blob file", oid, serial)
        
        if os.path.exists(blob_filename):
            return _accessed(blob_filename)

        # First, we'll create the directory for this oid, if it doesn't exist. 
        self.fshelper.createPathForOID(oid)

        # OK, it's not here and we (or someone) needs to get it.  We
        # want to avoid getting it multiple times.  We want to avoid
        # getting it multiple times even accross separate client
        # processes on the same machine. We'll use file locking.

        lockfilename = os.path.join(os.path.dirname(blob_filename), '.lock')
        while 1:
            try:
                lock = zc.lockfile.LockFile(lockfilename)
            except zc.lockfile.LockError:
                time.sleep(0.01)
            else:
                break

        try:
            # We got the lock, so it's our job to download it.  First,
            # we'll double check that someone didn't download it while we
            # were getting the lock:

            if os.path.exists(blob_filename):
                return _accessed(blob_filename)

            # Ask the server to send it to us.  When this function
            # returns, it will have been sent. (The recieving will
            # have been handled by the asyncore thread.)

            self._server.sendBlob(oid, serial)

            if os.path.exists(blob_filename):
                return _accessed(blob_filename)

            raise POSException.POSKeyError("No blob file", oid, serial)

        finally:
            lock.close()

    def openCommittedBlobFile(self, oid, serial, blob=None):
        blob_filename = self.loadBlob(oid, serial)
        try:
            if blob is None:
                return open(blob_filename, 'rb')
            else:
                return ZODB.blob.BlobFile(blob_filename, 'r', blob)
        except (IOError):
            # The file got removed while we were opening.
            # Fall through and try again with the protection of the lock.
            pass
        
        lockfilename = os.path.join(os.path.dirname(blob_filename), '.lock')
        while 1:
            try:
                lock = zc.lockfile.LockFile(lockfilename)
            except zc.lockfile.LockError:
                time.sleep(.01)
            else:
                break

        try:
            blob_filename = self.fshelper.getBlobFilename(oid, serial)
            if not os.path.exists(blob_filename):
                if self.shared_blob_dir:
                    # We're using a server shared cache.  If the file isn't
                    # here, it's not anywhere.
                    raise POSException.POSKeyError("No blob file", oid, serial)
                self._server.sendBlob(oid, serial)
                if not os.path.exists(blob_filename):
                    raise POSException.POSKeyError("No blob file", oid, serial)

            _accessed(blob_filename)
            if blob is None:
                return open(blob_filename, 'rb')
            else:
                return ZODB.blob.BlobFile(blob_filename, 'r', blob)
        finally:
            lock.close()
        

    def temporaryDirectory(self):
        return self.fshelper.temp_dir

    def tpc_vote(self, txn):
        """Storage API: vote on a transaction."""
        if txn is not self._transaction:
            return
        self._server.vote(id(txn))
        return self._check_serials()

    def tpc_transaction(self):
        return self._transaction

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
                logger.debug("%s ClientDisconnected in tpc_abort() ignored",
                             self.__name__)
        finally:
            self._tbuf.clear()
            self._seriald.clear()
            del self._serials[:]
            self._iterator_gc()
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

            try:
                self._lock.acquire()  # for atomic processing of invalidations
                try:
                    self._update_cache(tid)
                    if f is not None:
                        f(tid)
                finally:
                    self._lock.release()

                r = self._check_serials()
                assert r is None or len(r) == 0, "unhandled serialnos: %s" % r
            except:
                # The server successfully committed.  If we get a failure
                # here, our own state will be in question, so reconnect.
                self._connection.close()
                raise

            self.end_transaction()
        finally:
            self._load_lock.release()
            self._iterator_gc()

    def _update_cache(self, tid):
        """Internal helper to handle objects modified by a transaction.

        This iterates over the objects in the transaction buffer and
        update or invalidate the cache.

        """
        # Must be called with _lock already acquired.

        # Not sure why _update_cache() would be called on a closed storage.
        if self._cache is None:
            return

        for oid, data in self._tbuf:
            self._cache.invalidate(oid, tid, False)
            # If data is None, we just invalidate.
            if data is not None:
                s = self._seriald[oid]
                if s != ResolvedSerial:
                    assert s == tid, (s, tid)
                    self._cache.store(oid, s, None, data)

        if self.fshelper is not None:
            blobs = self._tbuf.blobs
            while blobs:
                oid, blobfilename = blobs.pop()
                self._blob_data_bytes_loaded += os.stat(blobfilename).st_size
                targetpath = self.fshelper.getPathForOID(oid, create=True)
                ZODB.blob.rename_or_copy_blob(
                    blobfilename,
                    self.fshelper.getBlobFilename(oid, tid),
                    )
                self._check_blob_size(self._blob_data_bytes_loaded)

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
            self._tbuf.invalidate(oid)
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

    # Recovery support

    def copyTransactionsFrom(self, other, verbose=0):
        """Copy transactions from another storage.

        This is typically used for converting data from one storage to
        another.  `other` must have an .iterator() method.
        """
        ZODB.BaseStorage.copy(other, self, verbose)

    def restore(self, oid, serial, data, version, prev_txn, transaction):
        """Write data already committed in a separate database."""
        assert not version
        self._check_trans(transaction)
        self._server.restorea(oid, serial, data, prev_txn, id(transaction))
        # Don't update the transaction buffer, because current data are
        # unaffected.
        return self._check_serials()

    # Below are methods invoked by the StorageServer

    def serialnos(self, args):
        """Server callback to pass a list of changed (oid, serial) pairs."""
        self._serials.extend(args)

    def info(self, dict):
        """Server callback to update the info dictionary."""
        self._info.update(dict)

    def verify_cache(self, server):
        """Internal routine called to verify the cache.

        The return value (indicating which path we took) is used by
        the test suite.
        """

        self._pending_server = server

        # setup tempfile to hold zeoVerify results and interim
        # invalidation results
        self._tfile = tempfile.TemporaryFile(suffix=".inv")
        self._pickler = cPickle.Pickler(self._tfile, 1)
        self._pickler.fast = 1 # Don't use the memo

        if self._connection.peer_protocol_version < 'Z309':
            client = ClientStorage308Adapter(self)
        else:
            client = self

        # allow incoming invalidations:
        self._connection.register_object(client)

        # If verify_cache() finishes the cache verification process,
        # it should set self._server.  If it goes through full cache
        # verification, then endVerify() should self._server.

        ltid = server.lastTransaction()
        if not self._cache:
            logger.info("%s No verification necessary -- empty cache",
                        self.__name__)
            if ltid and ltid != utils.z64:
                self._cache.setLastTid(ltid)
            self.finish_verification()
            return "empty cache"

        last_inval_tid = self._cache.getLastTid()
        if last_inval_tid is not None:
            if ltid == last_inval_tid:
                logger.info(
                    "%s No verification necessary (last_inval_tid up-to-date)",
                    self.__name__)
                self.finish_verification()
                return "no verification"
            elif ltid < last_inval_tid:
                message = ("%s Client has seen newer transactions than server!"
                           % self.__name__)
                logger.critical(message)
                raise ClientStorageError(message)

            # log some hints about last transaction
            logger.info("%s last inval tid: %r %s\n",
                        self.__name__, last_inval_tid,
                        tid2time(last_inval_tid))
            logger.info("%s last transaction: %r %s",
                        self.__name__, ltid, ltid and tid2time(ltid))

            pair = server.getInvalidations(last_inval_tid)
            if pair is not None:
                logger.info("%s Recovering %d invalidations",
                            self.__name__, len(pair[1]))
                self.finish_verification(pair)
                return "quick verification"
        elif ltid and ltid != utils.z64:
            self._cache.setLastTid(ltid)

        zope.event.notify(ZEO.interfaces.StaleCache(self))

        # From this point on, we do not have complete information about
        # the missed transactions.  The reason is that cache
        # verification only checks objects in the client cache and
        # there may be objects in the object caches that aren't in the
        # client cach that would need verification too. We avoid that
        # problem by just invalidating the objects in the object caches.
        if self._db is not None:
            self._db.invalidateCache()

        if self._cache and self._drop_cache_rather_verify:
            logger.critical("%s dropping stale cache", self.__name__)
            self._cache.clear()
            if ltid:
                self._cache.setLastTid(ltid)
            self.finish_verification()
            return "cache dropped"

        logger.info("%s Verifying cache", self.__name__)
        for oid, tid in self._cache.contents():
            server.verify(oid, tid)
        server.endZeoVerify()
        return "full verification"

    def invalidateVerify(self, oid):
        """Server callback to invalidate an oid pair.

        This is called as part of cache validation.
        """
        # Invalidation as result of verify_cache().
        # Queue an invalidate for the end the verification procedure.
        if self._pickler is None:
            # This should never happen.
            logger.error("%s invalidateVerify with no _pickler", self.__name__)
            return
        self._pickler.dump((None, [oid]))

    def endVerify(self):
        """Server callback to signal end of cache validation."""

        logger.info("%s endVerify finishing", self.__name__)
        self.finish_verification()
        logger.info("%s endVerify finished", self.__name__)

    def finish_verification(self, catch_up=None):
        self._lock.acquire()
        try:
            if catch_up:
                # process catch-up invalidations
                self._process_invalidations(*catch_up)
            
            if self._pickler is None:
                return
            # write end-of-data marker
            self._pickler.dump((None, None))
            self._pickler = None
            self._tfile.seek(0)
            unpickler = cPickle.Unpickler(self._tfile)
            min_tid = self._cache.getLastTid()
            while 1:
                tid, oids = unpickler.load()
                if oids is None:
                    break
                if ((tid is None)
                    or (min_tid is None)
                    or (tid > min_tid)
                    ):
                    self._process_invalidations(tid, oids)

            self._tfile.close()
            self._tfile = None
        finally:
            self._lock.release()

        self._server = self._pending_server
        self._ready.set()
        self._pending_server = None


    def invalidateTransaction(self, tid, oids):
        """Server callback: Invalidate objects modified by tid."""
        self._lock.acquire()
        try:
            if self._pickler is not None:
                logger.debug(
                    "%s Transactional invalidation during cache verification",
                    self.__name__)
                self._pickler.dump((tid, oids))
            else:
                self._process_invalidations(tid, oids)
        finally:
            self._lock.release()

    def _process_invalidations(self, tid, oids):
        for oid in oids:
            if oid == self._load_oid:
                self._load_status = 0
            self._cache.invalidate(oid, tid)

        if self._db is not None:
            self._db.invalidate(tid, oids)

    # The following are for compatibility with protocol version 2.0.0

    def invalidateTrans(self, oids):
        return self.invalidateTransaction(None, oids)

    invalidate = invalidateVerify
    end = endVerify
    Invalidate = invalidateTrans

    # IStorageIteration

    def iterator(self, start=None, stop=None):
        """Return an IStorageTransactionInformation iterator."""
        # iids are "iterator IDs" that can be used to query an iterator whose
        # status is held on the server.
        iid = self._server.iterator_start(start, stop)
        return self._setup_iterator(TransactionIterator, iid)

    def _setup_iterator(self, factory, iid, *args):
        self._iterators[iid] = iterator = factory(self, iid, *args)
        self._iterator_ids.add(iid)
        return iterator

    def _forget_iterator(self, iid):
        self._iterators.pop(iid, None)
        self._iterator_ids.remove(iid)

    def _iterator_gc(self, disconnected=False):
        if not self._iterator_ids:
            return

        if disconnected:
            for i in self._iterators.values():
                i._iid = -1
            self._iterators.clear()
            self._iterator_ids.clear()
            return

        iids = self._iterator_ids - set(self._iterators)
        if iids:
            try:
                self._server.iterator_gc(list(iids))
            except ClientDisconnected:
                # If we get disconnected, all of the iterators on the
                # server are thrown away.  We should clear ours too:
                return self._iterator_gc(True)
            self._iterator_ids -= iids


class TransactionIterator(object):

    def __init__(self, storage, iid, *args):
        self._storage = storage 
        self._iid = iid
        self._ended = False

    def __iter__(self):
        return self

    def next(self):
        if self._ended:
            raise ZODB.interfaces.StorageStopIteration()

        if self._iid < 0:
            raise ClientDisconnected("Disconnected iterator")

        tx_data = self._storage._server.iterator_next(self._iid)
        if tx_data is None:
            # The iterator is exhausted, and the server has already
            # disposed it.
            self._ended = True
            self._storage._forget_iterator(self._iid)
            raise ZODB.interfaces.StorageStopIteration()

        return ClientStorageTransactionInformation(
            self._storage, self, *tx_data)


class ClientStorageTransactionInformation(ZODB.BaseStorage.TransactionRecord):

    def __init__(self, storage, txiter, tid, status, user, description,
                 extension):
        self._storage = storage
        self._txiter = txiter
        self._completed = False
        self._riid = None

        self.tid = tid
        self.status = status
        self.user = user
        self.description = description
        self.extension = extension

    def __iter__(self):
        riid = self._storage._server.iterator_record_start(self._txiter._iid,
                                                           self.tid)
        return self._storage._setup_iterator(RecordIterator, riid)


class RecordIterator(object):

    def __init__(self, storage, riid):
        self._riid = riid
        self._completed = False
        self._storage = storage

    def __iter__(self):
        return self

    def next(self):
        if self._completed:
            # We finished iteration once already and the server can't know
            # about the iteration anymore.
            raise ZODB.interfaces.StorageStopIteration()
        item = self._storage._server.iterator_record_next(self._riid)
        if item is None:
            # The iterator is exhausted, and the server has already
            # disposed it.
            self._completed = True
            raise ZODB.interfaces.StorageStopIteration()
        return ZODB.BaseStorage.DataRecord(*item)


class ClientStorage308Adapter:

    def __init__(self, client):
        self.client = client

    def invalidateTransaction(self, tid, args):
        self.client.invalidateTransaction(tid, [arg[0] for arg in args])

    def invalidateVerify(self, arg):
        self.client.invalidateVerify(arg[0])

    def __getattr__(self, name):
        return getattr(self.client, name)


class BlobCacheLayout(object):

    size = 997

    def oid_to_path(self, oid):
        return str(utils.u64(oid) % self.size)

    def getBlobFilePath(self, oid, tid):
        base, rem = divmod(utils.u64(oid), self.size)
        return os.path.join(
            str(rem),
            "%s.%s%s" % (base, tid.encode('hex'), ZODB.blob.BLOB_SUFFIX)
            )

def _accessed(filename):
    try:
        os.utime(filename, (time.time(), os.stat(filename).st_mtime))
    except OSError:
        pass # We tried. :)
    return filename

cache_file_name = re.compile(r'\d+$').match
def _check_blob_cache_size(blob_dir, target):

    logger = logging.getLogger(__name__+'.check_blob_cache')
    logger.info("Checking blob cache size")
    
    layout = open(os.path.join(blob_dir, ZODB.blob.LAYOUT_MARKER)
                  ).read().strip()
    if not layout == 'zeocache':
        logger.critical("Invalid blob directory layout %s", layout)
        raise ValueError("Invalid blob directory layout", layout)

    try:
        check_lock = zc.lockfile.LockFile(
            os.path.join(blob_dir, 'check_size.lock'))
    except zc.lockfile.LockError:
        # Someone is already cleaning up, so don't bother
        logger.info("Another thread is checking the blob cache size")
        return
    
    try:
        size = 0
        blob_suffix = ZODB.blob.BLOB_SUFFIX
        files_by_atime = BTrees.IOBTree.BTree()

        for dirname in os.listdir(blob_dir):
            if not cache_file_name(dirname):
                continue
            base = os.path.join(blob_dir, dirname)
            if not os.path.isdir(base):
                continue
            for file_name in os.listdir(base):
                if not file_name.endswith(blob_suffix):
                    continue
                file_name = os.path.join(base, file_name)
                if not os.path.isfile(file_name):
                    continue
                stat = os.stat(file_name)
                size += stat.st_size
                t = int(stat.st_atime)
                if t not in files_by_atime:
                    files_by_atime[t] = []
                files_by_atime[t].append(file_name)

        logger.info("blob cache size: %s", size)

        while size > target and files_by_atime:
            for file_name in files_by_atime.pop(files_by_atime.minKey()):
                lockfilename = os.path.join(os.path.dirname(file_name),
                                            '.lock')
                try:
                    lock = zc.lockfile.LockFile(lockfilename)
                except zc.lockfile.LockError:
                    logger.info("Skipping locked %s",
                                os.path.basename(file_name))
                    continue  # In use, skip

                try:
                    fsize = os.stat(file_name).st_size
                    try:
                        ZODB.blob.remove_committed(file_name)
                    except OSError, v:
                        pass # probably open on windows
                    else:
                        size -= fsize
                finally:
                    lock.close()

        logger.info("reduced blob cache size: %s", size)

    finally:
        check_lock.close()

def check_blob_size_script(args=None):
    if args is None:
        args = sys.argv[1:]
    blob_dir, target = args
    _check_blob_cache_size(blob_dir, int(target))
