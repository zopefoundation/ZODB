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
"""The ClientStorage class and the exceptions that it may raise.

Public contents of this module:

ClientStorage -- the main class, implementing the Storage API
ClientStorageError -- exception raised by ClientStorage
UnrecognizedResult -- exception raised by ClientStorage
ClientDisconnected -- exception raised by ClientStorage

$Id: ClientStorage.py,v 1.69 2002/10/01 16:37:03 gvanrossum Exp $
"""

# XXX TO DO
# get rid of beginVerify, set up _tfile in verify_cache
# set self._storage = stub later, in endVerify
# if wait is given, wait until verify is complete

import cPickle
import os
import tempfile
import threading
import time

from ZEO import ClientCache, ServerStub
from ZEO.TransactionBuffer import TransactionBuffer
from ZEO.Exceptions import Disconnected
from ZEO.zrpc.client import ConnectionManager

from ZODB import POSException
from ZODB.TimeStamp import TimeStamp
from zLOG import LOG, PROBLEM, INFO, BLATHER

def log2(type, msg, subsys="ClientStorage:%d" % os.getpid()):
    LOG(subsys, type, msg)

try:
    from ZODB.ConflictResolution import ResolvedSerial
except ImportError:
    ResolvedSerial = 'rs'

class ClientStorageError(POSException.StorageError):
    """An error occured in the ZEO Client Storage."""

class UnrecognizedResult(ClientStorageError):
    """A server call returned an unrecognized result."""

class ClientDisconnected(ClientStorageError, Disconnected):
    """The database storage is disconnected from the storage."""

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

class ClientStorage:

    """A Storage class that is a network client to a remote storage.

    This is a faithful implementation of the Storage API.

    This class is thread-safe; transactions are serialized in
    tpc_begin().
    """

    def __init__(self, addr, storage='1', cache_size=20000000,
                 name='', client=None, debug=0, var=None,
                 min_disconnect_poll=5, max_disconnect_poll=300,
                 wait_for_server_on_startup=None, # deprecated alias for wait
                 wait=None, # defaults to 1
                 read_only=0, read_only_fallback=0):

        """ClientStorage constructor.

        This is typically invoked from a custom_zodb.py file.

        All arguments except addr should be keyword arguments.
        Arguments:

        addr -- The server address(es).  This is either a list of
            addresses, or a single address.  Each address can be a
            (hostname, port) tuple to signify a TCP/IP connection, or
            a pathname string to signify a Unix domain socket
            connection.  A hostname may be a DNS name or a dotted IP
            address.  Required.

        storage -- The storage name, defaulting to '1'.  This must
            match one of the storage names supported by the server(s)
            specified by the addr argument.

        cache_size -- The disk cache size, defaulting to 20 megabytes.
            This is passed to the ClientCache constructor.

        name -- The storage name, defaulting to ''.  If this is false,
            str(addr) is used as the storage name.

        client -- The client name, defaulting to None.  If this is
            false, the environment value ZEO_CLIENT is used.  if the
            effective value is true, the client cache is persistent.
            See ClientCache for more info.

        debug -- Ignored.  This is present only for backwards
            compatibility with ZEO 1.

        var -- The 'var' directory, defaulting to None.  This is
            passed to the ClientCache constructor, which picks a
            default if the value is None.

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
        """

        log2(INFO, "ClientStorage (pid=%d) created %s/%s for storage: %r" %
             (os.getpid(),
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
        self._server = disconnected_stub
        self._is_read_only = read_only
        self._storage = storage
        self._read_only_fallback = read_only_fallback
        self._connection = None

        self._info = {'length': 0, 'size': 0, 'name': 'ZEO Client',
                      'supportsUndo':0, 'supportsVersions': 0,
                      'supportsTransactionalUndo': 0}

        self._tbuf = TransactionBuffer()
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

        t = self._ts = get_timestamp()
        self._serial = `t`
        self._oid = '\0\0\0\0\0\0\0\0'

        # Decide whether to use non-temporary files
        client = client or os.environ.get('ZEO_CLIENT')
        self._cache = ClientCache.ClientCache(storage, cache_size,
                                              client=client, var=var)

        self._rpc_mgr = ConnectionManager(addr, self,
                                          tmin=min_disconnect_poll,
                                          tmax=max_disconnect_poll)

        if wait:
            self._rpc_mgr.connect(sync=1)
        else:
            if not self._rpc_mgr.attempt_connect():
                self._rpc_mgr.connect()

        # If we're connected at this point, the cache is opened as a
        # side effect of verify_cache().  If not, open it now.
        if not self.is_connected():
            self._cache.open()

    def close(self):
        """Storage API: finalize the storage, releasing external resources."""
        if self._tbuf is not None:
            self._tbuf.close()
            self._tbuf = None
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
        log2(INFO, "registerDB(%s, %s)" % (repr(db), repr(limit)))
        self._db = db

    def is_connected(self):
        """Return whether the storage is currently connected to a server."""
        if self._server is disconnected_stub:
            return 0
        else:
            return 1

    def sync(self):
        """Handle any pending invalidation messages.

        This is called by the sync method in ZODB.Connection.
        """
        self._server._update()

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
        stub = ServerStub.StorageServer(conn)
        try:
            stub.register(str(self._storage), self._is_read_only)
            return 1
        except POSException.ReadOnlyError:
            if not self._read_only_fallback:
                raise
            log2(INFO, "Got ReadOnlyError; trying again with read_only=1")
            stub.register(str(self._storage), read_only=1)
            return 0

    def notifyConnected(self, conn):
        """Internal: start using the given connection.

        This is called by ConnectionManager after it has decided which
        connection should be used.
        """
        if self._connection is not None:
            log2(INFO, "Reconnected to storage")
        else:
            log2(INFO, "Connected to storage")
        stub = ServerStub.StorageServer(conn)
        self._oids = []
        self._info.update(stub.get_info())
        self.verify_cache(stub)

        # XXX The stub should be saved here and set in endVerify() below.
        if self._connection is not None:
            self._connection.close()
        self._connection = conn
        self._server = stub

    def verify_cache(self, server):
        """Internal routine called to verify the cache."""
        # XXX beginZeoVerify ends up calling back to beginVerify() below.
        # That whole exchange is rather unnecessary.
        server.beginZeoVerify()
        self._cache.verify(server.zeoVerify)
        server.endZeoVerify()

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
        log2(PROBLEM, "Disconnected from storage")
        self._connection = None
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
        """Storage API: return whether we are in read-only mode.

        XXX In read-only fallback mode, this returns false, even if we
        are currently connected to a read-only server.
        """
        return self._is_read_only

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

    def loadSerial(self, oid, serial):
        """Storage API: load a historical revision of an object."""
        return self._server.loadSerial(oid, serial)

    def load(self, oid, version):
        """Storage API: return the data for a given object.

        This returns the pickle data and serial number for the object
        specified by the given object id and version, if they exist;
        otherwise a KeyError is raised.
        """
        p = self._cache.load(oid, version)
        if p:
            return p
        if self._server is None:
            raise ClientDisconnected()
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
        v = self._cache.modifiedInVersion(oid)
        if v is not None:
            return v
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

    def tpc_begin(self, transaction, tid=None, status=' '):
        """Storage API: begin a transaction."""
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self._tpc_cond.acquire()
        while self._transaction is not None:
            # It is allowable for a client to call two tpc_begins in a
            # row with the same transaction, and the second of these
            # must be ignored.
            if self._transaction == transaction:
                self._tpc_cond.release()
                return
            self._tpc_cond.wait(30)
        self._transaction = transaction
        self._tpc_cond.release()

        if tid is None:
            self._ts = get_timestamp(self._ts)
            id = `self._ts`
        else:
            self._ts = TimeStamp(tid)
            id = tid

        try:
            r = self._server.tpc_begin(id,
                                       transaction.user,
                                       transaction.description,
                                       transaction._extension,
                                       tid, status)
        except:
            # Client may have disconnected during the tpc_begin().
            if self._server is not disconnected_stub:
                self.end_transaction()
            raise

        self._serial = id
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

    def tpc_abort(self, transaction):
        """Storage API: abort a transaction."""
        if transaction is not self._transaction:
            return
        try:
            self._server.tpc_abort(self._serial)
            self._tbuf.clear()
            self._seriald.clear()
            del self._serials[:]
        finally:
            self.end_transaction()

    def tpc_finish(self, transaction, f=None):
        """Storage API: finish a transaction."""
        if transaction is not self._transaction:
            return
        try:
            if f is not None:
                f()

            self._server.tpc_finish(self._serial)

            r = self._check_serials()
            assert r is None or len(r) == 0, "unhandled serialnos: %s" % r

            self._update_cache()
        finally:
            self.end_transaction()

    def _update_cache(self):
        """Internal helper to handle objects modified by a transaction.

        This iterates over the objects in the transaction buffer and
        update or invalidate the cache.
        """
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
        # XXX what are the sync issues here?
        oids = self._server.undo(transaction_id)
        for oid in oids:
            self._cache.invalidate(oid, '')
        return oids

    def undoInfo(self, first=0, last=-20, specification=None):
        """Storage API: return undo information."""
        return self._server.undoInfo(first, last, specification)

    def undoLog(self, first=0, last=-20, filter=None):
        """Storage API: return a sequence of TransactionDescription objects."""
        if filter is not None:
            return () # can't pass a filter to server

        return self._server.undoLog(first, last) # Eek!

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

    def beginVerify(self):
        """Server callback to signal start of cache validation."""
        self._tfile = tempfile.TemporaryFile(suffix=".inv")
        self._pickler = cPickle.Pickler(self._tfile, 1)
        self._pickler.fast = 1 # Don't use the memo

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

    def endVerify(self):
        """Server callback to signal end of cache validation."""
        if self._pickler is None:
            return
        self._pickler.dump((0,0))
        self._tfile.seek(0)
        unpick = cPickle.Unpickler(self._tfile)
        f = self._tfile
        self._tfile = None

        while 1:
            oid, version = unpick.load()
            if not oid:
                break
            self._cache.invalidate(oid, version=version)
            self._db.invalidate(oid, version=version)
        f.close()

    def invalidateTrans(self, args):
        """Server callback to invalidate a list of (oid, version) pairs.

        This is called as the result of a transaction.
        """
        for oid, version in args:
            self._cache.invalidate(oid, version=version)
            try:
                self._db.invalidate(oid, version=version)
            except AttributeError, msg:
                log2(PROBLEM,
                    "Invalidate(%s, %s) failed for _db: %s" % (repr(oid),
                                                               repr(version),
                                                               msg))

    # Unfortunately, the ZEO 2 wire protocol uses different names for
    # several of the callback methods invoked by the StorageServer.
    # We can't change the wire protocol at this point because that
    # would require synchronized updates of clients and servers and we
    # don't want that.  So here we alias the old names to their new
    # implementations.

    begin = beginVerify
    invalidate = invalidateVerify
    end = endVerify
    Invalidate = invalidateTrans
