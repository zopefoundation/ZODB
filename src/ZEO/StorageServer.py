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
"""The StorageServer class and the exception that it may raise.

This server acts as a front-end for one or more real storages, like
file storage or Berkeley storage.

XXX Need some basic access control-- a declaration of the methods
exported for invocation by the server.
"""

import asyncore
import cPickle
import os
import sys
import threading
import time

from ZEO import ClientStub
from ZEO.CommitLog import CommitLog
from ZEO.monitor import StorageStats, StatsServer
from ZEO.zrpc.server import Dispatcher
from ZEO.zrpc.connection import ManagedServerConnection, Delay, MTDelay
from ZEO.zrpc.trigger import trigger
from ZEO.Exceptions import AuthError

import zLOG
from ZODB.ConflictResolution import ResolvedSerial
from ZODB.POSException import StorageError, StorageTransactionError
from ZODB.POSException import TransactionError, ReadOnlyError, ConflictError
from ZODB.referencesf import referencesf
from ZODB.Transaction import Transaction
from ZODB.utils import u64, oid_repr

_label = "ZSS" # Default label used for logging.

def set_label():
    """Internal helper to reset the logging label (e.g. after fork())."""
    global _label
    _label = "ZSS:%s" % os.getpid()

def log(message, level=zLOG.INFO, label=None, error=None):
    """Internal helper to log a message using zLOG."""
    zLOG.LOG(label or _label, level, message, error=error)

class StorageServerError(StorageError):
    """Error reported when an unpickleable exception is raised."""

class ZEOStorage:
    """Proxy to underlying storage for a single remote client."""

    # Classes we instantiate.  A subclass might override.
    ClientStorageStubClass = ClientStub.ClientStorage

    # A list of extension methods.  A subclass with extra methods
    # should override.
    extensions = []

    def __init__(self, server, read_only=0, auth_realm=None):
        self.server = server
        # timeout and stats will be initialized in register()
        self.timeout = None
        self.stats = None
        self.connection = None
        self.client = None
        self.storage = None
        self.storage_id = "uninitialized"
        self.transaction = None
        self.read_only = read_only
        self.locked = 0
        self.verifying = 0
        self.store_failed = 0
        self.log_label = _label
        self.authenticated = 0
        self.auth_realm = auth_realm
        # The authentication protocol may define extra methods.
        self._extensions = {}
        for func in self.extensions:
            self._extensions[func.func_name] = None

    def finish_auth(self, authenticated):
        if not self.auth_realm:
            return 1
        self.authenticated = authenticated
        return authenticated

    def set_database(self, database):
        self.database = database

    def notifyConnected(self, conn):
        self.connection = conn # For restart_other() below
        self.client = self.ClientStorageStubClass(conn)
        addr = conn.addr
        if isinstance(addr, type("")):
            label = addr
        else:
            host, port = addr
            label = str(host) + ":" + str(port)
        self.log_label = _label + "/" + label

    def notifyDisconnected(self):
        # When this storage closes, we must ensure that it aborts
        # any pending transaction.
        if self.transaction is not None:
            self.log("disconnected during transaction %s" % self.transaction)
            self._abort()
        else:
            self.log("disconnected")
        if self.stats is not None:
            self.stats.clients -= 1

    def __repr__(self):
        tid = self.transaction and repr(self.transaction.id)
        if self.storage:
            stid = (self.storage._transaction and
                    repr(self.storage._transaction.id))
        else:
            stid = None
        name = self.__class__.__name__
        return "<%s %X trans=%s s_trans=%s>" % (name, id(self), tid, stid)

    def log(self, msg, level=zLOG.INFO, error=None):
        zLOG.LOG(self.log_label, level, msg, error=error)

    def setup_delegation(self):
        """Delegate several methods to the storage"""
        self.versionEmpty = self.storage.versionEmpty
        self.versions = self.storage.versions
        self.getSerial = self.storage.getSerial
        self.history = self.storage.history
        self.load = self.storage.load
        self.loadSerial = self.storage.loadSerial
        self.modifiedInVersion = self.storage.modifiedInVersion
        try:
            fn = self.storage.getExtensionMethods
        except AttributeError:
            # We must be running with a ZODB which
            # predates adding getExtensionMethods to
            # BaseStorage. Eventually this try/except
            # can be removed
            pass
        else:
            d = fn()
            self._extensions.update(d)
            for name in d.keys():
                assert not hasattr(self, name)
                setattr(self, name, getattr(self.storage, name))
        self.lastTransaction = self.storage.lastTransaction

    def _check_tid(self, tid, exc=None):
        if self.read_only:
            raise ReadOnlyError()
        if self.transaction is None:
            caller = sys._getframe().f_back.f_code.co_name
            self.log("no current transaction: %s()" % caller, zLOG.PROBLEM)
            if exc is not None:
                raise exc(None, tid)
            else:
                return 0
        if self.transaction.id != tid:
            caller = sys._getframe().f_back.f_code.co_name
            self.log("%s(%s) invalid; current transaction = %s" %
                     (caller, repr(tid), repr(self.transaction.id)),
                     zLOG.PROBLEM)
            if exc is not None:
                raise exc(self.transaction.id, tid)
            else:
                return 0
        return 1

    def getAuthProtocol(self):
        """Return string specifying name of authentication module to use.

        The module name should be auth_%s where %s is auth_protocol."""
        protocol = self.server.auth_protocol
        if not protocol or protocol == 'none':
            return None
        return protocol

    def register(self, storage_id, read_only):
        """Select the storage that this client will use

        This method must be the first one called by the client.
        For authenticated storages this method will be called by the client
        immediately after authentication is finished.
        """
        if self.auth_realm and not self.authenticated:
            raise AuthError, "Client was never authenticated with server!"

        if self.storage is not None:
            self.log("duplicate register() call")
            raise ValueError, "duplicate register() call"
        storage = self.server.storages.get(storage_id)
        if storage is None:
            self.log("unknown storage_id: %s" % storage_id)
            raise ValueError, "unknown storage: %s" % storage_id

        if not read_only and (self.read_only or storage.isReadOnly()):
            raise ReadOnlyError()

        self.read_only = self.read_only or read_only
        self.storage_id = storage_id
        self.storage = storage
        self.setup_delegation()
        self.timeout, self.stats = self.server.register_connection(storage_id,
                                                                   self)

    def get_info(self):
        return {'length': len(self.storage),
                'size': self.storage.getSize(),
                'name': self.storage.getName(),
                'supportsUndo': self.storage.supportsUndo(),
                'supportsVersions': self.storage.supportsVersions(),
                'extensionMethods': self.getExtensionMethods(),
                }

    def get_size_info(self):
        return {'length': len(self.storage),
                'size': self.storage.getSize(),
                }

    def getExtensionMethods(self):
        return self._extensions

    def loadEx(self, oid, version):
        self.stats.loads += 1
        return self.storage.loadEx(oid, version)

    def loadBefore(self, oid, tid):
        self.stats.loads += 1
        return self.storage.loadBefore(oid, tid)

    def zeoLoad(self, oid):
        self.stats.loads += 1
        v = self.storage.modifiedInVersion(oid)
        if v:
            pv, sv = self.storage.load(oid, v)
        else:
            pv = sv = None
        try:
            p, s = self.storage.load(oid, '')
        except KeyError:
            if sv:
                # Created in version, no non-version data
                p = s = None
            else:
                raise
        return p, s, v, pv, sv

    def getInvalidations(self, tid):
        invtid, invlist = self.server.get_invalidations(tid)
        if invtid is None:
            return None
        self.log("Return %d invalidations up to tid %s"
                 % (len(invlist), u64(invtid)))
        return invtid, invlist

    def verify(self, oid, version, tid):
        try:
            t = self.storage.getTid(oid)
        except KeyError:
            self.client.invalidateVerify((oid, ""))
        else:
            if tid != t:
                # This will invalidate non-version data when the
                # client only has invalid version data.  Since this is
                # an uncommon case, we avoid the cost of checking
                # whether the serial number matches the current
                # non-version data.
                self.client.invalidateVerify((oid, version))

    def zeoVerify(self, oid, s, sv):
        if not self.verifying:
            self.verifying = 1
            self.stats.verifying_clients += 1
        try:
            os = self.storage.getTid(oid)
        except KeyError:
            self.client.invalidateVerify((oid, ''))
            # XXX It's not clear what we should do now.  The KeyError
            # could be caused by an object uncreation, in which case
            # invalidation is right.  It could be an application bug
            # that left a dangling reference, in which case it's bad.
        else:
            # If the client has version data, the logic is a bit more
            # complicated.  If the current serial number matches the
            # client serial number, then the non-version data must
            # also be valid.  If the current serialno is for a
            # version, then the non-version data can't change.

            # If the version serialno isn't valid, then the
            # non-version serialno may or may not be valid.  Rather
            # than trying to figure it whether it is valid, we just
            # invalidate it.  Sending an invalidation for the
            # non-version data implies invalidating the version data
            # too, since an update to non-version data can only occur
            # after the version is aborted or committed.
            if sv:
                if sv != os:
                    self.client.invalidateVerify((oid, ''))
            else:
                if s != os:
                    self.client.invalidateVerify((oid, ''))

    def endZeoVerify(self):
        if self.verifying:
            self.stats.verifying_clients -= 1
        self.verifying = 0
        self.client.endVerify()

    def pack(self, time, wait=1):
        # Yes, you can pack a read-only server or storage!
        if wait:
            return run_in_thread(self._pack_impl, time)
        else:
            # If the client isn't waiting for a reply, start a thread
            # and forget about it.
            t = threading.Thread(target=self._pack_impl, args=(time,))
            t.start()
            return None

    def _pack_impl(self, time):
        self.log("pack(time=%s) started..." % repr(time))
        self.storage.pack(time, referencesf)
        self.log("pack(time=%s) complete" % repr(time))
        # Broadcast new size statistics
        self.server.invalidate(0, self.storage_id, None,
                               (), self.get_size_info())

    def new_oids(self, n=100):
        """Return a sequence of n new oids, where n defaults to 100"""
        if self.read_only:
            raise ReadOnlyError()
        if n <= 0:
            n = 1
        return [self.storage.new_oid() for i in range(n)]

    # undoLog and undoInfo are potentially slow methods

    def undoInfo(self, first, last, spec):
        return run_in_thread(self.storage.undoInfo, first, last, spec)

    def undoLog(self, first, last):
        return run_in_thread(self.storage.undoLog, first, last)

    def tpc_begin(self, id, user, description, ext, tid=None, status=" "):
        if self.read_only:
            raise ReadOnlyError()
        if self.transaction is not None:
            if self.transaction.id == id:
                self.log("duplicate tpc_begin(%s)" % repr(id))
                return
            else:
                raise StorageTransactionError("Multiple simultaneous tpc_begin"
                                              " requests from one client.")

        self.transaction = t = Transaction()
        t.id = id
        t.user = user
        t.description = description
        t._extension = ext

        self.serials = []
        self.invalidated = []
        self.txnlog = CommitLog()
        self.tid = tid
        self.status = status
        self.store_failed = 0
        self.stats.active_txns += 1

    def tpc_finish(self, id):
        if not self._check_tid(id):
            return
        assert self.locked
        self.stats.active_txns -= 1
        self.stats.commits += 1
        self.storage.tpc_finish(self.transaction)
        tid = self.storage.lastTransaction()
        if self.invalidated:
            self.server.invalidate(self, self.storage_id, tid,
                                   self.invalidated, self.get_size_info())
        self._clear_transaction()
        # Return the tid, for cache invalidation optimization
        return tid

    def tpc_abort(self, id):
        if not self._check_tid(id):
            return
        self.stats.active_txns -= 1
        self.stats.aborts += 1
        if self.locked:
            self.storage.tpc_abort(self.transaction)
        self._clear_transaction()

    def _clear_transaction(self):
        # Common code at end of tpc_finish() and tpc_abort()
        self.transaction = None
        self.txnlog.close()
        if self.locked:
            self.locked = 0
            self.timeout.end(self)
            self.stats.lock_time = None
            self.log("Transaction released storage lock", zLOG.BLATHER)
            # _handle_waiting() can start another transaction (by
            # restarting a waiting one) so must be done last
            self._handle_waiting()

    def _abort(self):
        # called when a connection is closed unexpectedly
        if not self.locked:
            # Delete (d, zeo_storage) from the _waiting list, if found.
            waiting = self.storage._waiting
            for i in range(len(waiting)):
                d, z = waiting[i]
                if z is self:
                    del waiting[i]
                    self.log("Closed connection removed from waiting list."
                             " Clients waiting: %d." % len(waiting))
                    break

        if self.transaction:
            self.stats.active_txns -= 1
            self.stats.aborts += 1
            self.tpc_abort(self.transaction.id)

    # The public methods of the ZEO client API do not do the real work.
    # They defer work until after the storage lock has been acquired.
    # Most of the real implementations are in methods beginning with
    # an _.

    def storea(self, oid, serial, data, version, id):
        self._check_tid(id, exc=StorageTransactionError)
        self.stats.stores += 1
        self.txnlog.store(oid, serial, data, version)

    # The following four methods return values, so they must acquire
    # the storage lock and begin the transaction before returning.

    def vote(self, id):
        self._check_tid(id, exc=StorageTransactionError)
        if self.locked:
            return self._vote()
        else:
            return self._wait(lambda: self._vote())

    def abortVersion(self, src, id):
        self._check_tid(id, exc=StorageTransactionError)
        if self.locked:
            return self._abortVersion(src)
        else:
            return self._wait(lambda: self._abortVersion(src))

    def commitVersion(self, src, dest, id):
        self._check_tid(id, exc=StorageTransactionError)
        if self.locked:
            return self._commitVersion(src, dest)
        else:
            return self._wait(lambda: self._commitVersion(src, dest))

    def undo(self, trans_id, id):
        self._check_tid(id, exc=StorageTransactionError)
        if self.locked:
            return self._undo(trans_id)
        else:
            return self._wait(lambda: self._undo(trans_id))

    def _tpc_begin(self, txn, tid, status):
        self.locked = 1
        self.timeout.begin(self)
        self.stats.lock_time = time.time()
        self.storage.tpc_begin(txn, tid, status)

    def _store(self, oid, serial, data, version):
        err = None
        try:
            newserial = self.storage.store(oid, serial, data, version,
                                           self.transaction)
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception, err:
            self.store_failed = 1
            if isinstance(err, ConflictError):
                self.stats.conflicts += 1
                self.log("conflict error oid=%s msg=%s" %
                         (oid_repr(oid), str(err)), zLOG.BLATHER)
            if not isinstance(err, TransactionError):
                # Unexpected errors are logged and passed to the client
                exc_info = sys.exc_info()
                self.log("store error: %s, %s" % exc_info[:2],
                         zLOG.ERROR, error=exc_info)
                del exc_info
            # Try to pickle the exception.  If it can't be pickled,
            # the RPC response would fail, so use something else.
            pickler = cPickle.Pickler()
            pickler.fast = 1
            try:
                pickler.dump(err, 1)
            except:
                msg = "Couldn't pickle storage exception: %s" % repr(err)
                self.log(msg, zLOG.ERROR)
                err = StorageServerError(msg)
            # The exception is reported back as newserial for this oid
            newserial = err
        else:
            if serial != "\0\0\0\0\0\0\0\0":
                self.invalidated.append((oid, version))
        if newserial == ResolvedSerial:
            self.stats.conflicts_resolved += 1
            self.log("conflict resolved oid=%s" % oid_repr(oid), zLOG.BLATHER)
        self.serials.append((oid, newserial))
        return err is None

    def _vote(self):
        self.client.serialnos(self.serials)
        # If a store call failed, then return to the client immediately.
        # The serialnos() call will deliver an exception that will be
        # handled by the client in its tpc_vote() method.
        if self.store_failed:
            return
        return self.storage.tpc_vote(self.transaction)

    def _abortVersion(self, src):
        tid, oids = self.storage.abortVersion(src, self.transaction)
        inv = [(oid, src) for oid in oids]
        self.invalidated.extend(inv)
        return tid, oids

    def _commitVersion(self, src, dest):
        tid, oids = self.storage.commitVersion(src, dest, self.transaction)
        inv = [(oid, dest) for oid in oids]
        self.invalidated.extend(inv)
        if dest:
            inv = [(oid, src) for oid in oids]
            self.invalidated.extend(inv)
        return tid, oids

    def _undo(self, trans_id):
        tid, oids = self.storage.undo(trans_id, self.transaction)
        inv = [(oid, None) for oid in oids]
        self.invalidated.extend(inv)
        return tid, oids

    # When a delayed transaction is restarted, the dance is
    # complicated.  The restart occurs when one ZEOStorage instance
    # finishes as a transaction and finds another instance is in the
    # _waiting list.

    # XXX It might be better to have a mechanism to explicitly send
    # the finishing transaction's reply before restarting the waiting
    # transaction.  If the restart takes a long time, the previous
    # client will be blocked until it finishes.

    def _wait(self, thunk):
        # Wait for the storage lock to be acquired.
        self._thunk = thunk
        if self.storage._transaction:
            d = Delay()
            self.storage._waiting.append((d, self))
            self.log("Transaction blocked waiting for storage. "
                     "Clients waiting: %d." % len(self.storage._waiting))
            return d
        else:
            self.log("Transaction acquired storage lock.", zLOG.BLATHER)
            return self._restart()

    def _restart(self, delay=None):
        # Restart when the storage lock is available.
        if self.txnlog.stores == 1:
            template = "Preparing to commit transaction: %d object, %d bytes"
        else:
            template = "Preparing to commit transaction: %d objects, %d bytes"
        self.log(template % (self.txnlog.stores, self.txnlog.size()),
                 level=zLOG.BLATHER)
        self._tpc_begin(self.transaction, self.tid, self.status)
        loads, loader = self.txnlog.get_loader()
        for i in range(loads):
            # load oid, serial, data, version
            if not self._store(*loader.load()):
                break
        resp = self._thunk()
        if delay is not None:
            delay.reply(resp)
        else:
            return resp

    def _handle_waiting(self):
        # Restart any client waiting for the storage lock.
        while self.storage._waiting:
            delay, zeo_storage = self.storage._waiting.pop(0)
            if self._restart_other(zeo_storage, delay):
                if self.storage._waiting:
                    n = len(self.storage._waiting)
                    self.log("Blocked transaction restarted.  "
                             "Clients waiting: %d" % n)
                else:
                    self.log("Blocked transaction restarted.")
                return

    def _restart_other(self, zeo_storage, delay):
        # Return True if the server restarted.
        # call the restart() method on the appropriate server.
        try:
            zeo_storage._restart(delay)
        except:
            self.log("Unexpected error handling waiting transaction",
                     level=zLOG.WARNING, error=sys.exc_info())
            zeo_storage.connection.close()
            return 0
        else:
            return 1

class StorageServer:

    """The server side implementation of ZEO.

    The StorageServer is the 'manager' for incoming connections.  Each
    connection is associated with its own ZEOStorage instance (defined
    below).  The StorageServer may handle multiple storages; each
    ZEOStorage instance only handles a single storage.
    """

    # Classes we instantiate.  A subclass might override.

    DispatcherClass = Dispatcher
    ZEOStorageClass = ZEOStorage
    ManagedServerConnectionClass = ManagedServerConnection

    def __init__(self, addr, storages, read_only=0,
                 invalidation_queue_size=100,
                 transaction_timeout=None,
                 monitor_address=None,
                 auth_protocol=None,
                 auth_database=None,
                 auth_realm=None):
        """StorageServer constructor.

        This is typically invoked from the start.py script.

        Arguments (the first two are required and positional):

        addr -- the address at which the server should listen.  This
            can be a tuple (host, port) to signify a TCP/IP connection
            or a pathname string to signify a Unix domain socket
            connection.  A hostname may be a DNS name or a dotted IP
            address.

        storages -- a dictionary giving the storage(s) to handle.  The
            keys are the storage names, the values are the storage
            instances, typically FileStorage or Berkeley storage
            instances.  By convention, storage names are typically
            strings representing small integers starting at '1'.

        read_only -- an optional flag saying whether the server should
            operate in read-only mode.  Defaults to false.  Note that
            even if the server is operating in writable mode,
            individual storages may still be read-only.  But if the
            server is in read-only mode, no write operations are
            allowed, even if the storages are writable.  Note that
            pack() is considered a read-only operation.

        invalidation_queue_size -- The storage server keeps a queue
            of the objects modified by the last N transactions, where
            N == invalidation_queue_size.  This queue is used to
            speed client cache verification when a client disconnects
            for a short period of time.

        transaction_timeout -- The maximum amount of time to wait for
            a transaction to commit after acquiring the storage lock.
            If the transaction takes too long, the client connection
            will be closed and the transaction aborted.

        monitor_address -- The address at which the monitor server
            should listen.  If specified, a monitor server is started.
            The monitor server provides server statistics in a simple
            text format.

        auth_protocol -- The name of the authentication protocol to use.
            Examples are "digest" and "srp".

        auth_database -- The name of the password database filename.
            It should be in a format compatible with the authentication
            protocol used; for instance, "sha" and "srp" require different
            formats.

            Note that to implement an authentication protocol, a server
            and client authentication mechanism must be implemented in a
            auth_* module, which should be stored inside the "auth"
            subdirectory. This module may also define a DatabaseClass
            variable that should indicate what database should be used
            by the authenticator.
        """

        self.addr = addr
        self.storages = storages
        set_label()
        msg = ", ".join(
            ["%s:%s:%s" % (name, storage.isReadOnly() and "RO" or "RW",
                           storage.getName())
             for name, storage in storages.items()])
        log("%s created %s with storages: %s" %
            (self.__class__.__name__, read_only and "RO" or "RW", msg))
        for s in storages.values():
            s._waiting = []
        self.read_only = read_only
        self.auth_protocol = auth_protocol
        self.auth_database = auth_database
        self.auth_realm = auth_realm
        self.database = None
        if auth_protocol:
            self._setup_auth(auth_protocol)
        # A list of at most invalidation_queue_size invalidations.
        # The list is kept in sorted order with the most recent
        # invalidation at the front.  The list never has more than
        # self.invq_bound elements.
        self.invq = []
        self.invq_bound = invalidation_queue_size
        self.connections = {}
        self.dispatcher = self.DispatcherClass(addr,
                                               factory=self.new_connection)
        self.stats = {}
        self.timeouts = {}
        for name in self.storages.keys():
            self.stats[name] = StorageStats()
            if transaction_timeout is None:
                # An object with no-op methods
                timeout = StubTimeoutThread()
            else:
                timeout = TimeoutThread(transaction_timeout)
                timeout.start()
            self.timeouts[name] = timeout
        if monitor_address:
            self.monitor = StatsServer(monitor_address, self.stats)
        else:
            self.monitor = None

    def _setup_auth(self, protocol):
        # Can't be done in global scope, because of cyclic references
        from ZEO.auth import get_module

        name = self.__class__.__name__

        module = get_module(protocol)
        if not module:
            log("%s: no such an auth protocol: %s" % (name, protocol))
            return

        storage_class, client, db_class = module

        if not storage_class or not issubclass(storage_class, ZEOStorage):
            log(("%s: %s isn't a valid protocol, must have a StorageClass" %
                 (name, protocol)))
            self.auth_protocol = None
            return
        self.ZEOStorageClass = storage_class

        log("%s: using auth protocol: %s" % (name, protocol))

        # We create a Database instance here for use with the authenticator
        # modules. Having one instance allows it to be shared between multiple
        # storages, avoiding the need to bloat each with a new authenticator
        # Database that would contain the same info, and also avoiding any
        # possibly synchronization issues between them.
        self.database = db_class(self.auth_database)
        if self.database.realm != self.auth_realm:
            raise ValueError("password database realm %r "
                             "does not match storage realm %r"
                             % (self.database.realm, self.auth_realm))


    def new_connection(self, sock, addr):
        """Internal: factory to create a new connection.

        This is called by the Dispatcher class in ZEO.zrpc.server
        whenever accept() returns a socket for a new incoming
        connection.
        """
        if self.auth_protocol and self.database:
            zstorage = self.ZEOStorageClass(self, self.read_only,
                                            auth_realm=self.auth_realm)
            zstorage.set_database(self.database)
        else:
            zstorage = self.ZEOStorageClass(self, self.read_only)

        c = self.ManagedServerConnectionClass(sock, addr, zstorage, self)
        log("new connection %s: %s" % (addr, `c`))
        return c

    def register_connection(self, storage_id, conn):
        """Internal: register a connection with a particular storage.

        This is called by ZEOStorage.register().

        The dictionary self.connections maps each storage name to a
        list of current connections for that storage; this information
        is needed to handle invalidation.  This function updates this
        dictionary.

        Returns the timeout and stats objects for the appropriate storage.
        """
        l = self.connections.get(storage_id)
        if l is None:
            l = self.connections[storage_id] = []
        l.append(conn)
        stats = self.stats[storage_id]
        stats.clients += 1
        return self.timeouts[storage_id], stats

    def invalidate(self, conn, storage_id, tid, invalidated=(), info=None):
        """Internal: broadcast info and invalidations to clients.

        This is called from several ZEOStorage methods.

        This can do three different things:

        - If the invalidated argument is non-empty, it broadcasts
          invalidateTransaction() messages to all clients of the given
          storage except the current client (the conn argument).

        - If the invalidated argument is empty and the info argument
          is a non-empty dictionary, it broadcasts info() messages to
          all clients of the given storage, including the current
          client.

        - If both the invalidated argument and the info argument are
          non-empty, it broadcasts invalidateTransaction() messages to all
          clients except the current, and sends an info() message to
          the current client.

        """
        if invalidated:
            if len(self.invq) >= self.invq_bound:
                self.invq.pop()
            self.invq.insert(0, (tid, invalidated))
        for p in self.connections.get(storage_id, ()):
            if invalidated and p is not conn:
                p.client.invalidateTransaction(tid, invalidated)
            elif info is not None:
                p.client.info(info)

    def get_invalidations(self, tid):
        """Return a tid and list of all objects invalidation since tid.

        The tid is the most recent transaction id seen by the client.

        Returns None if it is unable to provide a complete list
        of invalidations for tid.  In this case, client should
        do full cache verification.
        """

        if not self.invq:
            log("invq empty")
            return None, []

        earliest_tid = self.invq[-1][0]
        if earliest_tid > tid:
            log("tid to old for invq %s < %s" % (u64(tid), u64(earliest_tid)))
            return None, []

        oids = {}
        for _tid, L in self.invq:
            if _tid <= tid:
                break
            for key in L:
                oids[key] = 1
        latest_tid = self.invq[0][0]
        return latest_tid, oids.keys()

    def close_server(self):
        """Close the dispatcher so that there are no new connections.

        This is only called from the test suite, AFAICT.
        """
        self.dispatcher.close()
        if self.monitor is not None:
            self.monitor.close()
        for storage in self.storages.values():
            storage.close()
        # Force the asyncore mainloop to exit by hackery, i.e. close
        # every socket in the map.  loop() will return when the map is
        # empty.
        for s in asyncore.socket_map.values():
            try:
                s.close()
            except:
                pass

    def close_conn(self, conn):
        """Internal: remove the given connection from self.connections.

        This is the inverse of register_connection().
        """
        for cl in self.connections.values():
            if conn.obj in cl:
                cl.remove(conn.obj)

class StubTimeoutThread:

    def begin(self, client):
        pass

    def end(self, client):
        pass

class TimeoutThread(threading.Thread):
    """Monitors transaction progress and generates timeouts."""

    # There is one TimeoutThread per storage, because there's one
    # transaction lock per storage.

    def __init__(self, timeout):
        threading.Thread.__init__(self)
        self.setDaemon(1)
        self._timeout = timeout
        self._client = None
        self._deadline = None
        self._cond = threading.Condition() # Protects _client and _deadline
        self._trigger = trigger()

    def begin(self, client):
        # Called from the restart code the "main" thread, whenever the
        # storage lock is being acquired.  (Serialized by asyncore.)
        self._cond.acquire()
        try:
            assert self._client is None
            self._client = client
            self._deadline = time.time() + self._timeout
            self._cond.notify()
        finally:
            self._cond.release()

    def end(self, client):
        # Called from the "main" thread whenever the storage lock is
        # being released.  (Serialized by asyncore.)
        self._cond.acquire()
        try:
            assert self._client is not None
            assert self._client is client
            self._client = None
            self._deadline = None
        finally:
            self._cond.release()

    def run(self):
        # Code running in the thread.
        while 1:
            self._cond.acquire()
            try:
                while self._deadline is None:
                    self._cond.wait()
                howlong = self._deadline - time.time()
                if howlong <= 0:
                    # Prevent reporting timeout more than once
                    self._deadline = None
                client = self._client # For the howlong <= 0 branch below
            finally:
                self._cond.release()
            if howlong <= 0:
                client.log("Transaction timeout after %s seconds" %
                           self._timeout)
                self._trigger.pull_trigger(lambda: client.connection.close())
            else:
                time.sleep(howlong)

def run_in_thread(method, *args):
    t = SlowMethodThread(method, args)
    t.start()
    return t.delay

class SlowMethodThread(threading.Thread):
    """Thread to run potentially slow storage methods.

    Clients can use the delay attribute to access the MTDelay object
    used to send a zrpc response at the right time.
    """

    # Some storage methods can take a long time to complete.  If we
    # run these methods via a standard asyncore read handler, they
    # will block all other server activity until they complete.  To
    # avoid blocking, we spawn a separate thread, return an MTDelay()
    # object, and have the thread reply() when it finishes.

    def __init__(self, method, args):
        threading.Thread.__init__(self)
        self._method = method
        self._args = args
        self.delay = MTDelay()

    def run(self):
        try:
            result = self._method(*self._args)
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception:
            self.delay.error(sys.exc_info())
        else:
            self.delay.reply(result)
