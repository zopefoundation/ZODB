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
"""The StorageServer class and the exception that it may raise.

This server acts as a front-end for one or more real storages, like
file storage or Berkeley storage.

TODO:  Need some basic access control-- a declaration of the methods
exported for invocation by the server.
"""

import asyncore
import cPickle
import logging
import os
import sys
import tempfile
import threading
import time
import itertools

import transaction

import ZODB.serialize
import ZODB.TimeStamp
import ZEO.zrpc.error

import zope.interface
from ZEO.CommitLog import CommitLog
from ZEO.monitor import StorageStats, StatsServer
from ZEO.zrpc.server import Dispatcher
from ZEO.zrpc.connection import ManagedServerConnection, Delay, MTDelay
from ZEO.zrpc.trigger import trigger
from ZEO.Exceptions import AuthError

from ZODB.ConflictResolution import ResolvedSerial
from ZODB.POSException import StorageError, StorageTransactionError
from ZODB.POSException import TransactionError, ReadOnlyError, ConflictError
from ZODB.serialize import referencesf
from ZODB.utils import u64, p64, oid_repr, mktemp
from ZODB.loglevels import BLATHER


logger = logging.getLogger('ZEO.StorageServer')

# TODO:  This used to say "ZSS", which is now implied in the logger name.
# Can this be either set to str(os.getpid()) (if that makes sense) or removed?
_label = "" # default label used for logging.


def set_label():
    """Internal helper to reset the logging label (e.g. after fork())."""
    global _label
    _label = "%s" % os.getpid()


def log(message, level=logging.INFO, label=None, exc_info=False):
    """Internal helper to log a message."""
    label = label or _label
    if label:
        message = "(%s) %s" % (label, message)
    logger.log(level, message, exc_info=exc_info)


class StorageServerError(StorageError):
    """Error reported when an unpicklable exception is raised."""


class ZEOStorage:
    """Proxy to underlying storage for a single remote client."""

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
        self.blob_tempfile = None
        self.blob_log = []
        # The authentication protocol may define extra methods.
        self._extensions = {}
        for func in self.extensions:
            self._extensions[func.func_name] = None
        self._iterators = {}
        self._iterator_ids = itertools.count()
        # Stores the last item that was handed out for a
        # transaction iterator.
        self._txn_iterators_last = {}

    def _finish_auth(self, authenticated):
        if not self.auth_realm:
            return 1
        self.authenticated = authenticated
        return authenticated

    def set_database(self, database):
        self.database = database

    def notifyConnected(self, conn):
        self.connection = conn
        assert conn.peer_protocol_version is not None
        if conn.peer_protocol_version < 'Z309':
            self.client = ClientStub308(conn)
            conn.register_object(ZEOStorage308Adapter(self))
        else:
            self.client = ClientStub(conn)
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
                self.tpc_abort(self.transaction.id)

        else:
            self.log("disconnected")

    def __repr__(self):
        tid = self.transaction and repr(self.transaction.id)
        if self.storage:
            stid = (self.tpc_transaction() and
                    repr(self.tpc_transaction().id))
        else:
            stid = None
        name = self.__class__.__name__
        return "<%s %X trans=%s s_trans=%s>" % (name, id(self), tid, stid)

    def log(self, msg, level=logging.INFO, exc_info=False):
        log(msg, level=level, label=self.log_label, exc_info=exc_info)

    def setup_delegation(self):
        """Delegate several methods to the storage
        """

        storage = self.storage

        info = self.get_info()

        if not info['supportsUndo']:
            self.undoLog = self.undoInfo = lambda *a,**k: ()
            def undo(*a, **k):
                raise NotImplementedError
            self.undo = undo

        self.getTid = storage.getTid
        self.load = storage.load
        self.loadSerial = storage.loadSerial
        record_iternext = getattr(storage, 'record_iternext', None)
        if record_iternext is not None:
            self.record_iternext = record_iternext

        try:
            fn = storage.getExtensionMethods
        except AttributeError:
            pass # no extension methods
        else:
            d = fn()
            self._extensions.update(d)
            for name in d:
                assert not hasattr(self, name)
                setattr(self, name, getattr(storage, name))
        self.lastTransaction = storage.lastTransaction

        try:
            self.tpc_transaction = storage.tpc_transaction
        except AttributeError:
            if hasattr(storage, '_transaction'):
                log("Storage %r doesn't have a tpc_transaction method.\n"
                    "See ZEO.interfaces.IServeable."
                    "Falling back to using _transaction attribute, which\n."
                    "is icky.",
                    logging.ERROR)
                self.tpc_transaction = lambda : storage._transaction
            else:
                raise

    def history(self,tid,size=1):
        # This caters for storages which still accept
        # a version parameter.
        return self.storage.history(tid,size=size)

    def _check_tid(self, tid, exc=None):
        if self.read_only:
            raise ReadOnlyError()
        if self.transaction is None:
            caller = sys._getframe().f_back.f_code.co_name
            self.log("no current transaction: %s()" % caller,
                     level=logging.WARNING)
            if exc is not None:
                raise exc(None, tid)
            else:
                return 0
        if self.transaction.id != tid:
            caller = sys._getframe().f_back.f_code.co_name
            self.log("%s(%s) invalid; current transaction = %s" %
                     (caller, repr(tid), repr(self.transaction.id)),
                     logging.WARNING)
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
            raise AuthError("Client was never authenticated with server!")

        if self.storage is not None:
            self.log("duplicate register() call")
            raise ValueError("duplicate register() call")
        storage = self.server.storages.get(storage_id)
        if storage is None:
            self.log("unknown storage_id: %s" % storage_id)
            raise ValueError("unknown storage: %s" % storage_id)

        if not read_only and (self.read_only or storage.isReadOnly()):
            raise ReadOnlyError()

        self.read_only = self.read_only or read_only
        self.storage_id = storage_id
        self.storage = storage
        self.setup_delegation()
        self.timeout, self.stats = self.server.register_connection(storage_id,
                                                                   self)

    def get_info(self):
        storage = self.storage

        try:
            supportsUndo = storage.supportsUndo
        except AttributeError:
            supportsUndo = False
        else:
            supportsUndo = supportsUndo()

        # Communicate the backend storage interfaces to the client
        storage_provides = zope.interface.providedBy(storage)
        interfaces = []
        for candidate in storage_provides.__iro__:
            interfaces.append((candidate.__module__, candidate.__name__))

        return {'length': len(storage),
                'size': storage.getSize(),
                'name': storage.getName(),
                'supportsUndo': supportsUndo,
                'extensionMethods': self.getExtensionMethods(),
                'supports_record_iternext': hasattr(self, 'record_iternext'),
                'interfaces': tuple(interfaces),
                }

    def get_size_info(self):
        return {'length': len(self.storage),
                'size': self.storage.getSize(),
                }

    def getExtensionMethods(self):
        return self._extensions

    def loadEx(self, oid):
        self.stats.loads += 1
        return self.storage.load(oid, '')

    def loadBefore(self, oid, tid):
        self.stats.loads += 1
        return self.storage.loadBefore(oid, tid)

    def getInvalidations(self, tid):
        invtid, invlist = self.server.get_invalidations(self.storage_id, tid)
        if invtid is None:
            return None
        self.log("Return %d invalidations up to tid %s"
                 % (len(invlist), u64(invtid)))
        return invtid, invlist

    def verify(self, oid, tid):
        try:
            t = self.getTid(oid)
        except KeyError:
            self.client.invalidateVerify(oid)
        else:
            if tid != t:
                self.client.invalidateVerify(oid)

    def zeoVerify(self, oid, s):
        if not self.verifying:
            self.verifying = 1
            self.stats.verifying_clients += 1
        try:
            os = self.getTid(oid)
        except KeyError:
            self.client.invalidateVerify((oid, ''))
            # It's not clear what we should do now.  The KeyError
            # could be caused by an object uncreation, in which case
            # invalidation is right.  It could be an application bug
            # that left a dangling reference, in which case it's bad.
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
        n = min(n, 100)
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

        t = transaction.Transaction()
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

        # Assign the transaction attribute last. This is so we don't
        # think we've entered TPC until everything is set.  Why?
        # Because if we have an error after this, the server will
        # think it is in TPC and the client will think it isn't.  At
        # that point, the client will keep trying to enter TPC and
        # server won't let it.  Errors *after* the tpc_begin call will
        # cause the client to abort the transaction.
        # (Also see https://bugs.launchpad.net/zodb/+bug/374737.)
        self.transaction = t

    def tpc_finish(self, id):
        if not self._check_tid(id):
            return
        assert self.locked
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
        self.stats.aborts += 1
        if self.locked:
            self.storage.tpc_abort(self.transaction)
        self._clear_transaction()

    def _clear_transaction(self):
        # Common code at end of tpc_finish() and tpc_abort()
        self.stats.active_txns -= 1
        self.transaction = None
        self.txnlog.close()
        if self.locked:
            self.locked = 0
            self.timeout.end(self)
            self.stats.lock_time = None
            self.log("Transaction released storage lock", BLATHER)

            # Restart any client waiting for the storage lock.
            while self.storage._waiting:
                delay, zeo_storage = self.storage._waiting.pop(0)
                try:
                    zeo_storage._restart(delay)
                except:
                    self.log("Unexpected error handling waiting transaction",
                             level=logging.WARNING, exc_info=True)
                    zeo_storage.connection.close()
                    continue

                if self.storage._waiting:
                    n = len(self.storage._waiting)
                    self.log("Blocked transaction restarted.  "
                             "Clients waiting: %d" % n)
                else:
                    self.log("Blocked transaction restarted.")

                break

    # The following two methods return values, so they must acquire
    # the storage lock and begin the transaction before returning.

    # It's a bit vile that undo can cause us to get the lock before vote.

    def undo(self, trans_id, id):
        self._check_tid(id, exc=StorageTransactionError)
        if self.locked:
            return self._undo(trans_id)
        else:
            return self._wait(lambda: self._undo(trans_id))

    def vote(self, id):
        self._check_tid(id, exc=StorageTransactionError)
        if self.locked:
            return self._vote()
        else:
            return self._wait(lambda: self._vote())

    # When a delayed transaction is restarted, the dance is
    # complicated.  The restart occurs when one ZEOStorage instance
    # finishes as a transaction and finds another instance is in the
    # _waiting list.

    # It might be better to have a mechanism to explicitly send
    # the finishing transaction's reply before restarting the waiting
    # transaction.  If the restart takes a long time, the previous
    # client will be blocked until it finishes.

    def _wait(self, thunk):
        # Wait for the storage lock to be acquired.
        self._thunk = thunk
        if self.tpc_transaction():
            d = Delay()
            self.storage._waiting.append((d, self))
            self.log("Transaction blocked waiting for storage. "
                     "Clients waiting: %d." % len(self.storage._waiting))
            return d
        else:
            self.log("Transaction acquired storage lock.", BLATHER)
            return self._restart()

    def _restart(self, delay=None):
        # Restart when the storage lock is available.
        if self.txnlog.stores == 1:
            template = "Preparing to commit transaction: %d object, %d bytes"
        else:
            template = "Preparing to commit transaction: %d objects, %d bytes"
        self.log(template % (self.txnlog.stores, self.txnlog.size()),
                 level=BLATHER)

        self.locked = 1
        self.timeout.begin(self)
        self.stats.lock_time = time.time()
        if (self.tid is not None) or (self.status != ' '):
            self.storage.tpc_begin(self.transaction, self.tid, self.status)
        else:
            self.storage.tpc_begin(self.transaction)

        try:
            loads, loader = self.txnlog.get_loader()
            for i in range(loads):
                store = loader.load()
                store_type = store[0]
                store_args = store[1:]

                if store_type == 'd':
                    do_store = self._delete
                elif store_type == 's':
                    do_store = self._store
                elif store_type == 'r':
                    do_store = self._restore
                else:
                    raise ValueError('Invalid store type: %r' % store_type)

                if not do_store(*store_args):
                    break

            # Blob support
            while self.blob_log and not self.store_failed:
                oid, oldserial, data, blobfilename = self.blob_log.pop()
                self._store(oid, oldserial, data, blobfilename)

        except:
            self.storage.tpc_abort(self.transaction)
            self._clear_transaction()
            raise

        resp = self._thunk()
        if delay is not None:
            delay.reply(resp)
        else:
            return resp

    # The public methods of the ZEO client API do not do the real work.
    # They defer work until after the storage lock has been acquired.
    # Most of the real implementations are in methods beginning with
    # an _.

    def deleteObject(self, oid, serial, id):
        self._check_tid(id, exc=StorageTransactionError)
        self.stats.stores += 1
        self.txnlog.delete(oid, serial)

    def storea(self, oid, serial, data, id):
        self._check_tid(id, exc=StorageTransactionError)
        self.stats.stores += 1
        self.txnlog.store(oid, serial, data)

    def restorea(self, oid, serial, data, prev_txn, id):
        self._check_tid(id, exc=StorageTransactionError)
        self.stats.stores += 1
        self.txnlog.restore(oid, serial, data, prev_txn)

    def storeBlobStart(self):
        assert self.blob_tempfile is None
        self.blob_tempfile = tempfile.mkstemp(
            dir=self.storage.temporaryDirectory())

    def storeBlobChunk(self, chunk):
        os.write(self.blob_tempfile[0], chunk)

    def storeBlobEnd(self, oid, serial, data, id):
        fd, tempname = self.blob_tempfile
        self.blob_tempfile = None
        os.close(fd)
        self.blob_log.append((oid, serial, data, tempname))

    def storeBlobShared(self, oid, serial, data, filename, id):
        # Reconstruct the full path from the filename in the OID directory

        if (os.path.sep in filename
            or not (filename.endswith('.tmp')
                    or filename[:-1].endswith('.tmp')
                    )
            ):
            logger.critical(
                "We're under attack! (bad filename to storeBlobShared, %r)",
                filename)
            raise ValueError(filename)

        filename = os.path.join(self.storage.fshelper.getPathForOID(oid),
                                filename)
        self.blob_log.append((oid, serial, data, filename))

    def sendBlob(self, oid, serial):
        self.client.storeBlob(oid, serial, self.storage.loadBlob(oid, serial))

    def _delete(self, oid, serial):
        err = None
        try:
            self.storage.deleteObject(oid, serial, self.transaction)
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception, err:
            self.store_failed = 1
            if isinstance(err, ConflictError):
                self.stats.conflicts += 1
                self.log("conflict error oid=%s msg=%s" %
                         (oid_repr(oid), str(err)), BLATHER)
            if not isinstance(err, TransactionError):
                # Unexpected errors are logged and passed to the client
                self.log("store error: %s, %s" % sys.exc_info()[:2],
                         logging.ERROR, exc_info=True)
            err = self._marshal_error(err)
            # The exception is reported back as newserial for this oid
            self.serials.append((oid, err))
        else:
            self.invalidated.append(oid)

        return err is None

    def _store(self, oid, serial, data, blobfile=None):
        err = None
        try:
            if blobfile is None:
                newserial = self.storage.store(
                    oid, serial, data, '', self.transaction)
            else:
                newserial = self.storage.storeBlob(
                    oid, serial, data, blobfile, '', self.transaction)
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception, err:
            self.store_failed = 1
            if isinstance(err, ConflictError):
                self.stats.conflicts += 1
                self.log("conflict error oid=%s msg=%s" %
                         (oid_repr(oid), str(err)), BLATHER)
            if not isinstance(err, TransactionError):
                # Unexpected errors are logged and passed to the client
                self.log("store error: %s, %s" % sys.exc_info()[:2],
                         logging.ERROR, exc_info=True)
            err = self._marshal_error(err)
            # The exception is reported back as newserial for this oid
            newserial = [(oid, err)]
        else:
            if serial != "\0\0\0\0\0\0\0\0":
                self.invalidated.append(oid)

            if isinstance(newserial, str):
                newserial = [(oid, newserial)]

        if newserial:
            for oid, s in newserial:

                if s == ResolvedSerial:
                    self.stats.conflicts_resolved += 1
                    self.log("conflict resolved oid=%s"
                             % oid_repr(oid), BLATHER)

                self.serials.append((oid, s))

        return err is None

    def _restore(self, oid, serial, data, prev_txn):
        err = None
        try:
            self.storage.restore(oid, serial, data, '', prev_txn,
                                 self.transaction)
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception, err:
            self.store_failed = 1
            if not isinstance(err, TransactionError):
                # Unexpected errors are logged and passed to the client
                self.log("store error: %s, %s" % sys.exc_info()[:2],
                         logging.ERROR, exc_info=True)
            err = self._marshal_error(err)
            # The exception is reported back as newserial for this oid
            self.serials.append((oid, err))

        return err is None

    def _marshal_error(self, error):
        # Try to pickle the exception.  If it can't be pickled,
        # the RPC response would fail, so use something that can be pickled.
        pickler = cPickle.Pickler()
        pickler.fast = 1
        try:
            pickler.dump(error, 1)
        except:
            msg = "Couldn't pickle storage exception: %s" % repr(error)
            self.log(msg, logging.ERROR)
            error = StorageServerError(msg)
        return error

    def _vote(self):
        if not self.store_failed:
            # Only call tpc_vote of no store call failed, otherwise
            # the serialnos() call will deliver an exception that will be
            # handled by the client in its tpc_vote() method.
            serials = self.storage.tpc_vote(self.transaction)
            if serials:
                self.serials.extend(serials)

        self.client.serialnos(self.serials)
        return

    def _undo(self, trans_id):
        tid, oids = self.storage.undo(trans_id, self.transaction)
        self.invalidated.extend(oids)
        return tid, oids

    # IStorageIteration support

    def iterator_start(self, start, stop):
        iid = self._iterator_ids.next()
        self._iterators[iid] = iter(self.storage.iterator(start, stop))
        return iid

    def iterator_next(self, iid):
        iterator = self._iterators[iid]
        try:
            info = iterator.next()
        except StopIteration:
            del self._iterators[iid]
            item = None
            if iid in self._txn_iterators_last:
                del self._txn_iterators_last[iid]
        else:
            item = (info.tid,
                    info.status,
                    info.user,
                    info.description,
                    info.extension)
            # Keep a reference to the last iterator result to allow starting a
            # record iterator off it.
            self._txn_iterators_last[iid] = info
        return item

    def iterator_record_start(self, txn_iid, tid):
        record_iid = self._iterator_ids.next()
        txn_info = self._txn_iterators_last[txn_iid]
        if txn_info.tid != tid:
            raise Exception(
                'Out-of-order request for record iterator for transaction %r'
                % tid)
        self._iterators[record_iid] = iter(txn_info)
        return record_iid

    def iterator_record_next(self, iid):
        iterator = self._iterators[iid]
        try:
            info = iterator.next()
        except StopIteration:
            del self._iterators[iid]
            item = None
        else:
            item = (info.oid,
                    info.tid,
                    info.data,
                    info.data_txn)
        return item

    def iterator_gc(self, iids):
        for iid in iids:
            self._iterators.pop(iid, None)


class StorageServerDB:

    def __init__(self, server, storage_id):
        self.server = server
        self.storage_id = storage_id
        self.references = ZODB.serialize.referencesf

    def invalidate(self, tid, oids, version=''):
        if version:
            raise StorageServerError("Versions aren't supported.")
        storage_id = self.storage_id
        self.server.invalidate(None, storage_id, tid, oids)
        for zeo_server in self.server.connections.get(storage_id, ())[:]:
            try:
                zeo_server.connection.poll()
            except ZEO.zrpc.error.DisconnectedError:
                pass
            else:
                break # We only need to pull one :)

    def invalidateCache(self):
        self.server._invalidateCache(self.storage_id)


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
                 invalidation_age=None,
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

        invalidation_age --
            If the invalidation queue isn't big enough to support a
            quick verification, but the last transaction seen by a
            client is younger than the invalidation age, then
            invalidations will be computed by iterating over
            transactions later than the given transaction.

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
        # A list, by server, of at most invalidation_queue_size invalidations.
        # The list is kept in sorted order with the most recent
        # invalidation at the front.  The list never has more than
        # self.invq_bound elements.
        self.invq_bound = invalidation_queue_size
        self.invq = {}
        for name, storage in storages.items():
            self._setup_invq(name, storage)
            storage.registerDB(StorageServerDB(self, name))
        self.invalidation_age = invalidation_age
        self.connections = {}
        self.dispatcher = self.DispatcherClass(addr,
                                               factory=self.new_connection)
        self.stats = {}
        self.timeouts = {}
        for name in self.storages.keys():
            self.connections[name] = []
            self.stats[name] = StorageStats(self.connections[name])
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

    def _setup_invq(self, name, storage):
        lastInvalidations = getattr(storage, 'lastInvalidations', None)
        if lastInvalidations is None:
            self.invq[name] = [(storage.lastTransaction(), None)]
        else:
            self.invq[name] = list(
                lastInvalidations(self.invq_bound)
                )
            self.invq[name].reverse()


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
        log("new connection %s: %s" % (addr, repr(c)))
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
        self.connections[storage_id].append(conn)
        return self.timeouts[storage_id], self.stats[storage_id]

    def _invalidateCache(self, storage_id):
        """We need to invalidate any caches we have.

        This basically means telling our clients to
        invalidate/revalidate their caches. We do this by closing them
        and making them reconnect.
        """

        # This method can be called from foreign threads.  We have to
        # worry about interaction with the main thread.

        # 1. We modify self.invq which is read by get_invalidations
        #    below. This is why get_invalidations makes a copy of
        #    self.invq.

        # 2. We access connections.  There are two dangers:
        #
        # a. We miss a new connection.  This is not a problem because
        #    if a client connects after we get the list of connections,
        #    then it will have to read the invalidation queue, which
        #    has already been reset.
        #
        # b. A connection is closes while we are iterating.  This
        #    doesn't matter, bacause we can call should_close on a closed
        #    connection.

        # Rebuild invq
        self._setup_invq(storage_id, self.storages[storage_id])

        # Make a copy since we are going to be mutating the
        # connections indirectoy by closing them.  We don't care about
        # later transactions since they will have to validate their
        # caches anyway.
        for p in self.connections[storage_id][:]:
            try:
                p.connection.should_close()
                p.connection.trigger.pull_trigger()
            except ZEO.zrpc.error.DisconnectedError:
                pass


    def invalidate(self, conn, storage_id, tid, invalidated=(), info=None):
        """Internal: broadcast info and invalidations to clients.

        This is called from several ZEOStorage methods.

        invalidated is a sequence of oids.

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

        # This method can be called from foreign threads.  We have to
        # worry about interaction with the main thread.

        # 1. We modify self.invq which is read by get_invalidations
        #    below. This is why get_invalidations makes a copy of
        #    self.invq.

        # 2. We access connections.  There are two dangers:
        #
        # a. We miss a new connection.  This is not a problem because
        #    we are called while the storage lock is held.  A new
        #    connection that tries to read data won't read committed
        #    data without first recieving an invalidation.  Also, if a
        #    client connects after getting the list of connections,
        #    then it will have to read the invalidation queue, which
        #    has been updated to reflect the invalidations.
        #
        # b. A connection is closes while we are iterating. We'll need
        #    to cactch and ignore Disconnected errors.


        if invalidated:
            invq = self.invq[storage_id]
            if len(invq) >= self.invq_bound:
                invq.pop()
            invq.insert(0, (tid, invalidated))

        for p in self.connections[storage_id]:
            try:
                if invalidated and p is not conn:
                    p.client.invalidateTransaction(tid, invalidated)
                elif info is not None:
                    p.client.info(info)
            except ZEO.zrpc.error.DisconnectedError:
                pass

    def get_invalidations(self, storage_id, tid):
        """Return a tid and list of all objects invalidation since tid.

        The tid is the most recent transaction id seen by the client.

        Returns None if it is unable to provide a complete list
        of invalidations for tid.  In this case, client should
        do full cache verification.
        """

        # We make a copy of invq because it might be modified by a
        # foreign (other than main thread) calling invalidate above.
        invq = self.invq[storage_id][:]

        oids = set()
        latest_tid = None
        if invq and invq[-1][0] <= tid:
            # We have needed data in the queue
            for _tid, L in invq:
                if _tid <= tid:
                    break
                oids.update(L)
            latest_tid = invq[0][0]
        elif (self.invalidation_age and
              (self.invalidation_age >
               (time.time()-ZODB.TimeStamp.TimeStamp(tid).timeTime())
               )
              ):
            for t in self.storages[storage_id].iterator(p64(u64(tid)+1)):
                for r in t:
                    oids.add(r.oid)
                latest_tid = t.tid
        elif not invq:
            log("invq empty")
        else:
            log("tid to old for invq %s < %s" % (u64(tid), u64(invq[-1][0])))

        return latest_tid, list(oids)

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
        asyncore.socket_map.clear()

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


class ClientStub:

    def __init__(self, rpc):
        self.rpc = rpc

    def beginVerify(self):
        self.rpc.callAsync('beginVerify')

    def invalidateVerify(self, args):
        self.rpc.callAsync('invalidateVerify', args)

    def endVerify(self):
        self.rpc.callAsync('endVerify')

    def invalidateTransaction(self, tid, args):
        self.rpc.callAsyncNoPoll('invalidateTransaction', tid, args)

    def serialnos(self, arg):
        self.rpc.callAsync('serialnos', arg)

    def info(self, arg):
        self.rpc.callAsync('info', arg)

    def storeBlob(self, oid, serial, blobfilename):

        def store():
            yield ('receiveBlobStart', (oid, serial))
            f = open(blobfilename, 'rb')
            while 1:
                chunk = f.read(59000)
                if not chunk:
                    break
                yield ('receiveBlobChunk', (oid, serial, chunk, ))
            f.close()
            yield ('receiveBlobStop', (oid, serial))

        self.rpc.callAsyncIterator(store())

class ClientStub308(ClientStub):

    def invalidateTransaction(self, tid, args):
        self.rpc.callAsyncNoPoll(
            'invalidateTransaction', tid, [(arg, '') for arg in args])

    def invalidateVerify(self, oid):
        self.rpc.callAsync('invalidateVerify', (oid, ''))

class ZEOStorage308Adapter:

    def __init__(self, storage):
        self.storage = storage

    def __eq__(self, other):
        return self is other or self.storage is other

    def getSerial(self, oid):
        return self.storage.loadEx(oid)[1] # Z200

    def history(self, oid, version, size=1):
        if version:
            raise ValueError("Versions aren't supported.")
        return self.storage.history(oid, size=size)

    def getInvalidations(self, tid):
        result = self.storage.getInvalidations(tid)
        if result is not None:
            result = result[0], [(oid, '') for oid in result[1]]
        return result

    def verify(self, oid, version, tid):
        if version:
            raise StorageServerError("Versions aren't supported.")
        return self.storage.verify(oid, tid)

    def loadEx(self, oid, version=''):
        if version:
            raise StorageServerError("Versions aren't supported.")
        data, serial = self.storage.loadEx(oid)
        return data, serial, ''

    def storea(self, oid, serial, data, version, id):
        if version:
            raise StorageServerError("Versions aren't supported.")
        self.storage.storea(oid, serial, data, id)

    def storeBlobEnd(self, oid, serial, data, version, id):
        if version:
            raise StorageServerError("Versions aren't supported.")
        self.storage.storeBlobEnd(oid, serial, data, id)

    def storeBlobShared(self, oid, serial, data, filename, version, id):
        if version:
            raise StorageServerError("Versions aren't supported.")
        self.storage.storeBlobShared(oid, serial, data, filename, id)

    def getInfo(self):
        result = self.storage.getInfo()
        result['supportsVersions'] = False
        return result

    def zeoVerify(self, oid, s, sv=None):
        if sv:
            raise StorageServerError("Versions aren't supported.")
        self.storage.zeoVerify(oid, s)

    def modifiedInVersion(self, oid):
        return ''

    def versions(self):
        return ()

    def versionEmpty(self, version):
        return True

    def commitVersion(self, *a, **k):
        raise NotImplementedError

    abortVersion = commitVersion

    def zeoLoad(self, oid):             # Z200
        p, s = self.storage.loadEx(oid)
        return p, s, '', None, None

    def __getattr__(self, name):
        return getattr(self.storage, name)


