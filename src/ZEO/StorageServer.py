##############################################################################
# 
# Zope Public License (ZPL) Version 1.0
# -------------------------------------
# 
# Copyright (c) Digital Creations.  All rights reserved.
# 
# This license has been certified as Open Source(tm).
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
# 1. Redistributions in source code must retain the above copyright
#    notice, this list of conditions, and the following disclaimer.
# 
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions, and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
# 
# 3. Digital Creations requests that attribution be given to Zope
#    in any manner possible. Zope includes a "Powered by Zope"
#    button that is installed by default. While it is not a license
#    violation to remove this button, it is requested that the
#    attribution remain. A significant investment has been put
#    into Zope, and this effort will continue if the Zope community
#    continues to grow. This is one way to assure that growth.
# 
# 4. All advertising materials and documentation mentioning
#    features derived from or use of this software must display
#    the following acknowledgement:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    In the event that the product being advertised includes an
#    intact Zope distribution (with copyright and license included)
#    then this clause is waived.
# 
# 5. Names associated with Zope or Digital Creations must not be used to
#    endorse or promote products derived from this software without
#    prior written permission from Digital Creations.
# 
# 6. Modified redistributions of any form whatsoever must retain
#    the following acknowledgment:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    Intact (re-)distributions of any official Zope release do not
#    require an external acknowledgement.
# 
# 7. Modifications are encouraged but must be packaged separately as
#    patches to official Zope releases.  Distributions that do not
#    clearly separate the patches from the original work must be clearly
#    labeled as unofficial distributions.  Modifications which do not
#    carry the name Zope may be packaged in any form, as long as they
#    conform to all of the clauses above.
#
# 
# Disclaimer
# 
#   THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS ``AS IS'' AND ANY
#   EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
#   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL CREATIONS OR ITS
#   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
#   USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#   ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#   OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
#   OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
#   SUCH DAMAGE.
# 
# 
# This software consists of contributions made by Digital Creations and
# many individuals on behalf of Digital Creations.  Specific
# attributions are listed in the accompanying credits file.
# 
##############################################################################
"""Network ZODB storage server

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
import types

import ClientStub
import zrpc2
import zLOG

from zrpc2 import Dispatcher, Handler, ManagedServerConnection, Delay
from ZODB.POSException import StorageError, StorageTransactionError, \
     TransactionError, ReadOnlyError
from ZODB.referencesf import referencesf
from ZODB.Transaction import Transaction

# We create a special fast pickler! This allows us
# to create slightly more efficient pickles and
# to create them a tad faster.
pickler = cPickle.Pickler()
pickler.fast = 1 # Don't use the memo
dump = pickler.dump

def log(message, level=zLOG.INFO, label="ZEO Server:%s" % os.getpid(),
        error=None):
    zLOG.LOG(label, level, message, error=error)

class StorageServerError(StorageError):
    pass

class StorageServer:
    def __init__(self, addr, storages, read_only=0):
        # XXX should read_only be a per-storage option? not yet...
        self.addr = addr
        self.storages = storages
        self.read_only = read_only
        self.connections = {}
        for name, store in storages.items():
            fixup_storage(store)
        self.dispatcher = Dispatcher(addr, factory=self.newConnection,
                                     reuse_addr=1)

    def newConnection(self, sock, addr, nil):
        c = ManagedServerConnection(sock, addr, None, self)
        c.register_object(StorageProxy(self, c))
        return c
        
    def register(self, storage_id, proxy):
        """Register a connection's use with a particular storage.

        This information is needed to handle invalidation.
        """
        l = self.connections.get(storage_id)
        if l is None:
            l = self.connections[storage_id] = []
            # intialize waiting list
            self.storages[storage_id]._StorageProxy__waiting = []
        l.append(proxy)

    def invalidate(self, conn, storage_id, invalidated=(), info=0):
        for p in self.connections[storage_id]:
            if invalidated and p is not conn:
                p.client.Invalidate(invalidated)
            else:
                p.client.info(info)

    def close_server(self):
        # Close the dispatcher so that there are no new connections.
        self.dispatcher.close()
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

    def close(self, conn):
        # XXX who calls this?
        # called when conn is closed
        # way too inefficient
        removed = 0
        for sid, cl in self.connections.items():
            if conn.obj in cl:
                cl.remove(conn.obj)
                removed = 1

class StorageProxy(Handler):
    def __init__(self, server, conn):
        self.server = server
        self.client = ClientStub.ClientStorage(conn)
        self.__storage = None
        self.__invalidated = []
        self._transaction = None

    def __repr__(self):
        tid = self._transaction and repr(self._transaction.id)
        if self.__storage:
            stid = self.__storage._transaction and \
                   repr(self.__storage._transaction.id)
        else:
            stid = None
        return "<StorageProxy %X trans=%s s_trans=%s>" % (id(self), tid,
                                                          stid)

    def _log(self, msg, level=zLOG.INFO, error=None, pid=os.getpid()):
        zLOG.LOG("ZEO Server %s %X" % (pid, id(self)),
                   level, msg, error=error)

    def setup_delegation(self):
        """Delegate several methods to the storage"""
        self.undoInfo = self.__storage.undoInfo
        self.undoLog = self.__storage.undoLog
        self.versionEmpty = self.__storage.versionEmpty
        self.versions = self.__storage.versions
        self.history = self.__storage.history
        self.load = self.__storage.load
        self.loadSerial = self.__storage.loadSerial

    def _check_tid(self, tid, exc=None):
        caller = sys._getframe().f_back.f_code.co_name
        if self._transaction is None:
            self._log("no current transaction: %s()" % caller,
                zLOG.PROBLEM)
            if exc is not None:
                raise exc(None, tid)
            else:
                return 0
        if self._transaction.id != tid:
            self._log("%s(%s) invalid; current transaction = %s" % \
                (caller, repr(tid), repr(self._transaction.id)),
                zLOG.PROBLEM)
            if exc is not None:
                raise exc(self._transaction.id, tid)
            else:
                return 0
        return 1

    def register(self, storage_id, read_only):
        """Select the storage that this client will use

        This method must be the first one called by the client.
        """
        storage = self.server.storages.get(storage_id)
        if storage is None:
            self._log("unknown storage_id: %s" % storage_id)
            raise ValueError, "unknown storage: %s" % storage_id

        if not read_only and (self.server.read_only or storage.isReadOnly()):
            raise ReadOnlyError()

        self.__storage_id = storage_id
        self.__storage = storage
        self.setup_delegation()
        self.server.register(storage_id, self)
        self._log("registered storage %s: %s" % (storage_id, storage))

    def get_info(self):
        return {'length': len(self.__storage),
                'size': self.__storage.getSize(),
                'name': self.__storage.getName(),
                'supportsUndo': self.__storage.supportsUndo(),
                'supportsVersions': self.__storage.supportsVersions(),
                'supportsTransactionalUndo':
                self.__storage.supportsTransactionalUndo(),
                }

    def get_size_info(self):
        return {'length': len(self.__storage),
                'size': self.__storage.getSize(),
                }

    def zeoLoad(self, oid):
        v = self.__storage.modifiedInVersion(oid)
        if v:
            pv, sv = self.__storage.load(oid, v)
        else:
            pv = sv = None
        try:
            p, s = self.__storage.load(oid, '')
        except KeyError:
            if sv:
                # Created in version, no non-version data
                p = s = None
            else:
                raise
        return p, s, v, pv, sv

    def beginZeoVerify(self):
        self.client.beginVerify()

    def zeoVerify(self, oid, s, sv):
        try:
            p, os, v, pv, osv = self.zeoLoad(oid)
        except: # except what?
            return None
        if os != s:
            self.client.invalidate((oid, ''))
        elif osv != sv:
            self.client.invalidate((oid, v))

    def endZeoVerify(self):
        self.client.endVerify()

    def pack(self, t, wait=0):
        t = threading.Thread(target=self._pack, args=(t, wait))
        t.start()

    def _pack(self, t, wait=0):
        try: 
            self.__storage.pack(t, referencesf)
        except:
            self._log('ZEO Server', zLOG.ERROR,
                      'Pack failed for %s' % self.__storage_id,
                      error=sys.exc_info())
            if wait:
                raise
        else:
            if not wait:
                # Broadcast new size statistics
                self.server.invalidate(0, self.__storage_id, (),
                                       self.get_size_info())

    def abortVersion(self, src, id):
        self._check_tid(id, exc=StorageTransactionError)
        oids = self.__storage.abortVersion(src, self._transaction)
        for oid in oids:
            self.__invalidated.append((oid, src))
        return oids

    def commitVersion(self, src, dest, id):
        self._check_tid(id, exc=StorageTransactionError)
        oids = self.__storage.commitVersion(src, dest, self._transaction)
        for oid in oids:
            self.__invalidated.append((oid, dest))
            if dest:
                self.__invalidated.append((oid, src))
        return oids

    def storea(self, oid, serial, data, version, id):
        self._check_tid(id, exc=StorageTransactionError)
        try:
            # XXX does this stmt need to be in the try/except?
        
            newserial = self.__storage.store(oid, serial, data, version,
                                             self._transaction)
        except TransactionError, v:
            # This is a normal transaction error such as a conflict error
            # or a version lock or conflict error. It doesn't need to be
            # logged.
            self._log("transaction error: %s" % repr(v))
            newserial = v
        except:
            # all errors need to be serialized to prevent unexpected
            # returns, which would screw up the return handling.
            # IOW, Anything that ends up here is evil enough to be logged.
            error = sys.exc_info()
            self._log('store error: %s: %s' % (error[0], error[1]),
                      zLOG.ERROR, error=error)
            newserial = sys.exc_info()[1]
        else:
            if serial != '\0\0\0\0\0\0\0\0':
                self.__invalidated.append((oid, version))

        try:
            nil = dump(newserial, 1)
        except:
            self._log("couldn't pickle newserial: %s" % repr(newserial),
                      zLOG.ERROR)
            dump('', 1) # clear pickler
            r = StorageServerError("Couldn't pickle exception %s" % \
                                   `newserial`)
            newserial = r

        self.client.serialno((oid, newserial))

    def vote(self, id):
        self._check_tid(id, exc=StorageTransactionError)
        self.__storage.tpc_vote(self._transaction)

    def transactionalUndo(self, trans_id, id):
        self._check_tid(id, exc=StorageTransactionError)
        return self.__storage.transactionalUndo(trans_id, self._transaction)
        
    def undo(self, transaction_id):
        oids = self.__storage.undo(transaction_id)
        if oids:
            self.server.invalidate(self, self.__storage_id,
                                   map(lambda oid: (oid, None, ''), oids))
            return oids
        return ()

    # When multiple clients are using a single storage, there are several
    # different _transaction attributes to keep track of.  Each
    # StorageProxy object has a single _transaction that refers to its
    # current transaction.  The storage (self.__storage) has another
    # _transaction that is used for the *real* transaction.

    # The real trick comes with the __waiting queue for a storage.
    # When a StorageProxy pulls a new transaction from the queue, it
    # must inform the new transaction's proxy.  (The two proxies may
    # be the same.)  The new transaction's proxy sets its _transaction
    # and continues from there.

    def tpc_begin(self, id, user, description, ext):
        if self._transaction is not None:
            if self._transaction.id == id:
                self._log("duplicate tpc_begin(%s)" % repr(id))
                return
            else:
                raise StorageTransactionError("Multiple simultaneous tpc_begin"
                                              " requests from one client.")

        t = Transaction()
        t.id = id
        t.user = user
        t.description = description
        t._extension = ext

        if self.__storage._transaction is not None:
            d = zrpc2.Delay()
            self.__storage.__waiting.append((d, self, t))
            return d

        self._transaction = t
        self.__storage.tpc_begin(t)
        self.__invalidated = []

    def tpc_finish(self, id):
        if not self._check_tid(id):
            return

        r = self.__storage.tpc_finish(self._transaction)
        assert self.__storage._transaction is None

        if self.__invalidated:
            self.server.invalidate(self, self.__storage_id,
                                   self.__invalidated,
                                   self.get_size_info())

        if not self._handle_waiting():
            self._transaction = None
            self.__invalidated = []

    def tpc_abort(self, id):
        if not self._check_tid(id):
            return
        r = self.__storage.tpc_abort(self._transaction)
        assert self.__storage._transaction is None

        if not self._handle_waiting():
            self._transaction = None
            self.__invalidated = []

    def _restart_delayed_transaction(self, delay, trans):
        self._transaction = trans
        self.__storage.tpc_begin(trans)
        self.__invalidated = []
        assert self._transaction.id == self.__storage._transaction.id
        delay.reply(None)

    def _handle_waiting(self):
        if self.__storage.__waiting:
            delay, proxy, trans = self.__storage.__waiting.pop(0)
            proxy._restart_delayed_transaction(delay, trans)
            if self is proxy:
                return 1
        
    def new_oids(self, n=100):
        """Return a sequence of n new oids, where n defaults to 100"""
        if n < 0:
            n = 1
        return [self.__storage.new_oid() for i in range(n)]

def fixup_storage(storage):
    # backwards compatibility hack
    if not hasattr(storage,'tpc_vote'):
        storage.tpc_vote = lambda *args: None
