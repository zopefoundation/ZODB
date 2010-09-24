##############################################################################
#
# Copyright (c) 2001, 2002, 2003 Zope Foundation and Contributors.
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
"""RPC stubs for interface exported by StorageServer."""

import os
import time
from ZODB.utils import z64

##
# ZEO storage server.
# <p>
# Remote method calls can be synchronous or asynchronous.  If the call
# is synchronous, the client thread blocks until the call returns.  A
# single client can only have one synchronous request outstanding.  If
# several threads share a single client, threads other than the caller
# will block only if the attempt to make another synchronous call.
# An asynchronous call does not cause the client thread to block.  An
# exception raised by an asynchronous method is logged on the server,
# but is not returned to the client.

class StorageServer:

    """An RPC stub class for the interface exported by ClientStorage.

    This is the interface presented by the StorageServer to the
    ClientStorage; i.e. the ClientStorage calls these methods and they
    are executed in the StorageServer.

    See the StorageServer module for documentation on these methods,
    with the exception of _update(), which is documented here.
    """

    def __init__(self, rpc):
        """Constructor.

        The argument is a connection: an instance of the
        zrpc.connection.Connection class.
        """
        self.rpc = rpc

    def extensionMethod(self, name):
        return ExtensionMethodWrapper(self.rpc, name).call

    ##
    # Register current connection with a storage and a mode.
    # In effect, it is like an open call.
    # @param storage_name a string naming the storage.  This argument
    #        is primarily for backwards compatibility with servers
    #        that supported multiple storages.
    # @param read_only boolean
    # @exception ValueError unknown storage_name or already registered
    # @exception ReadOnlyError storage is read-only and a read-write
    #            connectio was requested

    def register(self, storage_name, read_only):
        self.rpc.call('register', storage_name, read_only)

    ##
    # Return dictionary of meta-data about the storage.
    # @defreturn dict

    def get_info(self):
        return self.rpc.call('get_info')

    ##
    # Check whether the server requires authentication.  Returns
    # the name of the protocol.
    # @defreturn string

    def getAuthProtocol(self):
        return self.rpc.call('getAuthProtocol')

    ##
    # Return id of the last committed transaction
    # @defreturn string

    def lastTransaction(self):
        # Not in protocol version 2.0.0; see __init__()
        return self.rpc.call('lastTransaction') or z64

    ##
    # Return invalidations for all transactions after tid.
    # @param tid transaction id
    # @defreturn 2-tuple, (tid, list)
    # @return tuple containing the last committed transaction
    #         and a list of oids that were invalidated.  Returns
    #         None and an empty list if the server does not have
    #         the list of oids available.

    def getInvalidations(self, tid):
        # Not in protocol version 2.0.0; see __init__()
        return self.rpc.call('getInvalidations', tid)

    ##
    # Check whether a serial number is current for oid.
    # If the serial number is not current, the
    # server will make an asynchronous invalidateVerify() call.
    # @param oid object id
    # @param s serial number
    # @defreturn async

    def zeoVerify(self, oid, s):
        self.rpc.callAsync('zeoVerify', oid, s)

    ##
    # Check whether current serial number is valid for oid.
    # If the serial number is not current, the server will make an
    # asynchronous invalidateVerify() call.
    # @param oid object id
    # @param serial client's current serial number
    # @defreturn async

    def verify(self, oid, serial):
        self.rpc.callAsync('verify', oid, serial)

    ##
    # Signal to the server that cache verification is done.
    # @defreturn async

    def endZeoVerify(self):
        self.rpc.callAsync('endZeoVerify')

    ##
    # Generate a new set of oids.
    # @param n number of new oids to return
    # @defreturn list
    # @return list of oids

    def new_oids(self, n=None):
        if n is None:
            return self.rpc.call('new_oids')
        else:
            return self.rpc.call('new_oids', n)

    ##
    # Pack the storage.
    # @param t pack time
    # @param wait optional, boolean.  If true, the call will not
    #             return until the pack is complete.

    def pack(self, t, wait=None):
        if wait is None:
            self.rpc.call('pack', t)
        else:
            self.rpc.call('pack', t, wait)

    ##
    # Return current data for oid.
    # @param oid object id
    # @defreturn 2-tuple
    # @return 2-tuple, current non-version data, serial number
    # @exception KeyError if oid is not found

    def zeoLoad(self, oid):
        return self.rpc.call('zeoLoad', oid)[:2]

    ##
    # Return current data for oid, and the tid of the
    # transaction that wrote the most recent revision.
    # @param oid object id
    # @defreturn 2-tuple
    # @return data, transaction id
    # @exception KeyError if oid is not found

    def loadEx(self, oid):
        return self.rpc.call("loadEx", oid)

    ##
    # Return non-current data along with transaction ids that identify
    # the lifetime of the specific revision.
    # @param oid object id
    # @param tid a transaction id that provides an upper bound on
    #            the lifetime of the revision.  That is, loadBefore
    #            returns the revision that was current before tid committed.
    # @defreturn 4-tuple
    # @return data, serial numbr, start transaction id, end transaction id

    def loadBefore(self, oid, tid):
        return self.rpc.call("loadBefore", oid, tid)

    ##
    # Storage new revision of oid.
    # @param oid object id
    # @param serial serial number that this transaction read
    # @param data new data record for oid
    # @param id id of current transaction
    # @defreturn async

    def storea(self, oid, serial, data, id):
        self.rpc.callAsync('storea', oid, serial, data, id)

    def checkCurrentSerialInTransaction(self, oid, serial, id):
        self.rpc.callAsync('checkCurrentSerialInTransaction', oid, serial, id)

    def restorea(self, oid, serial, data, prev_txn, id):
        self.rpc.callAsync('restorea', oid, serial, data, prev_txn, id)

    def storeBlob(self, oid, serial, data, blobfilename, txn):

        # Store a blob to the server.  We don't want to real all of
        # the data into memory, so we use a message iterator.  This
        # allows us to read the blob data as needed.

        def store():
            yield ('storeBlobStart', ())
            f = open(blobfilename, 'rb')
            while 1:
                chunk = f.read(59000)
                if not chunk:
                    break
                yield ('storeBlobChunk', (chunk, ))
            f.close()
            yield ('storeBlobEnd', (oid, serial, data, id(txn)))

        self.rpc.callAsyncIterator(store())

    def storeBlobShared(self, oid, serial, data, filename, id):
        self.rpc.callAsync('storeBlobShared', oid, serial, data, filename, id)

    def deleteObject(self, oid, serial, id):
        self.rpc.callAsync('deleteObject', oid, serial, id)

    ##
    # Start two-phase commit for a transaction
    # @param id id used by client to identify current transaction.  The
    #        only purpose of this argument is to distinguish among multiple
    #        threads using a single ClientStorage.
    # @param user name of user committing transaction (can be "")
    # @param description string containing transaction metadata (can be "")
    # @param ext dictionary of extended metadata (?)
    # @param tid optional explicit tid to pass to underlying storage
    # @param status optional status character, e.g "p" for pack
    # @defreturn async

    def tpc_begin(self, id, user, descr, ext, tid, status):
        self.rpc.callAsync('tpc_begin', id, user, descr, ext, tid, status)

    def vote(self, trans_id):
        return self.rpc.call('vote', trans_id)

    def tpc_finish(self, id):
        return self.rpc.call('tpc_finish', id)

    def tpc_abort(self, id):
        self.rpc.callAsync('tpc_abort', id)

    def history(self, oid, length=None):
        if length is None:
            return self.rpc.call('history', oid)
        else:
            return self.rpc.call('history', oid, length)

    def record_iternext(self, next):
        return self.rpc.call('record_iternext', next)

    def sendBlob(self, oid, serial):
        return self.rpc.call('sendBlob', oid, serial)

    def getTid(self, oid):
        return self.rpc.call('getTid', oid)

    def loadSerial(self, oid, serial):
        return self.rpc.call('loadSerial', oid, serial)

    def new_oid(self):
        return self.rpc.call('new_oid')

    def undoa(self, trans_id, trans):
        self.rpc.callAsync('undoa', trans_id, trans)

    def undoLog(self, first, last):
        return self.rpc.call('undoLog', first, last)

    def undoInfo(self, first, last, spec):
        return self.rpc.call('undoInfo', first, last, spec)

    def iterator_start(self, start, stop):
        return self.rpc.call('iterator_start', start, stop)

    def iterator_next(self, iid):
        return self.rpc.call('iterator_next', iid)

    def iterator_record_start(self, txn_iid, tid):
        return self.rpc.call('iterator_record_start', txn_iid, tid)

    def iterator_record_next(self, iid):
        return self.rpc.call('iterator_record_next', iid)

    def iterator_gc(self, iids):
        return self.rpc.callAsync('iterator_gc', iids)

    def server_status(self):
        return self.rpc.call("server_status")

    def set_client_label(self, label):
        return self.rpc.callAsync('set_client_label', label)

class StorageServer308(StorageServer):

    def __init__(self, rpc):
        if rpc.peer_protocol_version == 'Z200':
            self.lastTransaction = lambda: z64
            self.getInvalidations = lambda tid: None
            self.getAuthProtocol = lambda: None

        StorageServer.__init__(self, rpc)

    def history(self, oid, length=None):
        if length is None:
            return self.rpc.call('history', oid, '')
        else:
            return self.rpc.call('history', oid, '', length)

    def getInvalidations(self, tid):
        # Not in protocol version 2.0.0; see __init__()
        result = self.rpc.call('getInvalidations', tid)
        if result is not None:
            result = result[0], [oid for (oid, version) in result[1]]
        return result

    def verify(self, oid, serial):
        self.rpc.callAsync('verify', oid, '', serial)

    def loadEx(self, oid):
        return self.rpc.call("loadEx", oid, '')[:2]

    def storea(self, oid, serial, data, id):
        self.rpc.callAsync('storea', oid, serial, data, '', id)

    def storeBlob(self, oid, serial, data, blobfilename, txn):

        # Store a blob to the server.  We don't want to real all of
        # the data into memory, so we use a message iterator.  This
        # allows us to read the blob data as needed.

        def store():
            yield ('storeBlobStart', ())
            f = open(blobfilename, 'rb')
            while 1:
                chunk = f.read(59000)
                if not chunk:
                    break
                yield ('storeBlobChunk', (chunk, ))
            f.close()
            yield ('storeBlobEnd', (oid, serial, data, '', id(txn)))

        self.rpc.callAsyncIterator(store())

    def storeBlobShared(self, oid, serial, data, filename, id):
        self.rpc.callAsync('storeBlobShared', oid, serial, data, filename,
                           '', id)

    def zeoVerify(self, oid, s):
        self.rpc.callAsync('zeoVerify', oid, s, None)

    def iterator_start(self, start, stop):
        raise NotImplementedError

    def iterator_next(self, iid):
        raise NotImplementedError

    def iterator_record_start(self, txn_iid, tid):
        raise NotImplementedError

    def iterator_record_next(self, iid):
        raise NotImplementedError

    def iterator_gc(self, iids):
        raise NotImplementedError

def stub(client, connection):
    start = time.time()
    # Wait until we know what version the other side is using.
    while connection.peer_protocol_version is None:
        if time.time()-start > 10:
            raise ValueError("Timeout waiting for protocol handshake")
        time.sleep(0.1)

    if connection.peer_protocol_version < 'Z309':
        return StorageServer308(connection)
    return StorageServer(connection)


class ExtensionMethodWrapper:
    def __init__(self, rpc, name):
        self.rpc = rpc
        self.name = name

    def call(self, *a, **kwa):
        return self.rpc.call(self.name, *a, **kwa)
