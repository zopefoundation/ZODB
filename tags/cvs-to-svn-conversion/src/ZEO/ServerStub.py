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
"""RPC stubs for interface exported by StorageServer."""

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
        # Wait until we know what version the other side is using.
        while rpc.peer_protocol_version is None:
            rpc.pending()
        if rpc.peer_protocol_version == 'Z200':
            self.lastTransaction = lambda: None
            self.getInvalidations = lambda tid: None
            self.getAuthProtocol = lambda: None

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
        return self.rpc.call('lastTransaction')

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
    # Check whether serial numbers s and sv are current for oid.
    # If one or both of the serial numbers are not current, the
    # server will make an asynchronous invalidateVerify() call.
    # @param oid object id
    # @param s serial number on non-version data
    # @param sv serial number of version data or None
    # @defreturn async

    def zeoVerify(self, oid, s, sv):
        self.rpc.callAsync('zeoVerify', oid, s, sv)

    ##
    # Check whether current serial number is valid for oid and version.
    # If the serial number is not current, the server will make an
    # asynchronous invalidateVerify() call.
    # @param oid object id
    # @param version name of version for oid
    # @param serial client's current serial number
    # @defreturn async

    def verify(self, oid, version, serial):
        self.rpc.callAsync('verify', oid, version, serial)

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
    # Return current data for oid.  Version data is returned if
    # present.
    # @param oid object id
    # @defreturn 5-tuple
    # @return 5-tuple, current non-version data, serial number,
    #         version name, version data, version data serial number
    # @exception KeyError if oid is not found

    def zeoLoad(self, oid):
        return self.rpc.call('zeoLoad', oid)

    ##
    # Return current data for oid along with tid if transaction that
    # wrote the date.
    # @param oid object id
    # @param version string, name of version
    # @defreturn 4-tuple
    # @return data, serial number, transaction id, version,
    #         where version is the name of the version the data came
    #         from or "" for non-version data
    # @exception KeyError if oid is not found

    def loadEx(self, oid, version):
        return self.rpc.call("loadEx", oid, version)

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
    # @param version name of version or ""
    # @param id id of current transaction
    # @defreturn async

    def storea(self, oid, serial, data, version, id):
        self.rpc.callAsync('storea', oid, serial, data, version, id)

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
        return self.rpc.call('tpc_begin', id, user, descr, ext, tid, status)

    def vote(self, trans_id):
        return self.rpc.call('vote', trans_id)

    def tpc_finish(self, id):
        return self.rpc.call('tpc_finish', id)

    def tpc_abort(self, id):
        self.rpc.callAsync('tpc_abort', id)

    def abortVersion(self, src, id):
        return self.rpc.call('abortVersion', src, id)

    def commitVersion(self, src, dest, id):
        return self.rpc.call('commitVersion', src, dest, id)

    def history(self, oid, version, length=None):
        if length is None:
            return self.rpc.call('history', oid, version)
        else:
            return self.rpc.call('history', oid, version, length)

    def load(self, oid, version):
        return self.rpc.call('load', oid, version)

    def getSerial(self, oid):
        return self.rpc.call('getSerial', oid)

    def loadSerial(self, oid, serial):
        return self.rpc.call('loadSerial', oid, serial)

    def modifiedInVersion(self, oid):
        return self.rpc.call('modifiedInVersion', oid)

    def new_oid(self, last=None):
        if last is None:
            return self.rpc.call('new_oid')
        else:
            return self.rpc.call('new_oid', last)

    def store(self, oid, serial, data, version, trans):
        return self.rpc.call('store', oid, serial, data, version, trans)

    def undo(self, trans_id, trans):
        return self.rpc.call('undo', trans_id, trans)

    def undoLog(self, first, last):
        return self.rpc.call('undoLog', first, last)

    def undoInfo(self, first, last, spec):
        return self.rpc.call('undoInfo', first, last, spec)

    def versionEmpty(self, vers):
        return self.rpc.call('versionEmpty', vers)

    def versions(self, max=None):
        if max is None:
            return self.rpc.call('versions')
        else:
            return self.rpc.call('versions', max)

class ExtensionMethodWrapper:
    def __init__(self, rpc, name):
        self.rpc = rpc
        self.name = name

    def call(self, *a, **kwa):
        return self.rpc.call(self.name, *a, **kwa)
