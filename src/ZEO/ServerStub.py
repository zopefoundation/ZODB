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
"""RPC stubs for interface exported by StorageServer."""

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

    def _update(self):
        """Handle pending incoming messages.

        This method is typically only used when no asyncore mainloop
        is already active.  It can cause arbitrary callbacks from the
        server to the client to be handled.
        """
        self.rpc.pending()

    def register(self, storage_name, read_only):
        self.rpc.call('register', storage_name, read_only)

    def get_info(self):
        return self.rpc.call('get_info')

    def beginZeoVerify(self):
        self.rpc.callAsync('beginZeoVerify')

    def zeoVerify(self, oid, s, sv):
        self.rpc.callAsync('zeoVerify', oid, s, sv)

    def endZeoVerify(self):
        self.rpc.callAsync('endZeoVerify')

    def new_oids(self, n=None):
        if n is None:
            return self.rpc.call('new_oids')
        else:
            return self.rpc.call('new_oids', n)

    def pack(self, t, wait=None):
        if wait is None:
            self.rpc.call('pack', t)
        else:
            self.rpc.call('pack', t, wait)

    def zeoLoad(self, oid):
        return self.rpc.call('zeoLoad', oid)

    def storea(self, oid, serial, data, version, id):
        self.rpc.callAsync('storea', oid, serial, data, version, id)

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

    def transactionalUndo(self, trans_id, trans):
        return self.rpc.call('transactionalUndo', trans_id, trans)

    def undo(self, trans_id):
        return self.rpc.call('undo', trans_id)

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
        return apply(self.rpc.call, (self.name,)+a, kwa)
