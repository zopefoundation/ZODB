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
"""RPC stubs for interface exported by ClientStorage."""

class ClientStorage:

    """An RPC stub class for the interface exported by ClientStorage.

    This is the interface presented by ClientStorage to the
    StorageServer; i.e. the StorageServer calls these methods and they
    are executed in the ClientStorage.

    See the ClientStorage class for documentation on these methods.

    It is currently important that all methods here are asynchronous
    (meaning they don't have a return value and the caller doesn't
    wait for them to complete), *and* that none of them cause any
    calls from the client to the storage.  This is due to limitations
    in the zrpc subpackage.

    The on-the-wire names of some of the methods don't match the
    Python method names.  That's because the on-the-wire protocol was
    fixed for ZEO 2 and we don't want to change it.  There are some
    aliases in ClientStorage.py to make up for this.
    """

    def __init__(self, rpc):
        """Constructor.

        The argument is a connection: an instance of the
        zrpc.connection.Connection class.
        """
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
