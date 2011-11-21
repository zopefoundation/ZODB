##############################################################################
#
# Copyright Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

# Testing the current ZEO implementation is rather hard due to the
# architecture, which mixes concerns, especially between application
# and networking.  Still, it's not as bad as it could be.

# The 2 most important classes in the architecture are ZEOStorage and
# StorageServer. A ZEOStorage is created for each client connection.
# The StorageServer maintains data shared or needed for coordination
# among clients.

# The other important part of the architecture is connections.
# Connections are used by ZEOStorages to send messages or return data
# to clients.

# Here, we'll try to provide some testing infrastructure to isolate
# servers from the network.

import ZEO.StorageServer
import ZEO.zrpc.connection
import ZEO.zrpc.error
import ZODB.MappingStorage

class StorageServer(ZEO.StorageServer.StorageServer):

    def __init__(self, addr='test_addr', storages=None, **kw):
        if storages is None:
            storages = {'1': ZODB.MappingStorage.MappingStorage()}
        ZEO.StorageServer.StorageServer.__init__(self, addr, storages, **kw)


    class DispatcherClass:
        __init__ = lambda *a, **kw: None
        class socket:
            getsockname = staticmethod(lambda : 'socket')

class Connection:

    peer_protocol_version = ZEO.zrpc.connection.Connection.current_protocol
    connected = True

    def __init__(self, name='connection', addr=''):
        name = str(name)
        self.name = name
        self.addr = addr or 'test-addr-'+name

    def close(self):
        print self.name, 'closed'
        self.connected = False

    def poll(self):
        if not self.connected:
            raise ZEO.zrpc.error.DisconnectedError()

    def callAsync(self, meth, *args):
        print self.name, 'callAsync', meth, repr(args)

    callAsyncNoPoll = callAsync

    def call_from_thread(self, *args):
        if args:
            args[0](*args[1:])

    def send_reply(self, *args):
        pass

def client(server, name='client', addr=''):
    zs = ZEO.StorageServer.ZEOStorage(server)
    zs.notifyConnected(Connection(name, addr))
    zs.register('1', 0)
    return zs
