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
import asyncore
import socket
import types

from ZEO.zrpc.connection import Connection, Delay
from ZEO.zrpc.log import log
import zLOG

# Export the main asyncore loop
loop = asyncore.loop

class Dispatcher(asyncore.dispatcher):
    """A server that accepts incoming RPC connections"""
    __super_init = asyncore.dispatcher.__init__

    reuse_addr = 1

    def __init__(self, addr, factory=Connection, reuse_addr=None):
        self.__super_init()
        self.addr = addr
        self.factory = factory
        self.clients = []
        if reuse_addr is not None:
            self.reuse_addr = reuse_addr
        self._open_socket()

    def _open_socket(self):
        if type(self.addr) == types.TupleType:
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.set_reuse_addr()
        log("listening on %s" % str(self.addr), zLOG.INFO)
        self.bind(self.addr)
        self.listen(5)

    def writable(self):
        return 0

    def readable(self):
        return 1

    def handle_accept(self):
        try:
            sock, addr = self.accept()
        except socket.error, msg:
            log("accepted failed: %s" % msg)
            return
        c = self.factory(sock, addr)
        log("connect from %s: %s" % (repr(addr), c))
        self.clients.append(c)
