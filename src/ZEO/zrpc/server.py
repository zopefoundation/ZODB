##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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
import asyncore
import socket

# _has_dualstack: True if the dual-stack sockets are supported
try:
    # Check whether IPv6 sockets can be created
    s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
except (socket.error, AttributeError):
    _has_dualstack = False
else:
    # Check whether enabling dualstack (disabling v6only) works
    try:
        s.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, False)
    except (socket.error, AttributeError):
        _has_dualstack = False
    else:
        _has_dualstack = True
    s.close()
    del s

from ZEO.zrpc.connection import Connection
from ZEO.zrpc.log import log
import ZEO.zrpc.log
import logging

# Export the main asyncore loop
loop = asyncore.loop

class Dispatcher(asyncore.dispatcher):
    """A server that accepts incoming RPC connections"""
    __super_init = asyncore.dispatcher.__init__

    def __init__(self, addr, factory=Connection, map=None):
        self.__super_init(map=map)
        self.addr = addr
        self.factory = factory
        self._open_socket()

    def _open_socket(self):
        if type(self.addr) == tuple:
            if self.addr[0] == '' and _has_dualstack:
                # Wildcard listen on all interfaces, both IPv4 and
                # IPv6 if possible
                self.create_socket(socket.AF_INET6, socket.SOCK_STREAM)
                self.socket.setsockopt(
                    socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, False)
            elif ':' in self.addr[0]:
                self.create_socket(socket.AF_INET6, socket.SOCK_STREAM)
                if _has_dualstack:
                    # On Linux, IPV6_V6ONLY is off by default.
                    # If the user explicitly asked for IPv6, don't bind to IPv4
                    self.socket.setsockopt(
                        socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, True)
            else:
                self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.set_reuse_addr()
        log("listening on %s" % str(self.addr), logging.INFO)
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


        # We could short-circuit the attempt below in some edge cases
        # and avoid a log message by checking for addr being None.
        # Unfortunately, our test for the code below,
        # quick_close_doesnt_kill_server, causes addr to be None and
        # we'd have to write a test for the non-None case, which is
        # *even* harder to provoke. :/ So we'll leave things as they
        # are for now.

        # It might be better to check whether the socket has been
        # closed, but I don't see a way to do that. :(

        # Drop flow-info from IPv6 addresses
        if addr: # Sometimes None on Mac. See above.
            addr = addr[:2]

        try:
            c = self.factory(sock, addr)
        except:
            if sock.fileno() in asyncore.socket_map:
                del asyncore.socket_map[sock.fileno()]
            ZEO.zrpc.log.logger.exception("Error in handle_accept")
        else:
            log("connect from %s: %s" % (repr(addr), c))
