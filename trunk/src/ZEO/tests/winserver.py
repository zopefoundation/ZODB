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
"""Helper file used to launch ZEO server for Windows tests"""

import asyncore
import os
import random
import socket
import threading
import types

import ZEO.StorageServer

class ZEOTestServer(asyncore.dispatcher):
    """A trivial server for killing a server at the end of a test

    The server calls os._exit() as soon as it is connected to.  No
    chance to even send some data down the socket.
    """
    __super_init = asyncore.dispatcher.__init__

    def __init__(self, addr, storage):
        self.__super_init()
        self.storage = storage
        if type(addr) == types.StringType:
            self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        else:
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.bind(addr)
        self.listen(5)

    def handle_accept(self):
        sock, addr = self.accept()
        self.storage.close()
        os._exit(0)

def load_storage_class(name):
    package = __import__("ZODB." + name)
    mod = getattr(package, name)
    return getattr(mod, name)

def main(args):
    ro_svr = 0
    if args[0] == "-r":
        ro_svr = 1
        del args[0]
    port, storage_name, rawargs = args[0], args[1], args[2:]
    klass = load_storage_class(storage_name)
    args = []
    for arg in rawargs:
        if arg.startswith('='):
            arg = eval(arg[1:], {'__builtins__': {}})
        args.append(arg)
    storage = klass(*args)
    zeo_port = int(port)
    test_port = zeo_port + 1
    t = ZEOTestServer(('', test_port), storage)
    addr = ('', zeo_port)
    serv = ZEO.StorageServer.StorageServer(addr, {'1': storage}, ro_svr)
    asyncore.loop()
    # XXX The code below is evil because it can cause deadlocks in zrpc.
    # (To fix it, calling ThreadedAsync._start_loop() might help.)
##    import zLOG
##    label = "winserver:%d" % os.getpid()
##    while asyncore.socket_map:
##        zLOG.LOG(label, zLOG.DEBUG, "map: %r" % asyncore.socket_map)
##        asyncore.poll(30.0)

if __name__ == "__main__":
    import sys
    main(sys.argv[1:])
