##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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
"""A debugging version of the server that records network activity."""

import struct
import time

from ZEO.StorageServer import StorageServer, log
from ZEO.zrpc.server import ManagedServerConnection

# a bunch of codes
NEW_CONN = 1
CLOSE_CONN = 2
DATA = 3
ERROR = 4

class DebugManagedServerConnection(ManagedServerConnection):

    def __init__(self, sock, addr, obj, mgr):
        # mgr is the DebugServer instance
        self.mgr = mgr
        self.__super_init(sock, addr, obj)
        record_id = mgr._record_connection(addr)
        self._record = lambda code, data: mgr._record(record_id, code, data)
        self.obj.notifyConnected(self)

    def close(self):
        self._record(CLOSE_CONN, "")
        ManagedServerConnection.close(self)

    # override the lowest-level of asyncore's connection

    def recv(self, buffer_size):
        try:
            data = self.socket.recv(buffer_size)
            if not data:
                # a closed connection is indicated by signaling
                # a read condition, and having recv() return 0.
                self.handle_close()
                return ''
            else:
                self._record(DATA, data)
                return data
        except socket.error, why:
            # winsock sometimes throws ENOTCONN
            self._record(ERROR, why)
            if why[0] in [ECONNRESET, ENOTCONN, ESHUTDOWN]:
                self.handle_close()
                return ''
            else:
                raise socket.error, why

class DebugServer(StorageServer):

    ZEOStorageClass = DebugZEOStorage
    ManagedServerConnectionClass = DebugManagerConnection

    def __init__(self, *args, **kwargs):
        StorageServer.__init__(*args, **kwargs)
        self._setup_record(kwargs["record"])
        self._conn_counter = 1

    def _setup_record(self, path):
        try:
            self._recordfile = open(path, "ab")
        except IOError, msg:
            self._recordfile = None
            log("failed to open recordfile %s: %s" % (path, msg))

    def _record_connection(self, addr):
        cid = self._conn_counter
        self._conn_counter += 1
        self._record(cid, NEW_CONN, str(addr))
        return cid

    def _record(self, conn, code, data):
        s = struct.pack(">iii", code, time.time(), len(data)) + data
        self._recordfile.write(s)
