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
"""Monitor behavior of ZEO server and record statistics.

$Id: monitor.py,v 1.2 2003/01/09 23:55:57 jeremy Exp $
"""

import asyncore
import socket
import time
import types

import ZEO
import zLOG

class StorageStats:
    """Per-storage usage statistics."""

    def __init__(self):
        self.loads = 0
        self.stores = 0
        self.commits = 0
        self.aborts = 0
        self.active_txns = 0
        self.clients = 0
        self.verifying_clients = 0
        self.lock_time = None
        self.conflicts = 0
        self.conflicts_resolved = 0
        self.start = time.ctime()

    def dump(self, f):
        print >> f, "Server started:", self.start
        print >> f, "Clients:", self.clients
        print >> f, "Clients verifying:", self.verifying_clients
        print >> f, "Active transactions:", self.active_txns
        if self.lock_time:
            howlong = time.time() - self.lock_time
            print >> f, "Commit lock held for:", int(howlong)
        print >> f, "Commits:", self.commits
        print >> f, "Aborts:", self.aborts
        print >> f, "Loads:", self.loads
        print >> f, "Stores:", self.stores
        print >> f, "Conflicts:", self.conflicts
        print >> f, "Conflicts resolved:", self.conflicts_resolved

class StatsClient(asyncore.dispatcher):

    def __init__(self, sock, addr):
        asyncore.dispatcher.__init__(self, sock)
        self.buf = []
        self.closed = 0

    def close(self):
        self.closed = 1
        # The socket is closed after all the data is written.
        # See handle_write().

    def write(self, s):
        self.buf.append(s)

    def writable(self):
        return len(self.buf)

    def readable(self):
        # XXX what goes here?
        return 0

    def handle_write(self):
        s = "".join(self.buf)
        self.buf = []
        n = self.socket.send(s)
        if n < len(s):
            self.buf.append(s[:n])
            
        if self.closed and not self.buf:
            asyncore.dispatcher.close(self)

class StatsServer(asyncore.dispatcher):

    StatsConnectionClass = StatsClient

    def __init__(self, addr, stats):
        asyncore.dispatcher.__init__(self)
        self.addr = addr
        self.stats = stats
        if type(self.addr) == types.TupleType:
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.set_reuse_addr()
        zLOG.LOG("ZSM", zLOG.INFO, "monitor listening on %s" % repr(self.addr))
        self.bind(self.addr)
        self.listen(5)

    def writable(self):
        return 0

    def readable(self):
        return 1

    def handle_accept(self):
        try:
            sock, addr = self.accept()
        except socket.error:
            return
        f = self.StatsConnectionClass(sock, addr)
        self.dump(f)
        f.close()

    def dump(self, f):
        print >> f, "ZEO monitor server version %s" % ZEO.version
        print >> f, time.ctime()
        print >> f

        L = self.stats.keys()
        L.sort()
        for k in L:
            stats = self.stats[k]
            print >> f, "Storage:", k
            stats.dump(f)
            print >> f
