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

"""A multi-client test of the ZEO storage server"""

import ZODB, ZODB.DB, ZODB.FileStorage, ZODB.POSException
import Persistence
import PersistentMapping
from ZEO.tests import forker

import asyncore
import os
import tempfile
import time
import types

VERBOSE = 1
CLIENTS = 4
RECORDS_PER_CLIENT = 100
CONFLICT_DELAY = 0.1
CONNECT_DELAY = 0.1
CLIENT_CACHE = '' # use temporary cache

class Record(Persistence.Persistent):
    def __init__(self, client=None, value=None):
        self.client = client
        self.value = None
        self.next = None

    def set_next(self, next):
        self.next = next

class Stats(Persistence.Persistent):
    def __init__(self):
        self.begin = time.time()
        self.end = None

    def done(self):
        self.end = time.time()

def init_storage():
    path = tempfile.mktemp()
    if VERBOSE:
        print "FileStorage path:", path
    fs = ZODB.FileStorage.FileStorage(path)

    db = ZODB.DB(fs)
    root = db.open().root()
    root["multi"] = PersistentMapping.PersistentMapping()
    get_transaction().commit()

    return fs

def start_server(addr):
    storage = init_storage()
    pid, exit = forker.start_zeo_server(storage, addr)
    return pid, exit

def start_client(addr, client_func=None):
    pid = os.fork()
    if pid == 0:
        try:
            import ZEO.ClientStorage
            if VERBOSE:
                print "Client process started:", os.getpid()
            cli = ZEO.ClientStorage.ClientStorage(addr, client=CLIENT_CACHE)
            if client_func is None:
                run(cli)
            else:
                client_func(cli)
            cli.close()
        finally:
            os._exit(0)
    else:
        return pid

def run(storage):
    if hasattr(storage, 'is_connected'):
        while not storage.is_connected():
            time.sleep(CONNECT_DELAY)
    pid = os.getpid()
    print "Client process connected:", pid, storage
    db = ZODB.DB(storage)
    root = db.open().root()
    while 1:
        try:
            s = root[pid] = Stats()
            get_transaction().commit()
        except ZODB.POSException.ConflictError:
            get_transaction().abort()
            time.sleep(CONFLICT_DELAY)
        else:
            break

    dict = root["multi"]
    prev = None
    i = 0
    while i < RECORDS_PER_CLIENT:
        try:
            size = len(dict)
            r = dict[size] = Record(pid, size)
            if prev:
                prev.set_next(r)
            get_transaction().commit()
        except ZODB.POSException.ConflictError, err:
            get_transaction().abort()
            time.sleep(CONFLICT_DELAY)
        else:
            i = i + 1
            if VERBOSE and (i < 5 or i % 10 == 0):
                print "Client %s: %s of %s" % (pid, i, RECORDS_PER_CLIENT)
    s.done()
    get_transaction().commit()

    print "Client completed:", pid

def main(client_func=None):
    if VERBOSE:
        print "Main process:", os.getpid()
    addr = tempfile.mktemp()
    t0 = time.time()
    server_pid, server = start_server(addr)
    t1 = time.time()
    pids = []
    for i in range(CLIENTS):
        pids.append(start_client(addr, client_func))
    for pid in pids:
        assert type(pid) == types.IntType, "invalid pid type: %s (%s)" % \
               (repr(pid), type(pid))
        try:
            if VERBOSE:
                print "waitpid(%s)" % repr(pid)
            os.waitpid(pid, 0)
        except os.error, err:
            print "waitpid(%s) failed: %s" % (repr(pid), err)
    t2 = time.time()
    server.close()
    os.waitpid(server_pid, 0)

    # XXX Should check that the results are consistent!

    print "Total time:", t2 - t0
    print "Server start time", t1 - t0
    print "Client time:", t2 - t1

if __name__ == "__main__":
    main()
