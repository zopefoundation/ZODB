"""A ZEO client-server stress test to look for leaks.

The stress test should run in an infinite loop and should involve
multiple connections.
"""
from __future__ import nested_scopes

import ZODB
from ZEO.ClientStorage import ClientStorage
from ZODB.MappingStorage import MappingStorage
from ZEO.tests import forker
from ZODB.tests import MinPO
import zLOG

import os
import random
import sys
import types

NUM_TRANSACTIONS_PER_CONN = 10
NUM_CONNECTIONS = 10
NUM_ROOTS = 20
MAX_DEPTH = 20
MIN_OBJSIZE = 128
MAX_OBJSIZE = 2048

def an_object():
    """Return an object suitable for a PersistentMapping key"""
    size = random.randrange(MIN_OBJSIZE, MAX_OBJSIZE)
    if os.path.exists("/dev/urandom"):
        f = open("/dev/urandom")
        buf = f.read(size)
        f.close()
        return buf
    else:
        f = open(MinPO.__file__)
        l = list(f.read(size))
        f.close()
        random.shuffle(l)
        return "".join(l)

def setup(cn):
    """Initialize the database with some objects"""
    root = cn.root()
    for i in range(NUM_ROOTS):
        prev = an_object()
        for j in range(random.randrange(1, MAX_DEPTH)):
            o = MinPO.MinPO(prev)
            prev = o
        root[an_object()] = o
        get_transaction().commit()
    cn.close()

def work(cn):
    """Do some work with a transaction"""
    cn.sync()
    root = cn.root()
    obj = random.choice(root.values())
    # walk down to the bottom
    while not isinstance(obj.value, types.StringType):
        obj = obj.value
    obj.value = an_object()
    get_transaction().commit()

def main():
    # Yuck!  Need to cleanup forker so that the API is consistent
    # across Unix and Windows, at least if that's possible.
    if os.name == "nt":
        zaddr, tport, pid = forker.start_zeo_server('MappingStorage', ())
        def exitserver():
            import socket
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(tport)
            s.close()
    else:
        zaddr = '', random.randrange(20000, 30000)
        pid, exitobj = forker.start_zeo_server(MappingStorage(), zaddr)
        def exitserver():
            exitobj.close()

    while 1:
        pid = start_child(zaddr)
        print "started", pid
        os.waitpid(pid, 0)

    exitserver()

def start_child(zaddr):

    pid = os.fork()
    if pid != 0:
        return pid
    
    try:
        storage = ClientStorage(zaddr, debug=1, min_disconnect_poll=0.5)
        db = ZODB.DB(storage, pool_size=NUM_CONNECTIONS)
        setup(db.open())
        conns = []
        conn_count = 0

        for i in range(NUM_CONNECTIONS):
            c = db.open()
            c.__count = 0
            conns.append(c)
            conn_count += 1

        while conn_count < 25:
            c = random.choice(conns)
            if c.__count > NUM_TRANSACTIONS_PER_CONN:
                conns.remove(c)
                c.close()
                conn_count += 1
                c = db.open()
                c.__count = 0
                conns.append(c)
            else:
                c.__count += 1
            work(c)

    finally:
        os._exit(0)

if __name__ == "__main__":
    main()
