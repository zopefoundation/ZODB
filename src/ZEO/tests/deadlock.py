import ZODB
from ZODB.POSException import ConflictError
from ZEO.ClientStorage import ClientStorage, ClientDisconnected
from ZEO.zrpc.error import DisconnectedError

import os
import random
import time

L = range(1, 100)

def main():
    z1 = ClientStorage(('localhost', 2001), wait=1)
    z2 = ClientStorage(('localhost', 2002), wait=2)
    db1 = ZODB.DB(z1)
    db2 = ZODB.DB(z2)
    c1 = db1.open()
    c2 = db2.open()
    r1 = c1.root()
    r2 = c2.root()

    while 1:
        try:
            try:
                update(r1, r2)
            except ConflictError, msg:
                print msg
                transaction.abort()
                c1.sync()
                c2.sync()
        except (ClientDisconnected, DisconnectedError), err:
            print "disconnected", err
            time.sleep(2)

def update(r1, r2):
    k1 = random.choice(L)
    k2 = random.choice(L)

    updates = [(k1, r1),
               (k2, r2)]
    random.shuffle(updates)
    for key, root in updates:
        root[key] = time.time()
    transaction.commit()
    print os.getpid(), k1, k2

if __name__ == "__main__":
    main()
