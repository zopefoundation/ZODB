#!python

"""Transaction timeout test script.

This script connects to a storage, begins a transaction, calls store()
and tpc_vote(), and then sleeps forever.  This should trigger the
transaction timeout feature of the server.

usage: timeout.py address delay [storage-name]

"""

import sys
import time

from ZODB.Transaction import Transaction
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_pickle
from ZEO.ClientStorage import ClientStorage

ZERO = '\0'*8

def main():
    if len(sys.argv) not in (3, 4):
        sys.stderr.write("Usage: timeout.py address delay [storage-name]\n" %
                         sys.argv[0])
        sys.exit(2)

    hostport = sys.argv[1]
    delay = float(sys.argv[2])
    if sys.argv[3:]:
        name = sys.argv[3]
    else:
        name = "1"

    if "/" in hostport:
        address = hostport
    else:
        if ":" in hostport:
            i = hostport.index(":")
            host, port = hostport[:i], hostport[i+1:]
        else:
            host, port = "", hostport
        port = int(port)
        address = (host, port)

    print "Connecting to %s..." % repr(address)
    storage = ClientStorage(address, name)
    print "Connected.  Now starting a transaction..."

    oid = storage.new_oid()
    version = ""
    revid = ZERO
    data = MinPO("timeout.py")
    pickled_data = zodb_pickle(data)
    t = Transaction()
    t.user = "timeout.py"
    storage.tpc_begin(t)
    storage.store(oid, revid, pickled_data, version, t)
    print "Stored.  Now voting..."
    storage.tpc_vote(t)

    print "Voted; now sleeping %s..." % delay
    time.sleep(delay)
    print "Done."

if __name__ == "__main__":
    main()
