#!python
"""Make sure a ZEO server is running.

usage: zeoup.py [options]

The test will connect to a ZEO server, load the root object, and attempt to
update the zeoup counter in the root.  It will report success if it updates
the counter or if it gets a ConflictError.  A ConflictError is considered a
success, because the client was able to start a transaction.

Options:

    -p port -- port to connect to

    -h host -- host to connect to (default is current host)

    -S storage -- storage name (default '1')

    -U path -- Unix-domain socket to connect to

    --nowrite -- Do not update the zeoup counter.

    -1 -- Connect to a ZEO 1.0 server.

You must specify either -p and -h or -U.
"""

import getopt
import socket
import sys
import time

import ZODB
from ZODB.POSException import ConflictError
from ZODB.tests.MinPO import MinPO
from ZEO.ClientStorage import ClientStorage
from persistent.mapping import PersistentMapping

ZEO_VERSION = 2

def check_server(addr, storage, write):
    t0 = time.time()
    if ZEO_VERSION == 2:
        # XXX should do retries w/ exponential backoff
        cs = ClientStorage(addr, storage=storage, wait=0,
                           read_only=(not write))
    else:
        cs = ClientStorage(addr, storage=storage, debug=1,
                           wait_for_server_on_startup=1)
    # _startup() is an artifact of the way ZEO 1.0 works.  The
    # ClientStorage doesn't get fully initialized until registerDB()
    # is called.  The only thing we care about, though, is that
    # registerDB() calls _startup().

    if write:
        db = ZODB.DB(cs)
        cn = db.open()
        root = cn.root()
        try:
            # We store the data in a special `monitor' dict under the root,
            # where other tools may also store such heartbeat and bookkeeping
            # type data.
            monitor = root.get('monitor')
            if monitor is None:
                monitor = root['monitor'] = PersistentMapping()
            obj = monitor['zeoup'] = monitor.get('zeoup', MinPO(0))
            obj.value += 1
            get_transaction().commit()
        except ConflictError:
            pass
        cn.close()
        db.close()
    else:
        data, serial = cs.load("\0\0\0\0\0\0\0\0", "")
        cs.close()
    t1 = time.time()
    print "Elapsed time: %.2f" % (t1 - t0)

def usage(exit=1):
    print __doc__
    print " ".join(sys.argv)
    sys.exit(exit)

def main():
    host = None
    port = None
    unix = None
    write = 1
    storage = '1'
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'p:h:U:S:1',
                                   ['nowrite'])
        for o, a in opts:
            if o == '-p':
                port = int(a)
            elif o == '-h':
                host = a
            elif o == '-U':
                unix = a
            elif o == '-S':
                storage = a
            elif o == '--nowrite':
                write = 0
            elif o == '-1':
                ZEO_VERSION = 1
    except Exception, err:
        s = str(err)
        if s:
            s = ": " + s
        print err.__class__.__name__ + s
        usage()

    if unix is not None:
        addr = unix
    else:
        if host is None:
            host = socket.gethostname()
        if port is None:
            usage()
        addr = host, port

    check_server(addr, storage, write)

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception, err:
        s = str(err)
        if s:
            s = ": " + s
        print err.__class__.__name__ + s
        sys.exit(1)
