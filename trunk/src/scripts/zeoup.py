#! /usr/bin/env python
"""Make sure a ZEO server is running.

Usage: zeoup.py [options]

The test will connect to a ZEO server, load the root object, and
attempt to update the zeoup counter in the root.  It will report
success if it updates to counter or if it gets a ConflictError.  A
ConflictError is considered a success, because the client was able to
start a transaction. 

Options:

    -p port -- port to connect to
    
    -h host -- host to connect to (default is current host)
    
    -U path -- Unix-domain socket to connect to

    --nowrite -- Do not update the zeoup counter.

    -1 -- Connect to a ZEO 1.0 server.

You must specify either -p and -h or -U.
"""

import getopt
import socket
import sys

import ZODB
from ZODB.POSException import ConflictError
from ZEO.ClientStorage import ClientStorage

ZEO_VERSION = 2

def check_server(addr, storage, write):
    if ZEO_VERSION == 2:
        cs = ClientStorage(addr, storage=storage, debug=1, wait=1)
    else:
        cs = ClientStorage(addr, storage=storage, debug=1,
                           wait_for_server_on_startup=1)
    # _startup() is an artifact of the way ZEO 1.0 works.  The
    # ClientStorage doesn't get fully initialized until registerDB()
    # is called.  The only thing we care about, though, is that
    # registerDB() calls _startup().

    db = ZODB.DB(cs)
    cn = db.open()
    root = cn.root()
    if write:
        try:
            root['zeoup'] = root.get('zeoup', 0)+ 1
            get_transaction().commit()
        except ConflictError:
            pass
    cn.close()
    db.close()

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
        print err
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
    except Exception, err:
        print err
        sys.exit(1)
