#! /usr/bin/env python
"""Make sure a ZEO server is running.

Usage: zeoup.py [options]

Options:

    -p port -- port to connect to
    
    -h host -- host to connect to (default is current host)
    
    -U path -- Unix-domain socket to connect to

You must specify either -p and -h or -U.
"""

import getopt
import socket
import sys

import ZODB
from ZEO.ClientStorage import ClientStorage

def check_server(addr, storage):
    cs = ClientStorage(addr, storage=storage, debug=1,
                       wait_for_server_on_startup=0)
    # _startup() is an artifact of the way ZEO 1.0 works.  The
    # ClientStorage doesn't get fully initialized until registerDB()
    # is called.  The only thing we care about, though, is that
    # registerDB() calls _startup().

    # XXX Is connecting a DB with wait_for_server_on_startup=0 a
    # sufficient test for upness?
    db = ZODB.DB(cs)
    db.close()

def usage(exit=1):
    print __doc__
    print " ".join(sys.argv)
    sys.exit(exit)

def main():
    host = None
    port = None
    unix = None
    storage = '1'
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'p:h:U:S:')
        for o, a in opts:
            if o == '-p':
                port = int(a)
            elif o == '-h':
                host = a
            elif o == '-U':
                unix = a
            elif o == '-S':
                storage = a
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

    check_server(addr, storage)

if __name__ == "__main__":
    try:
        main()
    except Exception, err:
        print err
        sys.exit(1)
