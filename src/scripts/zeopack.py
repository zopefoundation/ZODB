#! /usr/bin/env python
"""Connect to a ZEO server and ask it to pack.

Usage: zeopack.py [options]

Options:

    -p port -- port to connect to
    
    -h host -- host to connect to (default is current host)
    
    -U path -- Unix-domain socket to connect to
    
    -S name -- storage name (default is '1')

    -d days -- pack objects more than days old

You must specify either -p and -h or -U.
"""

import getopt
import socket
import sys
import time

from ZEO.ClientStorage import ClientStorage

WAIT = 10 # wait no more than 10 seconds for client to connect

def connect(storage):
    # The connect-on-startup logic that ZEO provides isn't too useful
    # for this script.  We'd like to client to attempt to startup, but
    # fail if it can't get through to the server after a reasonable
    # amount of time.  There's no external support for this, so we'll
    # expose the ZEO 1.0 internals.  (consenting adults only)
    t0 = time.time()
    while t0 + WAIT > time.time():
        storage._call.connect()
        if storage._connected:
            return
    raise RuntimeError, "Unable to connect to ZEO server"

def pack(addr, storage, days):
    cs = ClientStorage(addr, storage=storage, wait_for_server_on_startup=1)
    # _startup() is an artifact of the way ZEO 1.0 works.  The
    # ClientStorage doesn't get fully initialized until registerDB()
    # is called.  The only thing we care about, though, is that
    # registerDB() calls _startup().
    connect(cs)
    cs.pack(wait=1, days=days)

def usage(exit=1):
    print __doc__
    print " ".join(sys.argv)
    sys.exit(exit)

def main():
    host = None
    port = None
    unix = None
    storage = '1'
    days = 0
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'p:h:U:S:d:')
        for o, a in opts:
            if o == '-p':
                port = int(a)
            elif o == '-h':
                host = a
            elif o == '-U':
                unix = a
            elif o == '-S':
                storage = a
            elif o == '-d':
                days = int(a)
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
        
    pack(addr, storage, days)

if __name__ == "__main__":
    try:
        main()
    except Exception, err:
        print err
        sys.exit(1)
