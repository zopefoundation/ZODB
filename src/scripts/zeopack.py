#! /usr/bin/env python
"""Connect to a ZEO server and ask it to pack.

Usage: zeopack.py [options]

Options:

    -p -- port to connect to
    
    -h -- host to connect to (default is current host)
    
    -U -- Unix-domain socket to connect to
    
    -S -- storage name (default is '1')

You must specify either -p and -h or -U.
"""

from ZEO.ClientStorage import ClientStorage

def main(addr, storage):
    cs = ClientStorage(addr, storage=storage, wait_for_server_on_startup=1)
    # _startup() is an artifact of the way ZEO 1.0 works.  The
    # ClientStorage doesn't get fully initialized until registerDB()
    # is called.  The only thing we care about, though, is that
    # registerDB() calls _startup().
    cs._startup()
    cs.pack(wait=1)

def usage(exit=1):
    print __doc__
    print " ".join(sys.argv)
    sys.exit(exit)

if __name__ == "__main__":
    import getopt
    import socket
    import sys

    host = None
    port = None
    unix = None
    storage = '1'
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

    if unix is not None:
        addr = unix
    else:
        if host is None:
            host = socket.gethostname()
        if port is None:
            usage()
        addr = host, port
    
    main(addr, storage)
