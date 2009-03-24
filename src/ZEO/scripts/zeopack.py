#!/usr/bin/env python2.3

import logging
import optparse
import socket
import sys
import time
import traceback
import ZEO.ClientStorage

usage = """Usage: %prog [options] [servers]

Pack one or more storages hosted by ZEO servers.

The positional arguments specify 0 or more tcp servers to pack, where
each is of the form:

    host:port[:name]

"""

WAIT = 10 # wait no more than 10 seconds for client to connect

def _main(args=None, prog=None):
    if args is None:
        args = sys.argv[1:]

    parser = optparse.OptionParser(usage, prog=prog)

    parser.add_option(
        "-d", "--days", dest="days", type='int', default=0,
        help=("Pack objects that are older than this number of days")
        )

    parser.add_option(
        "-t", "--time", dest="time",
        help=("Time of day to pack to of the form: HH[:MM[:SS]]. "
              "Defaults to current time.")
        )

    parser.add_option(
        "-u", "--unix", dest="unix_sockets", action="append",
        help=("A unix-domain-socket server to connect to, of the form: "
              "path[:name]")
        )

    parser.remove_option('-h')
    parser.add_option(
        "-h", dest="host",
        help=("Deprecated: "
              "Used with the -p and -S options, specified the host to "
              "connect to.")
        )

    parser.add_option(
        "-p", type="int", dest="port",
        help=("Deprecated: "
              "Used with the -h and -S options, specifies "
              "the port to connect to.")
        )

    parser.add_option(
        "-S", dest="name", default='1',
        help=("Deprecated: Used with the -h and -p, options, or with the "
              "-U option specified the storage name to use. Defaults to 1.")
        )

    parser.add_option(
        "-U", dest="unix",
        help=("Deprecated: Used with the -S option, "
              "Unix-domain socket to connect to.")
        )

    if not args:
        parser.print_help()
        return

    def error(message):
        sys.stderr.write("Error:\n%s\n" % message)
        sys.exit(1)

    options, args = parser.parse_args(args)

    packt = time.time()
    if options.time:
        time_ = map(int, options.time.split(':'))
        if len(time_) == 1:
            time_ += (0, 0)
        elif len(time_) == 2:
            time_ += (0,)
        elif len(time_) > 3:
            error("Invalid time value: %r" % options.time)

        packt = time.localtime(packt)
        packt = time.mktime(packt[:3]+tuple(time_)+packt[6:])

    packt -= options.days * 86400

    servers = []

    if options.host:
        if not options.port:
            error("If host (-h) is specified then a port (-p) must be "
                  "specified as well.")
        servers.append(((options.host, options.port), options.name))
    elif options.port:
        error("If port (-p) is specified then a host (-h) must be "
              "specified as well.")

    if options.unix:
        servers.append((options.unix, options.name))

    for server in args:
        data = server.split(':')
        if len(data) in (2, 3):
            host = data[0]
            try:
                port = int(data[1])
            except ValueError:
                error("Invalid port in server specification: %r" % server)
            addr = host, port
            if len(data) == 2:
                name = '1'
            else:
                name = data[2]
        else:
            error("Invalid server specification: %r" % server)

        servers.append((addr, name))

    for server in options.unix_sockets or ():
        data = server.split(':')
        if len(data) == 1:
            addr = data[0]
            name = '1'
        elif len(data) == 2:
            addr = data[0]
            name = data[1]
        else:
            error("Invalid server specification: %r" % server)

        servers.append((addr, name))

    if not servers:
        error("No servers specified.")

    for addr, name in servers:
        try:
            cs = ZEO.ClientStorage.ClientStorage(
                addr, storage=name, wait=False, read_only=1)
            for i in range(60):
                if cs.is_connected():
                    break
                time.sleep(1)
            else:
                sys.stderr.write("Couldn't connect to: %r\n"
                                 % ((addr, name), ))
                cs.close()
                continue
            cs.pack(packt, wait=True)
            cs.close()
        except:
            traceback.print_exception(*(sys.exc_info()+(99, sys.stderr)))
            error("Error packing storage %s in %r" % (name, addr))

def main(*args):
    root_logger = logging.getLogger()
    old_level = root_logger.getEffectiveLevel()
    logging.getLogger().setLevel(logging.WARNING)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "%(name)s %(levelname)s %(message)s"))
    logging.getLogger().addHandler(handler)
    try:
        _main(*args)
    finally:
        logging.getLogger().setLevel(old_level)
        logging.getLogger().removeHandler(handler)

if __name__ == "__main__":
    main()

