#!python
##############################################################################
#
# Copyright (c) 2001, 2002, 2003 Zope Corporation and Contributors.
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
"""Start the ZEO storage server.

Usage: %s [-C URL] [-a ADDRESS] [-f FILENAME] [-h]

Options:
-C/--configuration URL -- configuration file or URL
-a/--address ADDRESS -- server address of the form PORT, HOST:PORT, or PATH
                        (a PATH must contain at least one "/")
-f/--filename FILENAME -- filename for FileStorage
-t/--timeout TIMEOUT -- transaction timeout in seconds (default no timeout)
-h/--help -- print this usage message and exit
-m/--monitor ADDRESS -- address of monitor server ([HOST:]PORT or PATH)

Unless -C is specified, -a and -f are required.
"""

# The code here is designed to be reused by other, similar servers.
# For the forseeable future, it must work under Python 2.1 as well as
# 2.2 and above.

import os
import sys
import signal
import socket
import logging

import ZConfig, ZConfig.datatypes
import ZEO
from zdaemon.zdoptions import ZDOptions

logger = logging.getLogger('ZEO.runzeo')
_pid = str(os.getpid())

def log(msg, level=logging.INFO, exc_info=False):
    """Internal: generic logging function."""
    message = "(%s) %s" % (_pid, msg)
    logger.log(level, message, exc_info=exc_info)


def parse_address(arg):
    # XXX Not part of the official ZConfig API
    obj = ZConfig.datatypes.SocketAddress(arg)
    return obj.family, obj.address


class ZEOOptionsMixin:

    storages = None

    def handle_address(self, arg):
        self.family, self.address = parse_address(arg)

    def handle_monitor_address(self, arg):
        self.monitor_family, self.monitor_address = parse_address(arg)

    def handle_filename(self, arg):
        from ZODB.config import FileStorage # That's a FileStorage *opener*!
        class FSConfig:
            def __init__(self, name, path):
                self._name = name
                self.path = path
                self.create = 0
                self.read_only = 0
                self.stop = None
                self.quota = None
            def getSectionName(self):
                return self._name
        if not self.storages:
            self.storages = []
        name = str(1 + len(self.storages))
        conf = FileStorage(FSConfig(name, arg))
        self.storages.append(conf)

    def add_zeo_options(self):
        self.add(None, None, "a:", "address=", self.handle_address)
        self.add(None, None, "f:", "filename=", self.handle_filename)
        self.add("family", "zeo.address.family")
        self.add("address", "zeo.address.address",
                 required="no server address specified; use -a or -C")
        self.add("read_only", "zeo.read_only", default=0)
        self.add("invalidation_queue_size", "zeo.invalidation_queue_size",
                 default=100)
        self.add("transaction_timeout", "zeo.transaction_timeout",
                 "t:", "timeout=", float)
        self.add("monitor_address", "zeo.monitor_address.address",
                 "m:", "monitor=", self.handle_monitor_address)
        self.add('auth_protocol', 'zeo.authentication_protocol',
                 None, 'auth-protocol=', default=None)
        self.add('auth_database', 'zeo.authentication_database',
                 None, 'auth-database=')
        self.add('auth_realm', 'zeo.authentication_realm',
                 None, 'auth-realm=')

class ZEOOptions(ZDOptions, ZEOOptionsMixin):

    logsectionname = "eventlog"
    schemadir = os.path.dirname(ZEO.__file__)

    def __init__(self):
        ZDOptions.__init__(self)
        self.add_zeo_options()
        self.add("storages", "storages",
                 required="no storages specified; use -f or -C")


class ZEOServer:

    def __init__(self, options):
        self.options = options

    def main(self):
        self.setup_default_logging()
        self.check_socket()
        self.clear_socket()
        try:
            self.open_storages()
            self.setup_signals()
            self.create_server()
            self.loop_forever()
        finally:
            self.close_storages()
            self.clear_socket()

    def setup_default_logging(self):
        if self.options.config_logger is not None:
            return
        # No log file is configured; default to stderr.
        logger = logging.getLogger()
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)

    def check_socket(self):
        if self.can_connect(self.options.family, self.options.address):
            self.options.usage("address %s already in use" %
                               repr(self.options.address))

    def can_connect(self, family, address):
        s = socket.socket(family, socket.SOCK_STREAM)
        try:
            s.connect(address)
        except socket.error:
            return 0
        else:
            s.close()
            return 1

    def clear_socket(self):
        if isinstance(self.options.address, type("")):
            try:
                os.unlink(self.options.address)
            except os.error:
                pass

    def open_storages(self):
        self.storages = {}
        for opener in self.options.storages:
            log("opening storage %r using %s"
                % (opener.name, opener.__class__.__name__))
            self.storages[opener.name] = opener.open()

    def setup_signals(self):
        """Set up signal handlers.

        The signal handler for SIGFOO is a method handle_sigfoo().
        If no handler method is defined for a signal, the signal
        action is not changed from its initial value.  The handler
        method is called without additional arguments.
        """
        if os.name != "posix":
            return
        if hasattr(signal, 'SIGXFSZ'):
            signal.signal(signal.SIGXFSZ, signal.SIG_IGN) # Special case
        init_signames()
        for sig, name in signames.items():
            method = getattr(self, "handle_" + name.lower(), None)
            if method is not None:
                def wrapper(sig_dummy, frame_dummy, method=method):
                    method()
                signal.signal(sig, wrapper)

    def create_server(self):
        from ZEO.StorageServer import StorageServer
        self.server = StorageServer(
            self.options.address,
            self.storages,
            read_only=self.options.read_only,
            invalidation_queue_size=self.options.invalidation_queue_size,
            transaction_timeout=self.options.transaction_timeout,
            monitor_address=self.options.monitor_address,
            auth_protocol=self.options.auth_protocol,
            auth_database=self.options.auth_database,  # XXX option spelling
            auth_realm=self.options.auth_realm)

    def loop_forever(self):
        import ThreadedAsync.LoopCallback
        ThreadedAsync.LoopCallback.loop()

    def handle_sigterm(self):
        log("terminated by SIGTERM")
        sys.exit(0)

    def handle_sigint(self):
        log("terminated by SIGINT")
        sys.exit(0)

    def handle_sighup(self):
        log("restarted by SIGHUP")
        sys.exit(1)

    def handle_sigusr2(self):
        # XXX this used to reinitialize zLOG. How do I achieve
        #     the same effect with Python's logging package?
        #     Should we restart as with SIGHUP?
        log("received SIGUSR2, but it was not handled!", level=logging.WARNING)

    def close_storages(self):
        for name, storage in self.storages.items():
            log("closing storage %r" % name)
            try:
                storage.close()
            except: # Keep going
                log("failed to close storage %r" % name,
                    level=logging.EXCEPTION, exc_info=True)


# Signal names

signames = None

def signame(sig):
    """Return a symbolic name for a signal.

    Return "signal NNN" if there is no corresponding SIG name in the
    signal module.
    """

    if signames is None:
        init_signames()
    return signames.get(sig) or "signal %d" % sig

def init_signames():
    global signames
    signames = {}
    for name, sig in signal.__dict__.items():
        k_startswith = getattr(name, "startswith", None)
        if k_startswith is None:
            continue
        if k_startswith("SIG") and not k_startswith("SIG_"):
            signames[sig] = name


# Main program

def main(args=None):
    options = ZEOOptions()
    options.realize(args)
    s = ZEOServer(options)
    s.main()

if __name__ == "__main__":
    main()
