#! /usr/bin/env python
##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
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

Usage: %s [-a ADDRESS] [-C URL] [-f FILENAME] [-h]

Options:
-a/--address ADDRESS -- server address of the form PORT, HOST:PORT, or PATH
                        (a PATH must contain at least one "/")
-C/--configuration URL -- configuration file or URL
-f/--filename FILENAME -- filename for FileStorage
-h/--help -- print this usage message and exit

Unless -C is specified, -a and -f are required.
"""

# The code here is designed to be reused by other, similar servers.
# For the forseeable future, it must work under Python 2.1 as well as
# 2.2 and above.

# XXX The option parsing infrastructure could be shared with zdaemon.py

import os
import sys
import getopt
import signal
import socket

import zLOG

import ZConfig
import ZConfig.Common
import ZConfig.Storage


class Options:

    """A class to parse and hold the command line options.

    Options are represented by various attributes (zeoport etc.).
    Positional arguments are represented by the args attribute.

    This also has a public usage() method that can be used to report
    errors related to the command line.
    """

    configuration = None
    rootconf = None

    args = []

    def __init__(self, args=None, progname=None, doc=None):
        """Constructor.

        Optional arguments:

        args     -- the command line arguments, less the program name
                    (default is sys.argv[1:] at the time of call)

        progname -- the program name (default sys.argv[0])

        doc      -- usage message (default, __main__.__doc__)
        """

        if args is None:
            args = sys.argv[1:]
        if progname is None:
            progname = sys.argv[0]
        self.progname = progname
        if doc is None:
            import __main__
            doc = __main__.__doc__
        if doc and not doc.endswith("\n"):
            doc += "\n"
        self.doc = doc
        try:
            self.options, self.args = getopt.getopt(args,
                                                    self._short_options,
                                                    self._long_options)
        except getopt.error, msg:
            self.usage(str(msg))
        for opt, arg in self.options:
            self.handle_option(opt, arg)
        self.check_options()

    # Default set of options.  Subclasses should override.
    _short_options = "C:h"
    _long_options = ["--configuration=", "--help"]

    def handle_option(self, opt, arg):
        """Handle one option.  Subclasses should override.

        This sets the various instance variables overriding the defaults.

        When -h is detected, print the module docstring to stdout and exit(0).
        """
        if opt in ("-C", "--configuration"):
            self.set_configuration(arg)
        if opt in ("-h", "--help"):
            self.help()

    def set_configuration(self, arg):
        self.configuration = arg

    def check_options(self):
        """Check options.  Subclasses may override.

        This can be used to ensure certain options are set, etc.
        """
        self.load_configuration()

    def load_configuration(self):
        if self.rootconf or not self.configuration:
            return
        self.rootconf = ZConfig.load(self.configuration)

    def help(self):
        """Print a long help message (self.doc) to stdout and exit(0).

        Occurrences of "%s" in self.doc are replaced by self.progname.
        """
        doc = self.doc
        if doc.find("%s") > 0:
            doc = doc.replace("%s", self.progname)
        print doc
        sys.exit(0)

    def usage(self, msg):
        """Print a brief error message to stderr and exit(2)."""
        sys.stderr.write("Error: %s\n" % str(msg))
        sys.stderr.write("For help, use %s -h\n" % self.progname)
        sys.exit(2)


class ZEOOptions(Options):

    hostname = None                     # A subclass may set this
    hostconf = None                     # <Host> section
    zeoconf = None                      # <ZEO> section

    family = None                       # set by -a; AF_UNIX or AF_INET
    address = None                      # set by -a; string or (host, port)
    storages = None                     # set by -f

    _short_options = "a:C:f:h"
    _long_options = [
        "--address=",
        "--configuration=",
        "--filename=",
        "--help",
        ]

    def handle_option(self, opt, arg):
        # Alphabetical order please!
        if opt in ("-a", "--address"):
            if "/" in arg:
                self.family = socket.AF_UNIX
                self.address = arg
            else:
                self.family = socket.AF_INET
                if ":" in arg:
                    host, port = arg.split(":", 1)
                else:
                    host = ""
                    port = arg
                try:
                    port = int(port)
                except: # int() can raise all sorts of errors
                    self.usage("invalid port number: %r" % port)
                self.address = (host, port)
        elif opt in ("-f", "--filename"):
            from ZODB.FileStorage import FileStorage
            self.storages = {"1": (FileStorage, {"file_name": arg})}
        else:
            # Pass it to the base class, for --help/-h
            Options.handle_option(self, opt, arg)

    def check_options(self):
        Options.check_options(self) # Calls load_configuration()
        if not self.storages:
            self.usage("no storages specified; use -f or -C")
        if self.family is None:
            self.usage("no server address specified; use -a or -C")
        if self.args:
            self.usage("positional arguments are not supported")

    def load_configuration(self):
        Options.load_configuration(self) # Sets self.rootconf
        if not self.rootconf:
            return
        try:
            self.hostconf = self.rootconf.getSection("Host")
        except ZConfig.Common.ConfigurationConflictingSectionError:
            if not self.hostname:
                self.hostname = socket.getfqdn()
            self.hostconf = self.rootconf.getSection("Host", self.hostname)
        if self.hostconf is None:
            # If no <Host> section exists, fall back to the root
            self.hostconf = self.rootconf
        self.zeoconf = self.hostconf.getSection("ZEO")
        if self.zeoconf is None:
            # If no <ZEO> section exists, fall back to the host (or root)
            self.zeoconf = self.hostconf

        # Now extract options from various configuration sections
        self.load_zeoconf()
        self.load_storages()

    def load_zeoconf(self):
        # Get some option defaults from the configuration
        if self.family:
            # -a option overrides
            return
        port = self.zeoconf.getint("server-port")
        path = self.zeoconf.get("path")
        if port and path:
            self.usage(
                "Configuration contains conflicting ZEO information:\n"
                "Exactly one of 'path' and 'server-port' may be given.")
        if port:
            host = self.hostconf.get("hostname", "")
            self.family = socket.AF_INET
            self.address = (host, port)
        elif path:
            self.family = socket.AF_UNIX
            self.address = path

    def load_storages(self):
        # Get the storage specifications
        if self.storages:
            # -f option overrides
            return
        storagesections = self.zeoconf.getChildSections("Storage")
        self.storages = {}
        for section in storagesections:
            name = section.name
            if not name:
                name = str(1 + len(self.storages))
            self.storages[name] = ZConfig.Storage.getStorageInfo(section)


class ZEOServer:

    OptionsClass = ZEOOptions

    def __init__(self, options=None):
        if options is None:
            options = self.OptionsClass()
        self.options = options

    def main(self):
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
        for name, (cls, args) in self.options.storages.items():
            info("open storage %r: %s.%s(**%r)" %
                 (name, cls.__module__, cls.__name__, args))
            self.storages[name] = cls(**args)

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
        self.server = StorageServer(self.options.address, self.storages)

    def loop_forever(self):
        import asyncore
        asyncore.loop()

    def handle_sigterm(self):
        info("terminated by SIGTERM")
        sys.exit(0)

    def handle_sigint(self):
        info("terminated by SIGINT")
        sys.exit(0)

    def handle_sigusr2(self):
        # This requires a modern zLOG (from Zope 2.6 or later); older
        # zLOG packages don't have the initialize() method
        info("reinitializing zLOG")
        # XXX Shouldn't this be below with _log()?
        import zLOG
        zLOG.initialize()

    def close_storages(self):
        for name, storage in self.storages.items():
            info("closing storage %r" % name)
            try:
                storage.close()
            except: # Keep going
                exception("failed to close storage %r" % name)


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


# Log messages with various severities.
# This uses zLOG, but the API is a simplified version of PEP 282

def critical(msg):
    """Log a critical message."""
    _log(msg, zLOG.PANIC)

def error(msg):
    """Log an error message."""
    _log(msg, zLOG.ERROR)

def exception(msg):
    """Log an exception (an error message with a traceback attached)."""
    _log(msg, zLOG.ERROR, error=sys.exc_info())

def warn(msg):
    """Log a warning message."""
    _log(msg, zLOG.PROBLEM)

def info(msg):
    """Log an informational message."""
    _log(msg, zLOG.INFO)

def debug(msg):
    """Log a debugging message."""
    _log(msg, zLOG.DEBUG)

def _log(msg, severity=zLOG.INFO, error=None):
    """Internal: generic logging function."""
    zLOG.LOG("RUNSVR", severity, msg, "", error)


# Main program

def main(args=None):
    options = ZEOOptions(args)
    s = ZEOServer(options)
    s.main()

if __name__ == "__main__":
    main()
