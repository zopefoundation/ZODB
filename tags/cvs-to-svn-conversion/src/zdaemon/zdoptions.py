##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

"""Option processing for zdaemon and related code."""

import os
import sys
import getopt

import ZConfig

class ZDOptions:

    doc = None
    progname = None
    configfile = None
    schemadir = None
    schemafile = "schema.xml"
    schema = None
    configroot = None

    # Class variable to control automatic processing of an <eventlog>
    # section.  This should be the (possibly dotted) name of something
    # accessible from configroot, typically "eventlog".
    logsectionname = None
    config_logger = None # The configured event logger, if any

    # Class variable deciding whether positional arguments are allowed.
    # If you want positional arguments, set this to 1 in your subclass.
    positional_args_allowed = 0

    def __init__(self):
        self.names_list = []
        self.short_options = []
        self.long_options = []
        self.options_map = {}
        self.default_map = {}
        self.required_map = {}
        self.environ_map = {}
        self.zconfig_options = []
        self.add(None, None, "h", "help", self.help)
        self.add("configfile", None, "C:", "configure=")
        self.add(None, None, "X:", handler=self.zconfig_options.append)

    def help(self, dummy):
        """Print a long help message (self.doc) to stdout and exit(0).

        Occurrences of "%s" in self.doc are replaced by self.progname.
        """
        doc = self.doc
        if doc.find("%s") > 0:
            doc = doc.replace("%s", self.progname)
        print doc,
        sys.exit(0)

    def usage(self, msg):
        """Print a brief error message to stderr and exit(2)."""
        sys.stderr.write("Error: %s\n" % str(msg))
        sys.stderr.write("For help, use %s -h\n" % self.progname)
        sys.exit(2)

    def remove(self,
               name=None,               # attribute name on self
               confname=None,           # name in ZConfig (may be dotted)
               short=None,              # short option name
               long=None,               # long option name
               ):
        """Remove all traces of name, confname, short and/or long."""
        if name:
            for n, cn in self.names_list[:]:
                if n == name:
                    self.names_list.remove((n, cn))
            if self.default_map.has_key(name):
                del self.default_map[name]
            if self.required_map.has_key(name):
                del self.required_map[name]
        if confname:
            for n, cn in self.names_list[:]:
                if cn == confname:
                    self.names_list.remove((n, cn))
        if short:
            key = "-" + short[0]
            if self.options_map.has_key(key):
                del self.options_map[key]
        if long:
            key = "--" + long
            if key[-1] == "=":
                key = key[:-1]
            if self.options_map.has_key(key):
                del self.options_map[key]

    def add(self,
            name=None,                  # attribute name on self
            confname=None,              # name in ZConfig (may be dotted)
            short=None,                 # short option name
            long=None,                  # long option name
            handler=None,               # handler (defaults to string)
            default=None,               # default value
            required=None,              # message if not provided
            flag=None,                  # if not None, flag value
            env=None,                   # if not None, environment variable
            ):
        """Add information about a configuration option.

        This can take several forms:

        add(name, confname)
            Configuration option 'confname' maps to attribute 'name'
        add(name, None, short, long)
            Command line option '-short' or '--long' maps to 'name'
        add(None, None, short, long, handler)
            Command line option calls handler
        add(name, None, short, long, handler)
            Assign handler return value to attribute 'name'

        In addition, one of the following keyword arguments may be given:

        default=...  -- if not None, the default value
        required=... -- if nonempty, an error message if no value provided
        flag=...     -- if not None, flag value for command line option
        env=...      -- if not None, name of environment variable that
                        overrides the configuration file or default
        """

        if flag is not None:
            if handler is not None:
                raise ValueError, "use at most one of flag= and handler="
            if not long and not short:
                raise ValueError, "flag= requires a command line flag"
            if short and short.endswith(":"):
                raise ValueError, "flag= requires a command line flag"
            if long and long.endswith("="):
                raise ValueError, "flag= requires a command line flag"
            handler = lambda arg, flag=flag: flag

        if short and long:
            if short.endswith(":") != long.endswith("="):
                raise ValueError, "inconsistent short/long options: %r %r" % (
                    short, long)

        if short:
            if short[0] == "-":
                raise ValueError, "short option should not start with '-'"
            key, rest = short[:1], short[1:]
            if rest not in ("", ":"):
                raise ValueError, "short option should be 'x' or 'x:'"
            key = "-" + key
            if self.options_map.has_key(key):
                raise ValueError, "duplicate short option key '%s'" % key
            self.options_map[key] = (name, handler)
            self.short_options.append(short)

        if long:
            if long[0] == "-":
                raise ValueError, "long option should not start with '-'"
            key = long
            if key[-1] == "=":
                key = key[:-1]
            key = "--" + key
            if self.options_map.has_key(key):
                raise ValueError, "duplicate long option key '%s'" % key
            self.options_map[key] = (name, handler)
            self.long_options.append(long)

        if env:
            self.environ_map[env] = (name, handler)

        if name:
            if not hasattr(self, name):
                setattr(self, name, None)
            self.names_list.append((name, confname))
            if default is not None:
                self.default_map[name] = default
            if required:
                self.required_map[name] = required

    def realize(self, args=None, progname=None, doc=None,
                raise_getopt_errs=True):
        """Realize a configuration.

        Optional arguments:

        args     -- the command line arguments, less the program name
                    (default is sys.argv[1:])

        progname -- the program name (default is sys.argv[0])

        doc      -- usage message (default is __main__.__doc__)
        """

         # Provide dynamic default method arguments
        if args is None:
            args = sys.argv[1:]
        if progname is None:
            progname = sys.argv[0]
        if doc is None:
            import __main__
            doc = __main__.__doc__
        self.progname = progname
        self.doc = doc

        self.options = []
        self.args = []

        # Call getopt
        try:
            self.options, self.args = getopt.getopt(
                args, "".join(self.short_options), self.long_options)
        except getopt.error, msg:
            if raise_getopt_errs:
                self.usage(msg)

        # Check for positional args
        if self.args and not self.positional_args_allowed:
            self.usage("positional arguments are not supported")

        # Process options returned by getopt
        for opt, arg in self.options:
            name, handler = self.options_map[opt]
            if handler is not None:
                try:
                    arg = handler(arg)
                except ValueError, msg:
                    self.usage("invalid value for %s %r: %s" % (opt, arg, msg))
            if name and arg is not None:
                if getattr(self, name) is not None:
                    self.usage("conflicting command line option %r" % opt)
                setattr(self, name, arg)

        # Process environment variables
        for envvar in self.environ_map.keys():
            name, handler = self.environ_map[envvar]
            if name and getattr(self, name, None) is not None:
                continue
            if os.environ.has_key(envvar):
                value = os.environ[envvar]
                if handler is not None:
                    try:
                        value = handler(value)
                    except ValueError, msg:
                        self.usage("invalid environment value for %s %r: %s"
                                   % (envvar, value, msg))
                if name and value is not None:
                    setattr(self, name, value)

        if self.configfile is None:
            self.configfile = self.default_configfile()
        if self.zconfig_options and self.configfile is None:
            self.usage("configuration overrides (-X) cannot be used"
                       " without a configuration file")
        if self.configfile is not None:
            # Process config file
            self.load_schema()
            try:
                self.load_configfile()
            except ZConfig.ConfigurationError, msg:
                self.usage(str(msg))

        # Copy config options to attributes of self.  This only fills
        # in options that aren't already set from the command line.
        for name, confname in self.names_list:
            if confname and getattr(self, name) is None:
                parts = confname.split(".")
                obj = self.configroot
                for part in parts:
                    if obj is None:
                        break
                    # Here AttributeError is not a user error!
                    obj = getattr(obj, part)
                setattr(self, name, obj)

        # Process defaults
        for name, value in self.default_map.items():
            if getattr(self, name) is None:
                setattr(self, name, value)

        # Process required options
        for name, message in self.required_map.items():
            if getattr(self, name) is None:
                self.usage(message)

        if self.logsectionname:
            self.load_logconf(self.logsectionname)

    def default_configfile(self):
        """Return the name of the default config file, or None."""
        # This allows a default configuration file to be used without
        # affecting the -C command line option; setting self.configfile
        # before calling realize() makes the -C option unusable since
        # then realize() thinks it has already seen the option.  If no
        # -C is used, realize() will call this method to try to locate
        # a configuration file.
        return None

    def load_schema(self):
        if self.schema is None:
            # Load schema
            if self.schemadir is None:
                self.schemadir = os.path.dirname(__file__)
            self.schemafile = os.path.join(self.schemadir, self.schemafile)
            self.schema = ZConfig.loadSchema(self.schemafile)

    def load_configfile(self):
        self.configroot, self.confighandlers = \
            ZConfig.loadConfig(self.schema, self.configfile,
                               self.zconfig_options)

    def load_logconf(self, sectname="eventlog"):
        parts = sectname.split(".")
        obj = self.configroot
        for p in parts:
            if obj == None:
                break
            obj = getattr(obj, p)
        self.config_logger = obj
        if obj is not None:
            obj.startup()


class RunnerOptions(ZDOptions):

    uid = gid = None

    def __init__(self):
        ZDOptions.__init__(self)
        self.add("backofflimit", "runner.backoff_limit",
                 "b:", "backoff-limit=", int, default=10)
        self.add("daemon", "runner.daemon", "d", "daemon", flag=1, default=0)
        self.add("forever", "runner.forever", "f", "forever",
                 flag=1, default=0)
        self.add("sockname", "runner.socket_name", "s:", "socket-name=",
                 ZConfig.datatypes.existing_dirpath, default="zdsock")
        self.add("exitcodes", "runner.exit_codes", "x:", "exit-codes=",
                 list_of_ints, default=[0, 2])
        self.add("user", "runner.user", "u:", "user=")
        self.add("umask", "runner.umask", "m:", "umask=", octal_type,
                 default=022)
        self.add("directory", "runner.directory", "z:", "directory=",
                 ZConfig.datatypes.existing_directory)
        self.add("hang_around", "runner.hang_around", default=0)

    def realize(self, *args, **kwds):
        ZDOptions.realize(self, *args, **kwds)

        # Additional checking of user option; set uid and gid
        if self.user is not None:
            import pwd
            try:
                uid = int(self.user)
            except ValueError:
                try:
                    pwrec = pwd.getpwnam(self.user)
                except KeyError:
                    self.usage("username %r not found" % self.user)
                uid = pwrec[2]
            else:
                try:
                    pwrec = pwd.getpwuid(uid)
                except KeyError:
                    self.usage("uid %r not found" % self.user)
            gid = pwrec[3]
            self.uid = uid
            self.gid = gid


# ZConfig datatype

def list_of_ints(arg):
    if not arg:
        return []
    else:
        return map(int, arg.split(","))

def octal_type(arg):
    return int(arg, 8)


def _test():
    # Stupid test program
    z = ZDOptions()
    z.add("program", "zdctl.program", "p:", "program=")
    print z.names_list
    z.realize()
    names = z.names_list[:]
    names.sort()
    for name, confname in names:
        print "%-20s = %.56r" % (name, getattr(z, name))

if __name__ == "__main__":
    __file__ = sys.argv[0]
    _test()
