#!python
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
"""%(program)s -- create a ZEO instance.

Usage: %(program)s home [port]

Given an "instance home directory" <home> and some configuration
options (all of which have default values), create the following:

<home>/etc/zeo.conf     -- ZEO config file
<home>/var/             -- Directory for data files: Data.fs etc.
<home>/log/             -- Directory for log files: zeo.log and zeoctl.log
<home>/bin/runzeo       -- the zeo server runner
<home>/bin/zeoctl       -- start/stop script (a shim for zeoctl.py)

The script will not overwrite existing files; instead, it will issue a
warning if an existing file is found that differs from the file that
would be written if it didn't exist.
"""

# WARNING!  Several templates and functions here are reused by ZRS.
# So be careful with changes.

import os
import sys
import stat
import getopt

zeo_conf_template = """\
# ZEO configuration file

%%define INSTANCE %(instance_home)s

<zeo>
  address %(port)d
  read-only false
  invalidation-queue-size 100
  # monitor-address PORT
  # transaction-timeout SECONDS
</zeo>

<filestorage 1>
  path $INSTANCE/var/Data.fs
</filestorage>

<eventlog>
  level info
  <logfile>
    path $INSTANCE/log/zeo.log
  </logfile>
</eventlog>

<runner>
  program $INSTANCE/bin/runzeo
  socket-name $INSTANCE/etc/%(package)s.zdsock
  daemon true
  forever false
  backoff-limit 10
  exit-codes 0, 2
  directory $INSTANCE
  default-to-interactive true
  # user zope
  python %(python)s
  zdrun %(zope_home)s/zdaemon/zdrun.py

  # This logfile should match the one in the %(package)s.conf file.
  # It is used by zdctl's logtail command, zdrun/zdctl doesn't write it.
  logfile $INSTANCE/log/%(package)s.log
</runner>
"""

zeoctl_template = """\
#!/bin/sh
# %(PACKAGE)s instance control script

# The following two lines are for chkconfig.  On Red Hat Linux (and
# some other systems), you can copy or symlink this script into
# /etc/rc.d/init.d/ and then use chkconfig(8) to automatically start
# %(PACKAGE)s at boot time.

# chkconfig: 345 90 10
# description: start a %(PACKAGE)s server

PYTHON="%(python)s"
ZOPE_HOME="%(zope_home)s"

CONFIG_FILE="%(instance_home)s/etc/%(package)s.conf"

PYTHONPATH="$ZOPE_HOME"
export PYTHONPATH

ZEOCTL="$ZOPE_HOME/ZEO/zeoctl.py"

exec "$PYTHON" "$ZEOCTL" -C "$CONFIG_FILE" ${1+"$@"}
"""

runzeo_template = """\
#!/bin/sh
# %(PACKAGE)s instance start script

PYTHON="%(python)s"
ZOPE_HOME="%(zope_home)s"

CONFIG_FILE="%(instance_home)s/etc/%(package)s.conf"

PYTHONPATH="$ZOPE_HOME"
export PYTHONPATH

RUNZEO="$ZOPE_HOME/ZEO/runzeo.py"

exec "$PYTHON" "$RUNZEO" -C "$CONFIG_FILE" ${1+"$@"}
"""

def main():
    ZEOInstanceBuilder().run()
    print "All done."

class ZEOInstanceBuilder:
    def run(self):
        try:
            opts, args = getopt.getopt(sys.argv[1:], "h", ["help"])
        except getopt.error, msg:
            print msg
            sys.exit(2)
        program = os.path.basename(sys.argv[0])
        if opts:
            # There's only the help options, so just dump some help:
            msg = __doc__ % {"program": program}
            print msg
            sys.exit()
        if len(args) not in [1, 2]:
            print "Usage: %s home [port]" % program
            sys.exit(2)

        instance_home = args[0]
        if not os.path.isabs(instance_home):
            instance_home = os.path.abspath(instance_home)

        for entry in sys.path:
            if os.path.exists(os.path.join(entry, 'Zope')):
                zope_home = entry
                break
        else:
            print "Can't find the Zope home (not in sys.path)"
            sys.exit(2)

        if args[1:]:
            port = int(args[1])
        else:
            port = 9999
        checkport(port)

        params = self.get_params(zope_home, instance_home, port)
        self.create(instance_home, params)

    def get_params(self, zope_home, instance_home, port):
        return {
            "package": "zeo",
            "PACKAGE": "ZEO",
            "zope_home": zope_home,
            "instance_home": instance_home,
            "port": port,
            "python": sys.executable,
            }

    def create(self, home, params):
        makedir(home)
        makedir(home, "etc")
        makedir(home, "var")
        makedir(home, "log")
        makedir(home, "bin")
        makefile(zeo_conf_template, home, "etc", "zeo.conf", **params)
        makexfile(zeoctl_template, home, "bin", "zeoctl", **params)
        makexfile(runzeo_template, home, "bin", "runzeo", **params)


def checkport(port):
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("", port))
    except socket.error:
        print "A process is already listening on port %d" % port
        sys.exit(2)
    s.close()

def which(program):
    strpath = os.getenv("PATH")
    binpath = strpath.split(os.pathsep)
    for dir in binpath:
        path = os.path.join(dir, program)
        if os.path.isfile(path) and os.access(path, os.X_OK):
            if not os.path.isabs(path):
                path = os.path.abspath(path)
            return path
    raise IOError, "can't find %r on path %r" % (program, strpath)

def makedir(*args):
    path = ""
    for arg in args:
        path = os.path.join(path, arg)
    mkdirs(path)
    return path

def mkdirs(path):
    if os.path.isdir(path):
        return
    head, tail = os.path.split(path)
    if head and tail and not os.path.isdir(head):
        mkdirs(head)
    os.mkdir(path)
    print "Created directory", path

def makefile(template, *args, **kwds):
    path = makedir(*args[:-1])
    path = os.path.join(path, args[-1])
    data = template % kwds
    if os.path.exists(path):
        f = open(path)
        olddata = f.read().strip()
        f.close()
        if olddata:
            if olddata != data.strip():
                print "Warning: not overwriting existing file %r" % path
            return path
    f = open(path, "w")
    f.write(data)
    f.close()
    print "Wrote file", path
    return path

def makexfile(template, *args, **kwds):
    path = makefile(template, *args, **kwds)
    umask = os.umask(022)
    os.umask(umask)
    mode = 0777 & ~umask
    if stat.S_IMODE(os.stat(path)[stat.ST_MODE]) != mode:
        os.chmod(path, mode)
        print "Changed mode for %s to %o" % (path, mode)
    return path

if __name__ == "__main__":
    main()
