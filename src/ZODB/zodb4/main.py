##############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
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
"""Script to convert a ZODB 4 file storage to a ZODB 3 file storage.

This is needed since Zope 3 is being changed to use ZODB 3 instead of
ZODB 4.

"""

import getopt
import os
import sys

try:
    __file__
except NameError:
    __file__ = os.path.realpath(sys.argv[0])

here = os.path.dirname(__file__)
topdir = os.path.dirname(os.path.dirname(here))

# Make sure that if we're run as a script, we can import the ZODB
# package and our sibling modules.
try:
    import ZODB.zodb4
except ImportError:
    sys.path.append(topdir)
    import ZODB.zodb4

from ZODB.lock_file import LockFile
from ZODB.zodb4 import conversion


class ConversionApp:

    def __init__(self, name=None, args=None):
        if name is None:
            name = os.path.basename(sys.argv[0])
        if args is None:
            args = sys.argv[1:]
        self.name = name
        self.verbosity = 0
        self.dbfile = None
        self.parse_args(args)

    def run(self):
        
        # Load server-independent site config
        from zope.configuration import xmlconfig
        context = xmlconfig.file('site.zcml', execute=True)

        if not os.path.exists(self.dbfile):
            self.error("input database does not exist: %s" % self.dbfile)
        base, ext = os.path.splitext(self.dbfile)
        if ext != ".fs":
            base = self.dbfile
        self.dbindex = self.dbfile + ".index"
        self.bakfile = base + ".fs4"
        self.bakindex = self.bakfile + ".index"
        if os.path.exists(self.bakfile):
            self.error("backup database already exists: %s\n"
                       "please move aside and try again" % self.bakfile)
        if os.path.exists(self.bakindex):
            self.error("backup database index already exists: %s\n"
                       "please move aside and try again" % self.bakindex)
        self.convert()

        # XXX the conversion script leaves an invalid index behind. Why?
        os.remove(self.dbindex)

    def convert(self):
        lock = LockFile(self.bakfile + ".lock")
        try:
            # move the ZODB 4 database to be the backup
            os.rename(self.dbfile, self.bakfile)
            if os.path.exists(self.dbindex):
                try:
                    os.rename(self.dbindex, self.bakindex)
                except:
                    # we couldn't rename *both*, so try to make sure we
                    # don't rename either
                    os.rename(self.bakfile, self.dbfile)
                    raise
            # go:
            converter = conversion.Conversion(self.bakfile, self.dbfile)
            converter.run()
        finally:
            lock.close()

    def parse_args(self, args):
        opts, args = getopt.getopt(args, "v", ["verbose"])
        for opt, arg in opts:
            if opt in ("-v", "--verbose"):
                self.verbosity += 1
        if len(args) == 0:
            # use default location for Data.fs
            self.dbfile = os.path.join(topdir, "Data.fs")
        elif len(args) == 1:
            self.dbfile = args[0]
        else:
            self.error("too many command-line arguments", rc=2)

    def error(self, message, rc=1):
        print >>sys.stderr, "%s: %s" % (self.name, message)
        sys.exit(rc)


def main():
    ConversionApp().run()

if __name__ == "__main__":
    main()
