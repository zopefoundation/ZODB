#!/usr/bin/python

##############################################################################
#
# Copyright (c) 2004 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################

"""FileStorage oid-tracer.

usage: fsoids.py [-f oid_file] Data.fs [oid]...

Display information about all occurrences of specified oids in a FileStorage.
This is meant for heavy debugging.

This includes all revisions of the oids, all objects referenced by the
oids, and all revisions of all objects referring to the oids.

If specified, oid_file is an input text file, containing one oid per
line.  oids are specified as integers, in any of Python's integer
notations (typically like 0x341a).  One or more oids can also be specified
on the command line.

The output is grouped by oid, from smallest to largest, and sub-grouped
by transaction, from oldest to newest.

This will not alter the FileStorage, but running against a live FileStorage
is not recommended (spurious error messages may result).

See testfsoids.py for a tutorial doctest.
"""
from __future__ import print_function

import sys

from ZODB.FileStorage.fsoids import Tracer


def usage():
    print(__doc__)


def main():
    import getopt

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'f:')
        if not args:
            usage()
            raise ValueError("Must specify a FileStorage")
        path = None
        for k, v in opts:
            if k == '-f':
                path = v
    except (getopt.error, ValueError):
        usage()
        raise

    c = Tracer(args[0])
    for oid in args[1:]:
        as_int = int(oid, 0)  # 0 == auto-detect base
        c.register_oids(as_int)
    if path is not None:
        for line in open(path):
            as_int = int(line, 0)
            c.register_oids(as_int)
    if not c.oids:
        raise ValueError("no oids specified")
    c.run()
    c.report()


if __name__ == "__main__":
    main()
