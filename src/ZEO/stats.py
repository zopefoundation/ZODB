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
"""Trace file statistics analyzer.

Usage: stats.py [-v] [-S] tracefile
-v: verbose; print each record
-S: don't print statistics (implies -v)
"""

import sys
import time
import getopt
import struct

def usage(msg):
    print >>sys.stderr, msg
    print >>sys.stderr, __doc__

def main():
    # Parse options
    verbose = 0
    dostats = 1
    try:
        opts, args = getopt.getopt(sys.argv[1:], "vS")
    except getopt.error, msg:
        usage(msg)
        return 2
    for o, a in opts:
        if o == "-v":
            verbose = 1
        if o == "-S":
            dostats = 0
            verbose = 1
    if len(args) != 1:
        usage("exactly one file argument required")
        return 2
    filename = args[0]

    # Open file
    try:
        f = open(filename, "rb")
    except IOError, msg:
        print "can't open %s: %s" % (filename, msg)
        return 1

    # Read file, gathering statistics, and printing each record if verbose
    bycode = {}
    records = 0
    while 1:
        r = f.read(24)
        if len(r) < 24:
            break
        records += 1
        ts, code, oid, serial = struct.unpack(">ii8s8s", r)
        dlen, code = code & 0x7fffff00, code & 0xff
        version = '-'
        if code & 0x80:
            version = 'V'
        current = code & 1
        code = code & 0x7e
        bycode[code] = bycode.get(code, 0) + 1
        if verbose:
            print "%s %d %02x %016x %016x %1s %s" % (
                time.ctime(ts)[4:-5],
                current,
                code,
                U64(oid),
                U64(serial),
                version,
                dlen and str(dlen) or "")
    bytes = f.tell()
    f.close()

    # Print statistics
    if dostats:
        print "\nStatistics for %d records (%d bytes):\n" % (records, bytes)
        codes = bycode.keys()
        codes.sort()
        print "%10s %4s %s" % ("Count", "Code", "Function (action)")
        for code in codes:
            print "%10d  %02x  %s" % (
                bycode.get(code, 0),
                code,
                explain.get(code) or "*** unknown code ***")

def U64(s):
    return struct.unpack(">Q", s)[0]

explain = {
    # The first hex digit shows the operation, the second the outcome.
    # If the second digit is in "02468" then it is a 'miss'.
    # If it is in "ACE" then it is a 'hit'.

    0x00: "_setup_trace (initialization)",

    0x10: "invalidate (miss)",
    0x1A: "invalidate (hit, version, writing 'n')",
    0x1C: "invalidate (hit, writing 'i')",

    0x20: "load (miss)",
    0x22: "load (miss, version, status 'n')",
    0x24: "load (miss, deleting index entry)",
    0x26: "load (miss, no non-version data)",
    0x28: "load (miss, version mismatch, no non-version data)",
    0x2A: "load (hit, returning non-version data)",
    0x2C: "load (hit, version mismatch, returning non-version data)",
    0x2E: "load (hit, returning version data)",

    0x3A: "update",

    0x40: "modifiedInVersion (miss)",
    0x4A: "modifiedInVersion (hit, return None, status 'n')",
    0x4C: "modifiedInVersion (hit, return '')",
    0x4E: "modifiedInVersion (hit, return version)",

    0x5A: "store (non-version data present)",
    0x5C: "store (only version data present)",

    0x70: "checkSize (cache flip)",
    }

if __name__ == "__main__":
    sys.exit(main())
