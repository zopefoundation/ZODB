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
"""Cache simulation.

Usage: simul.py -s size tracefile

-s size: cache size in MB (default 20 MB)
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
    cachelimit = 20*1000*1000
    try:
        opts, args = getopt.getopt(sys.argv[1:], "s:")
    except getopt.error, msg:
        usage(msg)
        return 2
    for o, a in opts:
        if o == '-s':
            cachelimit = int(float(a) * 1e6)
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

    # Set up statistics
    flips = 0
    loads = 0
    hits = 0
    invals = 0
    writes = 0
    ts0 = None
    total_loads = 0
    total_hits = 0

    # Set up simulation data
    filelimit = cachelimit / 2
    filesize = [4, 4] # account for magic number
    fileoids = [{}, {}]
    current = 0 # index into filesize, fileoids

    # Print header
    print "%12s %12s %6s %6s %6s %6s %6s %6s" % (
        "__START_TIME", "___STOP_TIME", "LOADS", "HITS",
        "INVALS", "WRITES", "FLIPS", "HIT%")

    # Read trace file, simulating cache behavior
    while 1:
        # Read a reacord
        r = f.read(24)
        if len(r) < 24:
            break

        # Decode it
        ts, code, oid, serial = struct.unpack(">ii8s8s", r)
        dlen, version, code, ignored = (code & 0x7fffff00,
                                        code & 0x80,
                                        code & 0x7e,
                                        code & 0x01)
        if ts0 is None:
            ts0 = ts

        # Simulate cache behavior.  Use load hits, updates and stores
        # only (each load miss is followed immediately by a store
        # unless the object in fact did not exist).  Updates always write.
        if dlen and code & 0x70 in (0x20, 0x30, 0x50):
            if code == 0x3A:
                writes += 1
            else:
                loads += 1
                total_loads += 1
            if code != 0x3A and (fileoids[current].get(oid) or
                                 fileoids[1-current].get(oid)):
                hits += 1
                total_hits += 1
            else:
                # Simulate a miss+store.  Fudge because dlen is
                # rounded up to multiples of 256.  (31 is header
                # overhead per cache record; 8 is min data size.)
                dlen = max(31 + 8, dlen + 31 - 128)
                if filesize[current] + dlen > filelimit:
                    # Cache flip
                    flips += 1
                    current = 1 - current
                    filesize[current] = 4
                    fileoids[current] = {}
                filesize[current] += dlen
                fileoids[current][oid] = 1
        elif code & 0x70 == 0x10:
            # Invalidate
            if fileoids[current].get(oid):
                invals += 1
                del fileoids[current][oid]
            elif fileoids[1-current].get(oid):
                invals += 1
                del fileoids[1-current][oid]
        elif code == 0x00:
            # Restart
            if loads:
                report(ts0, ts, loads, hits, invals, writes, flips)
            loads = 0
            hits = 0
            flips = 0
            invals = 0
            writes = 0
            ts0 = None
            filesize = [4, 4] # account for magic number
            fileoids = [{}, {}]
            current = 0 # index into filesize, fileoids

    if loads:
        report(ts0, ts, loads, hits, invals, writes, flips)

    if total_loads:
        print "Overall: %d loads, %d hits, hit rate %.1f%%" % (
            total_loads, total_hits, 100.0 * total_hits / total_loads)

def report(ts0, ts, loads, hits, invals, writes, flips):
    hr = 100.0 * hits / max(loads, 1)
    print "%s %s %6d %6d %6d %6d %6d %6.1f%%" % (
        time.ctime(ts0)[4:-8], time.ctime(ts)[4:-8],
        loads, hits, invals, writes, flips, hr)

if __name__ == "__main__":
    sys.exit(main())
