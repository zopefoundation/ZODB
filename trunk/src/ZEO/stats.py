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

Usage: stats.py [-h] [-i interval] [-q] [-S] [-v] tracefile
-h: print histogram
-i: summarizing interval in minutes (default 15; max 60)
-q: quiet; don't print summaries
-S: don't print statistics
-v: verbose; print each record
"""

"""File format:

Each record is 24 bytes, with the following layout.  Numbers are
big-endian integers.

Offset  Size  Contents

0       4     timestamp (seconds since 1/1/1970)
4       3     data size, in 256-byte increments, rounded up
7       1     code (see below)
8       8     object id
16      8     serial number

The code at offset 7 packs three fields:

Mask    bits  Contents

0x80    1     set if there was a non-empty version string
0x7e    6     function and outcome code
0x01    1     current cache file (0 or 1)

The function and outcome codes are documented in detail at the end of
this file in the 'explain' dictionary.  Note that the keys there (and
also the arguments to _trace() in ClientStorage.py) are 'code & 0x7e',
i.e. the low bit is always zero.
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
    quiet = 0
    dostats = 1
    print_histogram = 0
    interval = 900 # Every 15 minutes
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hi:qSv")
    except getopt.error, msg:
        usage(msg)
        return 2
    for o, a in opts:
        if o == '-h':
            print_histogram = 1
        if o == "-i":
            interval = int(60 * float(a))
            if interval <= 0:
                interval = 60
            elif interval > 3600:
                interval = 3600
        if o == "-q":
            quiet = 1
            verbose = 0
        if o == "-S":
            dostats = 0
        if o == "-v":
            verbose = 1
    if len(args) != 1:
        usage("exactly one file argument required")
        return 2
    filename = args[0]

    # Open file
    if filename.endswith(".gz"):
        # Open gzipped file
        try:
            import gzip
        except ImportError:
            print >>sys.stderr,  "can't read gzipped files (no module gzip)"
            return 1
        try:
            f = gzip.open(filename, "rb")
        except IOError, msg:
            print >>sys.stderr,  "can't open %s: %s" % (filename, msg)
            return 1
    elif filename == '-':
        # Read from stdin
        f = sys.stdin
    else:
        # Open regular file
        try:
            f = open(filename, "rb")
        except IOError, msg:
            print >>sys.stderr,  "can't open %s: %s" % (filename, msg)
            return 1

    # Read file, gathering statistics, and printing each record if verbose
    rt0 = time.time()
    bycode = {}
    records = 0
    versions = 0
    t0 = te = None
    datarecords = 0
    datasize = 0L
    file0 = file1 = 0
    oids = {}
    total_loads = 0
    byinterval = {}
    thisinterval = None
    h0 = he = None
    while 1:
        r = f.read(24)
        if len(r) < 24:
            break
        records += 1
        ts, code, oid, serial = struct.unpack(">ii8s8s", r)
        if t0 is None:
            t0 = ts
            thisinterval = t0 / interval
            h0 = he = ts
        te = ts
        if ts / interval != thisinterval:
            if not quiet:
                dumpbyinterval(byinterval, h0, he)
            byinterval = {}
            thisinterval = ts / interval
            h0 = ts
        he = ts
        dlen, code = code & 0x7fffff00, code & 0xff
        if dlen:
            datarecords += 1
            datasize += dlen
        version = '-'
        if code & 0x80:
            version = 'V'
            versions += 1
        current = code & 1
        if current:
            file1 += 1
        else:
            file0 += 1
        code = code & 0x7e
        bycode[code] = bycode.get(code, 0) + 1
        byinterval[code] = byinterval.get(code, 0) + 1
        if verbose:
            print "%s %d %02x %016x %016x %1s %s" % (
                time.ctime(ts)[4:-5],
                current,
                code,
                U64(oid),
                U64(serial),
                version,
                dlen and str(dlen) or "")
        if code & 0x70 == 0x20:
            oids[oid] = oids.get(oid, 0) + 1
            total_loads += 1
        if code in (0x00, 0x70):
            if not quiet:
                dumpbyinterval(byinterval, h0, he)
            byinterval = {}
            thisinterval = ts / interval
            h0 = he = ts
            if not quiet:
                print time.ctime(ts)[4:-5],
                if code == 0x00:
                    print '='*20, "Restart", '='*20
                else:
                    print '-'*20, "Flip->%d" % current, '-'*20

    f.close()
    rte = time.time()
    if not quiet:
        dumpbyinterval(byinterval, h0, he)

    # Error if nothing was read
    if not records:
        print >>sys.stderr, "No records processed"
        return 1

    # Print statistics
    if dostats:
        print
        print "Read %s records (%s bytes) in %.1f seconds" % (
            addcommas(records), addcommas(records*24), rte-rt0)
        print "Versions:   %s records used a version" % addcommas(versions)
        print "First time: %s" % time.ctime(t0)
        print "Last time:  %s" % time.ctime(te)
        print "Duration:   %s seconds" % addcommas(te-t0)
        print "File stats: %s in file 0; %s in file 1" % (
            addcommas(file0), addcommas(file1))
        print "Data recs:  %s (%.1f%%), average size %.1f KB" % (
            addcommas(datarecords),
            100.0 * datarecords / records,
            datasize / 1024.0 / datarecords)
        print "Hit rate:   %.1f%% (load hits / loads)" % hitrate(bycode)
        print
        codes = bycode.keys()
        codes.sort()
        print "%13s %4s %s" % ("Count", "Code", "Function (action)")
        for code in codes:
            print "%13s  %02x  %s" % (
                addcommas(bycode.get(code, 0)),
                code,
                explain.get(code) or "*** unknown code ***")

    # Print histogram
    if print_histogram:
        print
        print "Histogram of object load frequency"
        total = len(oids)
        print "Unique oids: %s" % addcommas(total)
        print "Total loads: %s" % addcommas(total_loads)
        s = addcommas(total)
        width = max(len(s), len("objects"))
        fmt = "%5d %" + str(width) + "s %5.1f%% %5.1f%% %5.1f%%"
        hdr = "%5s %" + str(width) + "s %6s %6s %6s"
        print hdr % ("loads", "objects", "%obj", "%load", "%cum")
        cum = 0.0
        for binsize, count in histogram(oids):
            obj_percent = 100.0 * count / total
            load_percent = 100.0 * count * binsize / total_loads
            cum += load_percent
            print fmt % (binsize, addcommas(count),
                         obj_percent, load_percent, cum)

def dumpbyinterval(byinterval, h0, he):
    loads = 0
    hits = 0
    for code in byinterval.keys():
        if code & 0x70 == 0x20:
            n = byinterval[code]
            loads += n
            if code in (0x2A, 0x2C, 0x2E):
                hits += n
    if not loads:
        return
    if loads:
        hr = 100.0 * hits / loads
    else:
        hr = 0.0
    print "%s-%s %10s loads, %10s hits,%5.1f%% hit rate" % (
        time.ctime(h0)[4:-8], time.ctime(he)[14:-8],
        addcommas(loads), addcommas(hits), hr)

def hitrate(bycode):
    loads = 0
    hits = 0
    for code in bycode.keys():
        if code & 0x70 == 0x20:
            n = bycode[code]
            loads += n
            if code in (0x2A, 0x2C, 0x2E):
                hits += n
    if loads:
        return 100.0 * hits / loads
    else:
        return 0.0

def histogram(d):
    bins = {}
    for v in d.itervalues():
        bins[v] = bins.get(v, 0) + 1
    L = bins.items()
    L.sort()
    return L

def U64(s):
    h, v = struct.unpack(">II", s)
    return (long(h) << 32) + v

def addcommas(n):
    sign, s = '', str(n)
    if s[0] == '-':
        sign, s = '-', s[1:]
    i = len(s) - 3
    while i > 0:
        s = s[:i] + ',' + s[i:]
        i -= 3
    return sign + s

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
