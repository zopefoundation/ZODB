##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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
"""Trace file statistics analyzer.

Usage: stats.py [-h] [-i interval] [-q] [-s] [-S] [-v] [-X] tracefile
-h: print histogram of object load frequencies
-i: summarizing interval in minutes (default 15; max 60)
-q: quiet; don't print summaries
-s: print histogram of object sizes
-S: don't print statistics
-v: verbose; print each record
-X: enable heuristic checking for misaligned records: oids > 2**32
    will be rejected; this requires the tracefile to be seekable
"""

"""File format:

Each record is 26 bytes, plus a variable number of bytes to store an oid,
with the following layout.  Numbers are big-endian integers.

Offset  Size  Contents

0       4     timestamp (seconds since 1/1/1970)
4       3     data size, in 256-byte increments, rounded up
7       1     code (see below)
8       2     object id length
10      8     start tid
18      8     end tid
26  variable  object id

The code at offset 7 packs three fields:

Mask    bits  Contents

0x80    1     set if there was a non-empty version string
0x7e    6     function and outcome code
0x01    1     current cache file (0 or 1)

The "current cache file" bit is no longer used; it refers to a 2-file
cache scheme used before ZODB 3.3.

The function and outcome codes are documented in detail at the end of
this file in the 'explain' dictionary.  Note that the keys there (and
also the arguments to _trace() in ClientStorage.py) are 'code & 0x7e',
i.e. the low bit is always zero.
"""

import sys
import time
import getopt
import struct

# we assign ctime locally to facilitate test replacement!
from time import ctime

def usage(msg):
    print >> sys.stderr, msg
    print >> sys.stderr, __doc__

def main(args=None):
    if args is None:
        args = sys.argv[1:]
    # Parse options
    verbose = False
    quiet = False
    dostats = True
    print_size_histogram = False
    print_histogram = False
    interval = 15*60 # Every 15 minutes
    heuristic = False
    try:
        opts, args = getopt.getopt(args, "hi:qsSvX")
    except getopt.error, msg:
        usage(msg)
        return 2
    for o, a in opts:
        if o == '-h':
            print_histogram = True
        elif o == "-i":
            interval = int(60 * float(a))
            if interval <= 0:
                interval = 60
            elif interval > 3600:
                interval = 3600
        elif o == "-q":
            quiet = True
            verbose = False
        elif o == "-s":
            print_size_histogram = True
        elif o == "-S":
            dostats = False
        elif o == "-v":
            verbose = True
        elif o == '-X':
            heuristic = True
        else:
            assert False, (o, opts)

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
            print >> sys.stderr, "can't read gzipped files (no module gzip)"
            return 1
        try:
            f = gzip.open(filename, "rb")
        except IOError, msg:
            print >> sys.stderr, "can't open %s: %s" % (filename, msg)
            return 1
    elif filename == '-':
        # Read from stdin
        f = sys.stdin
    else:
        # Open regular file
        try:
            f = open(filename, "rb")
        except IOError, msg:
            print >> sys.stderr, "can't open %s: %s" % (filename, msg)
            return 1

    rt0 = time.time()
    bycode = {}     # map code to count of occurrences
    byinterval = {} # map code to count in current interval
    records = 0     # number of trace records read
    versions = 0    # number of trace records with versions
    datarecords = 0 # number of records with dlen set
    datasize = 0L   # sum of dlen across records with dlen set
    oids = {}       # map oid to number of times it was loaded
    bysize = {}     # map data size to number of loads
    bysizew = {}    # map data size to number of writes
    total_loads = 0
    t0 = None       # first timestamp seen
    te = None       # most recent timestamp seen
    h0 = None       # timestamp at start of current interval
    he = None       # timestamp at end of current interval
    thisinterval = None  # generally te//interval
    f_read = f.read
    unpack = struct.unpack
    FMT = ">iiH8s8s"
    FMT_SIZE = struct.calcsize(FMT)
    assert FMT_SIZE == 26
    # Read file, gathering statistics, and printing each record if verbose.
    print ' '*16, "%7s %7s %7s %7s" % ('loads', 'hits', 'inv(h)', 'writes'),
    print 'hitrate'
    try:
        while 1:
            r = f_read(FMT_SIZE)
            if len(r) < FMT_SIZE:
                break
            ts, code, oidlen, start_tid, end_tid = unpack(FMT, r)
            if ts == 0:
                # Must be a misaligned record caused by a crash.
                if not quiet:
                    print "Skipping 8 bytes at offset", f.tell() - FMT_SIZE
                    f.seek(f.tell() - FMT_SIZE + 8)
                continue
            oid = f_read(oidlen)
            if len(oid) < oidlen:
                break
            records += 1
            if t0 is None:
                t0 = ts
                thisinterval = t0 // interval
                h0 = he = ts
            te = ts
            if ts // interval != thisinterval:
                if not quiet:
                    dumpbyinterval(byinterval, h0, he)
                byinterval = {}
                thisinterval = ts // interval
                h0 = ts
            he = ts
            dlen, code = (code & 0x7fffff00) >> 8, code & 0xff
            if dlen:
                datarecords += 1
                datasize += dlen
            if code & 0x80:
                version = 'V'
                versions += 1
            else:
                version = '-'
            code &= 0x7e
            bycode[code] = bycode.get(code, 0) + 1
            byinterval[code] = byinterval.get(code, 0) + 1
            if dlen:
                if code & 0x70 == 0x20: # All loads
                    bysize[dlen] = d = bysize.get(dlen) or {}
                    d[oid] = d.get(oid, 0) + 1
                elif code & 0x70 == 0x50: # All stores
                    bysizew[dlen] = d = bysizew.get(dlen) or {}
                    d[oid] = d.get(oid, 0) + 1
            if verbose:
                print "%s %02x %s %016x %016x %c%s" % (
                    ctime(ts)[4:-5],
                    code,
                    oid_repr(oid),
                    U64(start_tid),
                    U64(end_tid),
                    version,
                    dlen and (' '+str(dlen)) or "")
            if code & 0x70 == 0x20:
                oids[oid] = oids.get(oid, 0) + 1
                total_loads += 1
            elif code == 0x00:    # restart
                if not quiet:
                    dumpbyinterval(byinterval, h0, he)
                byinterval = {}
                thisinterval = ts // interval
                h0 = he = ts
                if not quiet:
                    print ctime(ts)[4:-5],
                    print '='*20, "Restart", '='*20
    except KeyboardInterrupt:
        print "\nInterrupted.  Stats so far:\n"

    end_pos = f.tell()
    f.close()
    rte = time.time()
    if not quiet:
        dumpbyinterval(byinterval, h0, he)

    # Error if nothing was read
    if not records:
        print >> sys.stderr, "No records processed"
        return 1

    # Print statistics
    if dostats:
        print
        print "Read %s trace records (%s bytes) in %.1f seconds" % (
            addcommas(records), addcommas(end_pos), rte-rt0)
        print "Versions:   %s records used a version" % addcommas(versions)
        print "First time: %s" % ctime(t0)
        print "Last time:  %s" % ctime(te)
        print "Duration:   %s seconds" % addcommas(te-t0)
        print "Data recs:  %s (%.1f%%), average size %d bytes" % (
            addcommas(datarecords),
            100.0 * datarecords / records,
            datasize / datarecords)
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

    # Print histogram.
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

    # Print size histogram.
    if print_size_histogram:
        print
        print "Histograms of object sizes"
        print
        dumpbysize(bysizew, "written", "writes")
        dumpbysize(bysize, "loaded", "loads")

def dumpbysize(bysize, how, how2):
    print
    print "Unique sizes %s: %s" % (how, addcommas(len(bysize)))
    print "%10s %6s %6s" % ("size", "objs", how2)
    sizes = bysize.keys()
    sizes.sort()
    for size in sizes:
        loads = 0
        for n in bysize[size].itervalues():
            loads += n
        print "%10s %6d %6d" % (addcommas(size),
                                len(bysize.get(size, "")),
                                loads)

def dumpbyinterval(byinterval, h0, he):
    loads = hits = invals = writes = 0
    for code in byinterval:
        if code & 0x20:
            n = byinterval[code]
            loads += n
            if code in (0x22, 0x26):
                hits += n
        elif code & 0x40:
            writes +=  byinterval[code]
        elif code & 0x10:
            if code != 0x10:
                invals += byinterval[code]

    if loads:
        hr = "%5.1f%%" % (100.0 * hits / loads)
    else:
        hr = 'n/a'

    print "%s-%s %7s %7s %7s %7s %7s" % (
        ctime(h0)[4:-8], ctime(he)[14:-8],
        loads, hits, invals, writes, hr)

def hitrate(bycode):
    loads = hits = 0
    for code in bycode:
        if code & 0x70 == 0x20:
            n = bycode[code]
            loads += n
            if code in (0x22, 0x26):
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
    return struct.unpack(">Q", s)[0]

def oid_repr(oid):
    if isinstance(oid, str) and len(oid) == 8:
        return '%16x' % U64(oid)
    else:
        return repr(oid)

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
    0x1A: "invalidate (hit, version)",
    0x1C: "invalidate (hit, saving non-current)",
    # 0x1E can occur during startup verification.
    0x1E: "invalidate (hit, discarding current or non-current)",

    0x20: "load (miss)",
    0x22: "load (hit)",
    0x24: "load (non-current, miss)",
    0x26: "load (non-current, hit)",

    0x50: "store (version)",
    0x52: "store (current, non-version)",
    0x54: "store (non-current)",
    }

if __name__ == "__main__":
    sys.exit(main())
