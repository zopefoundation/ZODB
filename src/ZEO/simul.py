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

Usage: simul.py [-s size] tracefile

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

    # Create simulation object
    sim = ZEOCacheSimulation(cachelimit)

    # Print output header
    sim.printheader()

    # Read trace file, simulating cache behavior
    while 1:
        # Read a record
        r = f.read(24)
        if len(r) < 24:
            break
        # Decode it
        ts, code, oid, serial = struct.unpack(">ii8s8s", r)
        dlen, version, code, current = (code & 0x7fffff00,
                                        code & 0x80,
                                        code & 0x7e,
                                        code & 0x01)
        # And pass it to the simulation
        sim.event(ts, dlen, version, code, current, oid, serial)

    # Finish simulation
    sim.finish()

    # Exit code from main()
    return 0

class Simulation:

    """Base class for simulations.

    The driver program calls: event(), printheader(), finish().

    The standard event() method calls these additional methods:
    write(), load(), inval(), report(), restart(); the standard
    finish() method also calls report().

    """

    def __init__(self):
        # Initialize global statistics
        self.epoch = None
        self.total_loads = 0
        self.total_hits = 0 # Subclass must increment
        self.total_invals = 0
        self.total_writes = 0
        # Reset per-run statistics and set up simulation data
        self.restart()

    def restart(self):
        # Reset per-run statistics
        self.loads = 0
        self.hits = 0 # Subclass must increment
        self.invals = 0
        self.writes = 0
        self.ts0 = None

    def event(self, ts, dlen, _version, code, _current, oid, _serial):
        # Record first and last timestamp seen
        if self.ts0 is None:
            self.ts0 = ts
            if self.epoch is None:
                self.epoch = ts
        self.ts1 = ts

        # Simulate cache behavior.  Use load hits, updates and stores
        # only (each load miss is followed immediately by a store
        # unless the object in fact did not exist).  Updates always write.
        if dlen and code & 0x70 in (0x20, 0x30, 0x50):
            if code == 0x3A:
                # Update
                self.writes += 1
                self.total_writes += 1
                self.write(oid, dlen)
            else:
                # Load hit or store -- these are really the load requests
                self.loads += 1
                self.total_loads += 1
                self.load(oid, dlen)
        elif code & 0x70 == 0x10:
            # Invalidate
            self.invals += 1
            self.total_invals += 1
            self.inval(oid)
        elif code == 0x00:
            # Restart
            self.report()
            self.restart()

    def printheader(self):
        pass

    def write(self, oid, size):
        pass

    def load(self, oid, size):
        pass

    def inval(self, oid):
        pass

    def finish(self):
        self.report()

    def report(self):
        pass

class ZEOCacheSimulation(Simulation):

    """Simulate the current (ZEO 1.0 and 2.0) ZEO cache behavior.

    This assumes the cache is not persistent (we don't know how to
    simulate cache validation.)

    """

    def __init__(self, cachelimit):
        # Initialize base class
        Simulation.__init__(self)
        # Store simulation parameters
        self.filelimit = cachelimit / 2
        # Initialize additional global statistics
        self.total_flips = 0

    def restart(self):
        # Reset base class
        Simulation.restart(self)
        # Reset additional per-run statistics
        self.flips = 0
        # Set up simulation
        self.filesize = [4, 4] # account for magic number
        self.fileoids = [{}, {}]
        self.current = 0 # index into filesize, fileoids

    def load(self, oid, size):
        if (self.fileoids[self.current].get(oid) or
            self.fileoids[1 - self.current].get(oid)):
            self.hits += 1
            self.total_hits += 1
        else:
            self.write(oid, size)

    def write(self, oid, size):
        # Fudge because size is rounded up to multiples of 256.  (31
        # is header overhead per cache record; 127 is to compensate
        # for rounding up to multiples of 256.)
        size = size + 31 - 127
        if self.filesize[self.current] + size > self.filelimit:
            # Cache flip
            self.flips += 1
            self.total_flips += 1
            self.current = 1 - self.current
            self.filesize[self.current] = 4
            self.fileoids[self.current] = {}
        self.filesize[self.current] += size
        self.fileoids[self.current][oid] = 1

    def inval(self, oid):
        if self.fileoids[self.current].get(oid):
            del self.fileoids[self.current][oid]
        elif self.fileoids[1 - self.current].get(oid):
            del self.fileoids[1 - self.current][oid]

    format = "%12s %9s %8s %8s %6s %6s %5s %6s"

    def printheader(self):
        print self.format % (
            "START TIME", "DURATION", "LOADS", "HITS",
            "INVALS", "WRITES", "FLIPS", "HITRATE")

    def report(self):
        if self.loads:
            print self.format % (
                time.ctime(self.ts0)[4:-8],
                duration(self.ts1 - self.ts0),
                self.loads, self.hits, self.invals, self.writes, self.flips,
                hitrate(self.loads, self.hits))

    def finish(self):
        if self.loads:
            self.report()
        if self.total_loads:
            print (self.format + " OVERALL") % (
                time.ctime(self.epoch)[4:-8],
                duration(self.ts1 - self.epoch),
                self.total_loads,
                self.total_hits,
                self.total_invals,
                self.total_writes,
                self.total_flips,
                hitrate(self.total_loads, self.total_hits))

def hitrate(loads, hits):
    return "%5.1f%%" % (100.0 * hits / max(1, loads))

def duration(secs):

    mm, ss = divmod(secs, 60)
    hh, mm = divmod(mm, 60)
    if hh:
        return "%d:%02d:%02d" % (hh, mm, ss)
    if mm:
        return "%d:%02d" % (mm, ss)
    return "%d" % ss

if __name__ == "__main__":
    sys.exit(main())
