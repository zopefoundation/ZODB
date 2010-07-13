#! /usr/bin/env python
##############################################################################
#
# Copyright (c) 2001-2005 Zope Foundation and Contributors.
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
"""Cache simulation.

Usage: simul.py [-s size] tracefile

Options:
-s size: cache size in MB (default 20 MB)
-i: summarizing interval in minutes (default 15; max 60)

Note:

- The simulation isn't perfect.

- The simulation will be far off if the trace file
  was created starting with a non-empty cache


"""

import bisect
import getopt
import math
import struct
import re
import sys
import time
import ZEO.cache

from ZODB.utils import z64, u64

# we assign ctime locally to facilitate test replacement!
from time import ctime

def usage(msg):
    print >> sys.stderr, msg
    print >> sys.stderr, __doc__

def main(args=None):
    if args is None:
        args = sys.argv[1:]
    # Parse options.
    MB = 1<<20
    cachelimit = 20*MB
    simclass = CircularCacheSimulation
    interval_step = 15
    try:
        opts, args = getopt.getopt(args, "s:i:")
    except getopt.error, msg:
        usage(msg)
        return 2
    for o, a in opts:
        if o == '-s':
            cachelimit = int(float(a)*MB)
        elif o == '-i':
            interval_step = int(a)
        else:
            assert False, (o, a)

    interval_step *= 60
    if interval_step <= 0:
        interval_step = 60
    elif interval_step > 3600:
        interval_step = 3600

    if len(args) != 1:
        usage("exactly one file argument required")
        return 2
    filename = args[0]

    # Open file.
    if filename.endswith(".gz"):
        # Open gzipped file.
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
    elif filename == "-":
        # Read from stdin.
        f = sys.stdin
    else:
        # Open regular file.
        try:
            f = open(filename, "rb")
        except IOError, msg:
            print >> sys.stderr, "can't open %s: %s" % (filename, msg)
            return 1

    # Create simulation object.
    sim = simclass(cachelimit)
    interval_sim = simclass(cachelimit)

    # Print output header.
    sim.printheader()

    # Read trace file, simulating cache behavior.
    f_read = f.read
    unpack = struct.unpack
    FMT = ">iiH8s8s"
    FMT_SIZE = struct.calcsize(FMT)
    assert FMT_SIZE == 26

    last_interval = None
    while 1:
        # Read a record and decode it.
        r = f_read(FMT_SIZE)
        if len(r) < FMT_SIZE:
            break
        ts, code, oidlen, start_tid, end_tid = unpack(FMT, r)
        if ts == 0:
            # Must be a misaligned record caused by a crash; skip 8 bytes
            # and try again.  Why 8?  Lost in the mist of history.
            f.seek(f.tell() - FMT_SIZE + 8)
            continue
        oid = f_read(oidlen)
        if len(oid) < oidlen:
            break
        # Decode the code.
        dlen, version, code = ((code & 0x7fffff00) >> 8,
                               code & 0x80,
                               code & 0x7e)
        # And pass it to the simulation.
        this_interval = int(ts)/interval_step
        if this_interval != last_interval:
            if last_interval is not None:
                interval_sim.report()
                interval_sim.restart()
            last_interval = this_interval
        sim.event(ts, dlen, version, code, oid, start_tid, end_tid)
        interval_sim.event(ts, dlen, version, code, oid, start_tid, end_tid)

    f.close()
    # Finish simulation.
    interval_sim.report()
    sim.finish()

class Simulation(object):
    """Base class for simulations.

    The driver program calls: event(), printheader(), finish().

    The standard event() method calls these additional methods:
    write(), load(), inval(), report(), restart(); the standard
    finish() method also calls report().
    """

    def __init__(self, cachelimit):
        self.cachelimit = cachelimit
        # Initialize global statistics.
        self.epoch = None
        self.total_loads = 0
        self.total_hits = 0       # subclass must increment
        self.total_invals = 0     # subclass must increment
        self.total_writes = 0
        if not hasattr(self, "extras"):
            self.extras = (self.extraname,)
        self.format = self.format + " %7s" * len(self.extras)
        # Reset per-run statistics and set up simulation data.
        self.restart()

    def restart(self):
        # Reset per-run statistics.
        self.loads = 0
        self.hits = 0       # subclass must increment
        self.invals = 0     # subclass must increment
        self.writes = 0
        self.ts0 = None

    def event(self, ts, dlen, _version, code, oid,
              start_tid, end_tid):
        # Record first and last timestamp seen.
        if self.ts0 is None:
            self.ts0 = ts
            if self.epoch is None:
                self.epoch = ts
        self.ts1 = ts

        # Simulate cache behavior.  Caution:  the codes in the trace file
        # record whether the actual cache missed or hit on each load, but
        # that bears no necessary relationship to whether the simulated cache
        # will hit or miss.  Relatedly, if the actual cache needed to store
        # an object, the simulated cache may not need to (it may already
        # have the data).
        action = code & 0x70
        if action & 0x20:
            # Load.
            self.loads += 1
            self.total_loads += 1
            # Asserting that dlen is 0 iff it's a load miss.
            # assert (dlen == 0) == (code in (0x20, 0x24))
            self.load(oid, dlen, start_tid, code)
        elif action & 0x40:
            # Store.
            assert dlen
            self.write(oid, dlen, start_tid, end_tid)
        elif action & 0x10:
            # Invalidate.
            self.inval(oid, start_tid)
        elif action == 0x00:
            # Restart.
            self.restart()
        else:
            raise ValueError("unknown trace code 0x%x" % code)

    def write(self, oid, size, start_tid, end_tid):
        pass

    def load(self, oid, size, start_tid, code):
        # Must increment .hits and .total_hits as appropriate.
        pass

    def inval(self, oid, start_tid):
        # Must increment .invals and .total_invals as appropriate.
        pass

    format = "%12s %6s %7s %7s %6s %6s %7s"

    # Subclass should override extraname to name known instance variables;
    # if extraname is 'foo', both self.foo and self.total_foo must exist:
    extraname = "*** please override ***"

    def printheader(self):
        print "%s, cache size %s bytes" % (self.__class__.__name__,
                                           addcommas(self.cachelimit))
        self.extraheader()
        extranames = tuple([s.upper() for s in self.extras])
        args = ("START TIME", "DUR.", "LOADS", "HITS",
                "INVALS", "WRITES", "HITRATE") + extranames
        print self.format % args

    def extraheader(self):
        pass

    nreports = 0

    def report(self):
        self.nreports += 1
        args = (ctime(self.ts0)[4:-8],
                duration(self.ts1 - self.ts0),
                self.loads, self.hits, self.invals, self.writes,
                hitrate(self.loads, self.hits))
        args += tuple([getattr(self, name) for name in self.extras])
        print self.format % args

    def finish(self):
        # Make sure that the last line of output ends with "OVERALL".  This
        # makes it much easier for another program parsing the output to
        # find summary statistics.
        print '-'*74
        if self.nreports < 2:
            self.report()
        else:
            self.report()
            args = (
                ctime(self.epoch)[4:-8],
                duration(self.ts1 - self.epoch),
                self.total_loads,
                self.total_hits,
                self.total_invals,
                self.total_writes,
                hitrate(self.total_loads, self.total_hits))
            args += tuple([getattr(self, "total_" + name)
                           for name in self.extras])
            print self.format % args


# For use in CircularCacheSimulation.
class CircularCacheEntry(object):
    __slots__ = (# object key:  an (oid, start_tid) pair, where
                 # start_tid is the tid of the transaction that created
                 # this revision of oid
                 'key',

                 # tid of transaction that created the next revision;
                 # z64 iff this is the current revision
                 'end_tid',

                 # Offset from start of file to the object's data
                 # record; this includes all overhead bytes (status
                 # byte, size bytes, etc).
                 'offset',
                )

    def __init__(self, key, end_tid, offset):
        self.key = key
        self.end_tid = end_tid
        self.offset = offset

from ZEO.cache import ZEC_HEADER_SIZE

class CircularCacheSimulation(Simulation):
    """Simulate the ZEO 3.0 cache."""

    # The cache is managed as a single file with a pointer that
    # goes around the file, circularly, forever.  New objects
    # are written at the current pointer, evicting whatever was
    # there previously.

    extras = "evicts", "inuse"

    def __init__(self, cachelimit):
        from ZEO import cache

        Simulation.__init__(self, cachelimit)
        self.total_evicts = 0  # number of cache evictions

        # Current offset in file.
        self.offset = ZEC_HEADER_SIZE

        # Map offset in file to (size, CircularCacheEntry) pair, or to
        # (size, None) if the offset starts a free block.
        self.filemap = {ZEC_HEADER_SIZE: (self.cachelimit - ZEC_HEADER_SIZE,
                                           None)}
        # Map key to CircularCacheEntry.  A key is an (oid, tid) pair.
        self.key2entry = {}

        # Map oid to tid of current revision.
        self.current = {}

        # Map oid to list of (start_tid, end_tid) pairs in sorted order.
        # Used to find matching key for load of non-current data.
        self.noncurrent = {}

        # The number of overhead bytes needed to store an object pickle
        # on disk (all bytes beyond those needed for the object pickle).
        self.overhead = ZEO.cache.allocated_record_overhead

        # save evictions so we can replay them, if necessary
        self.evicted = {}

    def restart(self):
        Simulation.restart(self)
        self.evicts = 0
        self.evicted_hit = self.evicted_miss = 0

    evicted_hit = evicted_miss = 0
    def load(self, oid, size, tid, code):
        if (code == 0x20) or (code == 0x22):
            # Trying to load current revision.
            if oid in self.current: # else it's a cache miss
                self.hits += 1
                self.total_hits += 1
            elif oid in self.evicted:
                size, e = self.evicted[oid]
                self.write(oid, size, e.key[1], z64, 1)
                self.evicted_hit += 1
            else:
                self.evicted_miss += 1

            return

        # May or may not be trying to load current revision.
        cur_tid = self.current.get(oid)
        if cur_tid == tid:
            self.hits += 1
            self.total_hits += 1
            return

        # It's a load for non-current data.  Do we know about this oid?
        L = self.noncurrent.get(oid)
        if L is None:
            return  # cache miss
        i = bisect.bisect_left(L, (tid, None))
        if i == 0:
            # This tid is smaller than any we know about -- miss.
            return
        lo, hi = L[i-1]
        assert lo < tid
        if tid > hi:
            # No data in the right tid range -- miss.
            return
        # Cache hit.
        self.hits += 1
        self.total_hits += 1

    # (oid, tid) is in the cache.  Remove it:  take it out of key2entry,
    # and in `filemap` mark the space it occupied as being free.  The
    # caller is responsible for removing it from `current` or `noncurrent`.
    def _remove(self, oid, tid):
        key = oid, tid
        e = self.key2entry.pop(key)
        pos = e.offset
        size, _e = self.filemap[pos]
        assert e is _e
        self.filemap[pos] = size, None

    def _remove_noncurrent_revisions(self, oid):
        noncurrent_list = self.noncurrent.get(oid)
        if noncurrent_list:
            self.invals += len(noncurrent_list)
            self.total_invals += len(noncurrent_list)
            for start_tid, end_tid in noncurrent_list:
                self._remove(oid, start_tid)
            del self.noncurrent[oid]

    def inval(self, oid, tid):
        if tid == z64:
            # This is part of startup cache verification:  forget everything
            # about this oid.
            self._remove_noncurrent_revisions(oid)

        cur_tid = self.current.get(oid)
        if cur_tid is None:
            # We don't have current data, so nothing more to do.
            return

        # We had current data for oid, but no longer.
        self.invals += 1
        self.total_invals += 1
        del self.current[oid]
        if tid == z64:
            # Startup cache verification:  forget this oid entirely.
            self._remove(oid, cur_tid)
            return

        # Our current data becomes non-current data.
        # Add the validity range to the list of non-current data for oid.
        assert cur_tid < tid
        L = self.noncurrent.setdefault(oid, [])
        bisect.insort_left(L, (cur_tid, tid))
        # Update the end of oid's validity range in its CircularCacheEntry.
        e = self.key2entry[oid, cur_tid]
        assert e.end_tid == z64
        e.end_tid = tid

    def write(self, oid, size, start_tid, end_tid, evhit=0):
        if end_tid == z64:
            # Storing current revision.
            if oid in self.current:  # we already have it in cache
                if evhit:
                    import pdb; pdb.set_trace()
                    raise ValueError('WTF')
                return
            self.current[oid] = start_tid
            self.writes += 1
            self.total_writes += 1
            self.add(oid, size, start_tid)
            return
        if evhit:
            import pdb; pdb.set_trace()
            raise ValueError('WTF')
        # Storing non-current revision.
        L = self.noncurrent.setdefault(oid, [])
        p = start_tid, end_tid
        if p in L:
            return  # we already have it in cache
        bisect.insort_left(L, p)
        self.writes += 1
        self.total_writes += 1
        self.add(oid, size, start_tid, end_tid)

    # Add `oid` to the cache, evicting objects as needed to make room.
    # This updates `filemap` and `key2entry`; it's the caller's
    # responsibilty to update `current` or `noncurrent` appropriately.
    def add(self, oid, size, start_tid, end_tid=z64):
        key = oid, start_tid
        assert key not in self.key2entry
        size += self.overhead
        avail = self.makeroom(size+1)   # see cache.py
        e = CircularCacheEntry(key, end_tid, self.offset)
        self.filemap[self.offset] = size, e
        self.key2entry[key] = e
        self.offset += size
        # All the space made available must be accounted for in filemap.
        excess = avail - size
        if excess:
            self.filemap[self.offset] = excess, None

    # Evict enough objects to make at least `need` contiguous bytes, starting
    # at `self.offset`, available.  Evicted objects are removed from
    # `filemap`, `key2entry`, `current` and `noncurrent`.  The caller is
    # responsible for adding new entries to `filemap` to account for all
    # the freed bytes, and for advancing `self.offset`.  The number of bytes
    # freed is the return value, and will be >= need.
    def makeroom(self, need):
        if self.offset + need > self.cachelimit:
            self.offset = ZEC_HEADER_SIZE
        pos = self.offset
        while need > 0:
            assert pos < self.cachelimit
            size, e = self.filemap.pop(pos)
            if e:   # there is an object here (else it's already free space)
                self.evicts += 1
                self.total_evicts += 1
                assert pos == e.offset
                _e = self.key2entry.pop(e.key)
                assert e is _e
                oid, start_tid = e.key
                if e.end_tid == z64:
                    del self.current[oid]
                    self.evicted[oid] = size-self.overhead, e
                else:
                    L = self.noncurrent[oid]
                    L.remove((start_tid, e.end_tid))
            need -= size
            pos += size
        return pos - self.offset  # total number of bytes freed

    def report(self):
        self.check()
        free = used = total = 0
        for size, e in self.filemap.itervalues():
            total += size
            if e:
                used += size
            else:
                free += size

        self.inuse = round(100.0 * used / total, 1)
        self.total_inuse = self.inuse
        Simulation.report(self)
        #print self.evicted_hit, self.evicted_miss

    def check(self):
        oidcount = 0
        pos = ZEC_HEADER_SIZE
        while pos < self.cachelimit:
            size, e = self.filemap[pos]
            if e:
                oidcount += 1
                assert self.key2entry[e.key].offset == pos
            pos += size
        assert oidcount == len(self.key2entry)
        assert pos == self.cachelimit

    def dump(self):
        print len(self.filemap)
        L = list(self.filemap)
        L.sort()
        for k in L:
            v = self.filemap[k]
            print k, v[0], repr(v[1])


def roundup(size):
    k = MINSIZE
    while k < size:
        k += k
    return k

def hitrate(loads, hits):
    if loads < 1:
        return 'n/a'
    return "%5.1f%%" % (100.0 * hits / loads)

def duration(secs):
    mm, ss = divmod(secs, 60)
    hh, mm = divmod(mm, 60)
    if hh:
        return "%d:%02d:%02d" % (hh, mm, ss)
    if mm:
        return "%d:%02d" % (mm, ss)
    return "%d" % ss

nre = re.compile('([=-]?)(\d+)([.]\d*)?').match
def addcommas(n):
    sign, s, d = nre(str(n)).group(1, 2, 3)
    if d == '.0':
        d = ''

    result = s[-3:]
    s = s[:-3]
    while s:
        result = s[-3:]+','+result
        s = s[:-3]

    return (sign or '') + result + (d or '')

import random

def maybe(f, p=0.5):
    if random.random() < p:
        f()

if __name__ == "__main__":
    sys.exit(main())
