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

Usage: simul.py [-bflyz] [-s size] tracefile

Use one of -b, -f, -l, -y or -z select the cache simulator:
-b: buddy system allocator
-f: simple free list allocator
-l: idealized LRU (no allocator)
-y: variation on the existing ZEO cache that copies to current file
-z: existing ZEO cache (default)

Options:
-s size: cache size in MB (default 20 MB)

Note: the buddy system allocator rounds the cache size up to a power of 2
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
    MB = 1000*1000
    cachelimit = 20*MB
    simclass = ZEOCacheSimulation
    try:
        opts, args = getopt.getopt(sys.argv[1:], "bflyzs:")
    except getopt.error, msg:
        usage(msg)
        return 2
    for o, a in opts:
        if o == '-b':
            simclass = BuddyCacheSimulation
        if o == '-f':
            simclass = SimpleCacheSimulation
        if o == '-l':
            simclass = LRUCacheSimulation
        if o == '-y':
            simclass = AltZEOCacheSimulation
        if o == '-z':
            simclass = ZEOCacheSimulation
        if o == '-s':
            cachelimit = int(float(a)*MB)
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
    elif filename == "-":
        # Read from stdin
        f = sys.stdin
    else:
        # Open regular file
        try:
            f = open(filename, "rb")
        except IOError, msg:
            print >>sys.stderr,  "can't open %s: %s" % (filename, msg)
            return 1

    # Create simulation object
    sim = simclass(cachelimit)

    # Print output header
    sim.printheader()

    # Read trace file, simulating cache behavior
    offset = 0
    records = 0
    f_read = f.read
    struct_unpack = struct.unpack
    while 1:
        # Read a record and decode it
        r = f_read(8)
        if len(r) < 8:
            break
        offset += 8
        ts, code = struct_unpack(">ii", r)
        if ts == 0:
            # Must be a misaligned record caused by a crash
            ##print "Skipping 8 bytes at offset", offset-8
            continue
        r = f_read(16)
        if len(r) < 16:
            break
        offset += 16
        records += 1
        oid, serial = struct_unpack(">8s8s", r)
        # Decode the code
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

    def __init__(self, cachelimit):
        self.cachelimit = cachelimit
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
            self.inval(oid)
        elif code == 0x00:
            # Restart
            self.report()
            self.restart()

    def write(self, oid, size):
        pass

    def load(self, oid, size):
        pass

    def inval(self, oid):
        pass

    format = "%12s %9s %8s %8s %6s %6s %6s %6s"

    # Subclass should override extraname to name known instance variables;
    # if extraname is 'foo', both self.foo and self.total_foo must exist:
    extraname = "*** please override ***"

    def printheader(self):
        print "%s, cache size %s bytes" % (self.__class__.__name__,
                                           addcommas(self.cachelimit))
        print self.format % (
            "START TIME", "DURATION", "LOADS", "HITS",
            "INVALS", "WRITES", self.extraname.upper(), "HITRATE")

    nreports = 0

    def report(self):
        if self.loads:
            self.nreports += 1
            print self.format % (
                time.ctime(self.ts0)[4:-8],
                duration(self.ts1 - self.ts0),
                self.loads, self.hits, self.invals, self.writes,
                getattr(self, self.extraname),
                hitrate(self.loads, self.hits))

    def finish(self):
        self.report()
        if self.nreports > 1:
            print (self.format + " OVERALL") % (
                time.ctime(self.epoch)[4:-8],
                duration(self.ts1 - self.epoch),
                self.total_loads,
                self.total_hits,
                self.total_invals,
                self.total_writes,
                getattr(self, "total_" + self.extraname),
                hitrate(self.total_loads, self.total_hits))

class ZEOCacheSimulation(Simulation):

    """Simulate the current (ZEO 1.0 and 2.0) ZEO cache behavior.

    This assumes the cache is not persistent (we don't know how to
    simulate cache validation.)

    """

    extraname = "flips"

    def __init__(self, cachelimit):
        # Initialize base class
        Simulation.__init__(self, cachelimit)
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
        if self.filesize[self.current] + size > self.cachelimit / 2:
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
            self.invals += 1
            self.total_invals += 1
            del self.fileoids[self.current][oid]
        elif self.fileoids[1 - self.current].get(oid):
            self.invals += 1
            self.total_invals += 1
            del self.fileoids[1 - self.current][oid]

class AltZEOCacheSimulation(ZEOCacheSimulation):

    """A variation of the ZEO cache that copies to the current file.

    When a hit is found in the non-current cache file, it is copied to
    the current cache file.  Exception: when the copy would cause a
    cache flip, we don't copy (this is part laziness, part concern
    over causing extraneous flips).
    """

    def load(self, oid, size):
        if self.fileoids[self.current].get(oid):
            self.hits += 1
            self.total_hits += 1
        elif self.fileoids[1 - self.current].get(oid):
            self.hits += 1
            self.total_hits += 1
            # Simulate a write, unless it would cause a flip
            size = size + 31 - 127
            if self.filesize[self.current] + size <= self.cachelimit / 2:
                self.filesize[self.current] += size
                self.fileoids[self.current][oid] = 1
                del self.fileoids[1 - self.current][oid]
        else:
            self.write(oid, size)

class LRUCacheSimulation(Simulation):

    extraname = "evicts"

    def __init__(self, cachelimit):
        # Initialize base class
        Simulation.__init__(self, cachelimit)
        # Initialize additional global statistics
        self.total_evicts = 0

    def restart(self):
        # Reset base class
        Simulation.restart(self)
        # Reset additional per-run statistics
        self.evicts = 0
        # Set up simulation
        self.cache = {}
        self.size = 0
        self.head = Node(None, None)
        self.head.linkbefore(self.head)

    def load(self, oid, size):
        node = self.cache.get(oid)
        if node is not None:
            self.hits += 1
            self.total_hits += 1
            node.linkbefore(self.head)
        else:
            self.write(oid, size)

    def write(self, oid, size):
        node = self.cache.get(oid)
        if node is not None:
            node.unlink()
            assert self.head.next is not None
            self.size -= node.size
        node = Node(oid, size)
        self.cache[oid] = node
        node.linkbefore(self.head)
        self.size += size
        # Evict LRU nodes
        while self.size > self.cachelimit:
            self.evicts += 1
            self.total_evicts += 1
            node = self.head.next
            assert node is not self.head
            node.unlink()
            assert self.head.next is not None
            del self.cache[node.oid]
            self.size -= node.size

    def inval(self, oid):
        node = self.cache.get(oid)
        if node is not None:
            assert node.oid == oid
            self.invals += 1
            self.total_invals += 1
            node.unlink()
            assert self.head.next is not None
            del self.cache[oid]
            self.size -= node.size
            assert self.size >= 0

class Node:

    """Node in a doubly-linked list, storing oid and size as payload.

    A node can be linked or unlinked; in the latter case, next and
    prev are None.  Initially a node is unlinked.

    """
    # Make it a new-style class in Python 2.2 and up; no effect in 2.1
    __metaclass__ = type
    __slots__ = ['prev', 'next', 'oid', 'size']

    def __init__(self, oid, size):
        self.oid = oid
        self.size = size
        self.prev = self.next = None

    def unlink(self):
        prev = self.prev
        next = self.next
        if prev is not None:
            assert next is not None
            assert prev.next is self
            assert next.prev is self
            prev.next = next
            next.prev = prev
            self.prev = self.next = None
        else:
            assert next is None

    def linkbefore(self, next):
        self.unlink()
        prev = next.prev
        if prev is None:
            assert next.next is None
            prev = next
        self.prev = prev
        self.next = next
        prev.next = next.prev = self

class BuddyCacheSimulation(LRUCacheSimulation):

    def __init__(self, cachelimit):
        LRUCacheSimulation.__init__(self, roundup(cachelimit))

    def restart(self):
        LRUCacheSimulation.restart(self)
        self.allocator = self.allocatorFactory(self.cachelimit)

    def allocatorFactory(self, size):
        return BuddyAllocator(size)

    # LRUCacheSimulation.load() is just fine

    def write(self, oid, size):
        node = self.cache.get(oid)
        if node is not None:
            node.unlink()
            assert self.head.next is not None
            self.size -= node.size
            self.allocator.free(node)
        while 1:
            node = self.allocator.alloc(size)
            if node is not None:
                break
            # Failure to allocate.  Evict something and try again.
            node = self.head.next
            assert node is not self.head
            self.evicts += 1
            self.total_evicts += 1
            node.unlink()
            assert self.head.next is not None
            del self.cache[node.oid]
            self.size -= node.size
            self.allocator.free(node)
        node.oid = oid
        self.cache[oid] = node
        node.linkbefore(self.head)
        self.size += node.size

    def inval(self, oid):
        node = self.cache.get(oid)
        if node is not None:
            assert node.oid == oid
            self.invals += 1
            self.total_invals += 1
            node.unlink()
            assert self.head.next is not None
            del self.cache[oid]
            self.size -= node.size
            assert self.size >= 0
            self.allocator.free(node)

class SimpleCacheSimulation(BuddyCacheSimulation):

    def allocatorFactory(self, size):
        return SimpleAllocator(size)

    def finish(self):
        BuddyCacheSimulation.finish(self)
        self.allocator.report()

MINSIZE = 256

class BuddyAllocator:

    def __init__(self, cachelimit):
        cachelimit = roundup(cachelimit)
        self.cachelimit = cachelimit
        self.avail = {} # Map rounded-up sizes to free list node heads
        self.nodes = {} # Map address to node
        k = MINSIZE
        while k <= cachelimit:
            self.avail[k] = n = Node(None, None) # Not BlockNode; has no addr
            n.linkbefore(n)
            k += k
        node = BlockNode(None, cachelimit, 0)
        self.nodes[0] = node
        node.linkbefore(self.avail[cachelimit])

    def alloc(self, size):
        size = roundup(size)
        k = size
        while k <= self.cachelimit:
            head = self.avail[k]
            node = head.next
            if node is not head:
                break
            k += k
        else:
            return None # Store is full, or block is too large
        node.unlink()
        size2 = node.size
        while size2 > size:
            size2 = size2 / 2
            assert size2 >= size
            node.size = size2
            buddy = BlockNode(None, size2, node.addr + size2)
            self.nodes[buddy.addr] = buddy
            buddy.linkbefore(self.avail[size2])
        node.oid = 1 # Flag as in-use
        return node

    def free(self, node):
        assert node is self.nodes[node.addr]
        assert node.prev is node.next is None
        node.oid = None # Flag as free
        while node.size < self.cachelimit:
            buddy_addr = node.addr ^ node.size
            buddy = self.nodes[buddy_addr]
            assert buddy.addr == buddy_addr
            if buddy.oid is not None or buddy.size != node.size:
                break
            # Merge node with buddy
            buddy.unlink()
            if buddy.addr < node.addr: # buddy prevails
                del self.nodes[node.addr]
                node = buddy
            else: # node prevails
                del self.nodes[buddy.addr]
            node.size *= 2
        assert node is self.nodes[node.addr]
        node.linkbefore(self.avail[node.size])

    def dump(self, msg=""):
        if msg:
            print msg,
        size = MINSIZE
        blocks = bytes = 0
        while size <= self.cachelimit:
            head = self.avail[size]
            node = head.next
            count = 0
            while node is not head:
                count += 1
                node = node.next
            if count:
                print "%d:%d" % (size, count),
            blocks += count
            bytes += count*size
            size += size
        print "-- %d, %d" % (bytes, blocks)

def roundup(size):
    k = MINSIZE
    while k < size:
        k += k
    return k

class SimpleAllocator:

    def __init__(self, arenasize):
        self.arenasize = arenasize
        self.avail = BlockNode(None, 0, 0) # Weird: empty block as list head
        self.rover = self.avail
        node = BlockNode(None, arenasize, 0)
        node.linkbefore(self.avail)
        self.taglo = {0: node}
        self.taghi = {arenasize: node}
        # Allocator statistics
        self.nallocs = 0
        self.nfrees = 0
        self.allocloops = 0
        self.freebytes = arenasize
        self.freeblocks = 1
        self.allocbytes = 0
        self.allocblocks = 0

    def report(self):
        print ("NA=%d AL=%d NF=%d ABy=%d ABl=%d FBy=%d FBl=%d" %
               (self.nallocs, self.allocloops,
                self.nfrees,
                self.allocbytes, self.allocblocks,
                self.freebytes, self.freeblocks))

    def alloc(self, size):
        self.nallocs += 1
        # First fit algorithm
        rover = stop = self.rover
        while 1:
            self.allocloops += 1
            if rover.size >= size:
                break
            rover = rover.next
            if rover is stop:
                return None # We went round the list without finding space
        if rover.size == size:
            self.rover = rover.next
            rover.unlink()
            del self.taglo[rover.addr]
            del self.taghi[rover.addr + size]
            self.freeblocks -= 1
            self.allocblocks += 1
            self.freebytes -= size
            self.allocbytes += size
            return rover
        # Take space from the beginning of the roving pointer
        assert rover.size > size
        node = BlockNode(None, size, rover.addr)
        del self.taglo[rover.addr]
        rover.size -= size
        rover.addr += size
        self.taglo[rover.addr] = rover
        #self.freeblocks += 0 # No change here
        self.allocblocks += 1
        self.freebytes -= size
        self.allocbytes += size
        return node

    def free(self, node):
        self.nfrees += 1
        self.freeblocks += 1
        self.allocblocks -= 1
        self.freebytes += node.size
        self.allocbytes -= node.size
        node.linkbefore(self.avail)
        self.taglo[node.addr] = node
        self.taghi[node.addr + node.size] = node
        x = self.taghi.get(node.addr)
        if x is not None:
            # Merge x into node
            x.unlink()
            self.freeblocks -= 1
            del self.taglo[x.addr]
            del self.taghi[x.addr + x.size]
            del self.taglo[node.addr]
            node.addr = x.addr
            node.size += x.size
            self.taglo[node.addr] = node
        x = self.taglo.get(node.addr + node.size)
        if x is not None:
            # Merge x into node
            x.unlink()
            self.freeblocks -= 1
            del self.taglo[x.addr]
            del self.taghi[x.addr + x.size]
            del self.taghi[node.addr + node.size]
            node.size += x.size
            self.taghi[node.addr + node.size] = node
        # It's possible that either one of the merges above invalidated
        # the rover.
        # It's simplest to simply reset the rover to the newly freed block.
        self.rover = node

    def dump(self, msg=""):
        if msg:
            print msg,
        count = 0
        bytes = 0
        node = self.avail.next
        while node is not self.avail:
            bytes += node.size
            count += 1
            node = node.next
        print count, "free blocks,", bytes, "free bytes"
        self.report()

class BlockNode(Node):

    __slots__ = ['addr']

    def __init__(self, oid, size, addr):
        Node.__init__(self, oid, size)
        self.addr = addr

def testallocator(factory=BuddyAllocator):
    # Run one of Knuth's experiments as a test
    import random
    import heapq # This only runs with Python 2.3, folks :-)
    reportfreq = 100
    cachelimit = 2**17
    cache = factory(cachelimit)
    queue = []
    T = 0
    blocks = 0
    while T < 5000:
        while queue and queue[0][0] <= T:
            time, node = heapq.heappop(queue)
            assert time == T
            ##print "free addr=%d, size=%d" % (node.addr, node.size)
            cache.free(node)
            blocks -= 1
        size = random.randint(100, 2000)
        lifetime = random.randint(1, 100)
        node = cache.alloc(size)
        if node is None:
            print "out of mem"
            cache.dump("T=%4d: %d blocks;" % (T, blocks))
            break
        else:
            ##print "alloc addr=%d, size=%d" % (node.addr, node.size)
            blocks += 1
            heapq.heappush(queue, (T + lifetime, node))
        T = T+1
        if T % reportfreq == 0:
            cache.dump("T=%4d: %d blocks;" % (T, blocks))

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

def addcommas(n):
    sign, s = '', str(n)
    if s[0] == '-':
        sign, s = '-', s[1:]
    i = len(s) - 3
    while i > 0:
        s = s[:i] + ',' + s[i:]
        i -= 3
    return sign + s

if __name__ == "__main__":
    sys.exit(main())
