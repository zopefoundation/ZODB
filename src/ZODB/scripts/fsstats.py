#!/usr/bin/env python2
"""Print details statistics from fsdump output."""
from __future__ import print_function

import re
import sys

import six
from six.moves import filter


rx_txn = re.compile(r"tid=([0-9a-f]+).*size=(\d+)")
rx_data = re.compile(r"oid=([0-9a-f]+) size=(\d+) class=(\S+)")


def sort_byhsize(seq, reverse=False):
    L = [(v.size(), k, v) for k, v in seq]
    L.sort()
    if reverse:
        L.reverse()
    return [(k, v) for n, k, v in L]


class Histogram(dict):

    def add(self, size):
        self[size] = self.get(size, 0) + 1

    def size(self):
        return sum(six.itervalues(self))

    def mean(self):
        product = sum([k * v for k, v in six.iteritems(self)])
        return product / self.size()

    def median(self):
        # close enough?
        n = self.size() / 2
        L = sorted(self.keys())
        L.reverse()
        while 1:
            k = L.pop()
            if self[k] > n:
                return k
            n -= self[k]

    def mode(self):
        mode = 0
        value = 0
        for k, v in six.iteritems(self):
            if v > value:
                value = v
                mode = k
        return mode

    def make_bins(self, binsize):
        try:
            maxkey = max(six.iterkeys(self))
        except ValueError:
            maxkey = 0
        self.binsize = binsize
        self.bins = [0] * (1 + maxkey // binsize)
        for k, v in six.iteritems(self):
            b = k // binsize
            self.bins[b] += v

    def report(self, name, binsize=50, usebins=False, gaps=True, skip=True):
        if usebins:
            # Use existing bins with whatever size they have
            binsize = self.binsize
        else:
            # Make new bins
            self.make_bins(binsize)
        maxval = max(self.bins)
        # Print up to 40 dots for a value
        dot = max(maxval / 40, 1)
        tot = sum(self.bins)
        print(name)
        print("Total", tot, end=' ')
        print("Median", self.median(), end=' ')
        print("Mean", self.mean(), end=' ')
        print("Mode", self.mode(), end=' ')
        print("Max", max(self))
        print("One * represents", dot)
        gap = False
        cum = 0
        for i, n in enumerate(self.bins):
            if gaps and (not n or (skip and not n / dot)):
                if not gap:
                    print("   ...")
                gap = True
                continue
            gap = False
            p = 100 * n / tot
            cum += n
            pc = 100 * cum / tot
            print("%6d %6d %3d%% %3d%% %s" % (
                i * binsize, n, p, pc, "*" * (n // dot)))
        print()


def class_detail(class_size):
    # summary of classes
    fmt = "%5s %6s %6s %6s   %-50.50s"
    labels = ["num", "median", "mean", "mode", "class"]
    print(fmt % tuple(labels))
    print(fmt % tuple(["-" * len(s) for s in labels]))
    for klass, h in sort_byhsize(six.iteritems(class_size)):
        print(fmt % (h.size(), h.median(), h.mean(), h.mode(), klass))
    print()

    # per class details
    for klass, h in sort_byhsize(six.iteritems(class_size), reverse=True):
        h.make_bins(50)
        if len(tuple(filter(None, h.bins))) == 1:
            continue
        h.report("Object size for %s" % klass, usebins=True)


def revision_detail(lifetimes, classes):
    # Report per-class details for any object modified more than once
    for name, oids in six.iteritems(classes):
        h = Histogram()
        keep = False
        for oid in dict.fromkeys(oids, 1):
            L = lifetimes.get(oid)
            n = len(L)
            h.add(n)
            if n > 1:
                keep = True
        if keep:
            h.report("Number of revisions for %s" % name, binsize=10)


def main(path=None):
    if path is None:
        path = sys.argv[1]
    txn_objects = Histogram()  # histogram of txn size in objects
    txn_bytes = Histogram()  # histogram of txn size in bytes
    obj_size = Histogram()  # histogram of object size
    n_updates = Histogram()  # oid -> num updates
    n_classes = Histogram()  # class -> num objects
    lifetimes = {}  # oid -> list of tids
    class_size = {}  # class -> histogram of object size
    classes = {}  # class -> list of oids

    MAX = 0
    objects = 0
    tid = None

    f = open(path, "r")
    for i, line in enumerate(f):
        if MAX and i > MAX:
            break
        if line.startswith("  data"):
            m = rx_data.search(line)
            if not m:
                continue
            oid, size, klass = m.groups()
            size = int(size)

            obj_size.add(size)
            n_updates.add(oid)
            n_classes.add(klass)

            h = class_size.get(klass)
            if h is None:
                h = class_size[klass] = Histogram()
            h.add(size)

            L = lifetimes.setdefault(oid, [])
            L.append(tid)

            L = classes.setdefault(klass, [])
            L.append(oid)
            objects += 1

        elif line.startswith("Trans"):

            if tid is not None:
                txn_objects.add(objects)

            m = rx_txn.search(line)
            if not m:
                continue
            tid, size = m.groups()
            size = int(size)
            objects = 0

            txn_bytes.add(size)
    if objects:
        txn_objects.add(objects)
    f.close()

    print("Summary: %d txns, %d objects, %d revisions" % (
        txn_objects.size(), len(n_updates), n_updates.size()))
    print()

    txn_bytes.report("Transaction size (bytes)", binsize=1024)
    txn_objects.report("Transaction size (objects)", binsize=10)
    obj_size.report("Object size", binsize=128)

    # object lifetime info
    h = Histogram()
    for k, v in lifetimes.items():
        h.add(len(v))
    h.report("Number of revisions", binsize=10, skip=False)

    # details about revisions
    revision_detail(lifetimes, classes)

    class_detail(class_size)


if __name__ == "__main__":
    main()
