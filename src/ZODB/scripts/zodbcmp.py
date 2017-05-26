#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2016  Nexedi SA and Contributors.
#                     Kirill Smelkov <kirr@nexedi.com>
"""Zodbcmp - Tool to compare two ZODB databases

Zodbcmp compares two ZODB databases in between tidmin..tidmax transaction range
with default range being -∞..+∞ - (whole database).

For comparison both databases are scanned at storage layer and every
transaction content is compared bit-to-bit between the two. The program stops
either at first difference found, or when whole requested transaction range is
scanned with no difference detected.

Exit status is 0 if inputs are the same, 1 if different, 2 if error.
"""

from __future__ import print_function
from time import time

def ashex(s):
    return s.encode('hex')

# something that is greater than everything else
class Inf:
    def __cmp__(self, other):
        return +1
inf = Inf()


# get next item from iter -> (item, !stop)
def nextitem(it):
    try:
        item = it.next()
    except StopIteration:
        return None, False
    else:
        return item, True

# objects of a IStorageTransactionInformation
def txnobjv(txn):
    objv = []
    for obj in txn:
        assert obj.tid == txn.tid
        assert obj.version == ''
        objv.append(obj)

    objv.sort(key = lambda obj: obj.oid)    # in canonical order
    return objv


# compare two storage transactions
# 0 - equal, 1 - non-equal
def txncmp(txn1, txn2):
    # metadata
    for attr in ('tid', 'status', 'user', 'description', 'extension'):
        attr1 = getattr(txn1, attr)
        attr2 = getattr(txn2, attr)
        if attr1 != attr2:
            return 1

    # data
    objv1 = txnobjv(txn1)
    objv2 = txnobjv(txn2)
    if len(objv1) != len(objv2):
        return 1

    for obj1, obj2 in zip(objv1, objv2):
        for attr in ('oid', 'data', 'data_txn'):
            attr1 = getattr(obj1, attr)
            attr2 = getattr(obj2, attr)
            if attr1 != attr2:
                return 1

    return 0


# compare two storages
# 0 - equal, 1 - non-equal
def storcmp(stor1, stor2, tidmin, tidmax, verbose=False):
    iter1 = stor1.iterator(tidmin, tidmax)
    iter2 = stor2.iterator(tidmin, tidmax)

    Tprev = time()
    txncount = 0
    while 1:
        txn1, ok1 = nextitem(iter1)
        txn2, ok2 = nextitem(iter2)

        # comparison finished
        if not ok1 and not ok2:
            if verbose:
                print("equal")
            return 0

        # one part has entry not present in another part
        if txn1 is None or txn2 is None or txn1.tid != txn2.tid:
            if verbose:
                tid1 = txn1.tid if txn1 else inf
                tid2 = txn2.tid if txn2 else inf
                l = [(tid1, 1,2), (tid2, 2,1)]
                l.sort()
                mintid, minstor, maxstor = l[0]
                print("not-equal: tid %s present in stor%i but not in stor%i" % (
                        ashex(mintid), minstor, maxstor))
            return 1

        # show current comparison state and speed
        if verbose:
            txncount += 1
            T = time()
            if T - Tprev > 5:
                print("@ %s  (%.2f TPS)" % (ashex(txn1.tid), txncount / (T - Tprev)))
                Tprev = T
                txncount = 0

        # actual txn comparison
        tcmp = txncmp(txn1, txn2)
        if tcmp:
            if verbose:
                print("not-equal: transaction %s is different")
            return 1


# ----------------------------------------
import ZODB.config
import sys, getopt
import traceback

def usage(out):
    print("""
Usage: zodbcmp [OPTIONS] <storage1> <storage2> [tidmin..tidmax]
Compare two ZODB databases.

<storageX> is a file with ZConfig-based storage definition, e.g.

    %import neo.client
    <NEOStorage>
        master_nodes    ...
        name            ...
    </NEOStorage>

Options:

    -v  --verbose   increase verbosity
    -h  --help      show this help
""", file=out)

# tidmin..tidmax -> (tidmin, tidmax)
class TidRangeInvalid(Exception):
    pass

def parse_tidrange(tidrange):
    try:
        tidmin, tidmax = tidrange.split("..")
    except ValueError:  # not exactly 2 parts in between ".."
        raise TidRangeInvalid(tidrange)

    try:
        tidmin = tidmin.decode("hex")
        tidmax = tidmax.decode("hex")
    except TypeError:   # hex decoding error
        raise TidRangeInvalid(tidrange)

    # empty tid means -inf / +inf respectively
    # ( which is None in IStorage.iterator() )
    return (tidmin or None, tidmax or None)

def main2():
    verbose = False

    try:
        optv, argv = getopt.getopt(sys.argv[1:], "hv", ["help", "verbose"])
    except getopt.GetoptError as e:
        print(e, file=sys.stderr)
        usage(sys.stderr)
        sys.exit(2)

    for opt, _ in optv:
        if opt in ("-h", "--help"):
            usage(sys.stdout)
            sys.exit(0)
        if opt in ("-v", "--verbose"):
            verbose = True

    try:
        storconf1, storconf2 = argv[0:2]
    except ValueError:
        usage(sys.stderr)
        sys.exit(2)

    # parse tidmin..tidmax
    tidmin = tidmax = None
    if len(argv) > 2:
        try:
            tidmin, tidmax = parse_tidrange(argv[2])
        except TidRangeInvalid as e:
            print("E: invalid tidrange: %s" % e, file=sys.stderr)
            sys.exit(2)

    stor1 = ZODB.config.storageFromFile(open(storconf1, 'r'))
    stor2 = ZODB.config.storageFromFile(open(storconf2, 'r'))

    zcmp = storcmp(stor1, stor2, tidmin, tidmax, verbose)
    sys.exit(1 if zcmp else 0)

def main():
    try:
        main2()
    except SystemExit:
        raise   # this was sys.exit() call, not an error
    except:
        traceback.print_exc()
        sys.exit(2)

if __name__ == '__main__':
    main()
