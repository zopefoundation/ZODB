#!python
##############################################################################
#
# Copyright (c) 2001, 2002, 2003 Zope Corporation and Contributors.
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

"""A script to gather statistics while doing a storage migration.

This is very similar to a standard storage's copyTransactionsFrom() method,
except that it's geared to run as a script, and it collects useful pieces of
information as it's working.  This script can be used to stress test a storage
since it blasts transactions at it as fast as possible.  You can get a good
sense of the performance of a storage by running this script.

Actually it just counts the size of pickles in the transaction via the
iterator protocol, so storage overheads aren't counted.

Usage: %(PROGRAM)s [options] [source-storage-args] [destination-storage-args]
Options:
    -S sourcetype
    --stype=sourcetype
        This is the name of a recognized type for the source database.  Use -T
        to print out the known types.  Defaults to "file".

    -D desttype
    --dtype=desttype
        This is the name of the recognized type for the destination database.
        Use -T to print out the known types.  Defaults to "file".

    -o filename
    --output=filename
        Print results in filename, otherwise stdout.

    -m txncount
    --max=txncount
        Stop after committing txncount transactions.

    -k txncount
    --skip=txncount
        Skip the first txncount transactions.

    -p/--profile
        Turn on specialized profiling.

    -t/--timestamps
        Print tids as timestamps.

    -T/--storage_types
        Print all the recognized storage types and exit.

    -v/--verbose
        Turns on verbose output.  Multiple -v options increase the verbosity.

    -h/--help
        Print this message and exit.

Positional arguments:

    source-storage-args:
        Semicolon separated list of arguments for the source storage, as
        key=val pairs.  E.g. "file_name=Data.fs;read_only=1"

    destination-storage-args:
        Comma separated list of arguments for the source storage, as key=val
        pairs.  E.g. "name=full;frequency=3600"
"""

import os
import re
import sys
import time
import errno
import getopt
import marshal
import profile
import traceback

import ZODB
from ZODB import utils
from ZODB import StorageTypes
from ZODB.TimeStamp import TimeStamp

PROGRAM = sys.argv[0]
ZERO = '\0'*8

try:
    True, False
except NameError:
    True = 1
    False = 0



def usage(code, msg=''):
    print >> sys.stderr, __doc__ % globals()
    if msg:
        print >> sys.stderr, msg
    sys.exit(code)


def error(code, msg):
    print >> sys.stderr, msg
    print "use --help for usage message"
    sys.exit(code)



def main():
    try:
        opts, args = getopt.getopt(
            sys.argv[1:],
            'hvo:pm:k:D:S:Tt',
            ['help', 'verbose',
             'output=', 'profile', 'storage_types',
             'max=', 'skip=', 'dtype=', 'stype=', 'timestamps'])
    except getopt.error, msg:
        error(2, msg)

    class Options:
        stype = 'FileStorage'
        dtype = 'FileStorage'
        verbose = 0
        outfile = None
        profilep = False
        maxtxn = -1
        skiptxn = -1
        timestamps = False

    options = Options()

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            usage(0)
        elif opt in ('-v', '--verbose'):
            options.verbose += 1
        elif opt in ('-T', '--storage_types'):
            print_types()
            sys.exit(0)
        elif opt in ('-S', '--stype'):
            options.stype = arg
        elif opt in ('-D', '--dtype'):
            options.dtype = arg
        elif opt in ('-o', '--output'):
            options.outfile = arg
        elif opt in ('-p', '--profile'):
            options.profilep = True
        elif opt in ('-m', '--max'):
            options.maxtxn = int(arg)
        elif opt in ('-k', '--skip'):
            options.skiptxn = int(arg)
        elif opt in ('-t', '--timestamps'):
            options.timestamps = True

    if len(args) > 2:
        error(2, "too many arguments")

    srckws = {}
    if len(args) > 0:
        srcargs = args[0]
        for kv in re.split(r';\s*', srcargs):
            key, val = kv.split('=')
            srckws[key] = val

    destkws = {}
    if len(args) > 1:
        destargs = args[1]
        for kv in re.split(r';\s*', destargs):
            key, val = kv.split('=')
            destkws[key] = val

    if options.stype not in StorageTypes.storage_types.keys():
        usage(2, 'Source database type must be provided')
    if options.dtype not in StorageTypes.storage_types.keys():
        usage(2, 'Destination database type must be provided')

    # Open the output file
    if options.outfile is None:
        options.outfp = sys.stdout
        options.outclosep = False
    else:
        options.outfp = open(options.outfile, 'w')
        options.outclosep = True

    if options.verbose > 0:
        print 'Opening source database...'
    modname, sconv = StorageTypes.storage_types[options.stype]
    kw = sconv(**srckws)
    __import__(modname)
    sclass = getattr(sys.modules[modname], options.stype)
    srcdb = sclass(**kw)

    if options.verbose > 0:
        print 'Opening destination database...'
    modname, dconv = StorageTypes.storage_types[options.dtype]
    kw = dconv(**destkws)
    __import__(modname)
    dclass = getattr(sys.modules[modname], options.dtype)
    dstdb = dclass(**kw)

    try:
        t0 = time.time()
        doit(srcdb, dstdb, options)
        t1 = time.time()
        if options.verbose > 0:
            print 'Migration time:          %8.3f' % (t1-t0)
    finally:
        # Done
        srcdb.close()
        dstdb.close()
        if options.outclosep:
            options.outfp.close()



def doit(srcdb, dstdb, options):
    outfp = options.outfp
    profilep = options.profilep
    verbose = options.verbose
    # some global information
    largest_pickle = 0
    largest_txn_in_size = 0
    largest_txn_in_objects = 0
    total_pickle_size = 0L
    total_object_count = 0
    # Ripped from BaseStorage.copyTransactionsFrom()
    ts = None
    ok = True
    prevrevids = {}
    counter = 0
    skipper = 0
    if options.timestamps:
        print "%4s. %26s %6s %8s %5s %5s %5s %5s %5s" % (
            "NUM", "TID AS TIMESTAMP", "OBJS", "BYTES",
            # Does anybody know what these times mean?
            "t4-t0", "t1-t0", "t2-t1", "t3-t2", "t4-t3")
    else:
        print "%4s. %20s %6s %8s %6s %6s %6s %6s %6s" % (
            "NUM", "TRANSACTION ID", "OBJS", "BYTES",
            # Does anybody know what these times mean?
            "t4-t0", "t1-t0", "t2-t1", "t3-t2", "t4-t3")
    for txn in srcdb.iterator():
        skipper += 1
        if skipper <= options.skiptxn:
            continue
        counter += 1
        if counter > options.maxtxn >= 0:
            break
        tid = txn.tid
        if ts is None:
            ts = TimeStamp(tid)
        else:
            t = TimeStamp(tid)
            if t <= ts:
                if ok:
                    print >> sys.stderr, \
                          'Time stamps are out of order %s, %s' % (ts, t)
                    ok = False
                    ts = t.laterThan(ts)
                    tid = `ts`
                else:
                    ts = t
                    if not ok:
                        print >> sys.stderr, \
                              'Time stamps are back in order %s' % t
                        ok = True
        if verbose > 1:
            print ts

        prof = None
        if profilep and (counter % 100) == 0:
            prof = profile.Profile()
        objects = 0
        size = 0
        newrevids = RevidAccumulator()
        t0 = time.time()
        dstdb.tpc_begin(txn, tid, txn.status)
        t1 = time.time()
        for r in txn:
            oid = r.oid
            objects += 1
            thissize = len(r.data)
            size += thissize
            if thissize > largest_pickle:
                largest_pickle = thissize
            if verbose > 1:
                if not r.version:
                    vstr = 'norev'
                else:
                    vstr = r.version
                print utils.U64(oid), vstr, len(r.data)
            oldrevid = prevrevids.get(oid, ZERO)
            result = dstdb.store(oid, oldrevid, r.data, r.version, txn)
            newrevids.store(oid, result)
        t2 = time.time()
        result = dstdb.tpc_vote(txn)
        t3 = time.time()
        newrevids.tpc_vote(result)
        prevrevids.update(newrevids.get_dict())
        # Profile every 100 transactions
        if prof:
            prof.runcall(dstdb.tpc_finish, txn)
        else:
            dstdb.tpc_finish(txn)
        t4 = time.time()

        # record the results
        if objects > largest_txn_in_objects:
            largest_txn_in_objects = objects
        if size > largest_txn_in_size:
            largest_txn_in_size = size
        if options.timestamps:
            tidstr = str(TimeStamp(tid))
            format = "%4d. %26s %6d %8d %5.3f %5.3f %5.3f %5.3f %5.3f"
        else:
            tidstr = utils.U64(tid)
            format = "%4d. %20s %6d %8d %6.4f %6.4f %6.4f %6.4f %6.4f"
        print >> outfp, format % (skipper, tidstr, objects, size,
                                  t4-t0, t1-t0, t2-t1, t3-t2, t4-t3)
        total_pickle_size += size
        total_object_count += objects

        if prof:
            prof.create_stats()
            fp = open('profile-%02d.txt' % (counter / 100), 'wb')
            marshal.dump(prof.stats, fp)
            fp.close()
    print >> outfp, "Largest pickle:          %8d" % largest_pickle
    print >> outfp, "Largest transaction:     %8d" % largest_txn_in_size
    print >> outfp, "Largest object count:    %8d" % largest_txn_in_objects
    print >> outfp, "Total pickle size: %14d" % total_pickle_size
    print >> outfp, "Total object count:      %8d" % total_object_count



# helper to deal with differences between old-style store() return and
# new-style store() return that supports ZEO
import types

class RevidAccumulator:

    def __init__(self):
        self.data = {}

    def _update_from_list(self, list):
        for oid, serial in list:
            if not isinstance(serial, types.StringType):
                raise serial
            self.data[oid] = serial

    def store(self, oid, result):
        if isinstance(result, types.StringType):
            self.data[oid] = result
        elif result is not None:
            self._update_from_list(result)

    def tpc_vote(self, result):
        if result is not None:
            self._update_from_list(result)

    def get_dict(self):
        return self.data



if __name__ == '__main__':
    main()
