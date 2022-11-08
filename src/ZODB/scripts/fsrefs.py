#!/usr/bin/env python
##############################################################################
#
# Copyright (c) 2002 Zope Foundation and Contributors.
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

"""Check FileStorage for dangling references.

usage: fsrefs.py [-v] data.fs

fsrefs.py checks object sanity by trying to load the current revision of
every object O in the database, and also verifies that every object
directly reachable from each such O exists in the database.

It's hard to explain exactly what it does because it relies on undocumented
features in Python's cPickle module:  many of the crucial steps of loading
an object are taken, but application objects aren't actually created.  This
saves a lot of time, and allows fsrefs to be run even if the code
implementing the object classes isn't available.

A read-only connection to the specified FileStorage is made, but it is not
recommended to run fsrefs against a live FileStorage.  Because a live
FileStorage is mutating while fsrefs runs, it's not possible for fsrefs to
get a wholly consistent view of the database across the entire time fsrefs
is running; spurious error messages may result.

fsrefs doesn't normally produce any output.  If an object fails to load, the
oid of the object is given in a message saying so, and if -v was specified
then the traceback corresponding to the load failure is also displayed
(this is the only effect of the -v flag).

Three other kinds of errors are also detected, when an object O loads OK,
and directly refers to a persistent object P but there's a problem with P:

 - If P doesn't exist in the database, a message saying so is displayed.
   The unsatisifiable reference to P is often called a "dangling
   reference"; P is called "missing" in the error output.

 - If the current state of the database is such that P's creation has
   been undone, then P can't be loaded either.  This is also a kind of
   dangling reference, but is identified as "object creation was undone".

 - If P can't be loaded (but does exist in the database), a message saying
   that O refers to an object that can't be loaded is displayed.

fsrefs also (indirectly) checks that the .index file is sane, because
fsrefs uses the index to get its idea of what constitutes "all the objects
in the database".

Note these limitations:  because fsrefs only looks at the current revision
of objects, it does not attempt to load objects in versions, or non-current
revisions of objects; therefore fsrefs cannot find problems in versions or
in non-current revisions.
"""
from __future__ import print_function

import traceback

from BTrees.QQBTree import QQBTree

from ZODB.FileStorage import FileStorage
from ZODB.POSException import POSKeyError
from ZODB.serialize import get_refs
from ZODB.TimeStamp import TimeStamp
from ZODB.utils import get_pickle_metadata
from ZODB.utils import load_current
from ZODB.utils import oid_repr
from ZODB.utils import p64
from ZODB.utils import u64


# There's a problem with oid.  'data' is its pickle, and 'serial' its
# serial number.  'missing' is a list of (oid, class, reason) triples,
# explaining what the problem(s) is(are).


def report(oid, data, serial, missing):
    from_mod, from_class = get_pickle_metadata(data)
    if len(missing) > 1:
        plural = "s"
    else:
        plural = ""
    ts = TimeStamp(serial)
    print("oid %s %s.%s" % (hex(u64(oid)), from_mod, from_class))
    print("last updated: %s, tid=%s" % (ts, hex(u64(serial))))
    print("refers to invalid object%s:" % plural)
    for oid, info, reason in missing:
        if isinstance(info, tuple):
            description = "%s.%s" % info
        else:
            description = str(info)
        print("\toid %s %s: %r" % (oid_repr(oid), reason, description))
    print()


def main(path=None):
    verbose = 0
    if path is None:
        import getopt
        import sys

        opts, args = getopt.getopt(sys.argv[1:], "v")
        for k, v in opts:
            if k == "-v":
                verbose += 1

        path, = args

    fs = FileStorage(path, read_only=1)

    # Set of oids in the index that failed to load due to POSKeyError.
    # This is what happens if undo is applied to the transaction creating
    # the object (the oid is still in the index, but its current data
    # record has a backpointer of 0, and POSKeyError is raised then
    # because of that backpointer).
    undone = {}

    # Set of oids that were present in the index but failed to load.
    # This does not include oids in undone.
    noload = {}

    # build {pos -> oid} index that is reverse to {oid -> pos} fs._index
    # we'll need this to iterate objects in order of ascending file position to
    # optimize disk IO.
    pos2oid = QQBTree()  # pos -> u64(oid)
    for oid, pos in fs._index.iteritems():
        pos2oid[pos] = u64(oid)

    # pass 1: load all objects listed in the index and remember those objects
    # that are deleted or load with an error. Iterate objects in order of
    # ascending file position to optimize disk IO.
    for oid64 in pos2oid.itervalues():
        oid = p64(oid64)
        try:
            data, serial = load_current(fs, oid)
        except (KeyboardInterrupt, SystemExit):
            raise
        except POSKeyError:
            undone[oid] = 1
        except:  # noqa: E722 do not use bare 'except'
            if verbose:
                traceback.print_exc()
            noload[oid] = 1

    # pass 2: go through all objects again and verify that their references do
    # not point to problematic object set. Iterate objects in order of
    # ascending file position to optimize disk IO.
    inactive = noload.copy()
    inactive.update(undone)
    for oid64 in pos2oid.itervalues():
        oid = p64(oid64)
        if oid in inactive:
            continue
        data, serial = load_current(fs, oid)
        refs = get_refs(data)
        missing = []  # contains 3-tuples of oid, klass-metadata, reason
        for ref, klass in refs:
            if klass is None:
                klass = '<unknown>'
            if ref not in fs._index:
                missing.append((ref, klass, "missing"))
            if ref in noload:
                missing.append((ref, klass, "failed to load"))
            if ref in undone:
                missing.append((ref, klass, "object creation was undone"))
        if missing:
            report(oid, data, serial, missing)


if __name__ == "__main__":
    main()
