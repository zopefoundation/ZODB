#!/usr/bin/env python2.3

##############################################################################
#
# Copyright (c) 2002 Zope Corporation and Contributors.
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

Two other kinds of errors are also detected, one strongly related to
"failed to load", when an object O loads OK, and directly refers to a
persistent object P but there's a problem with P:

 - If P doesn't exist in the database, a message saying so is displayed.
   The unsatisifiable reference to P is often called a "dangling
   reference"; P is called "missing" in the error output.

 - If it was earlier determined that P could not be loaded (but does exist
   in the database), a message saying that O refers to an object that can't
   be loaded is displayed.  Note that fsrefs only makes one pass over the
   database, so if an object O refers to an unloadable object P, and O is
   seen by fsrefs before P, an "O refers to the unloadable P" message will
   not be produced; a message saying that P can't be loaded will be
   produced when fsrefs later tries to load P, though.

fsrefs also (indirectly) checks that the .index file is sane, because
fsrefs uses the index to get its idea of what constitutes "all the objects
in the database".

Note these limitations:  because fsrefs only looks at the current revision
of objects, it does not attempt to load objects in versions, or non-current
revisions of objects; therefore fsrefs cannot find problems in versions or
in non-current revisions.
"""

from ZODB.FileStorage import FileStorage
from ZODB.TimeStamp import TimeStamp
from ZODB.utils import u64
from ZODB.FileStorage.fsdump import get_pickle_metadata

import cPickle
import cStringIO
import traceback
import types

VERBOSE = 0

def get_refs(pickle):
    refs = []
    f = cStringIO.StringIO(pickle)
    u = cPickle.Unpickler(f)
    u.persistent_load = refs
    u.noload()
    u.noload()
    return refs

def report(oid, data, serial, fs, missing):
    from_mod, from_class = get_pickle_metadata(data)
    if len(missing) > 1:
        plural = "s"
    else:
        plural = ""
    ts = TimeStamp(serial)
    print "oid %s %s.%s" % (hex(u64(oid)), from_mod, from_class)
    print "last updated: %s, tid=%s" % (ts, hex(u64(serial)))
    print "refers to invalid object%s:" % plural
    for oid, info, reason in missing:
        if isinstance(info, types.TupleType):
            description = "%s.%s" % info
        else:
            description = str(info)
        print "\toid %s %s: %r" % (hex(u64(oid)), reason, description)
    print

def main(path):
    fs = FileStorage(path, read_only=1)
    noload = {}
    for oid in fs._index.keys():
        try:
            data, serial = fs.load(oid, "")
        except:
            print "oid %s failed to load" % hex(u64(oid))
            if VERBOSE:
                traceback.print_exc()
            noload[oid] = 1

            # If we get here after we've already loaded objects
            # that refer to this one, we will not have gotten error reports
            # from the latter about the current object being unloadable.
            # We could fix this by making two passes over the storage, but
            # that seems like overkill.
            continue

        refs = get_refs(data)
        missing = [] # contains 3-tuples of oid, klass-metadata, reason
        for info in refs:
            try:
                ref, klass = info
            except (ValueError, TypeError):
                # failed to unpack
                ref = info
                klass = '<unknown>'
            if not fs._index.has_key(ref):
                missing.append((ref, klass, "missing"))
            if noload.has_key(ref):
                missing.append((ref, klass, "failed to load"))
        if missing:
            report(oid, data, serial, fs, missing)

if __name__ == "__main__":
    import sys
    import getopt

    opts, args = getopt.getopt(sys.argv[1:], "v")
    for k, v in opts:
        if k == "-v":
            VERBOSE += 1

    path, = args
    main(path)
