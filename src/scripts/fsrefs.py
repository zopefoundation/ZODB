#!python

##############################################################################
#
# Copyright (c) 2002 Zope Corporation and Contributors.
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

"""Check FileStorage for dangling references.

usage: fsrefs.py data.fs

This script ignores versions, which might produce incorrect results
for storages that use versions.
"""

from ZODB.FileStorage import FileStorage
from ZODB.TimeStamp import TimeStamp
from ZODB.utils import u64
from ZODB.fsdump import get_pickle_metadata

import cPickle
import cStringIO
import traceback
import types

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
        print "\toid %s %s: %s" % (hex(u64(oid)), reason, description)
    print

def main(path):
    fs = FileStorage(path, read_only=1)
    noload = {}
    for oid in fs._index.keys():
        try:
            data, serial = fs.load(oid, "")
        except:
            print "oid %s failed to load" % hex(u64(oid))
            traceback.print_exc()
            noload[oid] = 1

            # XXX If we get here after we've already loaded objects
            # that refer to this one, we won't get error reports from
            # them.  We could fix this by making two passes over the
            # storage, but that seems like overkill.
            
        refs = get_refs(data)
        missing = [] # contains 3-tuples of oid, klass-metadata, reason
        for info in refs:
            try:
                ref, klass = info
            except TypeError:
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
    main(sys.argv[1])
