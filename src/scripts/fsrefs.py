#! /usr/bin/env python

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
    print "refers to unknown object%s:" % plural
    for oid, info in missing:
        if isinstance(info, types.TupleType):
            description = "%s.%s" % info
        else:
            description = str(info)
        print "\toid %s: %s" % (hex(u64(oid)), description)
    print

def main(path):
    fs = FileStorage(path, read_only=1)
    for oid in fs._index.keys():
        data, serial = fs.load(oid, "")
        refs = get_refs(data)
        missing = []
        for ref, klass in refs:
            if not fs._index.has_key(ref):
                missing.append((ref, klass))
        if missing:
            report(oid, data, serial, fs, missing)

if __name__ == "__main__":
    import sys
    main(sys.argv[1])
