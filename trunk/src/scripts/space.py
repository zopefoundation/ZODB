#! /usr/bin/env python

"""Report on the space used by objects in a storage.

usage: space.py data.fs

The current implementation only supports FileStorage.

Current limitations / simplifications: Ignores revisions and versions.
"""

from ZODB.FileStorage import FileStorage
from ZODB.utils import U64
from ZODB.fsdump import get_pickle_metadata

def run(path, v=0):
    fs = FileStorage(path, read_only=1)
    # break into the file implementation
    if hasattr(fs._index, 'iterkeys'):
        iter = fs._index.iterkeys()
    else:
        iter = fs._index.keys()
    totals = {}
    for oid in iter:
        data, serialno = fs.load(oid, '')
        mod, klass = get_pickle_metadata(data)
        key = "%s.%s" % (mod, klass)
        bytes, count = totals.get(key, (0, 0))
        bytes += len(data)
        count += 1
        totals[key] = bytes, count
        if v:
            print "%8s %5d %s" % (U64(oid), len(data), key)
    L = totals.items()
    L.sort(lambda a, b: cmp(a[1], b[1]))
    L.reverse()
    print "Totals per object class:"
    for key, (bytes, count) in L:
        print "%8d %8d %s" % (count, bytes, key)

def main():
    import sys
    import getopt
    try:
        opts, args = getopt.getopt(sys.argv[1:], "v")
    except getopt.error, msg:
        print msg
        print "usage: space.py [-v] Data.fs"
        sys.exit(2)
    if len(args) != 1:
        print "usage: space.py [-v] Data.fs"
        sys.exit(2)
    v = 0
    for o, a in opts:
        if o == "-v":
            v += 1
    path = args[0]
    run(path, v)

if __name__ == "__main__":
    main()
