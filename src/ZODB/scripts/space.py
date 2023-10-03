#!/usr/bin/env python
"""Report on the space used by objects in a storage.

usage: space.py data.fs

The current implementation only supports FileStorage.

Current limitations / simplifications: Ignores revisions and versions.
"""

from operator import itemgetter

from ZODB.FileStorage import FileStorage
from ZODB.utils import U64
from ZODB.utils import get_pickle_metadata
from ZODB.utils import load_current


def run(path, v=0):
    fs = FileStorage(path, read_only=1)
    # break into the file implementation
    if hasattr(fs._index, 'iterkeys'):
        iter = fs._index.keys()
    else:
        iter = fs._index.keys()
    totals = {}
    for oid in iter:
        data, serialno = load_current(fs, oid)
        mod, klass = get_pickle_metadata(data)
        key = "{}.{}".format(mod, klass)
        bytes, count = totals.get(key, (0, 0))
        bytes += len(data)
        count += 1
        totals[key] = bytes, count
        if v:
            print("%8s %5d %s" % (U64(oid), len(data), key))
    L = sorted(totals.items(), key=itemgetter(1), reverse=True)
    print("Totals per object class:")
    for key, (bytes, count) in L:
        print("%8d %8d %s" % (count, bytes, key))


def main():
    import getopt
    import sys
    try:
        opts, args = getopt.getopt(sys.argv[1:], "v")
    except getopt.error as msg:
        print(msg)
        print("usage: space.py [-v] Data.fs")
        sys.exit(2)
    if len(args) != 1:
        print("usage: space.py [-v] Data.fs")
        sys.exit(2)
    v = 0
    for o, a in opts:
        if o == "-v":
            v += 1
    path = args[0]
    run(path, v)


if __name__ == "__main__":
    main()
