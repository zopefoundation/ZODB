#! /usr/bin/env python

"""Report on the space used by objects in a storage.

usage: space.py data.fs

The current implementation only supports FileStorage.

Current limitations / simplifications: Ignores revisions and versions.
"""

import ZODB
from ZODB.FileStorage import FileStorage
from ZODB.utils import U64
from ZODB.fsdump import get_pickle_metadata

def main(path):
    fs = FileStorage(path, read_only=1)
    # break into the file implementation
    if hasattr(fs._index, 'iterkeys'):
        iter = fs._index.iterkeys()
    else:
        iter = fs._index.keys()
    for oid in iter:
        data, serialno = fs.load(oid, '')
        mod, klass = get_pickle_metadata(data)
        print "%8s %5d %s.%s" % (U64(oid), len(data), mod, klass)

if __name__ == "__main__":
    import sys
    try:
        path, = sys.argv[1:]
    except ValueError:
        print __doc__
        sys.exit(2)
    main(path)
