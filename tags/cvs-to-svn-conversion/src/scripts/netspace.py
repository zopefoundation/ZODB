#!/usr/bin/env python2.3

"""Report on the net size of objects counting subobjects.

usage: netspace.py [-P | -v] data.fs

-P: do a pack first
-v: print info for all objects, even if a traversal path isn't found
"""

import ZODB
from ZODB.FileStorage import FileStorage
from ZODB.utils import U64
from ZODB.fsdump import get_pickle_metadata
from ZODB.referencesf import referencesf

def find_paths(root, maxdist):
    """Find Python attribute traversal paths for objects to maxdist distance.

    Starting at a root object, traverse attributes up to distance levels
    from the root, looking for persistent objects.  Return a dict
    mapping oids to traversal paths.

    XXX Assumes that the keys of the root are not themselves
    persistent objects.

    XXX Doesn't traverse containers.
    """
    paths = {}

    # Handle the root as a special case because it's a dict
    objs = []
    for k, v in root.items():
        oid = getattr(v, '_p_oid', None)
        objs.append((k, v, oid, 0))

    for path, obj, oid, dist in objs:
        if oid is not None:
            paths[oid] = path
        if dist < maxdist:
            getattr(obj, 'foo', None) # unghostify
            try:
                items = obj.__dict__.items()
            except AttributeError:
                continue
            for k, v in items:
                oid = getattr(v, '_p_oid', None)
                objs.append(("%s.%s" % (path, k), v, oid, dist + 1))

    return paths

def main(path):
    fs = FileStorage(path, read_only=1)
    if PACK:
        fs.pack()

    db = ZODB.DB(fs)
    rt = db.open().root()
    paths = find_paths(rt, 3)

    def total_size(oid):
        cache = {}
        cache_size = 1000
        def _total_size(oid, seen):
            v = cache.get(oid)
            if v is not None:
                return v
            data, serialno = fs.load(oid, '')
            size = len(data)
            for suboid in referencesf(data):
                if seen.has_key(suboid):
                    continue
                seen[suboid] = 1
                size += _total_size(suboid, seen)
            cache[oid] = size
            if len(cache) == cache_size:
                cache.popitem()
            return size
        return _total_size(oid, {})

    keys = fs._index.keys()
    keys.sort()
    keys.reverse()

    if not VERBOSE:
        # If not running verbosely, don't print an entry for an object
        # unless it has an entry in paths.
        keys = filter(paths.has_key, keys)

    fmt = "%8s %5d %8d %s %s.%s"

    for oid in keys:
        data, serialno = fs.load(oid, '')
        mod, klass = get_pickle_metadata(data)
        refs = referencesf(data)
        path = paths.get(oid, '-')
        print fmt % (U64(oid), len(data), total_size(oid), path, mod, klass)

if __name__ == "__main__":
    import sys
    import getopt

    PACK = 0
    VERBOSE = 0
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'Pv')
        path, = args
    except getopt.error, err:
        print err
        print __doc__
        sys.exit(2)
    except ValueError:
        print "expected one argument, got", len(args)
        print __doc__
        sys.exit(2)
    for o, v in opts:
        if o == '-P':
            PACK = 1
        if o == '-v':
            VERBOSE += 1
    main(path)
