#! /usr/bin/env python
"""Check the consistency of BTrees in a Data.fs

usage: checkbtrees.py data.fs

Try to find all the BTrees in a Data.fs and call their _check() methods.
"""

from types import IntType

import ZODB
from ZODB.FileStorage import FileStorage

def add_if_persistent(L, obj, path):
    getattr(obj, '_', None) # unghostify
    if hasattr(obj, '_p_oid'):
        L.append((obj, path))

def get_subobjects(obj):
    getattr(obj, '_', None) # unghostify
    sub = []
    try:
        attrs = obj.__dict__.items()
    except AttributeError:
        attrs = ()
    for pair in attrs:
        sub.append(pair)
        
    # what if it is a mapping?
    try:
        items = obj.items()
    except AttributeError:
        items = ()
    for k, v in items:
        if not isinstance(k, IntType):
            sub.append(("<key>", k))
        if not isinstance(v, IntType):
            sub.append(("[%s]" % repr(k), v))

    # what if it is a sequence?
    i = 0
    while 1:
        try:
            elt = obj[i]
        except:
            break
        sub.append(("[%d]" % i, elt))
        i += 1

    return sub

def main(fname):
    fs = FileStorage(fname, read_only=1)
    cn = ZODB.DB(fs).open()
    rt = cn.root()
    todo = []
    add_if_persistent(todo, rt, '')

    found = 0
    while todo:
        obj, path = todo.pop(0)
        found += 1
        if not path:
            print "<root>", repr(obj)
        else:
            print path, repr(obj)

        mod = str(obj.__class__.__module__)
        if mod.startswith("BTrees"):
            if hasattr(obj, "_check"):
                try:
                    obj._check()
                except AssertionError, msg:
                    print "*" * 60
                    print msg
                    print "*" * 60

        if found % 100 == 0:
            cn.cacheMinimize()

        for k, v in get_subobjects(obj):
            if k.startswith('['):
                # getitem
                newpath = "%s%s" % (path, k)
            else:
                newpath = "%s.%s" % (path, k)
            add_if_persistent(todo, v, newpath)

    print "total", len(fs._index), "found", found

if __name__ == "__main__":
    import sys
    try:
        fname, = sys.argv[1:]
    except:
        print __doc__
        sys.exit(2)

    main(fname)
