#! /usr/bin/env python
"""Print a text summary of the contents of a FileStorage."""

from ZODB.FileStorage import FileIterator
from ZODB.utils import U64
from ZODB.tests.StorageTestBase import zodb_unpickle

from cPickle import Unpickler
from cStringIO import StringIO
import md5
import types

def get_pickle_metadata(data):
    # ZODB's data records contain two pickles.  The first is the class
    # of the object, the second is the object.
    if data.startswith('(c'):
        # Don't actually unpickle a class, because it will attempt to
        # load the class.  Just break open the pickle and get the
        # module and class from it.
        modname, classname, rest = data.split('\n', 2)
        modname = modname[2:]
        return modname, classname
    f = StringIO(data)
    u = Unpickler(f)
    try:
        class_info = u.load()
    except Exception, err:
        print err
        return '', ''
    if isinstance(class_info, types.TupleType):
        if isinstance(class_info[0], types.TupleType):
            modname, classname = class_info[0]
        else:
            modname, classname = class_info
    else:
        # XXX not sure what to do here
        modname = repr(class_info)
        classname = ''
    return modname, classname

def main(path):
    i = 0
    for trans in FileIterator(path):
        print "T %6d %016x %s %s %s" % (i, U64(trans.tid), trans.status,
                                        trans.user, trans.description)
        j = 0
        for rec in trans:
            modname, classname = get_pickle_metadata(rec.data)
            dig = md5.new(rec.data).hexdigest()
            fullclass = "%s.%s" % (modname, classname)
            # special case for testing purposes
            if fullclass == "ZODB.tests.MinPO.MinPO":
                obj = zodb_unpickle(rec.data)
                fullclass = "%s %s" % (fullclass, obj.value)
            print "D %6d %016x %016x %s %s" % (j, U64(rec.oid),
                                               U64(rec.serial),
                                               rec.version, fullclass)
            j += 1
        print
        i += 1
            
if __name__ == "__main__":
    import sys
    main(sys.argv[1])
