from ZODB.FileStorage import FileIterator
from ZODB.TimeStamp import TimeStamp
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
        print "Error", err
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

def fsdump(path, file=None, with_offset=1):
    i = 0
    iter = FileIterator(path)
    for trans in iter:
        if with_offset:
            print >> file, "Trans #%05d tid=%016x time=%s offset=%d" % \
                  (i, U64(trans.tid), str(TimeStamp(trans.tid)), trans._pos)
        else:
            print >> file, "Trans #%05d tid=%016x time=%s" % \
                  (i, U64(trans.tid), str(TimeStamp(trans.tid)))
        print >> file, "\tstatus=%s user=%s description=%s" % \
              (`trans.status`, trans.user, trans.description)
        j = 0
        for rec in trans:
            if rec.data is None:
                fullclass = "undo or abort of object creation"
            else:
                modname, classname = get_pickle_metadata(rec.data)
                dig = md5.new(rec.data).hexdigest()
                fullclass = "%s.%s" % (modname, classname)
            # special case for testing purposes
            if fullclass == "ZODB.tests.MinPO.MinPO":
                obj = zodb_unpickle(rec.data)
                fullclass = "%s %s" % (fullclass, obj.value)
            if rec.version:
                version = "version=%s " % rec.version
            else:
                version = ''
            if rec.data_txn:
                # XXX It would be nice to print the transaction number
                # (i) but it would be too expensive to keep track of.
                bp = "bp=%016x" % U64(rec.data_txn)
            else:
                bp = ""
            print >> file, "  data #%05d oid=%016x %sclass=%s %s" % \
                  (j, U64(rec.oid), version, fullclass, bp)
            j += 1
        print >> file
        i += 1
    iter.close()
