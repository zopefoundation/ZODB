from ZODB.FileStorage import FileIterator
from ZODB.TimeStamp import TimeStamp
from ZODB.utils import u64
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
                  (i, u64(trans.tid), str(TimeStamp(trans.tid)), trans._pos)
        else:
            print >> file, "Trans #%05d tid=%016x time=%s" % \
                  (i, u64(trans.tid), str(TimeStamp(trans.tid)))
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
                bp = "bp=%016x" % u64(rec.data_txn)
            else:
                bp = ""
            print >> file, "  data #%05d oid=%016x %sclass=%s %s" % \
                  (j, u64(rec.oid), version, fullclass, bp)
            j += 1
        print >> file
        i += 1
    iter.close()

import struct
from ZODB.FileStorage import TRANS_HDR, TRANS_HDR_LEN
from ZODB.FileStorage import DATA_HDR, DATA_HDR_LEN

def fmt(p64):
    # Return a nicely formatted string for a packaged 64-bit value
    return "%016x" % u64(p64)

class Dumper:
    """A very verbose dumper for debuggin FileStorage problems."""

    def __init__(self, path, dest=None):
        self.file = open(path, "rb")
        self.dest = dest

    def dump(self):
        fid = self.file.read(4)
        print >> self.dest, "*" * 60
        print >> self.dest, "file identifier: %r" % fid
        while self.dump_txn():
            pass

    def dump_txn(self):
        pos = self.file.tell()
        h = self.file.read(TRANS_HDR_LEN)
        if not h:
            return False
        tid, stlen, status, ul, dl, el = struct.unpack(TRANS_HDR, h)
        end = pos + u64(stlen)
        print >> self.dest, "=" * 60
        print >> self.dest, "offset: %d" % pos
        print >> self.dest, "end pos: %d" % end
        print >> self.dest, "transaction id: %s" % fmt(tid)
        print >> self.dest, "trec len: %d" % u64(stlen)
        print >> self.dest, "status: %r" % status
        user = descr = extra = ""
        if ul:
            user = self.file.read(ul)
        if dl:
            descr = self.file.read(dl)
        if el:
            extra = self.file.read(el)
        print >> self.dest, "user: %r" % user
        print >> self.dest, "description: %r" % descr
        print >> self.dest, "len(extra): %d" % el
        while self.file.tell() < end:
            self.dump_data(pos)
        stlen2 = self.file.read(8)
        print >> self.dest, "redundant trec len: %d" % u64(stlen2)
        return True

    def dump_data(self, tloc):
        pos = self.file.tell()
        h = self.file.read(DATA_HDR_LEN)
        assert len(h) == DATA_HDR_LEN
        oid, revid, sprev, stloc, vlen, sdlen = struct.unpack(DATA_HDR, h)
        dlen = u64(sdlen)
        print >> self.dest, "-" * 60
        print >> self.dest, "offset: %d" % pos
        print >> self.dest, "oid: %s" % fmt(oid)
        print >> self.dest, "revid: %s" % fmt(revid)
        print >> self.dest, "previous record offset: %d" % u64(sprev)
        print >> self.dest, "transaction offset: %d" % u64(stloc)
        if vlen:
            pnv = self.file.read(8)
            sprevdata = self.file.read(8)
            version = self.file.read(vlen)
            print >> self.dest, "version: %r" % version
            print >> self.dest, "non-version data offset: %d" % u64(pnv)
            print >> self.dest, \
                  "previous version data offset: %d" % u64(sprevdata)
        print >> self.dest, "len(data): %d" % dlen
        self.file.read(dlen)
        if not dlen:
            sbp = self.file.read(8)
            print >> self.dest, "backpointer: %d" % u64(sbp)
