##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
import struct

from ZODB.FileStorage import FileIterator
from ZODB.FileStorage.format import TRANS_HDR, TRANS_HDR_LEN, DATA_HDR, DATA_HDR_LEN
from ZODB.FileStorage.format import DATA_HDR_LEN
from ZODB.TimeStamp import TimeStamp
from ZODB.utils import u64, get_pickle_metadata
from ZODB.tests.StorageTestBase import zodb_unpickle

def fsdump(path, file=None, with_offset=1):
    iter = FileIterator(path)
    for i, trans in enumerate(iter):
        if with_offset:
            print >> file, ("Trans #%05d tid=%016x time=%s offset=%d" %
                  (i, u64(trans.tid), TimeStamp(trans.tid), trans._pos))
        else:
            print >> file, ("Trans #%05d tid=%016x time=%s" %
                  (i, u64(trans.tid), TimeStamp(trans.tid)))
        print >> file, ("    status=%r user=%r description=%r" %
              (trans.status, trans.user, trans.description))

        for j, rec in enumerate(trans):
            if rec.data is None:
                fullclass = "undo or abort of object creation"
                size = ""
            else:
                modname, classname = get_pickle_metadata(rec.data)
                size = " size=%d" % len(rec.data)
                fullclass = "%s.%s" % (modname, classname)

            if rec.version:
                version = " version=%r" % rec.version
            else:
                version = ""

            if rec.data_txn:
                # It would be nice to print the transaction number
                # (i) but it would be expensive to keep track of.
                bp = " bp=%016x" % u64(rec.data_txn)
            else:
                bp = ""

            print >> file, ("  data #%05d oid=%016x%s%s class=%s%s" %
                  (j, u64(rec.oid), version, size, fullclass, bp))
    iter.close()

def fmt(p64):
    # Return a nicely formatted string for a packaged 64-bit value
    return "%016x" % u64(p64)

class Dumper:
    """A very verbose dumper for debuggin FileStorage problems."""

    # TODO:  Should revise this class to use FileStorageFormatter.

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
        tid, tlen, status, ul, dl, el = struct.unpack(TRANS_HDR, h)
        end = pos + tlen
        print >> self.dest, "=" * 60
        print >> self.dest, "offset: %d" % pos
        print >> self.dest, "end pos: %d" % end
        print >> self.dest, "transaction id: %s" % fmt(tid)
        print >> self.dest, "trec len: %d" % tlen
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
        stlen = self.file.read(8)
        print >> self.dest, "redundant trec len: %d" % u64(stlen)
        return 1

    def dump_data(self, tloc):
        pos = self.file.tell()
        h = self.file.read(DATA_HDR_LEN)
        assert len(h) == DATA_HDR_LEN
        oid, revid, prev, tloc, vlen, dlen = struct.unpack(DATA_HDR, h)
        print >> self.dest, "-" * 60
        print >> self.dest, "offset: %d" % pos
        print >> self.dest, "oid: %s" % fmt(oid)
        print >> self.dest, "revid: %s" % fmt(revid)
        print >> self.dest, "previous record offset: %d" % prev
        print >> self.dest, "transaction offset: %d" % tloc
        if vlen:
            pnv = self.file.read(8)
            sprevdata = self.file.read(8)
            version = self.file.read(vlen)
            print >> self.dest, "version: %r" % version
            print >> self.dest, "non-version data offset: %d" % u64(pnv)
            print >> self.dest, ("previous version data offset: %d" %
                                 u64(sprevdata))
        print >> self.dest, "len(data): %d" % dlen
        self.file.read(dlen)
        if not dlen:
            sbp = self.file.read(8)
            print >> self.dest, "backpointer: %d" % u64(sbp)

def main():
    import sys
    fsdump(sys.argv[1])
