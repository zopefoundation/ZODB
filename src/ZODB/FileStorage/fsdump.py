from __future__ import print_function

##############################################################################
#
# Copyright (c) 2003 Zope Foundation and Contributors.
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
from ZODB.FileStorage.format import DATA_HDR
from ZODB.FileStorage.format import DATA_HDR_LEN
from ZODB.FileStorage.format import TRANS_HDR
from ZODB.FileStorage.format import TRANS_HDR_LEN
from ZODB.TimeStamp import TimeStamp
from ZODB.utils import get_pickle_metadata
from ZODB.utils import u64


def fsdump(path, file=None, with_offset=1):
    iter = FileIterator(path)
    for i, trans in enumerate(iter):
        size = trans._tend - trans._tpos
        if with_offset:
            print(("Trans #%05d tid=%016x size=%d time=%s offset=%d" %
                   (i, u64(trans.tid), size,
                    TimeStamp(trans.tid), trans._pos)), file=file)
        else:
            print(("Trans #%05d tid=%016x size=%d time=%s" %
                   (i, u64(trans.tid), size, TimeStamp(trans.tid))), file=file)
        print(("    status=%r user=%r description=%r" %
               (trans.status, trans.user, trans.description)), file=file)

        for j, rec in enumerate(trans):
            if rec.data is None:
                fullclass = "undo or abort of object creation"
                size = ""
            else:
                modname, classname = get_pickle_metadata(rec.data)
                size = " size=%d" % len(rec.data)
                fullclass = "%s.%s" % (modname, classname)

            if rec.data_txn:
                # It would be nice to print the transaction number
                # (i) but it would be expensive to keep track of.
                bp = " bp=%016x" % u64(rec.data_txn)
            else:
                bp = ""

            print(("  data #%05d oid=%016x%s class=%s%s" %
                   (j, u64(rec.oid), size, fullclass, bp)), file=file)
    iter.close()


def fmt(p64):
    # Return a nicely formatted string for a packaged 64-bit value
    return "%016x" % u64(p64)


class Dumper(object):
    """A very verbose dumper for debugging FileStorage problems."""

    # TODO:  Should revise this class to use FileStorageFormatter.

    def __init__(self, path, dest=None):
        self.file = open(path, "rb")
        self.dest = dest

    def dump(self):
        fid = self.file.read(4)
        print("*" * 60, file=self.dest)
        print("file identifier: %r" % fid, file=self.dest)
        while self.dump_txn():
            pass

    def dump_txn(self):
        pos = self.file.tell()
        h = self.file.read(TRANS_HDR_LEN)
        if not h:
            return False
        tid, tlen, status, ul, dl, el = struct.unpack(TRANS_HDR, h)
        end = pos + tlen
        print("=" * 60, file=self.dest)
        print("offset: %d" % pos, file=self.dest)
        print("end pos: %d" % end, file=self.dest)
        print("transaction id: %s" % fmt(tid), file=self.dest)
        print("trec len: %d" % tlen, file=self.dest)
        print("status: %r" % status, file=self.dest)
        user = descr = ""
        if ul:
            user = self.file.read(ul)
        if dl:
            descr = self.file.read(dl)
        if el:
            self.file.read(el)
        print("user: %r" % user, file=self.dest)
        print("description: %r" % descr, file=self.dest)
        print("len(extra): %d" % el, file=self.dest)
        while self.file.tell() < end:
            self.dump_data(pos)
        stlen = self.file.read(8)
        print("redundant trec len: %d" % u64(stlen), file=self.dest)
        return 1

    def dump_data(self, tloc):
        pos = self.file.tell()
        h = self.file.read(DATA_HDR_LEN)
        assert len(h) == DATA_HDR_LEN
        oid, revid, prev, tloc, vlen, dlen = struct.unpack(DATA_HDR, h)
        print("-" * 60, file=self.dest)
        print("offset: %d" % pos, file=self.dest)
        print("oid: %s" % fmt(oid), file=self.dest)
        print("revid: %s" % fmt(revid), file=self.dest)
        print("previous record offset: %d" % prev, file=self.dest)
        print("transaction offset: %d" % tloc, file=self.dest)
        assert not vlen
        print("len(data): %d" % dlen, file=self.dest)
        self.file.read(dlen)
        if not dlen:
            sbp = self.file.read(8)
            print("backpointer: %d" % u64(sbp), file=self.dest)


def main():
    import sys
    fsdump(sys.argv[1])


if __name__ == "__main__":
    main()
