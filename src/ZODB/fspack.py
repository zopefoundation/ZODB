##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""FileStorage helper to perform pack.

A storage contains an ordered set of object revisions.  When a storage
is packed, object revisions that are not reachable as of the pack time
are deleted.  The notion of reachability is complicated by
backpointers -- object revisions that point to earlier revisions of
the same object.

An object revisions is reachable at a certain time if it is reachable
from the revision of the root at that time or if it is reachable from
a backpointer after that time.
"""

# This module contains code backported from ZODB4 from the
# zodb.storage.file package.  It's been edited heavily to work with
# ZODB3 code and storage layout.

import os
import struct
from types import StringType

from ZODB.referencesf import referencesf
from ZODB.utils import p64, u64, z64, oid_repr
from zLOG import LOG, BLATHER, WARNING, ERROR, PANIC

try:
    from ZODB.fsIndex import fsIndex
except ImportError:
    def fsIndex():
        return {}

class CorruptedError(Exception):
    pass

class CorruptedDataError(CorruptedError):

    def __init__(self, oid=None, buf=None, pos=None):
        self.oid = oid
        self.buf = buf
        self.pos = pos

    def __str__(self):
        if self.oid:
            msg = "Error reading oid %s.  Found %r" % (oid_repr(self.oid),
                                                       self.buf)
        else:
            msg = "Error reading unknown oid.  Found %r" % self.buf
        if self.pos:
            msg += " at %d" % self.pos
        return msg

# the struct formats for the headers
TRANS_HDR = ">8s8scHHH"
DATA_HDR = ">8s8s8s8sH8s"
# constants to support various header sizes
TRANS_HDR_LEN = 23
DATA_HDR_LEN = 42
DATA_VERSION_HDR_LEN = 58
assert struct.calcsize(TRANS_HDR) == TRANS_HDR_LEN
assert struct.calcsize(DATA_HDR) == DATA_HDR_LEN

class FileStorageFormatter:
    """Mixin class that can read and write the low-level format."""

    # subclasses must provide _file

    _metadata_size = 4L
    _format_version = "21"

    def _read_num(self, pos):
        """Read an 8-byte number."""
        self._file.seek(pos)
        return u64(self._file.read(8))

    def _read_data_header(self, pos, oid=None):
        """Return a DataHeader object for data record at pos.

        If ois is not None, raise CorruptedDataError if oid passed
        does not match oid in file.

        If there is version data, reads the version part of the header.
        If there is no pickle data, reads the back pointer.
        """
        self._file.seek(pos)
        s = self._file.read(DATA_HDR_LEN)
        if len(s) != DATA_HDR_LEN:
            raise CorruptedDataError(oid, s, pos)
        h = DataHeaderFromString(s)
        if oid is not None and oid != h.oid:
            raise CorruptedDataError(oid, s, pos)
        if h.vlen:
            s = self._file.read(16 + h.vlen)
            h.parseVersion(s)
        if not h.plen:
            h.back = u64(self._file.read(8))
        return h

    def _write_version_header(self, file, pnv, vprev, version):
        s = struct.pack(">8s8s", pnv, vprev)
        file.write(s + version)

    def _read_txn_header(self, pos, tid=None):
        self._file.seek(pos)
        s = self._file.read(TRANS_HDR_LEN)
        if len(s) != TRANS_HDR_LEN:
            raise CorruptedDataError(tid, s, pos)
        h = TxnHeaderFromString(s)
        if tid is not None and tid != h.tid:
            raise CorruptedDataError(tid, s, pos)
        h.user = self._file.read(h.ulen)
        h.descr = self._file.read(h.dlen)
        h.ext = self._file.read(h.elen)
        return h

    def _loadBack_impl(self, oid, back, fail):
        # shared implementation used by various _loadBack methods
        #
        # If the backpointer ultimately resolves to 0:
        # If fail is 1, raise KeyError for zero backpointer.
        # If fail is 0, return the empty data from the record
        # with no backpointer.
        while 1:
            if not back:
                # If backpointer is 0, object does not currently exist.
                raise POSKeyError(oid)
            h = self._read_data_header(back)
            if h.plen:
                return self._file.read(h.plen), h.serial, back, h.tloc
            if h.back == 0 and not fail:
                return None, h.serial, back, h.tloc
            back = h.back

    def _loadBackTxn(self, oid, back, fail=1):
        """Return data, serial, and txn id for backpointer."""
        data, serial, old, tloc = self._loadBack_impl(oid, back, fail)
        self._file.seek(tloc)
        h = self._file.read(TRANS_HDR_LEN)
        tid = h[:8]
        return data, serial, tid

    def getTxnFromData(self, oid, back):
        """Return transaction id for data at back."""
        h = self._read_data_header(back, oid)
        self._file.seek(h.tloc)
        # seek to transaction header, where tid is first 8 bytes
        return self._file.read(8)

    def fail(self, pos, msg, *args):
        s = ("%s:%s:" + msg) % ((self._name, pos) + args)
        LOG("FS pack", ERROR, s)
        raise CorruptedError(s)

    def checkTxn(self, th, pos):
        if th.tid <= self.ltid:
            self.fail(pos, "time-stamp reduction: %s <= %s",
                      oid_repr(th.tid), oid_repr(self.ltid))
        self.ltid = th.tid
        if th.status == "c":
            self.fail(pos, "transaction with checkpoint flag set")
        if not th.status in " pu": # recognize " ", "p", and "u" as valid
            self.fail(pos, "invalid transaction status: %r", th.status)
        if th.tlen < th.headerlen():
            self.fail(pos, "invalid transaction header: "
                      "txnlen (%d) < headerlen(%d)", th.tlen, th.headerlen())

    def checkData(self, th, tpos, dh, pos):
        if dh.tloc != tpos:
            self.fail(pos, "data record does not point to transaction header"
                      ": %d != %d", dh.tloc, tpos)
        if pos + dh.recordlen() > tpos + th.tlen:
            self.fail(pos, "data record size exceeds transaction size: "
                      "%d > %d", pos + dh.recordlen(), tpos + th.tlen)
        if dh.prev >= pos:
            self.fail(pos, "invalid previous pointer: %d", dh.prev)
        if dh.back:
            if dh.back >= pos:
                self.fail(pos, "invalid back pointer: %d", dh.prev)
            if dh.plen:
                self.fail(pos, "data record has back pointer and data")

def DataHeaderFromString(s):
    return DataHeader(*struct.unpack(DATA_HDR, s))

class DataHeader(object):
    """Header for a data record."""

    __slots__ = (
        "oid", "serial", "prev", "tloc", "vlen", "plen", "back",
        # These three attributes are only defined when vlen > 0
        "pnv", "vprev", "version")

    def __init__(self, oid, serial, prev, tloc, vlen, plen):
        self.back = 0 # default
        self.version = "" # default
        self.oid = oid
        self.serial = serial
        if isinstance(prev, StringType):
            prev = u64(prev)
        if isinstance(tloc, StringType):
            tloc = u64(tloc)
        self.prev = prev
        self.tloc = tloc

        self.vlen = vlen
        if isinstance(plen, StringType):
            plen = u64(plen)
        self.plen = plen

    def asString(self):
        s = struct.pack(DATA_HDR, self.oid, self.serial, p64(self.prev),
                        p64(self.tloc), self.vlen, p64(self.plen))
        if self.version:
            v = struct.pack(">8s8s", p64(self.pnv), p64(self.vprev))
            return s + v + self.version
        else:
            return s

    def setVersion(self, version, pnv, vprev):
        self.version = version
        self.vlen = len(version)
        self.pnv = pnv
        self.vprev = vprev

    def parseVersion(self, buf):
        pnv, vprev = struct.unpack(">8s8s", buf[:16])
        self.pnv = u64(pnv)
        self.vprev = u64(vprev)
        self.version = buf[16:]

    def recordlen(self):
        rlen = DATA_HDR_LEN + (self.plen or 8)
        if self.version:
            rlen += 16 + self.vlen
        return rlen

def TxnHeaderFromString(s):
    return TxnHeader(*struct.unpack(TRANS_HDR, s))

class TxnHeader(object):
    """Header for a transaction record."""

    __slots__ = ("tid", "tlen", "status", "user", "descr", "ext",
                 "ulen", "dlen", "elen")

    def __init__(self, tid, tlen, status, ulen, dlen, elen):
        self.tid = tid
        self.tlen = u64(tlen)
        self.status = status
        self.ulen = ulen
        self.dlen = dlen
        self.elen = elen

    def asString(self):
        s = struct.pack(TRANS_HDR, self.tid, p64(self.tlen), self.status,
                        self.ulen, self.dlen, self.elen)
        return "".join([s, self.user, self.descr, self.ext])

    def headerlen(self):
        return TRANS_HDR_LEN + self.ulen + self.dlen + self.elen


class DataCopier(FileStorageFormatter):
    """Mixin class for copying transactions into a storage.

    The restore() and pack() methods share a need to copy data records
    and update pointers to data in earlier transaction records.  This
    class provides the shared logic.

    The mixin extends the FileStorageFormatter with a copy() method.
    It also requires that the concrete class provides the following
    attributes:

    _file -- file with earlier destination data
    _tfile -- destination file for copied data
    _packt -- p64() representation of latest pack time
    _pos -- file pos of destination transaction
    _tindex -- maps oid to data record file pos
    _tvindex -- maps version name to data record file pos

    _tindex and _tvindex are updated by copy().

    The copy() method does not do any locking.
    """

    def _txn_find(self, tid, stop_at_pack):
        # _pos always points just past the last transaction
        pos = self._pos
        while pos > 4:
            self._file.seek(pos - 8)
            pos = pos - u64(self._file.read(8)) - 8
            self._file.seek(pos)
            h = self._file.read(TRANS_HDR_LEN)
            _tid = h[:8]
            if _tid == tid:
                return pos
            if stop_at_pack:
                if h[16] == 'p':
                    break
        raise UndoError(None, "Invalid transaction id")

    def _data_find(self, tpos, oid, data):
        # Return backpointer to oid in data record for in transaction at tpos.
        # It should contain a pickle identical to data. Returns 0 on failure.
        # Must call with lock held.
        h = self._read_txn_header(tpos)
        tend = tpos + h.tlen
        pos = self._file.tell()
        while pos < tend:
            h = self._read_data_header(pos)
            if h.oid == oid:
                # Make sure this looks like the right data record
                if h.plen == 0:
                    # This is also a backpointer.  Gotta trust it.
                    return pos
                if h.plen != len(data):
                    # The expected data doesn't match what's in the
                    # backpointer.  Something is wrong.
                    error("Mismatch between data and backpointer at %d", pos)
                    return 0
                _data = self._file.read(h.plen)
                if data != _data:
                    return 0
                return pos
            pos += h.recordlen()
        return 0

    def _restore_pnv(self, oid, prev, version, bp):
        # Find a valid pnv (previous non-version) pointer for this version.

        # If there is no previous record, there can't be a pnv.
        if not prev:
            return None

        h = self._read_data_header(prev, oid)
        # If the previous record is for a version, it must have
        # a valid pnv.
        if h.version:
            return h.pnv
        elif bp:
            # XXX Not sure the following is always true:
            # The previous record is not for this version, yet we
            # have a backpointer to it.  The current record must
            # be an undo of an abort or commit, so the backpointer
            # must be to a version record with a pnv.
            h2 = self._read_data_header(bp, oid)
            if h2.version:
                return h2.pnv
            else:
                warn("restore could not find previous non-version data "
                     "at %d or %d", prev, bp)
                return None

    def _resolve_backpointer(self, prev_txn, oid, data):
        prev_pos = 0
        if prev_txn is not None:
            prev_txn_pos = self._txn_find(prev_txn, 0)
            if prev_txn_pos:
                prev_pos = self._data_find(prev_txn_pos, oid, data)
        return prev_pos

    def copy(self, oid, serial, data, version, prev_txn,
             txnpos, datapos):
        prev_pos = self._resolve_backpointer(prev_txn, oid, data)
        old = self._index.get(oid, 0)
        # Calculate the pos the record will have in the storage.
        here = datapos
        # And update the temp file index
        self._tindex[oid] = here
        if prev_pos:
            # If there is a valid prev_pos, don't write data.
            data = None
        if data is None:
            dlen = 0
        else:
            dlen = len(data)
        # Write the recovery data record
        h = DataHeader(oid, serial, old, txnpos, len(version), dlen)
        if version:
            h.version = version
            pnv = self._restore_pnv(oid, old, version, prev_pos)
            if pnv is not None:
                h.pnv = pnv
            else:
                h.pnv = old
            # Link to the last record for this version
            h.vprev = self._tvindex.get(version, 0)
            if not h.vprev:
                h.vprev = self._vindex.get(version, 0)
            self._tvindex[version] = here

        self._tfile.write(h.asString())
        # Write the data or a backpointer
        if data is None:
            if prev_pos:
                self._tfile.write(p64(prev_pos))
            else:
                # Write a zero backpointer, which indicates an
                # un-creation transaction.
                self._tfile.write(z64)
        else:
            self._tfile.write(data)

class GC(FileStorageFormatter):

    def __init__(self, file, eof, packtime):
        self._file = file
        self._name = file.name
        self.eof = eof
        self.packtime = packtime
        # packpos: position of first txn header after pack time
        self.packpos = None
        self.oid2curpos = fsIndex() # maps oid to current data record position
        self.oid2verpos = fsIndex() # maps oid to current version data

        # The set of reachable revisions of each object.
        #
        # This set as managed using two data structures.  The first is
        # an fsIndex mapping oids to one data record pos.  Since only
        # a few objects will have more than one revision, we use this
        # efficient data structure to handle the common case.  The
        # second is a dictionary mapping objects to lists of
        # positions; it is used to handle the same number of objects
        # for which we must keep multiple revisions.

        self.reachable = fsIndex()
        self.reach_ex = {}

        # keep ltid for consistency checks during initial scan
        self.ltid = z64

    def isReachable(self, oid, pos):
        """Return 1 if revision of `oid` at `pos` is reachable."""

        rpos = self.reachable.get(oid)
        if rpos is None:
            return 0
        if rpos == pos:
            return 1
        return pos in self.reach_ex.get(oid, [])

    def findReachable(self):
        self.buildPackIndex()
        self.findReachableAtPacktime([z64])
        self.findReachableFromFuture()
        # These mappings are no longer needed and may consume a lot
        # of space.
        del self.oid2verpos
        del self.oid2curpos

    def buildPackIndex(self):
        pos = 4L
        while pos < self.eof:
            th = self._read_txn_header(pos)
            if th.tid > self.packtime:
                break
            self.checkTxn(th, pos)

            tpos = pos
            end = pos + th.tlen
            pos += th.headerlen()

            while pos < end:
                dh = self._read_data_header(pos)
                self.checkData(th, tpos, dh, pos)
                if dh.version:
                    self.oid2verpos[dh.oid] = pos
                else:
                    self.oid2curpos[dh.oid] = pos
                pos += dh.recordlen()

            tlen = self._read_num(pos)
            if tlen != th.tlen:
                self.fail(pos, "redundant transaction length does not "
                          "match initial transaction length: %d != %d",
                          u64(s), th.tlen)
            pos += 8

        self.packpos = pos

    def findReachableAtPacktime(self, roots):
        """Mark all objects reachable from the oids in roots as reachable."""
        todo = list(roots)
        while todo:
            oid = todo.pop()
            if self.reachable.has_key(oid):
                continue

            L = []

            pos = self.oid2curpos.get(oid)
            if pos is not None:
                L.append(pos)
                todo.extend(self.findrefs(pos))

            pos = self.oid2verpos.get(oid)
            if pos is not None:
                L.append(pos)
                todo.extend(self.findrefs(pos))

            if not L:
                continue

            pos = L.pop()
            self.reachable[oid] = pos
            if L:
                self.reach_ex[oid] = L

    def findReachableFromFuture(self):
        # In this pass, the roots are positions of object revisions.
        # We add a pos to extra_roots when there is a backpointer to a
        # revision that was not current at the packtime.  The
        # non-current revision could refer to objects that were
        # otherwise unreachable at the packtime.
        extra_roots = []

        pos = self.packpos
        while pos < self.eof:
            th = self._read_txn_header(pos)
            self.checkTxn(th, pos)
            tpos = pos
            end = pos + th.tlen
            pos += th.headerlen()

            while pos < end:
                dh = self._read_data_header(pos)
                self.checkData(th, tpos, dh, pos)

                if dh.back and dh.back < self.packpos:
                    if self.reachable.has_key(dh.oid):
                        L = self.reach_ex.setdefault(dh.oid, [])
                        if dh.back not in L:
                            L.append(dh.back)
                            extra_roots.append(dh.back)
                    else:
                        self.reachable[dh.oid] = dh.back

                if dh.version and dh.pnv:
                    if self.reachable.has_key(dh.oid):
                        L = self.reach_ex.setdefault(dh.oid, [])
                        if dh.pnv not in L:
                            L.append(dh.pnv)
                            extra_roots.append(dh.pnv)
                    else:
                        self.reachable[dh.oid] = dh.back

                pos += dh.recordlen()

            tlen = self._read_num(pos)
            if tlen != th.tlen:
                self.fail(pos, "redundant transaction length does not "
                          "match initial transaction length: %d != %d",
                          u64(s), th.tlen)
            pos += 8

        for pos in extra_roots:
            refs = self.findrefs(pos)
            self.findReachableAtPacktime(refs)

    def findrefs(self, pos):
        """Return a list of oids referenced as of packtime."""
        dh = self._read_data_header(pos)
        # Chase backpointers until we get to the record with the refs
        while dh.back:
            dh = self._read_data_header(dh.back)
        if dh.plen:
            return referencesf(self._file.read(dh.plen))
        else:
            return []

class PackCopier(DataCopier):

    # PackCopier has to cope with _file and _tfile being the
    # same file.  The copy() implementation is written assuming
    # that they are different, so that using one object doesn't
    # mess up the file pointer for the other object.

    # PackCopier overrides _resolve_backpointer() and _restore_pnv()
    # to guarantee that they keep the file pointer for _tfile in
    # the right place.

    def __init__(self, f, index, vindex, tindex, tvindex):
        self._file = f
        self._tfile = f
        self._index = index
        self._vindex = vindex
        self._tindex = tindex
        self._tvindex = tvindex
        self._pos = None

    def setTxnPos(self, pos):
        self._pos = pos

    def _resolve_backpointer(self, prev_txn, oid, data):
        pos = self._tfile.tell()
        try:
            return DataCopier._resolve_backpointer(self, prev_txn, oid, data)
        finally:
            self._tfile.seek(pos)

    def _restore_pnv(self, oid, prev, version, bp):
        pos = self._tfile.tell()
        try:
            return DataCopier._restore_pnv(self, oid, prev, version, bp)
        finally:
            self._tfile.seek(pos)

class FileStoragePacker(FileStorageFormatter):

    def __init__(self, path, stop, la, lr, cla, clr):
        self._name = path
        self._file = open(path, "rb")
        self._stop = stop
        self._packt = None
        self.locked = 0
        self._file.seek(0, 2)
        self.file_end = self._file.tell()
        self._file.seek(0)

        self.gc = GC(self._file, self.file_end, self._stop)

        # The packer needs to acquire the parent's commit lock
        # during the copying stage, so the two sets of lock acquire
        # and release methods are passed to the constructor.
        self._lock_acquire = la
        self._lock_release = lr
        self._commit_lock_acquire = cla
        self._commit_lock_release = clr

        # The packer will use several indexes.
        # index: oid -> pos
        # vindex: version -> pos of XXX
        # tindex: oid -> pos, for current txn
        # tvindex: version -> pos of XXX, for current txn
        # oid2serial: not used by the packer

        self.index = fsIndex()
        self.vindex = {}
        self.tindex = {}
        self.tvindex = {}
        self.oid2serial = {}
        self.toid2serial = {}
        self.toid2serial_delete = {}

        # Index for non-version data.  This is a temporary structure
        # to reduce I/O during packing
        self.nvindex = fsIndex()

    def pack(self):
        # Pack copies all data reachable at the pack time or later.
        #
        # Copying occurs in two phases.  In the first phase, txns
        # before the pack time are copied if the contain any reachable
        # data.  In the second phase, all txns after the pack time
        # are copied.
        #
        # Txn and data records contain pointers to previous records.
        # Because these pointers are stored as file offsets, they
        # must be updated when we copy data.

        # XXX Need to add sanity checking to pack

        self.gc.findReachable()

        # Setup the destination file and copy the metadata.
        # XXX rename from _tfile to something clearer
        self._tfile = open(self._name + ".pack", "w+b")
        self._file.seek(0)
        self._tfile.write(self._file.read(self._metadata_size))

        self._copier = PackCopier(self._tfile, self.index, self.vindex,
                                  self.tindex, self.tvindex)

        ipos, opos = self.copyToPacktime()
        assert ipos == self.gc.packpos
        if ipos == opos:
            # pack didn't free any data.  there's no point in continuing.
            self._tfile.close()
            os.remove(self._name + ".pack")
            return None
        self._commit_lock_acquire()
        self.locked = 1
        self._lock_acquire()
        try:
            self._file.seek(0, 2)
            self.file_end = self._file.tell()
        finally:
            self._lock_release()
        if ipos < self.file_end:
            self.copyRest(ipos)

        # OK, we've copied everything. Now we need to wrap things up.
        pos = self._tfile.tell()
        self._tfile.flush()
        self._tfile.close()
        self._file.close()

        return pos

    def copyToPacktime(self):
        pos = self._metadata_size
        new_pos = pos

        while pos < self.gc.packpos:
            th = self._read_txn_header(pos)
            new_tpos, pos = self.copyDataRecords(pos, th)

            if new_tpos:
                new_pos = self._tfile.tell() + 8
                tlen = new_pos - new_tpos - 8
                # Update the transaction length
                self._tfile.seek(new_tpos + 8)
                self._tfile.write(p64(tlen))
                self._tfile.seek(new_pos - 8)
                self._tfile.write(p64(tlen))


            tlen = self._read_num(pos)
            if tlen != th.tlen:
                self.fail(pos, "redundant transaction length does not "
                          "match initial transaction length: %d != %d",
                          u64(s), th.tlen)
            pos += 8

        return pos, new_pos

    def fetchBackpointer(self, oid, back):
        """Return data and refs backpointer `back` to object `oid.

        If `back` is 0 or ultimately resolves to 0, return None
        and None.  In this case, the transaction undoes the object
        creation.
        """
        if back == 0:
            return None
        data, serial, tid = self._loadBackTxn(oid, back, 0)
        return data

    def copyDataRecords(self, pos, th):
        """Copy any current data records between pos and tend.

        Returns position of txn header in output file and position
        of next record in the input file.

        If any data records are copied, also write txn header (th).
        """
        copy = 0
        new_tpos = 0L
        tend = pos + th.tlen
        pos += th.headerlen()
        while pos < tend:
            h = self._read_data_header(pos)
            if not self.gc.isReachable(h.oid, pos):
                pos += h.recordlen()
                continue
            pos += h.recordlen()

            # If we are going to copy any data, we need to copy
            # the transaction header.  Note that we will need to
            # patch up the transaction length when we are done.
            if not copy:
                th.status = "p"
                s = th.asString()
                new_tpos = self._tfile.tell()
                self._tfile.write(s)
                copy = 1

            if h.plen:
                data = self._file.read(h.plen)
            else:
                # If a current record has a backpointer, fetch
                # refs and data from the backpointer.  We need
                # to write the data in the new record.
                data = self.fetchBackpointer(h.oid, h.back)

            self.writePackedDataRecord(h, data, new_tpos)

        return new_tpos, pos

    def writePackedDataRecord(self, h, data, new_tpos):
        # Update the header to reflect current information, then write
        # it to the output file.
        if data is None:
            data = ""
        h.prev = 0
        h.back = 0
        h.plen = len(data)
        h.tloc = new_tpos
        pos = self._tfile.tell()
        if h.version:
            h.pnv = self.index.get(h.oid, 0)
            h.vprev = self.vindex.get(h.version, 0)
            self.vindex[h.version] = pos
        self.index[h.oid] = pos
        if h.version:
            self.vindex[h.version] = pos
        self._tfile.write(h.asString())
        self._tfile.write(data)
        if not data:
            # Packed records never have backpointers (?).
            # If there is no data, write a z64 backpointer.
            # This is a George Bailey event.
            self._tfile.write(z64)

    def copyRest(self, ipos):
        # After the pack time, all data records are copied.
        # Copy one txn at a time, using copy() for data.

        # Release the commit lock every 20 copies
        self._lock_counter = 0

        try:
            while 1:
                ipos = self.copyOne(ipos)
        except CorruptedDataError, err:
            # The last call to copyOne() will raise
            # CorruptedDataError, because it will attempt to read past
            # the end of the file.  Double-check that the exception
            # occurred for this reason.
            self._file.seek(0, 2)
            endpos = self._file.tell()
            if endpos != err.pos:
                raise

    def copyOne(self, ipos):
        # The call below will raise CorruptedDataError at EOF.
        th = self._read_txn_header(ipos)
        self._lock_counter += 1
        if self._lock_counter % 20 == 0:
            self._commit_lock_release()
        pos = self._tfile.tell()
        self._copier.setTxnPos(pos)
        self._tfile.write(th.asString())
        tend = ipos + th.tlen
        ipos += th.headerlen()

        while ipos < tend:
            h = self._read_data_header(ipos)
            ipos += h.recordlen()
            prev_txn = None
            if h.plen:
                data = self._file.read(h.plen)
            else:
                data = self.fetchBackpointer(h.oid, h.back)
                if h.back:
                    prev_txn = self.getTxnFromData(h.oid, h.back)

            self._copier.copy(h.oid, h.serial, data, h.version,
                              prev_txn, pos, self._tfile.tell())

        tlen = self._tfile.tell() - pos
        assert tlen == th.tlen
        self._tfile.write(p64(tlen))
        ipos += 8

        self.index.update(self.tindex)
        self.tindex.clear()
        self.vindex.update(self.tvindex)
        self.tvindex.clear()
        if self._lock_counter % 20 == 0:
            self._commit_lock_acquire()
        return ipos
