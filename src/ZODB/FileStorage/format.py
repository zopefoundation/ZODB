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
# FOR A PARTICULAR PURPOSE
#
##############################################################################
#
#  File-based ZODB storage
#
# Files are arranged as follows.
#
#   - The first 4 bytes are a file identifier.
#
#   - The rest of the file consists of a sequence of transaction
#     "records".
#
# A transaction record consists of:
#
#   - 8-byte transaction id, which is also a time stamp.
#
#   - 8-byte transaction record length - 8.
#
#   - 1-byte status code
#
#   - 2-byte length of user name
#
#   - 2-byte length of description
#
#   - 2-byte length of extension attributes
#
#   -   user name
#
#   -   description
#
#   -   extension attributes
#
#   * A sequence of data records
#
#   - 8-byte redundant transaction length -8
#
# A data record consists of
#
#   - 8-byte oid.
#
#   - 8-byte tid, which matches the transaction id in the transaction record.
#
#   - 8-byte previous-record file-position.
#
#   - 8-byte beginning of transaction record file position.
#
#   - 2-byte version length
#
#   - 8-byte data length
#
#   ? 8-byte position of non-version data
#     (if version length > 0)
#
#   ? 8-byte position of previous record in this version
#     (if version length > 0)
#
#   ?   version string
#     (if version length > 0)
#
#   ?   data
#     (data length > 0)
#
#   ? 8-byte position of data record containing data
#     (data length == 0)
#
# Note that the lengths and positions are all big-endian.
# Also, the object ids time stamps are big-endian, so comparisons
# are meaningful.
#
# Version handling
#
#   There isn't a separate store for versions.  Each record has a
#   version field, indicating what version it is in.  The records in a
#   version form a linked list.  Each record that has a non-empty
#   version string has a pointer to the previous record in the version.
#   Version back pointers are retained *even* when versions are
#   committed or aborted or when transactions are undone.
#
#   There is a notion of "current" version records, which are the
#   records in a version that are the current records for their
#   respective objects.  When a version is comitted, the current records
#   are committed to the destination version.  When a version is
#   aborted, the current records are aborted.
#
#   When committing or aborting, we search backward through the linked
#   list until we find a record for an object that does not have a
#   current record in the version.  If we find a record for which the
#   non-version pointer is the same as the previous pointer, then we
#   forget that the corresponding object had a current record in the
#   version. This strategy allows us to avoid searching backward through
#   previously committed or aborted version records.
#
#   Of course, we ignore records in undone transactions when committing
#   or aborting.
#
# Backpointers
#
#   When we commit or abort a version, we don't copy (or delete)
#   and data.  Instead, we write records with back pointers.
#
#   A version record *never* has a back pointer to a non-version
#   record, because we never abort to a version.  A non-version record
#   may have a back pointer to a version record or to a non-version
#   record.

import struct

from ZODB.POSException import POSKeyError
from ZODB.serialize import referencesf
from ZODB.utils import p64, u64, z64, oid_repr, t32
from zLOG import LOG, BLATHER, WARNING, ERROR, PANIC

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
TRANS_HDR = ">8sQcHHH"
DATA_HDR = ">8s8sQQHQ"
# constants to support various header sizes
TRANS_HDR_LEN = 23
DATA_HDR_LEN = 42
DATA_VERSION_HDR_LEN = 58
assert struct.calcsize(TRANS_HDR) == TRANS_HDR_LEN
assert struct.calcsize(DATA_HDR) == DATA_HDR_LEN

class FileStorageFormatter(object):
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

    def _loadBack_impl(self, oid, back, fail=True):
        # shared implementation used by various _loadBack methods
        #
        # If the backpointer ultimately resolves to 0:
        # If fail is True, raise KeyError for zero backpointer.
        # If fail is False, return the empty data from the record
        # with no backpointer.
        while 1:
            if not back:
                # If backpointer is 0, object does not currently exist.
                raise POSKeyError(oid)
            h = self._read_data_header(back)
            if h.plen:
                return self._file.read(h.plen), h.tid, back, h.tloc
            if h.back == 0 and not fail:
                return None, h.tid, back, h.tloc
            back = h.back

    def _loadBackTxn(self, oid, back, fail=True):
        """Return data and txn id for backpointer."""
        return self._loadBack_impl(oid, back, fail)[:2]

    def _loadBackPOS(self, oid, back):
        return self._loadBack_impl(oid, back)[2]

    def getTxnFromData(self, oid, back):
        """Return transaction id for data at back."""
        h = self._read_data_header(back, oid)
        return h.tid

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
        "oid", "tid", "prev", "tloc", "vlen", "plen", "back",
        # These three attributes are only defined when vlen > 0
        "pnv", "vprev", "version")

    def __init__(self, oid, tid, prev, tloc, vlen, plen):
        self.back = 0 # default
        self.version = "" # default
        self.oid = oid
        self.tid = tid
        self.prev = prev
        self.tloc = tloc
        self.vlen = vlen
        self.plen = plen

    def asString(self):
        s = struct.pack(DATA_HDR, self.oid, self.tid, self.prev,
                        self.tloc, self.vlen, self.plen)
        if self.version:
            v = struct.pack(">QQ", self.pnv, self.vprev)
            return s + v + self.version
        else:
            return s

    def setVersion(self, version, pnv, vprev):
        self.version = version
        self.vlen = len(version)
        self.pnv = pnv
        self.vprev = vprev

    def parseVersion(self, buf):
        pnv, vprev = struct.unpack(">QQ", buf[:16])
        self.pnv = pnv
        self.vprev = vprev
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
        self.tlen = tlen
        self.status = status
        self.ulen = ulen
        self.dlen = dlen
        self.elen = elen
        if elen < 0:
            self.elen = t32 - elen

    def asString(self):
        s = struct.pack(TRANS_HDR, self.tid, self.tlen, self.status,
                        self.ulen, self.dlen, self.elen)
        return "".join([s, self.user, self.descr, self.ext])

    def headerlen(self):
        return TRANS_HDR_LEN + self.ulen + self.dlen + self.elen
