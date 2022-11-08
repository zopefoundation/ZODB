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
#     ' '  (a blank) completed transaction that hasn't been packed
#     'p'  completed transaction that has been packed
#     'c'  checkpoint -- a transaction in progress, at the end of the file;
#          it's been thru vote() but not finish(); if finish() completes
#          normally, it will be overwritten with a blank; if finish() dies
#          (e.g., out of disk space), cleanup code will try to truncate
#          the file to chop off this incomplete transaction
#     'u'  uncertain; no longer used; was previously used to record something
#          about non-transactional undo
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
#   - 2-bytes with zero values. (Was version length.)
#
#   - 8-byte data length
#
#   ? data
#     (data length > 0)
#
#   ? 8-byte position of data record containing data
#     (data length == 0)
#
# Note that the lengths and positions are all big-endian.
# Also, the object ids time stamps are big-endian, so comparisons
# are meaningful.
#
# Backpointers
#
#   When we undo a record, we don't copy (or delete)
#   data.  Instead, we write records with back pointers.

import logging
import struct

from ZODB._compat import PY3
from ZODB.POSException import POSKeyError
from ZODB.utils import as_bytes
from ZODB.utils import oid_repr
from ZODB.utils import u64


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
assert struct.calcsize(TRANS_HDR) == TRANS_HDR_LEN
assert struct.calcsize(DATA_HDR) == DATA_HDR_LEN

logger = logging.getLogger('ZODB.FileStorage.format')


class FileStorageFormatter(object):
    """Mixin class that can read and write the low-level format."""

    # subclasses must provide _file

    _metadata_size = 4
    _format_version = "21"

    def _read_num(self, pos):
        """Read an 8-byte number."""
        self._file.seek(pos)
        return u64(self._file.read(8))

    def _read_data_header(self, pos, oid=None, _file=None):
        """Return a DataHeader object for data record at pos.

        If ois is not None, raise CorruptedDataError if oid passed
        does not match oid in file.
        """
        if _file is None:
            _file = self._file

        _file.seek(pos)
        s = _file.read(DATA_HDR_LEN)
        if len(s) != DATA_HDR_LEN:
            raise CorruptedDataError(oid, s, pos)
        h = DataHeaderFromString(s)
        if oid is not None and oid != h.oid:
            raise CorruptedDataError(oid, s, pos)
        if not h.plen:
            h.back = u64(_file.read(8))
        return h

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

    def _loadBack_impl(self, oid, back, fail=True, _file=None):
        # shared implementation used by various _loadBack methods
        #
        # If the backpointer ultimately resolves to 0:
        # If fail is True, raise KeyError for zero backpointer.
        # If fail is False, return the empty data from the record
        # with no backpointer.
        if _file is None:
            _file = self._file
        while 1:
            if not back:
                # If backpointer is 0, object does not currently exist.
                raise POSKeyError(oid)
            h = self._read_data_header(back, _file=_file)
            if h.plen:
                return _file.read(h.plen), h.tid, back, h.tloc
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
        logger.error(s)
        raise CorruptedError(s)

    def checkTxn(self, th, pos):
        if th.tid <= self.ltid:
            self.fail(pos, "time-stamp reduction: %s <= %s",
                      oid_repr(th.tid), oid_repr(self.ltid))
        self.ltid = th.tid
        if th.status == "c":
            self.fail(pos, "transaction with checkpoint flag set")
        if th.status not in " pu":  # recognize " ", "p", and "u" as valid
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

    __slots__ = ("oid", "tid", "prev", "tloc", "plen", "back")

    def __init__(self, oid, tid, prev, tloc, vlen, plen):
        if vlen:
            raise ValueError(
                "Non-zero version length. Versions aren't supported.")

        self.oid = oid
        self.tid = tid
        self.prev = prev
        self.tloc = tloc
        self.plen = plen
        self.back = 0  # default

    def asString(self):
        return struct.pack(DATA_HDR, self.oid, self.tid, self.prev,
                           self.tloc, 0, self.plen)

    def recordlen(self):
        return DATA_HDR_LEN + (self.plen or 8)


def TxnHeaderFromString(s):
    res = TxnHeader(*struct.unpack(TRANS_HDR, s))
    if PY3:
        res.status = res.status.decode('ascii')
    return res


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
        assert elen >= 0

    def asString(self):
        s = struct.pack(TRANS_HDR, self.tid, self.tlen, as_bytes(self.status),
                        self.ulen, self.dlen, self.elen)
        return b"".join(map(as_bytes, [s, self.user, self.descr, self.ext]))

    def headerlen(self):
        return TRANS_HDR_LEN + self.ulen + self.dlen + self.elen
