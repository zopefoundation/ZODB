##############################################################################
#
# Copyright (c) 2001 Zope Corporation and Contributors.
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

# originally zodb.storage.file.format

"""Tools for working with the low-level FileStorage format.

Files are arranged as follows.

  - The first 1024 bytes are a storage metadata section.

  The first two bytes are the characters F and S.
  The next two bytes are a storage format version id, currently 43.
  The next four bytes are the database version string.

  The rest of the section is reserved.

A transaction record consists of:

  - 8-byte transaction id, which is also a time stamp.

  - 8-byte transaction record length - 8.

  - 1-byte status code

  - 2-byte length of user name

  - 2-byte length of description

  - 2-byte length of extension attributes

  -   user name

  -   description

  -   extension attributes

  * A sequence of data records

  - 8-byte redundant transaction length -8

A data record consists of

  - 8-byte oid.

  - 8-byte serial, which is a type stamp that matches the
    transaction timestamp.

  - 8-byte previous-record file-position.

  - 8-byte beginning of transaction record file position.

  - 2-byte version length

  - 4-byte number of object references (oids)

  - 8-byte data length

  ? 8-byte position of non-version data
    (if version length > 0)

  ? 8-byte position of previous record in this version
    (if version length > 0)

  ? version string (if version length > 0)

  ? reference oids (length == # of oids * 8)

  ? data (if data length > 0)

  ? 8-byte position of data record containing data
    (data length == 0)

Note that the lengths and positions are all big-endian.
Also, the object ids time stamps are big-endian, so comparisons
are meaningful.

Version handling

  There isn't a separate store for versions.  Each record has a
  version field, indicating what version it is in.  The records in a
  version form a linked list.  Each record that has a non-empty
  version string has a pointer to the previous record in the version.
  Version back pointers are retained *even* when versions are
  committed or aborted or when transactions are undone.

  There is a notion of 'current' version records, which are the
  records in a version that are the current records for their
  respective objects.  When a version is comitted, the current records
  are committed to the destination version.  When a version is
  aborted, the current records are aborted.

  When committing or aborting, we search backward through the linked
  list until we find a record for an object that does not have a
  current record in the version.  If we find a record for which the
  non-version pointer is the same as the previous pointer, then we
  forget that the corresponding object had a current record in the
  version. This strategy allows us to avoid searching backward through
  previously committed or aborted version records.

  Of course, we ignore records in undone transactions when committing
  or aborting.

Backpointers

  When we commit or abort a version, we don't copy (or delete)
  and data.  Instead, we write records with back pointers.

  A version record *never* has a back pointer to a non-version
  record, because we never abort to a version.  A non-version record
  may have a back pointer to a version record or to a non-version
  record.
"""

import logging
import struct

from ZODB.zodb4.z4base import splitrefs
from ZODB.zodb4.z4interfaces import ZERO, MAXTID, POSKeyError, _fmt_oid
from ZODB.zodb4.z4utils import u64, p64
from ZODB.zodb4.z4errors \
     import CorruptedDataError, CorruptedError, FileStorageFormatError

# the struct formats for the headers
TRANS_HDR = ">8sQcHHH"
DATA_HDR = ">8s8sQQHIQ"
# constants to support various header sizes
TRANS_HDR_LEN = 23
DATA_HDR_LEN = 46
DATA_VERSION_HDR_LEN = 62
assert struct.calcsize(TRANS_HDR) == TRANS_HDR_LEN
assert struct.calcsize(DATA_HDR) == DATA_HDR_LEN

logger = logging.getLogger("zodb.storage.file")

def panic(message, *data):
    logger.critical(message, *data)
    raise CorruptedError(message % data)

class FileStorageFormatter:
    """Mixin class that can read and write the low-level format."""

    # subclasses must provide _file

    def _read_index(self, index, vindex, tindex, stop=MAXTID,
                    ltid=ZERO, start=None, maxoid=ZERO, recover=0,
                    read_only=0):
        """Scan the entire file storage and recreate the index.

        Returns file position, max oid, and last transaction id.  It also
        stores index information in the three dictionary arguments.

        Arguments:
        index -- dictionary, oid -> data record
        vindex -- dictionary, oid -> data record for version data
        tindex -- dictionary, oid -> data record
           XXX tindex is cleared before return, so it will be empty

        There are several default arguments that affect the scan or the
        return values.  XXX should document them.

        The file position returned is the position just after the last
        valid transaction record.  The oid returned is the maximum object
        id in the data.  The transaction id is the tid of the last
        transaction.
        """
        self._file.seek(0, 2)
        file_size = self._file.tell()
        self._file.seek(0)

        if start is None:
            start = self._metadata_size

        if file_size:
            if file_size < start:
                raise FileStorageFormatError(self._file.name)
            self._read_metadata()
        else:
            if not read_only:
                self._write_metadata()
            return self._metadata_size, maxoid, ltid

        pos = start
        self._file.seek(start)
        tid = '\0' * 7 + '\1'

        while True:
            # Read the transaction record
            h = self._file.read(TRANS_HDR_LEN)
            if not h:
                break
            if len(h) != TRANS_HDR_LEN:
                if not read_only:
                    logger.warn('%s truncated at %s', self._file.name, pos)
                    self._file.seek(pos)
                    self._file.truncate()
                break

            tid, tl, status, ul, dl, el = struct.unpack(TRANS_HDR, h)
            if el < 0:
                el = t32 - el

            if tid <= ltid:
                logger.warn("%s time-stamp reduction at %s",
                            self._file.name, pos)
            ltid = tid

            if pos+(tl+8) > file_size or status=='c':
                # Hm, the data were truncated or the checkpoint flag
                # wasn't cleared.  They may also be corrupted, in
                # which case, we don't want to totally lose the data.
                if not read_only:
                    logger.warn("%s truncated, possibly due "
                                "to damaged records at %s",
                                self._file.name, pos)
                    _truncate(self._file, self._file.name, pos)
                break

            if status not in ' up':
                logger.warn('%s has invalid status, %s, at %s',
                            self._file.name, status, pos)

            if tl < (TRANS_HDR_LEN+ul+dl+el):
                # We're in trouble. Find out if this is bad data in
                # the middle of the file, or just a turd that Win 9x
                # dropped at the end when the system crashed.  Skip to
                # the end and read what should be the transaction
                # length of the last transaction.
                self._file.seek(-8, 2)
                rtl = u64(self._file.read(8))
                # Now check to see if the redundant transaction length is
                # reasonable:
                if file_size - rtl < pos or rtl < TRANS_HDR_LEN:
                    logger.critical('%s has invalid transaction header at %s',
                              self._file.name, pos)
                    if not read_only:
                        logger.warn("It appears that there is invalid data "
                                    "at the end of the file, possibly due "
                                    "to a system crash.  %s truncated "
                                    "to recover from bad data at end.",
                                    self._file.name)
                        _truncate(file, self._file.name, pos)
                    break
                else:
                    if recover:
                        return pos, None, None
                    panic('%s has invalid transaction header at %s',
                          self._file.name, pos)

            if tid >= stop:
                break

            tpos = pos
            tend = tpos + tl

            if status == 'u':
                # Undone transaction, skip it
                self._file.seek(tend)
                h = self._file.read(8)
                if h != stl:
                    if recover: return tpos, None, None
                    panic('%s has inconsistent transaction length at %s',
                          self._file.name, pos)
                pos = tend + 8
                continue

            pos = tpos + (TRANS_HDR_LEN + ul + dl + el)
            while pos < tend:
                # Read the data records for this transaction
                h = self._read_data_header(pos)
                dlen = h.recordlen()
                tindex[h.oid] = pos

                if h.version:
                    vindex[h.version] = pos

                if pos + dlen > tend or h.tloc != tpos:
                    if recover:
                        return tpos, None, None
                    panic("%s data record exceeds transaction record at %s",
                          self._file.name, pos)

                if index.get(h.oid, 0) != h.prev:
                    if h.prev:
                        if recover:
                            return tpos, None, None
                    logger.error("%s incorrect previous pointer at %s: "
                                 "index says %r record says %r",
                                 self._file.name, pos, index.get(h.oid),
                                 h.prev)

                pos += dlen

            if pos != tend:
                if recover:
                    return tpos, None, None
                panic("%s data records don't add up at %s",
                      self._file.name, tpos)

            # Read the (intentionally redundant) transaction length
            self._file.seek(pos)
            l = u64(self._file.read(8))
            if l != tl:
                if recover:
                    return tpos, None, None
                panic("%s redundant transaction length check failed at %s",
                      self._file.name, pos)
            pos += 8

            if tindex: # avoid the pathological empty transaction case
                _maxoid = max(tindex.keys()) # in 2.2, just max(tindex)
                maxoid = max(_maxoid, maxoid)
                index.update(tindex)
                tindex.clear()

        return pos, maxoid, ltid

    _metadata_size = 1024
    _format_version = "43"

    def _read_metadata(self):
        # Read the 1K metadata block at the beginning of the storage.
        self._file.seek(0)
        fs = self._file.read(2)
        if fs != "FS":
            raise FileStorageFormatError(self._file.name)
        fsver = self._file.read(2)
        if fsver != self._format_version:
            raise FileStorageFormatError(self._file.name)
        ver = self._file.read(4)
        if ver != "\0" * 4:
            self._version = ver

    def _write_metadata(self):
        # Write the 1K metadata block at the beginning of the storage.
        self._file.seek(0)
        self._file.write("FS")
        self._file.write(self._format_version)
        # If self._version is not yet set, write all zeros.
        if self._version is not None:
            self._file.write(self._version)
        else:
            self._file.write("\0" * 4)
        # Fill the rest with null bytes
        self._file.write("\0" * (self._metadata_size - 8))

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
        h = DataHeader.fromString(s)
        if oid is not None and oid != h.oid:
            raise CorruptedDataError(oid, s, pos)
        if h.vlen:
            s = self._file.read(16 + h.vlen)
            h.parseVersion(s)
        if not h.plen:
            h.back = u64(self._file.read(8))
        return h

    def _write_version_header(self, file, pnv, vprev, version):
        s = struct.pack(">QQ", pnv, vprev)
        file.write(s + version)

    def _read_txn_header(self, pos, tid=None):
        self._file.seek(pos)
        s = self._file.read(TRANS_HDR_LEN)
        if len(s) != TRANS_HDR_LEN:
            raise CorruptedDataError(tid, s, pos)
        h = TxnHeader.fromString(s)
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
        # If fail is True, raise KeyError for zero backpointer.
        # If fail is False, return the empty data from the record
        # with no backpointer.
        while 1:
            if not back:
                # If backpointer is 0, object does not currently exist.
                raise POSKeyError(oid)
            h = self._read_data_header(back)
            refs = self._file.read(h.nrefs * 8)
            if h.plen:
                return self._file.read(h.plen), refs, h.serial, back, h.tloc
            if h.back == 0 and not fail:
                assert h.nrefs == 0
                return None, None, h.serial, back, h.tloc
            back = h.back

    def _loadBack(self, oid, back, fail=True):
        data, refs, serial, old, tloc = self._loadBack_impl(oid, back, fail)
        return data, serial

    def _loadBackPOS(self, oid, back, fail=True):
        """Return position of data record for backpointer."""
        data, refs, serial, old, tloc = self._loadBack_impl(oid, back, fail)
        return old

    def _loadBackTxn(self, oid, back, fail=True):
        """Return data, serial, and txn id for backpointer."""
        data, refs, serial, old, tloc = self._loadBack_impl(oid, back, fail)
        self._file.seek(tloc)
        h = self._file.read(TRANS_HDR_LEN)
        tid = h[:8]
        refs = splitrefs(refs)
        return data, refs, serial, tid

    def getTxnFromData(self, oid, back):
        """Return transaction id for data at back."""
        h = self._read_data_header(back, oid)
        self._file.seek(h.tloc)
        # seek to transaction header, where tid is first 8 bytes
        return self._file.read(8)

    def fail(self, pos, msg, *args):
        s = ("%s:%s:" + msg) % ((self._name, pos) + args)
        logger.error(s)
        raise CorruptedError(s)

    def checkTxn(self, th, pos):
        if th.tid <= self.ltid:
            self.fail(pos, "time-stamp reduction: %s <= %s",
                      _fmt_oid(th.tid), _fmt_oid(self.ltid))
        self.ltid = th.tid
        if th.status == "c":
            self.fail(pos, "transaction with checkpoint flag set")
        if not (th.status == " " or th.status == "p"):
            self.fail(pos, "invalid transaction status: %r", th.status)
        if th.tlen < th.headerlen():
            self.fail(pos, "invalid transaction header: "
                      "txnlen (%d) < headerlen(%d)", th.tlen, th.headerlen())

    def checkData(self, th, tpos, dh, pos):
        tend = tpos + th.tlen
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
            if dh.nrefs or dh.plen:
                self.fail(pos, "data record has back pointer and data")

class DataHeader:
    """Header for a data record."""

    __slots__ = (
        "oid", "serial", "prev", "tloc", "vlen", "plen", "nrefs", "back",
        # These three attributes are only defined when vlen > 0
        "pnv", "vprev", "version")

    version = ""
    back = 0

    def __init__(self, oid, serial, prev, tloc, vlen, nrefs, plen):
        self.oid = oid
        self.serial = serial
        self.prev = prev
        self.tloc = tloc

        self.vlen = vlen
        self.nrefs = nrefs
        self.plen = plen

    def fromString(cls, s):
        return cls(*struct.unpack(DATA_HDR, s))

    fromString = classmethod(fromString)

    def asString(self):
        s = struct.pack(DATA_HDR, self.oid, self.serial, self.prev,
                        self.tloc, self.vlen, self.nrefs, self.plen)
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
        self.pnv, self.vprev = struct.unpack(">QQ", buf[:16])
        self.version = buf[16:]

    def recordlen(self):
        rlen = DATA_HDR_LEN + (self.nrefs * 8) + (self.plen or 8)
        if self.version:
            rlen += 16 + self.vlen
        return rlen

class TxnHeader:
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

    def fromString(cls, s):
        return cls(*struct.unpack(TRANS_HDR, s))

    fromString = classmethod(fromString)

    def asString(self):
        s = struct.pack(TRANS_HDR, self.tid, self.tlen, self.status,
                        self.ulen, self.dlen, self.elen)
        return "".join([s, self.user, self.descr, self.ext])

    def headerlen(self):
        return TRANS_HDR_LEN + self.ulen + self.dlen + self.elen
