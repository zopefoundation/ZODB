##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
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

"""Tools for using FileStorage data files.

XXX This module needs tests.
XXX This file needs to be kept in sync with FileStorage.py.
"""

import cPickle
import struct

from ZODB.FileStorage import TRANS_HDR, DATA_HDR, TRANS_HDR_LEN, \
     DATA_HDR_LEN, DATA_VERSION_HDR_LEN
from ZODB.utils import p64, u64
from ZODB.TimeStamp import TimeStamp

class TxnHeader:
    """Object representing a transaction record header.

    Attribute   Position  Value
    ---------   --------  -----
    tid           0- 8    transaction id
    length        8-16    length of entire transaction record - 8
    status       16-17    status of transaction (' ', 'u', 'p'?)
    user_len     17-19    length of user field (pack code H)
    descr_len    19-21    length of description field (pack code H)
    ext_len      21-23    length of extensions (pack code H)
    """

    def __init__(self, file, pos):
        self._file = file
        self._pos = pos
        self._read_header()

    def _read_header(self):
        self._file.seek(self._pos)
        self._hdr = self._file.read(TRANS_HDR_LEN)
        (self.tid, length, self.status, self.user_len, self.descr_len,
         self.ext_len) = struct.unpack(TRANS_HDR, self._hdr)
        self.length = u64(length)

    def read_meta(self):
        """Load user, descr, and ext attributes."""
        self.user = ""
        self.descr = ""
        self.ext = {}
        if not (self.user_len or self.descr_len or self.ext_len):
            return
        self._file.seek(self._pos + TRANS_HDR_LEN)
        if self.user_len:
            self.user = self._file.read(self.user_len)
        if self.descr_len:
            self.descr = self._file.read(self.descr_len)
        if self.ext_len:
            self._ext = self._file.read(self.ext_len)
            self.ext = cPickle.loads(self._ext)

    def get_data_offset(self):
        return (self._pos + TRANS_HDR_LEN + self.user_len + self.descr_len
                + self.ext_len)

    def get_timestamp(self):
        return TimeStamp(self.tid)

    def get_raw_data(self):
        data_off = self.get_data_offset()
        data_len = self.length - (data_off - self._pos)
        self._file.seek(data_off)
        return self._file.read(data_len)

    def next_txn(self):
        off = self._pos + self.length + 8
        self._file.seek(off)
        s = self._file.read(8)
        if not s:
            return None
        return TxnHeader(self._file, off)

    def prev_txn(self):
        if self._pos == 4:
            return None
        self._file.seek(self._pos - 8)
        tlen = u64(self._file.read(8))
        return TxnHeader(self._file, self._pos - (tlen + 8))

class DataHeader:
    """Object representing a data record header.

    Attribute         Position  Value
    ---------         --------  -----
    oid                 0- 8    object id
    serial              8-16    object serial numver
    prev_rec_pos       16-24    position of previous data record for object
    txn_pos            24-32    position of txn header
    version_len        32-34    length of version
    data_len           34-42    length of data
    nonversion_pos     42-50*   position of nonversion data record
    prev_version_pos   50-58*   pos of previous version data record

    * these attributes are only present if version_len != 0.
    """

    def __init__(self, file, pos):
        self._file = file
        self._pos = pos
        self._read_header()

    def _read_header(self):
        self._file.seek(self._pos)
        self._hdr = self._file.read(DATA_VERSION_HDR_LEN)
        # always read the longer header, just in case
        (self.oid, self.serial, prev_rec_pos, txn_pos, self.version_len,
         data_len) = struct.unpack(DATA_HDR, self._hdr[:DATA_HDR_LEN])
        self.prev_rec_pos = u64(prev_rec_pos)
        self.txn_pos = u64(txn_pos)
        self.data_len = u64(data_len)
        if self.version_len:
            s = self._hdr[DATA_HDR_LEN:]
            self.nonversion_pos = u64(s[:8])
            self.prev_version_pos = u64(s[8:])
        else:
            self.nonversion_pos = None
            self.prev_version_pos = None

    def next_offset(self):
        """Return offset of next record."""
        off = self._pos + self.data_len
        if self.version_len:
            off += self.version_len + DATA_VERSION_HDR_LEN
        else:
            off += DATA_HDR_LEN
        if self.data_len == 0:
            off += 8 # backpointer
        return off

def prev_txn(f):
    """Return transaction located before current file position."""
    f.seek(-8, 1)
    tlen = u64(f.read(8)) + 8
    return TxnHeader(f, f.tell() - tlen)
