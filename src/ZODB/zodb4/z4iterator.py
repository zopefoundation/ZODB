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
"""Iterator support for ZODB 4 databases."""

from cPickle import loads
from struct import unpack

from ZODB.zodb4.z4interfaces import ZERO
from ZODB.zodb4.z4utils import u64, splitrefs
from ZODB.zodb4.z4format import FileStorageFormatter
from ZODB.zodb4.z4format import TRANS_HDR, TRANS_HDR_LEN, DATA_HDR
from ZODB.zodb4.z4format import DATA_HDR_LEN, DATA_VERSION_HDR_LEN

# originally from zodb.storage.file.main

class FileIterator(FileStorageFormatter):
    """Iterate over the transactions in a FileStorage file."""
    _ltid = ZERO

##    implements(IStorageIterator)

    def __init__(self, file):
        # - removed start and stop arguments
        if isinstance(file, str):
            file = open(file, 'rb')
        self._file = file
        self._read_metadata()
        self._file.seek(0,2)
        self._file_size = self._file.tell()
        self._pos = self._metadata_size

    def close(self):
        file = self._file
        if file is not None:
            self._file = None
            file.close()

    def __iter__(self):
        if self._file is None:
            # A closed iterator.  XXX: Is IOError the best we can do?  For
            # now, mimic a read on a closed file.
            raise IOError("iterator is closed")
        file = self._file
        seek = file.seek
        read = file.read

        pos = self._pos
        while True:
            # Read the transaction record
            seek(pos)
            h = read(TRANS_HDR_LEN)
            if len(h) < TRANS_HDR_LEN:
                break

            tid, tl, status, ul, dl, el = unpack(TRANS_HDR,h)
            if el < 0:
                el = (1L<<32) - el

            if tid <= self._ltid:
                warn("%s time-stamp reduction at %s", self._file.name, pos)
            self._ltid = tid

            if pos+(tl+8) > self._file_size or status=='c':
                # Hm, the data were truncated or the checkpoint flag wasn't
                # cleared.  They may also be corrupted,
                # in which case, we don't want to totally lose the data.
                warn("%s truncated, possibly due to damaged records at %s",
                     self._file.name, pos)
                break

            if status not in ' p':
                warn('%s has invalid status, %s, at %s', self._file.name,
                     status, pos)

            if tl < (TRANS_HDR_LEN+ul+dl+el):
                # We're in trouble. Find out if this is bad data in
                # the middle of the file, or just a turd that Win 9x
                # dropped at the end when the system crashed.  Skip to
                # the end and read what should be the transaction
                # length of the last transaction.
                seek(-8, 2)
                rtl = u64(read(8))
                # Now check to see if the redundant transaction length is
                # reasonable:
                if self._file_size - rtl < pos or rtl < TRANS_HDR_LEN:
                    logger.critical('%s has invalid transaction header at %s',
                                    self._file.name, pos)
                    warn("It appears that there is invalid data at the end of "
                         "the file, possibly due to a system crash.  %s "
                         "truncated to recover from bad data at end.",
                         self._file.name)
                    break
                else:
                    warn('%s has invalid transaction header at %s',
                         self._file.name, pos)
                    break

            tpos = pos
            tend = tpos+tl

            pos = tpos+(TRANS_HDR_LEN+ul+dl+el)
            # user and description are utf-8 encoded strings
            user = read(ul).decode('utf-8')
            description = read(dl).decode('utf-8')
            e = {}
            if el:
                try:
                    e = loads(read(el))
                # XXX can we do better?
                except:
                    pass

            result = RecordIterator(tid, status, user, description, e, pos,
                                    tend, file, tpos)
            pos = tend

            # Read the (intentionally redundant) transaction length
            seek(pos)
            l = u64(read(8))
            if l != tl:
                warn("%s redundant transaction length check failed at %s",
                     self._file.name, pos)
                break
            pos += 8
            yield result


class RecordIterator(FileStorageFormatter):
    """Iterate over data records for a transaction in a FileStorage."""

##    implements(ITransactionRecordIterator, ITransactionAttrs)

    def __init__(self, tid, status, user, desc, ext, pos, tend, file, tpos):
        self.tid = tid
        self.status = status
        self.user = user
        self.description = desc
        self._extension = ext
        self._pos = pos
        self._tend = tend
        self._file = file
        self._tpos = tpos

    def __iter__(self):
        pos = self._pos
        while pos < self._tend:
            # Read the data records for this transaction
            h = self._read_data_header(pos)
            dlen = h.recordlen()
            if pos + dlen > self._tend or h.tloc != self._tpos:
                warn("%s data record exceeds transaction record at %s",
                     file.name, pos)
                return

            pos += dlen
            prev_txn = None

            if h.plen:
                refsdata = self._file.read(h.nrefs * 8)
                refs = splitrefs(refsdata)
                data = self._file.read(h.plen)
            else:
                if not h.back:
                    # If the backpointer is 0, then this transaction
                    # undoes the object creation.  It either aborts
                    # the version that created the object or undid the
                    # transaction that created it.  Return None
                    # for data and refs because the backpointer has
                    # the real data and refs.
                    data = None
                    refs = None
                else:
                    data, refs, _s, tid = self._loadBackTxn(h.oid, h.back)
                    prev_txn = self.getTxnFromData(h.oid, h.back)

            yield Record(h.oid, h.serial, h.version, data, prev_txn, refs)

class Record:
    """An abstract database record."""

##    implements(IDataRecord)

    def __init__(self, oid, serial, version, data, data_txn, refs):
        self.oid = oid
        self.serial = serial
        self.version = version
        self.data = data
        self.data_txn = data_txn
        self.refs = refs
