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
"""Storage implementation using a log written to a single file.

$Revision: 1.6 $
"""

import base64
from cPickle import Pickler, Unpickler, loads
import errno
import os
import struct
import sys
import time
from types import StringType, DictType
from struct import pack, unpack

# Not all platforms have fsync
fsync = getattr(os, "fsync", None)

from ZODB import BaseStorage, ConflictResolution, POSException
from ZODB.POSException \
     import UndoError, POSKeyError, MultipleUndoErrors, VersionLockError
from persistent.TimeStamp import TimeStamp
from ZODB.lock_file import LockFile
from ZODB.utils import p64, u64, cp, z64
from ZODB.FileStorage.fspack import FileStoragePacker
from ZODB.FileStorage.format \
     import FileStorageFormatter, DataHeader, TxnHeader, DATA_HDR, \
     DATA_HDR_LEN, TRANS_HDR, TRANS_HDR_LEN, CorruptedDataError, \
     DATA_VERSION_HDR_LEN

try:
    from ZODB.fsIndex import fsIndex
except ImportError:
    def fsIndex():
        return {}

from zLOG import LOG, BLATHER, WARNING, ERROR, PANIC

t32 = 1L << 32

packed_version = "FS21"

def blather(message, *data):
    LOG('ZODB FS', BLATHER, "%s blather: %s\n" % (packed_version,
                                                  message % data))

def warn(message, *data):
    LOG('ZODB FS', WARNING, "%s  warn: %s\n" % (packed_version,
                                                message % data))

def error(message, *data, **kwargs):
    LOG('ZODB FS', ERROR, "%s ERROR: %s\n" % (packed_version,
                                              message % data), **kwargs)

def nearPanic(message, *data):
    LOG('ZODB FS', PANIC, "%s ERROR: %s\n" % (packed_version,
                                              message % data))

def panic(message, *data):
    message = message % data
    LOG('ZODB FS', PANIC, "%s ERROR: %s\n" % (packed_version, message))
    raise CorruptedTransactionError(message)

class FileStorageError(POSException.StorageError):
    pass

class PackError(FileStorageError):
    pass

class FileStorageFormatError(FileStorageError):
    """Invalid file format

    The format of the given file is not valid.
    """

class CorruptedFileStorageError(FileStorageError,
                                POSException.StorageSystemError):
    """Corrupted file storage."""

class CorruptedTransactionError(CorruptedFileStorageError):
    pass

class FileStorageQuotaError(FileStorageError,
                            POSException.StorageSystemError):
    """File storage quota exceeded."""

class TempFormatter(FileStorageFormatter):
    """Helper class used to read formatted FileStorage data."""

    def __init__(self, afile):
        self._file = afile

class FileStorage(BaseStorage.BaseStorage,
                  ConflictResolution.ConflictResolvingStorage,
                  FileStorageFormatter):

    # default pack time is 0
    _packt = z64

    _records_before_save = 10000

    def __init__(self, file_name, create=False, read_only=False, stop=None,
                 quota=None):

        if read_only:
            self._is_read_only = True
            if create:
                raise ValueError("can't create a read-only file")
        elif stop is not None:
            raise ValueError("time-travel only supported in read-only mode")

        if stop is None:
            stop='\377'*8

        # Lock the database and set up the temp file.
        if not read_only:
            # Create the lock file
            self._lock_file = LockFile(file_name + '.lock')
            self._tfile = open(file_name + '.tmp', 'w+b')
            self._tfmt = TempFormatter(self._tfile)
        else:
            self._tfile = None

        self._file_name = file_name

        BaseStorage.BaseStorage.__init__(self, file_name)

        (index, vindex, tindex, tvindex,
         oid2tid, toid2tid, toid2tid_delete) = self._newIndexes()
        self._initIndex(index, vindex, tindex, tvindex,
                        oid2tid, toid2tid, toid2tid_delete)

        # Now open the file

        self._file = None
        if not create:
            try:
                self._file = open(file_name, read_only and 'rb' or 'r+b')
            except IOError, exc:
                if exc.errno == errno.EFBIG:
                    # The file is too big to open.  Fail visibly.
                    raise
                if exc.errno == errno.ENOENT:
                    # The file doesn't exist.  Create it.
                    create = 1
                # If something else went wrong, it's hard to guess
                # what the problem was.  If the file does not exist,
                # create it.  Otherwise, fail.
                if os.path.exists(file_name):
                    raise
                else:
                    create = 1

        if self._file is None and create:
            if os.path.exists(file_name):
                os.remove(file_name)
            self._file = open(file_name, 'w+b')
            self._file.write(packed_version)

        r = self._restore_index()
        if r is not None:
            self._used_index = 1 # Marker for testing
            index, vindex, start, maxoid, ltid = r

            self._initIndex(index, vindex, tindex, tvindex,
                            oid2tid, toid2tid, toid2tid_delete)
            self._pos, self._oid, tid = read_index(
                self._file, file_name, index, vindex, tindex, stop,
                ltid=ltid, start=start, maxoid=maxoid,
                read_only=read_only,
                )
        else:
            self._used_index = 0 # Marker for testing
            self._pos, self._oid, tid = read_index(
                self._file, file_name, index, vindex, tindex, stop,
                read_only=read_only,
                )
            self._save_index()

        self._records_before_save = max(self._records_before_save,
                                        len(self._index))
        self._ltid = tid

        # self._pos should always point just past the last
        # transaction.  During 2PC, data is written after _pos.
        # invariant is restored at tpc_abort() or tpc_finish().

        self._ts = tid = TimeStamp(tid)
        t = time.time()
        t = TimeStamp(*time.gmtime(t)[:5] + (t % 60,))
        if tid > t:
            warn("%s Database records in the future", file_name);
            if tid.timeTime() - t.timeTime() > 86400*30:
                # a month in the future? This is bogus, use current time
                self._ts = t

        self._quota = quota

        # tid cache statistics.
        self._oid2tid_nlookups = self._oid2tid_nhits = 0

    def _initIndex(self, index, vindex, tindex, tvindex,
                   oid2tid, toid2tid, toid2tid_delete):
        self._index=index
        self._vindex=vindex
        self._tindex=tindex
        self._tvindex=tvindex
        self._index_get=index.get
        self._vindex_get=vindex.get

        # .store() needs to compare the passed-in serial to the
        # current tid in the database.  _oid2tid caches the oid ->
        # current tid mapping for non-version data (if the current
        # record for oid is version data, the oid is not a key in
        # _oid2tid).  The point is that otherwise seeking into the
        # storage is needed to extract the current tid, and that's
        # an expensive operation.  For example, if a transaction
        # stores 4000 objects, and each random seek + read takes 7ms
        # (that was approximately true on Linux and Windows tests in
        # mid-2003), that's 28 seconds just to find the old tids.
        # XXX Probably better to junk this and redefine _index as mapping
        # XXX oid to (offset, tid) pair, via a new memory-efficient
        # XXX BTree type.
        self._oid2tid = oid2tid
        # oid->tid map to transactionally add to _oid2tid.
        self._toid2tid = toid2tid
        # Set of oids to transactionally delete from _oid2tid (e.g.,
        # oids reverted by undo, or for which the most recent record
        # becomes version data).
        self._toid2tid_delete = toid2tid_delete

    def __len__(self):
        return len(self._index)

    def _newIndexes(self):
        # hook to use something other than builtin dict
        return fsIndex(), {}, {}, {}, {}, {}, {}

    _saved = 0
    def _save_index(self):
        """Write the database index to a file to support quick startup."""

        index_name = self.__name__ + '.index'
        tmp_name = index_name + '.index_tmp'

        f=open(tmp_name,'wb')
        p=Pickler(f,1)

        info={'index': self._index, 'pos': self._pos,
              'oid': self._oid, 'vindex': self._vindex}

        p.dump(info)
        f.flush()
        f.close()

        try:
            try:
                os.remove(index_name)
            except OSError:
                pass
            os.rename(tmp_name, index_name)
        except: pass

        self._saved += 1

    def _clear_index(self):
        index_name = self.__name__ + '.index'
        if os.path.exists(index_name):
            try:
                os.remove(index_name)
            except OSError:
                pass

    def _sane(self, index, pos):
        """Sanity check saved index data by reading the last undone trans

        Basically, we read the last not undone transaction and
        check to see that the included records are consistent
        with the index.  Any invalid record records or inconsistent
        object positions cause zero to be returned.
        """
        r = self._check_sanity(index, pos)
        if not r:
            warn("Ignoring index for %s", self._file_name)
        return r

    def _check_sanity(self, index, pos):

        if pos < 100:
            return 0 # insane
        self._file.seek(0, 2)
        if self._file.tell() < pos:
            return 0 # insane
        ltid = None

        max_checked = 5
        checked = 0

        while checked < max_checked:
            self._file.seek(pos - 8)
            rstl = self._file.read(8)
            tl = u64(rstl)
            pos = pos - tl - 8
            if pos < 4:
                return 0 # insane
            h = self._read_txn_header(pos)
            if not ltid:
                ltid = h.tid
            if h.tlen != tl:
                return 0 # inconsistent lengths
            if h.status == 'u':
                continue # undone trans, search back
            if h.status not in ' p':
                return 0 # insane
            if tl < h.headerlen():
                return 0 # insane
            tend = pos + tl
            opos = pos + h.headerlen()
            if opos == tend:
                continue # empty trans

            while opos < tend and checked < max_checked:
                # Read the data records for this transaction
                h = self._read_data_header(opos)

                if opos + h.recordlen() > tend or h.tloc != pos:
                    return 0

                if index.get(h.oid, 0) != opos:
                    return 0 # insane

                checked += 1

                opos = opos + h.recordlen()

            return ltid

    def _restore_index(self):
        """Load database index to support quick startup."""
        file_name=self.__name__
        index_name=file_name+'.index'

        try: f=open(index_name,'rb')
        except: return None

        p=Unpickler(f)

        try:
            info=p.load()
        except:
            exc, err = sys.exc_info()[:2]
            warn("Failed to load database index: %s: %s" %
                 (exc, err))
            return None
        index = info.get('index')
        pos = info.get('pos')
        oid = info.get('oid')
        vindex = info.get('vindex')
        if index is None or pos is None or oid is None or vindex is None:
            return None
        pos = long(pos)

        if isinstance(index, DictType) and not self._is_read_only:
            # Convert to fsIndex
            newindex = fsIndex()
            if type(newindex) is not type(index):
                # And we have fsIndex
                newindex.update(index)

                # Now save the index
                f = open(index_name, 'wb')
                p = Pickler(f, 1)
                info['index'] = newindex
                p.dump(info)
                f.close()

                # Now call this method again to get the new data
                return self._restore_index()

        tid = self._sane(index, pos)
        if not tid:
            return None

        return index, vindex, pos, oid, tid

    def close(self):
        self._file.close()
        if hasattr(self,'_lock_file'):
            self._lock_file.close()
        if self._tfile:
            self._tfile.close()
        try:
            self._save_index()
        except:
            # Log the error and continue
            LOG("ZODB FS", ERROR, "Error saving index on close()",
                error=sys.exc_info())

    # Return tid of most recent record for oid if that's in the
    # _oid2tid cache.  Else return None.  It's important to use this
    # instead of indexing _oid2tid directly so that cache statistics
    # can be logged.
    def _get_cached_tid(self, oid):
        self._oid2tid_nlookups += 1
        result = self._oid2tid.get(oid)
        if result is not None:
            self._oid2tid_nhits += 1

        # Log a msg every ~8000 tries, and prevent overflow.
        if self._oid2tid_nlookups & 0x1fff == 0:
            if self._oid2tid_nlookups >> 30:
                # In older Pythons, we may overflow if we keep it an int.
                self._oid2tid_nlookups = long(self._oid2tid_nlookups)
                self._oid2tid_nhits = long(self._oid2tid_nhits)
            blather("_oid2tid size %s lookups %s hits %s rate %.1f%%",
                    len(self._oid2tid),
                    self._oid2tid_nlookups,
                    self._oid2tid_nhits,
                    100.0 * self._oid2tid_nhits /
                            self._oid2tid_nlookups)

        return result

    def abortVersion(self, src, transaction):
        return self.commitVersion(src, '', transaction, abort=True)

    def commitVersion(self, src, dest, transaction, abort=False):
        # We are going to commit by simply storing back pointers.
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        if not (src and isinstance(src, StringType)
                and isinstance(dest, StringType)):
            raise POSException.VersionCommitError('Invalid source version')

        if src == dest:
            raise POSException.VersionCommitError(
                "Can't commit to same version: %s" % repr(src))

        if dest and abort:
            raise POSException.VersionCommitError(
                "Internal error, can't abort to a version")

        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)

        self._lock_acquire()
        try:
            return self._commitVersion(src, dest, transaction, abort)
        finally:
            self._lock_release()

    def _commitVersion(self, src, dest, transaction, abort=False):
        # call after checking arguments and acquiring lock
        srcpos = self._vindex_get(src, 0)
        spos = p64(srcpos)
        # middle holds bytes 16:34 of a data record:
        #    pos of transaction, len of version name, data length
        #    commit version never writes data, so data length is always 0
        middle = struct.pack(">8sH8s", p64(self._pos), len(dest), z64)

        if dest:
            sd = p64(self._vindex_get(dest, 0))
            heredelta = 66 + len(dest)
        else:
            sd = ''
            heredelta = 50

        here = self._pos + (self._tfile.tell() + self._thl)
        oids = []
        current_oids = {}

        while srcpos:
            h = self._read_data_header(srcpos)

            if self._index.get(h.oid) == srcpos:
                # This is a current record!
                self._tindex[h.oid] = here
                oids.append(h.oid)
                self._tfile.write(h.oid + self._tid + spos + middle)
                if dest:
                    self._tvindex[dest] = here
                    self._tfile.write(p64(h.pnv) + sd + dest)
                    sd = p64(here)

                self._tfile.write(abort and p64(h.pnv) or spos)
                # data backpointer to src data
                here += heredelta

                current_oids[h.oid] = 1
            else:
                # Hm.  This is a non-current record.  Is there a
                # current record for this oid?
                if not current_oids.has_key(h.oid):
                    break

            srcpos = h.vprev
            spos = p64(srcpos)
        self._toid2tid_delete.update(current_oids)
        return self._tid, oids

    def getSize(self):
        return self._pos

    def _lookup_pos(self, oid):
        try:
            return self._index[oid]
        except KeyError:
            raise POSKeyError(oid)
        except TypeError:
            raise TypeError("invalid oid %r" % (oid,))

    def loadEx(self, oid, version):
        # A variant of load() that also returns a transaction id.
        # ZEO wants this for managing its cache.
        self._lock_acquire()
        try:
            pos = self._lookup_pos(oid)
            h = self._read_data_header(pos, oid)
            if h.version and h.version != version:
                # Return data and tid from pnv (non-version data).

                # If we return the old record's transaction id, then
                # it will look to the cache like old data is current.
                # The tid for the current data must always be greater
                # than any non-current data.
                data = self._loadBack_impl(oid, h.pnv)[0]
                return data, h.tid, ""
            if h.plen:
                data = self._file.read(h.plen)
                return data, h.tid, h.version
            else:
                # Get the data from the backpointer, but tid from
                # currnt txn.
                data, _, _, _ = self._loadBack_impl(oid, h.back)
                th = self._read_txn_header(h.tloc)
                return data, h.tid, h.version
        finally:
            self._lock_release()

    def load(self, oid, version):
        self._lock_acquire()
        try:
            pos = self._lookup_pos(oid)
            h = self._read_data_header(pos, oid)
            if h.version and h.version != version:
                data = self._loadBack_impl(oid, h.pnv)[0]
                return data, h.tid
            if h.plen:
                return self._file.read(h.plen), h.tid
            else:
                data = self._loadBack_impl(oid, h.back)[0]
                return data, h.tid
        finally:
            self._lock_release()

    def loadSerial(self, oid, serial):
        # loadSerial must always return non-version data, because it
        # is used by conflict resolution.
        self._lock_acquire()
        try:
            pos = self._lookup_pos(oid)
            while 1:
                h = self._read_data_header(pos, oid)
                if h.tid == serial:
                    break
                pos = h.prev
                if not pos:
                    raise POSKeyError(oid)
            if h.version:
                return self._loadBack_impl(oid, h.pnv)[0]
            if h.plen:
                return self._file.read(h.plen)
            else:
                return self._loadBack_impl(oid, h.back)[0]
        finally:
            self._lock_release()

    def loadBefore(self, oid, tid):
        self._lock_acquire()
        try:
            pos = self._lookup_pos(oid)
            end_tid = None
            while True:
                h = self._read_data_header(pos, oid)
                if h.version:
                    # Just follow the pnv pointer to the previous
                    # non-version data.
                    if not h.pnv:
                        # Object was created in version.  There is no
                        # before data to find.
                        return None
                    pos = h.pnv
                    # The end_tid for the non-version data is not affected
                    # by versioned data records.
                    continue

                if h.tid < tid:
                    break

                pos = h.prev
                end_tid = h.tid
                if not pos:
                    return None

            if h.back:
                data, _, _, _ = self._loadBack_impl(oid, h.back)
                return data, h.tid, end_tid
            else:
                return self._file.read(h.plen), h.tid, end_tid

        finally:
            self._lock_release()

    def modifiedInVersion(self, oid):
        self._lock_acquire()
        try:
            pos = self._lookup_pos(oid)
            h = self._read_data_header(pos, oid)
            return h.version
        finally:
            self._lock_release()

    def store(self, oid, serial, data, version, transaction):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)

        self._lock_acquire()
        try:
            old = self._index_get(oid, 0)
            cached_tid = None
            pnv = None
            if old:
                cached_tid = self._get_cached_tid(oid)
                if cached_tid is None:
                    h = self._read_data_header(old, oid)
                    if h.version:
                        if h.version != version:
                            raise VersionLockError(oid, h.version)
                        pnv = h.pnv
                    cached_tid = h.tid

                if serial != cached_tid:
                    rdata = self.tryToResolveConflict(oid, cached_tid,
                                                     serial, data)
                    if rdata is None:
                        raise POSException.ConflictError(
                            oid=oid, serials=(cached_tid, serial), data=data)
                    else:
                        data = rdata

            pos = self._pos
            here = pos + self._tfile.tell() + self._thl
            self._tindex[oid] = here
            new = DataHeader(oid, self._tid, old, pos, len(version),
                             len(data))

            if version:
                # Link to last record for this version:
                pv = (self._tvindex.get(version, 0)
                      or self._vindex.get(version, 0))
                if pnv is None:
                    pnv = old
                new.setVersion(version, pnv, pv)
                self._tvindex[version] = here
                self._toid2tid_delete[oid] = 1
            else:
                self._toid2tid[oid] = self._tid

            self._tfile.write(new.asString())
            self._tfile.write(data)

            # Check quota
            if self._quota is not None and here > self._quota:
                raise FileStorageQuotaError(
                    "The storage quota has been exceeded.")

            if old and serial != cached_tid:
                return ConflictResolution.ResolvedSerial
            else:
                return self._tid

        finally:
            self._lock_release()

    def _data_find(self, tpos, oid, data):
        # Return backpointer to oid in data record for in transaction at tpos.
        # It should contain a pickle identical to data. Returns 0 on failure.
        # Must call with lock held.
        self._file.seek(tpos)
        h = self._file.read(TRANS_HDR_LEN)
        tid, tl, status, ul, dl, el = struct.unpack(TRANS_HDR, h)
        self._file.read(ul + dl + el)
        tend = tpos + tl + 8
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
            self._file.seek(pos)
        return 0

    def restore(self, oid, serial, data, version, prev_txn, transaction):
        # A lot like store() but without all the consistency checks.  This
        # should only be used when we /know/ the data is good, hence the
        # method name.  While the signature looks like store() there are some
        # differences:
        #
        # - serial is the serial number of /this/ revision, not of the
        #   previous revision.  It is used instead of self._tid, which is
        #   ignored.
        #
        # - Nothing is returned
        #
        # - data can be None, which indicates a George Bailey object
        #   (i.e. one who's creation has been transactionally undone).
        #
        # prev_txn is a backpointer.  In the original database, it's possible
        # that the data was actually living in a previous transaction.  This
        # can happen for transactional undo and other operations, and is used
        # as a space saving optimization.  Under some circumstances the
        # prev_txn may not actually exist in the target database (i.e. self)
        # for example, if it's been packed away.  In that case, the prev_txn
        # should be considered just a hint, and is ignored if the transaction
        # doesn't exist.
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)

        self._lock_acquire()
        try:
            prev_pos = 0
            if prev_txn is not None:
                prev_txn_pos = self._txn_find(prev_txn, 0)
                if prev_txn_pos:
                    prev_pos = self._data_find(prev_txn_pos, oid, data)
            old = self._index_get(oid, 0)
            # Calculate the file position in the temporary file
            here = self._pos + self._tfile.tell() + self._thl
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
            new = DataHeader(oid, serial, old, self._pos, len(version), dlen)
            if version:
                pnv = self._restore_pnv(oid, old, version, prev_pos) or old
                vprev = self._tvindex.get(version, 0)
                if not vprev:
                    vprev = self._vindex.get(version, 0)
                new.setVersion(version, pnv, vprev)
                self._tvindex[version] = here
                self._toid2tid_delete[oid] = 1
            else:
                self._toid2tid[oid] = serial

            self._tfile.write(new.asString())

            # Finally, write the data or a backpointer.
            if data is None:
                if prev_pos:
                    self._tfile.write(p64(prev_pos))
                else:
                    # Write a zero backpointer, which indicates an
                    # un-creation transaction.
                    self._tfile.write(z64)
            else:
                self._tfile.write(data)
        finally:
            self._lock_release()

    def _restore_pnv(self, oid, prev, version, bp):
        # Find a valid pnv (previous non-version) pointer for this version.

        # If there is no previous record, there can't be a pnv.
        if not prev:
            return None

        # Load the record pointed to be prev
        h = self._read_data_header(prev, oid)
        if h.version:
            return h.pnv
        if h.back:
            # XXX Not sure the following is always true:
            # The previous record is not for this version, yet we
            # have a backpointer to it.  The current record must
            # be an undo of an abort or commit, so the backpointer
            # must be to a version record with a pnv.
            h2 = self._read_data_header(h.back, oid)
            if h2.version:
                return h2.pnv

        return None

    def supportsUndo(self):
        return 1

    def supportsVersions(self):
        return 1

    def _clear_temp(self):
        self._tindex.clear()
        self._tvindex.clear()
        self._toid2tid.clear()
        self._toid2tid_delete.clear()
        if self._tfile is not None:
            self._tfile.seek(0)

    def _begin(self, tid, u, d, e):
        self._nextpos = 0
        self._thl = TRANS_HDR_LEN + len(u) + len(d) + len(e)
        if self._thl > 65535:
            # one of u, d, or e may be > 65535
            # We have to check lengths here because struct.pack
            # doesn't raise an exception on overflow!
            if len(u) > 65535:
                raise FileStorageError('user name too long')
            if len(d) > 65535:
                raise FileStorageError('description too long')
            if len(e) > 65535:
                raise FileStorageError('too much extension data')


    def tpc_vote(self, transaction):
        self._lock_acquire()
        try:
            if transaction is not self._transaction:
                return
            dlen = self._tfile.tell()
            if not dlen:
                return # No data in this trans
            self._tfile.seek(0)
            user, descr, ext = self._ude

            self._file.seek(self._pos)
            tl = self._thl + dlen

            try:
                h = TxnHeader(self._tid, tl, "c", len(user),
                              len(descr), len(ext))
                h.user = user
                h.descr = descr
                h.ext = ext
                self._file.write(h.asString())
                cp(self._tfile, self._file, dlen)
                self._file.write(p64(tl))
                self._file.flush()
            except:
                # Hm, an error occured writing out the data. Maybe the
                # disk is full. We don't want any turd at the end.
                self._file.truncate(self._pos)
                raise
            self._nextpos = self._pos + (tl + 8)
        finally:
            self._lock_release()

    # Keep track of the number of records that we've written
    _records_written = 0

    def _finish(self, tid, u, d, e):
        nextpos=self._nextpos
        if nextpos:
            file=self._file

            # Clear the checkpoint flag
            file.seek(self._pos+16)
            file.write(self._tstatus)
            file.flush()

            if fsync is not None: fsync(file.fileno())

            self._pos = nextpos

            self._index.update(self._tindex)
            self._vindex.update(self._tvindex)
            self._oid2tid.update(self._toid2tid)
            for oid in self._toid2tid_delete.keys():
                try:
                    del self._oid2tid[oid]
                except KeyError:
                    pass

            # Update the number of records that we've written
            # +1 for the transaction record
            self._records_written += len(self._tindex) + 1
            if self._records_written >= self._records_before_save:
                self._save_index()
                self._records_written = 0
                self._records_before_save = max(self._records_before_save,
                                                len(self._index))

        self._ltid = tid

    def _abort(self):
        if self._nextpos:
            self._file.truncate(self._pos)
            self._nextpos=0

    def supportsTransactionalUndo(self):
        return 1

    def _undoDataInfo(self, oid, pos, tpos):
        """Return the tid, data pointer, data, and version for the oid
        record at pos"""
        if tpos:
            pos = tpos - self._pos - self._thl
            tpos = self._tfile.tell()
            h = self._tfmt._read_data_header(pos, oid)
            afile = self._tfile
        else:
            h = self._read_data_header(pos, oid)
            afile = self._file
        if h.oid != oid:
            raise UndoError("Invalid undo transaction id", oid)

        if h.plen:
            data = afile.read(h.plen)
        else:
            data = ''
            pos = h.back

        if tpos:
            self._tfile.seek(tpos) # Restore temp file to end

        return h.tid, pos, data, h.version

    def getTid(self, oid):
        self._lock_acquire()
        try:
            result = self._get_cached_tid(oid)
            if result is None:
                pos = self._lookup_pos(oid)
                result = self._getTid(oid, pos)
            return result
        finally:
            self._lock_release()

    def _getTid(self, oid, pos):
        self._file.seek(pos)
        h = self._file.read(16)
        assert oid == h[:8]
        return h[8:]

    def _getVersion(self, oid, pos):
        h = self._read_data_header(pos, oid)
        if h.version:
            return h.version, h.pnv
        else:
            return "", None

    def _transactionalUndoRecord(self, oid, pos, tid, pre, version):
        """Get the indo information for a data record

        Return a 5-tuple consisting of a pickle, data pointer,
        version, packed non-version data pointer, and current
        position.  If the pickle is true, then the data pointer must
        be 0, but the pickle can be empty *and* the pointer 0.
        """

        copy = 1 # Can we just copy a data pointer

        # First check if it is possible to undo this record.
        tpos = self._tindex.get(oid, 0)
        ipos = self._index.get(oid, 0)
        tipos = tpos or ipos

        if tipos != pos:
            # Eek, a later transaction modified the data, but,
            # maybe it is pointing at the same data we are.
            ctid, cdataptr, cdata, cver = self._undoDataInfo(oid, ipos, tpos)
            # Versions of undone record and current record *must* match!
            if cver != version:
                raise UndoError('Current and undone versions differ', oid)

            if cdataptr != pos:
                # We aren't sure if we are talking about the same data
                try:
                    if (
                        # The current record wrote a new pickle
                        cdataptr == tipos
                        or
                        # Backpointers are different
                        self._loadBackPOS(oid, pos) !=
                        self._loadBackPOS(oid, cdataptr)
                        ):
                        if pre and not tpos:
                            copy = 0 # we'll try to do conflict resolution
                        else:
                            # We bail if:
                            # - We don't have a previous record, which should
                            #   be impossible.
                            raise UndoError("no previous record", oid)
                except KeyError:
                    # LoadBack gave us a key error. Bail.
                    raise UndoError("_loadBack() failed", oid)

        # Return the data that should be written in the undo record.
        if not pre:
            # There is no previous revision, because the object creation
            # is being undone.
            return "", 0, "", "", ipos

        version, snv = self._getVersion(oid, pre)
        if copy:
            # we can just copy our previous-record pointer forward
            return "", pre, version, snv, ipos

        try:
            bdata = self._loadBack_impl(oid, pre)[0]
        except KeyError:
            # couldn't find oid; what's the real explanation for this?
            raise UndoError("_loadBack() failed for %s", oid)
        data = self.tryToResolveConflict(oid, ctid, tid, bdata, cdata)

        if data:
            return data, 0, version, snv, ipos

        raise UndoError("Some data were modified by a later transaction", oid)

    # undoLog() returns a description dict that includes an id entry.
    # The id is opaque to the client, but contains the transaction id.
    # The transactionalUndo() implementation does a simple linear
    # search through the file (from the end) to find the transaction.

    def undoLog(self, first=0, last=-20, filter=None):
        if last < 0:
            last = first - last + 1
        self._lock_acquire()
        try:
            if self._packt is None:
                raise UndoError(
                    'Undo is currently disabled for database maintenance.<p>')
            us = UndoSearch(self._file, self._pos, self._packt,
                            first, last, filter)
            while not us.finished():
                # Hold lock for batches of 20 searches, so default search
                # parameters will finish without letting another thread run.
                for i in range(20):
                    if us.finished():
                        break
                    us.search()
                # Give another thread a chance, so that a long undoLog()
                # operation doesn't block all other activity.
                self._lock_release()
                self._lock_acquire()
            return us.results
        finally:
            self._lock_release()

    def undo(self, transaction_id, transaction):
        """Undo a transaction, given by transaction_id.

        Do so by writing new data that reverses the action taken by
        the transaction.

        Usually, we can get by with just copying a data pointer, by
        writing a file position rather than a pickle. Sometimes, we
        may do conflict resolution, in which case we actually copy
        new data that results from resolution.
        """

        if self._is_read_only:
            raise POSException.ReadOnlyError()
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)

        self._lock_acquire()
        try:
            return self._txn_undo(transaction_id)
        finally:
            self._lock_release()

    def _txn_undo(self, transaction_id):
        # Find the right transaction to undo and call _txn_undo_write().
        tid = base64.decodestring(transaction_id + '\n')
        assert len(tid) == 8
        tpos = self._txn_find(tid, 1)
        tindex = self._txn_undo_write(tpos)
        self._tindex.update(tindex)
        # Arrange to clear the affected oids from the oid2tid cache.
        # It's too painful to try to update them to correct current
        # values instead.
        self._toid2tid_delete.update(tindex)
        return self._tid, tindex.keys()

    def _txn_find(self, tid, stop_at_pack):
        pos = self._pos
        while pos > 39:
            self._file.seek(pos - 8)
            pos = pos - u64(self._file.read(8)) - 8
            self._file.seek(pos)
            h = self._file.read(TRANS_HDR_LEN)
            _tid = h[:8]
            if _tid == tid:
                return pos
            if stop_at_pack:
                # check the status field of the transaction header
                if h[16] == 'p' or _tid < self._packt:
                    break
        raise UndoError("Invalid transaction id")

    def _txn_undo_write(self, tpos):
        # a helper function to write the data records for transactional undo

        otloc = self._pos
        here = self._pos + self._tfile.tell() + self._thl
        base = here - self._tfile.tell()
        # Let's move the file pointer back to the start of the txn record.
        th = self._read_txn_header(tpos)
        if th.status != " ":
            raise UndoError('non-undoable transaction')
        tend = tpos + th.tlen
        pos = tpos + th.headerlen()
        tindex = {}

        # keep track of failures, cause we may succeed later
        failures = {}
        # Read the data records for this transaction
        while pos < tend:
            h = self._read_data_header(pos)
            if h.oid in failures:
                del failures[h.oid] # second chance!

            assert base + self._tfile.tell() == here, (here, base,
                                                       self._tfile.tell())
            try:
                p, prev, v, snv, ipos = self._transactionalUndoRecord(
                    h.oid, pos, h.tid, h.prev, h.version)
            except UndoError, v:
                # Don't fail right away. We may be redeemed later!
                failures[h.oid] = v
            else:
                new = DataHeader(h.oid, self._tid, ipos, otloc, len(v),
                                 len(p))
                if v:
                    vprev = self._tvindex.get(v, 0) or self._vindex.get(v, 0)
                    new.setVersion(v, snv, vprev)
                    self._tvindex[v] = here

                # XXX This seek shouldn't be necessary, but some other
                # bit of code is messig with the file pointer.
                assert self._tfile.tell() == here - base, (here, base,
                                                           self._tfile.tell())
                self._tfile.write(new.asString())
                if p:
                    self._tfile.write(p)
                else:
                    self._tfile.write(p64(prev))
                tindex[h.oid] = here
                here += new.recordlen()

            pos += h.recordlen()
            if pos > tend:
                raise UndoError("non-undoable transaction")

        if failures:
            raise MultipleUndoErrors(failures.items())

        return tindex


    def versionEmpty(self, version):
        if not version:
            # The interface is silent on this case. I think that this should
            # be an error, but Barry thinks this should return 1 if we have
            # any non-version data. This would be excruciatingly painful to
            # test, so I must be right. ;)
            raise POSException.VersionError(
                'The version must be an non-empty string')
        self._lock_acquire()
        try:
            index=self._index
            file=self._file
            seek=file.seek
            read=file.read
            srcpos=self._vindex_get(version, 0)
            t=tstatus=None
            while srcpos:
                seek(srcpos)
                oid=read(8)
                if index[oid]==srcpos: return 0
                h=read(50) # serial, prev(oid), tloc, vlen, plen, pnv, pv
                tloc=h[16:24]
                if t != tloc:
                    # We haven't checked this transaction before,
                    # get its status.
                    t=tloc
                    seek(u64(t)+16)
                    tstatus=read(1)

                if tstatus != 'u': return 1

                spos=h[-8:]
                srcpos=u64(spos)

            return 1
        finally: self._lock_release()

    def versions(self, max=None):
        r=[]
        a=r.append
        keys=self._vindex.keys()
        if max is not None: keys=keys[:max]
        for version in keys:
            if self.versionEmpty(version): continue
            a(version)
            if max and len(r) >= max: return r

        return r

    def history(self, oid, version=None, size=1, filter=None):
        self._lock_acquire()
        try:
            r = []
            pos = self._lookup_pos(oid)
            wantver = version

            while 1:
                if len(r) >= size: return r
                h = self._read_data_header(pos)

                if h.version:
                    if wantver is not None and h.version != wantver:
                        if h.prev:
                            pos = h.prev
                            continue
                        else:
                            return r
                else:
                    version = ""
                    wantver = None

                th = self._read_txn_header(h.tloc)
                user_name = self._file.read(th.ulen)
                description = self._file.read(th.dlen)
                if th.elen:
                    d = loads(self._file.read(th.elen))
                else:
                    d = {}

                d.update({"time": TimeStamp(h.tid).timeTime(),
                          "user_name": user_name,
                          "description": description,
                          "tid": h.tid,
                          "version": h.version,
                          "size": h.plen,
                          })

                if filter is None or filter(d):
                    r.append(d)

                if h.prev:
                    pos = h.prev
                else:
                    return r
        finally:
            self._lock_release()

    def _redundant_pack(self, file, pos):
        assert pos > 8, pos
        file.seek(pos - 8)
        p = u64(file.read(8))
        file.seek(pos - p + 8)
        return file.read(1) not in ' u'

    def pack(self, t, referencesf):
        """Copy data from the current database file to a packed file

        Non-current records from transactions with time-stamp strings less
        than packtss are ommitted. As are all undone records.

        Also, data back pointers that point before packtss are resolved and
        the associated data are copied, since the old records are not copied.
        """
        if self._is_read_only:
            raise POSException.ReadOnlyError()

        stop=`TimeStamp(*time.gmtime(t)[:5]+(t%60,))`
        if stop==z64: raise FileStorageError, 'Invalid pack time'

        # If the storage is empty, there's nothing to do.
        if not self._index:
            return

        # Record pack time so we don't undo while packing
        self._lock_acquire()
        try:
            if self._packt != z64:
                # Already packing.
                raise FileStorageError, 'Already packing'
            self._packt = None
        finally:
            self._lock_release()

        p = FileStoragePacker(self._file_name, stop,
                              self._lock_acquire, self._lock_release,
                              self._commit_lock_acquire,
                              self._commit_lock_release)
        try:
            opos = p.pack()
            if opos is None:
                return
            oldpath = self._file_name + ".old"
            self._lock_acquire()
            try:
                self._file.close()
                try:
                    if os.path.exists(oldpath):
                        os.remove(oldpath)
                    os.rename(self._file_name, oldpath)
                except Exception:
                    self._file = open(self._file_name, 'r+b')
                    raise

                # OK, we're beyond the point of no return
                os.rename(self._file_name + '.pack', self._file_name)
                self._file = open(self._file_name, 'r+b')
                self._initIndex(p.index, p.vindex, p.tindex, p.tvindex,
                                p.oid2tid, p.toid2tid,
                                p.toid2tid_delete)
                self._pos = opos
                self._save_index()
            finally:
                self._lock_release()
        finally:
            if p.locked:
                self._commit_lock_release()
            self._lock_acquire()
            self._packt = z64
            self._lock_release()

    def iterator(self, start=None, stop=None):
        return FileIterator(self._file_name, start, stop)

    def lastTransaction(self):
        """Return transaction id for last committed transaction"""
        return self._ltid

    def lastTid(self, oid):
        """Return last serialno committed for object oid.

        If there is no serialno for this oid -- which can only occur
        if it is a new object -- return None.
        """
        try:
            return self.getTid(oid)
        except KeyError:
            return None

    def cleanup(self):
        """Remove all files created by this storage."""
        for ext in '', '.old', '.tmp', '.lock', '.index', '.pack':
            try:
                os.remove(self._file_name + ext)
            except OSError, e:
                if e.errno != errno.ENOENT:
                    raise


def shift_transactions_forward(index, vindex, tindex, file, pos, opos):
    """Copy transactions forward in the data file

    This might be done as part of a recovery effort
    """

    # Cache a bunch of methods
    seek=file.seek
    read=file.read
    write=file.write

    index_get=index.get
    vindex_get=vindex.get

    # Initialize,
    pv=z64
    p1=opos
    p2=pos
    offset=p2-p1

    # Copy the data in two stages.  In the packing stage,
    # we skip records that are non-current or that are for
    # unreferenced objects. We also skip undone transactions.
    #
    # After the packing stage, we copy everything but undone
    # transactions, however, we have to update various back pointers.
    # We have to have the storage lock in the second phase to keep
    # data from being changed while we're copying.
    pnv=None
    while 1:

        # Read the transaction record
        seek(pos)
        h=read(TRANS_HDR_LEN)
        if len(h) < TRANS_HDR_LEN: break
        tid, stl, status, ul, dl, el = unpack(TRANS_HDR,h)
        if status=='c': break # Oops. we found a checkpoint flag.
        tl=u64(stl)
        tpos=pos
        tend=tpos+tl

        otpos=opos # start pos of output trans

        thl=ul+dl+el
        h2=read(thl)
        if len(h2) != thl:
            raise PackError(opos)

        # write out the transaction record
        seek(opos)
        write(h)
        write(h2)

        thl=TRANS_HDR_LEN+thl
        pos=tpos+thl
        opos=otpos+thl

        while pos < tend:
            # Read the data records for this transaction
            seek(pos)
            h=read(DATA_HDR_LEN)
            oid,serial,sprev,stloc,vlen,splen = unpack(DATA_HDR, h)
            plen=u64(splen)
            dlen=DATA_HDR_LEN+(plen or 8)

            if vlen:
                dlen=dlen+(16+vlen)
                pnv=u64(read(8))
                # skip position of previous version record
                seek(8,1)
                version=read(vlen)
                pv=p64(vindex_get(version, 0))
                if status != 'u': vindex[version]=opos

            tindex[oid]=opos

            if plen: p=read(plen)
            else:
                p=read(8)
                p=u64(p)
                if p >= p2: p=p-offset
                elif p >= p1:
                    # Ick, we're in trouble. Let's bail
                    # to the index and hope for the best
                    p=index_get(oid, 0)
                p=p64(p)

            # WRITE
            seek(opos)
            sprev=p64(index_get(oid, 0))
            write(pack(DATA_HDR,
                       oid,serial,sprev,p64(otpos),vlen,splen))
            if vlen:
                if not pnv: write(z64)
                else:
                    if pnv >= p2: pnv=pnv-offset
                    elif pnv >= p1:
                        pnv=index_get(oid, 0)

                    write(p64(pnv))
                write(pv)
                write(version)

            write(p)

            opos=opos+dlen
            pos=pos+dlen

        # skip the (intentionally redundant) transaction length
        pos=pos+8

        if status != 'u':
            index.update(tindex) # Record the position

        tindex.clear()

        write(stl)
        opos=opos+8

    return opos

def search_back(file, pos):
    seek=file.seek
    read=file.read
    seek(0,2)
    s=p=file.tell()
    while p > pos:
        seek(p-8)
        l=u64(read(8))
        if l <= 0: break
        p=p-l-8

    return p, s

def recover(file_name):
    file=open(file_name, 'r+b')
    index={}
    vindex={}
    tindex={}

    pos, oid, tid = read_index(
        file, file_name, index, vindex, tindex, recover=1)
    if oid is not None:
        print "Nothing to recover"
        return

    opos=pos
    pos, sz = search_back(file, pos)
    if pos < sz:
        npos = shift_transactions_forward(
            index, vindex, tindex, file, pos, opos,
            )

    file.truncate(npos)

    print "Recovered file, lost %s, ended up with %s bytes" % (
        pos-opos, npos)



def read_index(file, name, index, vindex, tindex, stop='\377'*8,
               ltid=z64, start=4L, maxoid=z64, recover=0, read_only=0):
    """Scan the entire file storage and recreate the index.

    Returns file position, max oid, and last transaction id.  It also
    stores index information in the three dictionary arguments.

    Arguments:
    file -- a file object (the Data.fs)
    name -- the name of the file (presumably file.name)
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

    read = file.read
    seek = file.seek
    seek(0, 2)
    file_size=file.tell()
    fmt = TempFormatter(file)

    if file_size:
        if file_size < start: raise FileStorageFormatError, file.name
        seek(0)
        if read(4) != packed_version:
            raise FileStorageFormatError, name
    else:
        if not read_only:
            file.write(packed_version)
        return 4L, maxoid, ltid

    index_get=index.get

    pos=start
    seek(start)
    tid='\0'*7+'\1'

    while 1:
        # Read the transaction record
        h=read(TRANS_HDR_LEN)
        if not h: break
        if len(h) != TRANS_HDR_LEN:
            if not read_only:
                warn('%s truncated at %s', name, pos)
                seek(pos)
                file.truncate()
            break

        tid, tl, status, ul, dl, el = unpack(TRANS_HDR,h)
        if el < 0: el=t32-el

        if tid <= ltid:
            warn("%s time-stamp reduction at %s", name, pos)
        ltid = tid

        if pos+(tl+8) > file_size or status=='c':
            # Hm, the data were truncated or the checkpoint flag wasn't
            # cleared.  They may also be corrupted,
            # in which case, we don't want to totally lose the data.
            if not read_only:
                warn("%s truncated, possibly due to damaged records at %s",
                     name, pos)
                _truncate(file, name, pos)
            break

        if status not in ' up':
            warn('%s has invalid status, %s, at %s', name, status, pos)

        if tl < (TRANS_HDR_LEN+ul+dl+el):
            # We're in trouble. Find out if this is bad data in the
            # middle of the file, or just a turd that Win 9x dropped
            # at the end when the system crashed.
            # Skip to the end and read what should be the transaction length
            # of the last transaction.
            seek(-8, 2)
            rtl=u64(read(8))
            # Now check to see if the redundant transaction length is
            # reasonable:
            if file_size - rtl < pos or rtl < TRANS_HDR_LEN:
                nearPanic('%s has invalid transaction header at %s', name, pos)
                if not read_only:
                    warn("It appears that there is invalid data at the end of "
                         "the file, possibly due to a system crash.  %s "
                         "truncated to recover from bad data at end."
                         % name)
                    _truncate(file, name, pos)
                break
            else:
                if recover: return pos, None, None
                panic('%s has invalid transaction header at %s', name, pos)

        if tid >= stop:
            break

        tpos=pos
        tend=tpos+tl

        if status=='u':
            # Undone transaction, skip it
            seek(tend)
            h=u64(read(8))
            if h != tl:
                if recover: return tpos, None, None
                panic('%s has inconsistent transaction length at %s',
                      name, pos)
            pos=tend+8
            continue

        pos = tpos+ TRANS_HDR_LEN + ul + dl + el
        while pos < tend:
            # Read the data records for this transaction
            h = fmt._read_data_header(pos)
            dlen = h.recordlen()
            tindex[h.oid] = pos

            if h.version:
                vindex[h.version] = pos

            if pos + dlen > tend or h.tloc != tpos:
                if recover:
                    return tpos, None, None
                panic("%s data record exceeds transaction record at %s",
                      name, pos)

            if index_get(h.oid, 0) != h.prev:
                if prev:
                    if recover: return tpos, None, None
                    error("%s incorrect previous pointer at %s", name, pos)
                else:
                    warn("%s incorrect previous pointer at %s", name, pos)

            pos=pos+dlen

        if pos != tend:
            if recover: return tpos, None, None
            panic("%s data records don't add up at %s",name,tpos)

        # Read the (intentionally redundant) transaction length
        seek(pos)
        h = u64(read(8))
        if h != tl:
            if recover: return tpos, None, None
            panic("%s redundant transaction length check failed at %s",
                  name, pos)
        pos=pos+8

        if tindex: # avoid the pathological empty transaction case
            _maxoid = max(tindex.keys()) # in 2.2, just max(tindex)
            maxoid = max(_maxoid, maxoid)
            index.update(tindex)
            tindex.clear()

    return pos, maxoid, ltid


def _truncate(file, name, pos):
    file.seek(0, 2)
    file_size = file.tell()
    try:
        i = 0
        while 1:
            oname='%s.tr%s' % (name, i)
            if os.path.exists(oname):
                i += 1
            else:
                warn("Writing truncated data from %s to %s", name, oname)
                o = open(oname,'wb')
                file.seek(pos)
                cp(file, o, file_size-pos)
                o.close()
                break
    except:
        error("couldn\'t write truncated data for %s", name,
              error=sys.exc_info())
        raise POSException.StorageSystemError, (
            "Couldn't save truncated data")

    file.seek(pos)
    file.truncate()

class Iterator:
    """A General simple iterator that uses the Python for-loop index protocol
    """
    __index=-1
    __current=None

    def __getitem__(self, i):
        __index=self.__index
        while i > __index:
            __index=__index+1
            self.__current=self.next(__index)

        self.__index=__index
        return self.__current


class FileIterator(Iterator, FileStorageFormatter):
    """Iterate over the transactions in a FileStorage file.
    """
    _ltid = z64
    _file = None

    def __init__(self, file, start=None, stop=None):
        if isinstance(file, str):
            file = open(file, 'rb')
        self._file = file
        if file.read(4) != packed_version:
            raise FileStorageFormatError, file.name
        file.seek(0,2)
        self._file_size = file.tell()
        self._pos = 4L
        assert start is None or isinstance(start, str)
        assert stop is None or isinstance(stop, str)
        if start:
            self._skip_to_start(start)
        self._stop = stop

    def __len__(self):
        # Define a bogus __len__() to make the iterator work
        # with code like builtin list() and tuple() in Python 2.1.
        # There's a lot of C code that expects a sequence to have
        # an __len__() but can cope with any sort of mistake in its
        # implementation.  So just return 0.
        return 0

    # This allows us to pass an iterator as the `other' argument to
    # copyTransactionsFrom() in BaseStorage.  The advantage here is that we
    # can create the iterator manually, e.g. setting start and stop, and then
    # just let copyTransactionsFrom() do its thing.
    def iterator(self):
        return self

    def close(self):
        file = self._file
        if file is not None:
            self._file = None
            file.close()

    def _skip_to_start(self, start):
        # Scan through the transaction records doing almost no sanity
        # checks.
        while 1:
            self._file.seek(self._pos)
            h = self._file.read(16)
            if len(h) < 16:
                return
            tid, stl = unpack(">8s8s", h)
            if tid >= start:
                return
            tl = u64(stl)
            try:
                self._pos += tl + 8
            except OverflowError:
                self._pos = long(self._pos) + tl + 8
            if __debug__:
                # Sanity check
                self._file.seek(self._pos - 8, 0)
                rtl = self._file.read(8)
                if rtl != stl:
                    pos = self._file.tell() - 8
                    panic("%s has inconsistent transaction length at %s "
                          "(%s != %s)",
                          self._file.name, pos, u64(rtl), u64(stl))

    def next(self, index=0):
        if self._file is None:
            # A closed iterator.  XXX: Is IOError the best we can do?  For
            # now, mimic a read on a closed file.
            raise IOError, 'iterator is closed'

        pos = self._pos
        while 1:
            # Read the transaction record
            try:
                h = self._read_txn_header(pos)
            except CorruptedDataError, err:
                # If buf is empty, we've reached EOF.
                if not err.buf:
                    break
                raise

            if h.tid <= self._ltid:
                warn("%s time-stamp reduction at %s", self._file.name, pos)
            self._ltid = h.tid

            if self._stop is not None and h.tid > self._stop:
                raise IndexError, index

            if h.status == "c":
                # Assume we've hit the last, in-progress transaction
                raise IndexError, index

            if pos + h.tlen + 8 > self._file_size:
                # Hm, the data were truncated or the checkpoint flag wasn't
                # cleared.  They may also be corrupted,
                # in which case, we don't want to totally lose the data.
                warn("%s truncated, possibly due to damaged records at %s",
                     self._file.name, pos)
                break

            if h.status not in " up":
                warn('%s has invalid status, %s, at %s', self._file.name,
                     h.status, pos)

            if h.tlen < h.headerlen():
                # We're in trouble. Find out if this is bad data in
                # the middle of the file, or just a turd that Win 9x
                # dropped at the end when the system crashed.  Skip to
                # the end and read what should be the transaction
                # length of the last transaction.
                self._file.seek(-8, 2)
                rtl = u64(self._file.read(8))
                # Now check to see if the redundant transaction length is
                # reasonable:
                if self._file_size - rtl < pos or rtl < TRANS_HDR_LEN:
                    nearPanic("%s has invalid transaction header at %s",
                              self._file.name, pos)
                    warn("It appears that there is invalid data at the end of "
                         "the file, possibly due to a system crash.  %s "
                         "truncated to recover from bad data at end."
                         % self._file.name)
                    break
                else:
                    warn("%s has invalid transaction header at %s",
                         self._file.name, pos)
                    break

            tpos = pos
            tend = tpos + h.tlen

            if h.status != "u":
                pos = tpos + h.headerlen()
                user = self._file.read(h.ulen)
                description = self._file.read(h.dlen)
                e = {}
                if h.elen:
                    try:
                        e = loads(self._file.read(h.elen))
                    except:
                        pass

                result = RecordIterator(h.tid, h.status, user, description,
                                        e, pos, tend, self._file, tpos)

            # Read the (intentionally redundant) transaction length
            self._file.seek(tend)
            rtl = u64(self._file.read(8))
            if rtl != h.tlen:
                warn("%s redundant transaction length check failed at %s",
                     self._file.name, tend)
                break
            self._pos = tend + 8

            return result

        raise IndexError, index

class RecordIterator(Iterator, BaseStorage.TransactionRecord,
                     FileStorageFormatter):
    """Iterate over the transactions in a FileStorage file."""
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

    def next(self, index=0):
        pos = self._pos
        while pos < self._tend:
            # Read the data records for this transaction
            h = self._read_data_header(pos)
            dlen = h.recordlen()

            if pos + dlen > self._tend or h.tloc != self._tpos:
                warn("%s data record exceeds transaction record at %s",
                     file.name, pos)
                break

            self._pos = pos + dlen
            prev_txn = None
            if h.plen:
                data = self._file.read(h.plen)
            else:
                if h.back == 0:
                    # If the backpointer is 0, then this transaction
                    # undoes the object creation.  It either aborts
                    # the version that created the object or undid the
                    # transaction that created it.  Return None
                    # instead of a pickle to indicate this.
                    data = None
                else:
                    data, tid = self._loadBackTxn(h.oid, h.back, False)
                    # XXX looks like this only goes one link back, should
                    # it go to the original data like BDBFullStorage?
                    prev_txn = self.getTxnFromData(h.oid, h.back)

            r = Record(h.oid, h.tid, h.version, data, prev_txn)

            return r

        raise IndexError, index

class Record(BaseStorage.DataRecord):
    """An abstract database record."""
    def __init__(self, *args):
        self.oid, self.tid, self.version, self.data, self.data_txn = args

class UndoSearch:

    def __init__(self, file, pos, packt, first, last, filter=None):
        self.file = file
        self.pos = pos
        self.packt = packt
        self.first = first
        self.last = last
        self.filter = filter
        self.i = 0
        self.results = []
        self.stop = 0

    def finished(self):
        """Return True if UndoSearch has found enough records."""
        # BAW: Why 39 please?  This makes no sense (see also below).
        return self.i >= self.last or self.pos < 39 or self.stop

    def search(self):
        """Search for another record."""
        dict = self._readnext()
        if dict is not None and (self.filter is None or self.filter(dict)):
            if self.i >= self.first:
                self.results.append(dict)
            self.i += 1

    def _readnext(self):
        """Read the next record from the storage."""
        self.file.seek(self.pos - 8)
        self.pos -= u64(self.file.read(8)) + 8
        self.file.seek(self.pos)
        h = self.file.read(TRANS_HDR_LEN)
        tid, tl, status, ul, dl, el = struct.unpack(TRANS_HDR, h)
        if tid < self.packt or status == 'p':
            self.stop = 1
            return None
        if status != ' ':
            return None
        d = u = ''
        if ul:
            u = self.file.read(ul)
        if dl:
            d = self.file.read(dl)
        e = {}
        if el:
            try:
                e = loads(self.file.read(el))
            except:
                pass
        d = {'id': base64.encodestring(tid).rstrip(),
             'time': TimeStamp(tid).timeTime(),
             'user_name': u,
             'description': d}
        d.update(e)
        return d
