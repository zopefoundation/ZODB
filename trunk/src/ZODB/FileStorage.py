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
#   - 8-byte serial, which is a type stamp that matches the
#     transaction timestamp.
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
#
__version__='$Revision: 1.134 $'[11:-2]

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
from ZODB.POSException import UndoError, POSKeyError, MultipleUndoErrors
from ZODB.TimeStamp import TimeStamp
from ZODB.lock_file import LockFile
from ZODB.utils import p64, u64, cp, z64
from ZODB.fspack import FileStoragePacker

try:
    from ZODB.fsIndex import fsIndex
except ImportError:
    def fsIndex():
        return {}

from zLOG import LOG, BLATHER, WARNING, ERROR, PANIC

t32 = 1L << 32
# the struct formats for the headers
TRANS_HDR = ">8s8scHHH"
DATA_HDR = ">8s8s8s8sH8s"
# constants to support various header sizes
TRANS_HDR_LEN = 23
DATA_HDR_LEN = 42
DATA_VERSION_HDR_LEN = 58

assert struct.calcsize(TRANS_HDR) == TRANS_HDR_LEN
assert struct.calcsize(DATA_HDR) == DATA_HDR_LEN

def warn(message, *data):
    LOG('ZODB FS', WARNING, "%s  warn: %s\n" % (packed_version,
                                                (message % data)))

def error(message, *data):
    LOG('ZODB FS', ERROR, "%s ERROR: %s\n" % (packed_version,
                                              (message % data)))

def nearPanic(message, *data):
    LOG('ZODB FS', PANIC, "%s ERROR: %s\n" % (packed_version,
                                              (message % data)))

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

class CorruptedDataError(CorruptedFileStorageError):
    pass

class FileStorageQuotaError(FileStorageError,
                            POSException.StorageSystemError):
    """File storage quota exceeded."""

packed_version='FS21'

class FileStorage(BaseStorage.BaseStorage,
                  ConflictResolution.ConflictResolvingStorage):
    # default pack time is 0
    _packt = z64

    _records_before_save = 10000

    def __init__(self, file_name, create=0, read_only=0, stop=None,
                 quota=None):

        if read_only:
            self._is_read_only = 1
            if create:
                raise ValueError, "can't create a read-only file"
        elif stop is not None:
            raise ValueError, "time-travel is only supported in read-only mode"

        if stop is None:
            stop='\377'*8

        # Lock the database and set up the temp file.
        if not read_only:
            # Create the lock file
            self._lock_file = LockFile(file_name + '.lock')
            self._tfile = open(file_name + '.tmp', 'w+b')
        else:
            self._tfile = None

        self._file_name = file_name

        BaseStorage.BaseStorage.__init__(self, file_name)

        index, vindex, tindex, tvindex = self._newIndexes()
        self._initIndex(index, vindex, tindex, tvindex)

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

            self._initIndex(index, vindex, tindex, tvindex)
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
        t = apply(TimeStamp, (time.gmtime(t)[:5] + (t % 60,)))
        if tid > t:
            warn("%s Database records in the future", file_name);
            if tid.timeTime() - t.timeTime() > 86400*30:
                # a month in the future? This is bogus, use current time
                self._ts = t

        self._quota = quota

    def _initIndex(self, index, vindex, tindex, tvindex):
        self._index=index
        self._vindex=vindex
        self._tindex=tindex
        self._tvindex=tvindex
        self._index_get=index.get
        self._vindex_get=vindex.get

    def __len__(self):
        return len(self._index)

    def _newIndexes(self):
        # hook to use something other than builtin dict
        return fsIndex(), {}, {}, {}

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

        if pos < 100:
            return 0 # insane
        file = self._file
        seek = file.seek
        read = file.read
        seek(0,2)
        if file.tell() < pos:
            return 0 # insane
        ltid = None

        max_checked = 5
        checked = 0

        while checked < max_checked:
            seek(pos-8)
            rstl = read(8)
            tl = u64(rstl)
            pos = pos-tl-8
            if pos < 4:
                return 0 # insane
            seek(pos)
            s = read(TRANS_HDR_LEN)
            tid, stl, status, ul, dl, el = unpack(TRANS_HDR, s)
            if not ltid:
                ltid = tid
            if stl != rstl:
                return 0 # inconsistent lengths
            if status == 'u':
                continue # undone trans, search back
            if status not in ' p':
                return 0 # insane
            if tl < (TRANS_HDR_LEN + ul + dl + el):
                return 0 # insane
            tend = pos+tl
            opos = pos+(TRANS_HDR_LEN + ul + dl + el)
            if opos == tend:
                continue # empty trans

            while opos < tend and checked < max_checked:
                # Read the data records for this transaction
                seek(opos)
                h = read(DATA_HDR_LEN)
                oid, serial, sprev, stloc, vlen, splen = unpack(DATA_HDR, h)
                tloc = u64(stloc)
                plen = u64(splen)

                dlen = DATA_HDR_LEN+(plen or 8)
                if vlen:
                    dlen = dlen+(16+vlen)

                if opos+dlen > tend or tloc != pos:
                    return 0 # insane

                if index.get(oid, 0) != opos:
                    return 0 # insane

                checked += 1

                opos = opos+dlen

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
            # XXX should log the error, though
            pass # We don't care if this fails.

    def abortVersion(self, src, transaction):
        return self.commitVersion(src, '', transaction, abort=1)

    def commitVersion(self, src, dest, transaction, abort=None):
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

    def _commitVersion(self, src, dest, transaction, abort=None):
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
        t = None
        tstatus = ' '
        if abort is None:
            newserial = self._serial

        while srcpos:
            self._file.seek(srcpos)
            h = self._file.read(DATA_VERSION_HDR_LEN)
            # h -> oid, serial, prev(oid), tloc, vlen, plen, pnv, pv
            oid = h[:8]
            pnv = h[-16:-8]
            if abort:
                # If we are aborting, the serialno in the new data
                # record should be the same as the serialno in the last
                # non-version data record.
                # XXX This might be the only time that the serialno
                # of a data record does not match the transaction id.
                self._file.seek(u64(pnv))
                h_pnv = self._file.read(DATA_VERSION_HDR_LEN)
                newserial = h_pnv[8:16]
            
            if self._index.get(oid) == srcpos:
                # This is a current record!
                self._tindex[oid] = here
                oids.append(oid)
                self._tfile.write(oid + newserial + spos + middle)
                if dest:
                    self._tvindex[dest] = here
                    self._tfile.write(pnv + sd + dest)
                    sd = p64(here)

                self._tfile.write(abort and pnv or spos)
                # data backpointer to src data
                here += heredelta

                current_oids[oid] = 1

            else:
                # Hm.  This is a non-current record.  Is there a
                # current record for this oid?
                if not current_oids.has_key(oid):
                    # Nope. We're done *if* this transaction wasn't undone.
                    tloc = h[24:32]
                    if t != tloc:
                        # We haven't checked this transaction before,
                        # get its status.
                        t = tloc
                        self._file.seek(u64(t) + 16)
                        tstatus = self._file.read(1)
                    if tstatus != 'u':
                        # Yee ha! We can quit
                        break

            spos = h[-8:]
            srcpos = u64(spos)
        return oids

    def getSize(self): return self._pos

    def _loada(self, oid, _index, file):
        "Read any version and return the version"
        try:
            pos=_index[oid]
        except KeyError:
            raise POSKeyError(oid)
        except TypeError:
            raise TypeError, 'invalid oid %r' % (oid,)
        file.seek(pos)
        read=file.read
        h=read(DATA_HDR_LEN)
        doid,serial,prev,tloc,vlen,plen = unpack(DATA_HDR, h)
        if vlen:
            nv = u64(read(8))
            read(8) # Skip previous version record pointer
            version = read(vlen)
        else:
            version = ''
            nv = 0

        if plen != z64:
            return read(u64(plen)), version, nv
        return _loadBack(file, oid, read(8))[0], version, nv

    def _load(self, oid, version, _index, file):
        try:
            pos = _index[oid]
        except KeyError:
            raise POSKeyError(oid)
        except TypeError:
            raise TypeError, 'invalid oid %r' % (oid,)
        file.seek(pos)
        read = file.read
        h = read(DATA_HDR_LEN)
        doid, serial, prev, tloc, vlen, plen = unpack(DATA_HDR, h)
        if doid != oid:
            raise CorruptedDataError, h
        if vlen:
            pnv = read(8) # Read location of non-version data
            if (not version or len(version) != vlen or
                (read(8) # skip past version link
                 and version != read(vlen))):
                return _loadBack(file, oid, pnv)

        # If we get here, then either this was not a version record,
        # or we've already read past the version data!
        if plen != z64:
            return read(u64(plen)), serial
        pnv = read(8)
        # We use the current serial, since that is the one that
        # will get checked when we store.
        return _loadBack(file, oid, pnv)[0], serial

    def load(self, oid, version):
        self._lock_acquire()
        try:
            return self._load(oid, version, self._index, self._file)
        finally:
            self._lock_release()

    def loadSerial(self, oid, serial):
        self._lock_acquire()
        try:
            file=self._file
            seek=file.seek
            read=file.read
            try:
                pos = self._index[oid]
            except KeyError:
                raise POSKeyError(oid)
            except TypeError:
                raise TypeError, 'invalid oid %r' % (oid,)
            while 1:
                seek(pos)
                h=read(DATA_HDR_LEN)
                doid,dserial,prev,tloc,vlen,plen = unpack(DATA_HDR, h)
                if doid != oid: raise CorruptedDataError(h)
                if dserial == serial: break # Yeee ha!
                # Keep looking for serial
                pos = u64(prev)
                if not pos:
                    raise POSKeyError(serial)
                continue

            if vlen:
                pnv=read(8) # Read location of non-version data
                read(8) # skip past version link
                read(vlen) # skip version

            if plen != z64: return read(u64(plen))

            # We got a backpointer, probably from a commit.
            pnv=read(8)
            return _loadBack(file, oid, pnv)[0]
        finally: self._lock_release()

    def modifiedInVersion(self, oid):
        self._lock_acquire()
        try:
            try:
                pos = self._index[oid]
            except KeyError:
                raise POSKeyError(oid)
            except TypeError:
                raise TypeError, 'invalid oid %r' % (oid,)
            file=self._file
            file.seek(pos)
            doid,serial,prev,tloc,vlen = unpack(">8s8s8s8sH", file.read(34))
            if doid != oid:
                raise CorruptedDataError(pos)
            if vlen:
                file.read(24) # skip plen, pnv, and pv
                return file.read(vlen)
            return ''
        finally: self._lock_release()

    def store(self, oid, serial, data, version, transaction):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)

        self._lock_acquire()
        try:
            old=self._index_get(oid, 0)
            pnv=None
            if old:
                self._file.seek(old)
                h=self._file.read(DATA_HDR_LEN)
                doid,oserial,sprev,stloc,vlen,splen = unpack(DATA_HDR, h)
                if doid != oid: raise CorruptedDataError(h)
                if vlen:
                    pnv=self._file.read(8) # non-version data pointer
                    self._file.read(8) # skip past version link
                    locked_version=self._file.read(vlen)
                    if version != locked_version:
                        raise POSException.VersionLockError, (
                            `oid`, locked_version)

                if serial != oserial:
                    data = self.tryToResolveConflict(oid, oserial, serial,
                                                     data)
                    if data is None:
                        raise POSException.ConflictError(oid=oid,
                                                serials=(oserial, serial))
            else:
                oserial=serial

            tfile=self._tfile
            write=tfile.write
            pos=self._pos
            here=pos+(tfile.tell()+self._thl)
            self._tindex[oid]=here
            newserial=self._serial
            write(pack(DATA_HDR,
                       oid, newserial, p64(old), p64(pos),
                       len(version), p64(len(data))
                       )
                  )
            if version:
                if pnv: write(pnv)
                else:   write(p64(old))
                # Link to last record for this version:
                tvindex=self._tvindex
                pv=tvindex.get(version, 0) or self._vindex_get(version, 0)
                write(p64(pv))
                tvindex[version]=here
                write(version)

            write(data)

            # Check quota
            quota=self._quota
            if quota is not None and pos+(tfile.tell()+self._thl) > quota:
                raise FileStorageQuotaError, (
                    'The storage quota has been exceeded.')

            return (serial == oserial and newserial
                    or ConflictResolution.ResolvedSerial)

        finally:
            self._lock_release()

    def _data_find(self, tpos, oid, data):
        # Return backpointer to oid in data record for in transaction at tpos.
        # It should contain a pickle identical to data. Returns 0 on failure.
        # Must call with lock held.
        self._file.seek(tpos)
        h = self._file.read(TRANS_HDR_LEN)
        tid, stl, status, ul, dl, el = struct.unpack(TRANS_HDR, h)
        self._file.read(ul + dl + el)
        tend = tpos + u64(stl) + 8
        pos = self._file.tell()
        while pos < tend:
            h = self._file.read(DATA_HDR_LEN)
            _oid, serial, sprev, stpos, vl, sdl = struct.unpack(DATA_HDR, h)
            dl = u64(sdl)
            reclen = DATA_HDR_LEN + vl + dl
            if vl:
                reclen += 16
            if _oid == oid:
                if vl:
                    self._file.read(vl + 16)
                # Make sure this looks like the right data record
                if dl == 0:
                    # This is also a backpointer.  Gotta trust it.
                    return pos
                if dl != len(data):
                    # The expected data doesn't match what's in the
                    # backpointer.  Something is wrong.
                    error("Mismatch between data and backpointer at %d", pos)
                    return 0
                _data = self._file.read(dl)
                if data != _data:
                    return 0
                return pos
            pos += reclen
            self._file.seek(pos)
        return 0

    def restore(self, oid, serial, data, version, prev_txn, transaction):
        # A lot like store() but without all the consistency checks.  This
        # should only be used when we /know/ the data is good, hence the
        # method name.  While the signature looks like store() there are some
        # differences:
        #
        # - serial is the serial number of /this/ revision, not of the
        #   previous revision.  It is used instead of self._serial, which is
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
            self._tfile.write(pack(DATA_HDR,
                                   oid, serial, p64(old), p64(self._pos),
                                   len(version), p64(dlen)))
            # We need to write some version information if this revision is
            # happening in a version.
            if version:
                pnv = self._restore_pnv(oid, old, version, prev_pos)
                if pnv:
                    self._tfile.write(pnv)
                else:
                    self._tfile.write(p64(old))
                # Link to the last record for this version
                pv = self._tvindex.get(version, 0)
                if not pv:
                    pv = self._vindex_get(version, 0)
                self._tfile.write(p64(pv))
                self._tvindex[version] = here
                self._tfile.write(version)
            # And finally, write the data or a backpointer
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

        pnv = None

        # Load the record pointed to be prev
        self._file.seek(prev)
        h = self._file.read(DATA_HDR_LEN)
        doid, x, y, z, vlen, w = unpack(DATA_HDR, h)
        if doid != oid:
            raise CorruptedDataError, h
        # If the previous record is for a version, it must have
        # a valid pnv.
        if vlen > 0:
            pnv = self._file.read(8)
            pv = self._file.read(8)
            v = self._file.read(vlen)
        elif bp:
            # XXX Not sure the following is always true:
            # The previous record is not for this version, yet we
            # have a backpointer to it.  The current record must
            # be an undo of an abort or commit, so the backpointer
            # must be to a version record with a pnv.
            self._file.seek(bp)
            h2 = self._file.read(DATA_HDR_LEN)
            doid2, x, y, z, vlen2, sdl = unpack(DATA_HDR, h2)
            dl = u64(sdl)
            if oid != doid2:
                raise CorruptedDataError, h2
            if vlen2 > 0:
                pnv = self._file.read(8)
                pv = self._file.read(8)
                v = self._file.read(8)
            else:
                warn("restore could not find previous non-version data "
                     "at %d or %d" % (prev, bp))
            
        return pnv

    def supportsUndo(self):
        return 1

    def supportsVersions(self):
        return 1

    def _clear_temp(self):
        self._tindex.clear()
        self._tvindex.clear()
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
            user, desc, ext = self._ude
            luser = len(user)
            ldesc = len(desc)
            lext = len(ext)

            self._file.seek(self._pos)
            tl = self._thl + dlen
            stl = p64(tl)

            try:
                # Note that we use a status of 'c', for checkpoint.
                # If this flag isn't cleared, anything after this is
                # suspect.
                self._file.write(pack(
                    ">8s"          "8s" "c"  "H"        "H"        "H"
                     ,self._serial, stl,'c',  luser,     ldesc,     lext,
                    ))
                if user:
                    self._file.write(user)
                if desc:
                    self._file.write(desc)
                if ext:
                    self._file.write(ext)

                cp(self._tfile, self._file, dlen)

                self._file.write(stl)
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
        """Return the serial, data pointer, data, and version for the oid
        record at pos"""
        if tpos:
            file=self._tfile
            pos = tpos - self._pos - self._thl
            tpos=file.tell()
        else:
            file=self._file

        read=file.read
        file.seek(pos)
        h=read(DATA_HDR_LEN)
        roid,serial,sprev,stloc,vlen,splen = unpack(DATA_HDR, h)
        if roid != oid:
            raise UndoError('Invalid undo transaction id', oid)
        if vlen:
            read(16) # skip nv pointer and version previous pointer
            version=read(vlen)
        else:
            version=''

        plen = u64(splen)
        if plen:
            data = read(plen)
        else:
            data=''
            pos=u64(read(8))

        if tpos: file.seek(tpos) # Restore temp file to end

        return serial, pos, data, version

    def _getVersion(self, oid, pos):
        self._file.seek(pos)
        h = self._file.read(DATA_HDR_LEN)
        doid, serial, sprev, stloc, vlen, splen = unpack(DATA_HDR, h)
        assert doid == oid
        if vlen:
            h = self._file.read(16)
            return self._file.read(vlen), h[:8]
        else:
            return '', ''

    def getSerial(self, oid):
        self._lock_acquire()
        try:
            try:
                return self._getSerial(oid, self._index[oid])
            except KeyError:
                raise POSKeyError(oid)
            except TypeError:
                raise TypeError, 'invalid oid %r' % (oid,)
        finally:
            self._lock_release()

    def _getSerial(self, oid, pos):
        self._file.seek(pos)
        h = self._file.read(DATA_HDR_LEN)
        oid2, serial, sprev, stloc, vlen, splen = unpack(DATA_HDR, h)
        assert oid == oid2
        if splen==z64:
            # a back pointer
            bp = self._file.read(8)
            if bp == z64:
                # If the backpointer is 0 (encoded as z64), then
                # this transaction undoes the object creation.
                raise KeyError(oid)
        return serial

    def _transactionalUndoRecord(self, oid, pos, serial, pre, version):
        """Get the indo information for a data record

        Return a 5-tuple consisting of a pickle, data pointer,
        version, packed non-version data pointer, and current
        position.  If the pickle is true, then the data pointer must
        be 0, but the pickle can be empty *and* the pointer 0.
        """

        copy=1 # Can we just copy a data pointer

        # First check if it is possible to undo this record.
        tpos=self._tindex.get(oid, 0)
        ipos=self._index.get(oid, 0)
        tipos=tpos or ipos
        if tipos != pos:
            # Eek, a later transaction modified the data, but,
            # maybe it is pointing at the same data we are.
            cserial, cdataptr, cdata, cver = self._undoDataInfo(
                oid, ipos, tpos)
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
                        _loadBackPOS(self._file, oid, p64(pos)) !=
                        _loadBackPOS(self._file, oid, p64(cdataptr))
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
            return '', 0, '', '', ipos

        version, snv = self._getVersion(oid, pre)
        if copy:
            # we can just copy our previous-record pointer forward
            return '', pre, version, snv, ipos

        try:
            # returns data, serial tuple
            bdata = _loadBack(self._file, oid, p64(pre))[0]
        except KeyError:
            # couldn't find oid; what's the real explanation for this?
            raise UndoError("_loadBack() failed for %s", oid)
        data=self.tryToResolveConflict(oid, cserial, serial, bdata, cdata)

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

    def transactionalUndo(self, transaction_id, transaction):
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
        return tindex.keys()

    def _txn_find(self, tid, stop_at_pack):
        pos = self._pos
        # XXX Why 39?  Only because undoLog() uses it as a boundary.
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

        ostloc = p64(self._pos)
        here = self._pos + (self._tfile.tell() + self._thl)
        # Let's move the file pointer back to the start of the txn record.
        self._file.seek(tpos)
        h = self._file.read(TRANS_HDR_LEN)
        if h[16] == 'u':
            return
        if h[16] != ' ':
            raise UndoError('non-undoable transaction')
        tl = u64(h[8:16])
        ul, dl, el = struct.unpack(">HHH", h[17:TRANS_HDR_LEN])
        tend = tpos + tl
        pos = tpos + (TRANS_HDR_LEN + ul + dl + el)
        tindex = {}
        failures = {} # keep track of failures, cause we may succeed later
        failed = failures.has_key
        # Read the data records for this transaction
        while pos < tend:
            self._file.seek(pos)
            h = self._file.read(DATA_HDR_LEN)
            oid, serial, sprev, stloc, vlen, splen = \
                 struct.unpack(DATA_HDR, h)
            if failed(oid):
                del failures[oid] # second chance!
            plen = u64(splen)
            prev = u64(sprev)
            if vlen:
                dlen = DATA_VERSION_HDR_LEN + vlen + (plen or 8)
                self._file.seek(16, 1)
                version = self._file.read(vlen)
            else:
                dlen = DATA_HDR_LEN + (plen or 8)
                version = ''

            try:
                p, prev, v, snv, ipos = self._transactionalUndoRecord(
                    oid, pos, serial, prev, version)
            except UndoError, v:
                # Don't fail right away. We may be redeemed later!
                failures[oid] = v
            else:
                plen = len(p)
                self._tfile.write(pack(DATA_HDR,
                                       oid, self._serial, p64(ipos),
                                       ostloc, len(v), p64(plen)))
                if v:
                    vprev=self._tvindex.get(v, 0) or self._vindex.get(v, 0)
                    self._tfile.write(snv + p64(vprev) + v)
                    self._tvindex[v] = here
                    odlen = DATA_VERSION_HDR_LEN + len(v)+(plen or 8)
                else:
                    odlen = DATA_HDR_LEN + (plen or 8)

                if p:
                    self._tfile.write(p)
                else:
                    self._tfile.write(p64(prev))
                tindex[oid] = here
                here += odlen

            pos += dlen
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
            r=[]
            file=self._file
            seek=file.seek
            read=file.read
            try:
                pos=self._index[oid]
            except KeyError:
                raise POSKeyError(oid)
            except TypeError:
                raise TypeError, 'invalid oid %r' % (oid,)
            wantver=version

            while 1:
                if len(r) >= size: return r
                seek(pos)
                h=read(DATA_HDR_LEN)
                doid,serial,prev,tloc,vlen,plen = unpack(DATA_HDR, h)
                prev=u64(prev)

                if vlen:
                    read(16)
                    version=read(vlen)
                    if wantver is not None and version != wantver:
                        if prev:
                            pos=prev
                            continue
                        else:
                            return r
                else:
                    version=''
                    wantver=None

                seek(u64(tloc))
                h=read(TRANS_HDR_LEN)
                tid, stl, status, ul, dl, el = unpack(TRANS_HDR,h)
                user_name=read(ul)
                description=read(dl)
                if el: d=loads(read(el))
                else: d={}

                d['time']=TimeStamp(serial).timeTime()
                d['user_name']=user_name
                d['description']=description
                d['serial']=serial
                d['version']=version
                d['size']=u64(plen)

                if filter is None or filter(d):
                    r.append(d)

                if prev: pos=prev
                else: return r
        finally: self._lock_release()

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
        
        stop=`apply(TimeStamp, time.gmtime(t)[:5]+(t%60,))`
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
                except Exception, msg:
                    self._file = open(self._file_name, 'r+b')
                    raise

                # OK, we're beyond the point of no return
                os.rename(self._file_name + '.pack', self._file_name)
                self._file = open(self._file_name, 'r+b')
                self._initIndex(p.index, p.vindex, p.tindex, p.tvindex)
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

    def lastSerial(self, oid):
        """Return last serialno committed for object oid.

        If there is no serialno for this oid -- which can only occur
        if it is a new object -- return None.
        """
        try:
            pos = self._index[oid]
        except KeyError:
            return None
        except TypeError:
            raise TypeError, 'invalid oid %r' % (oid,)
        self._file.seek(pos)
        # first 8 bytes are oid, second 8 bytes are serialno
        h = self._file.read(16)
        if len(h) < 16:
            raise CorruptedDataError(h)
        if h[:8] != oid:
            h = h + self._file.read(26) # get rest of header
            raise CorruptedDataError(h)
        return h[8:]

    def cleanup(self):
        """Remove all files created by this storage."""
        cleanup(self._file_name)


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

    if file_size:
        if file_size < start: raise FileStorageFormatError, file.name
        seek(0)
        if read(4) != packed_version: raise FileStorageFormatError, name
    else:
        if not read_only: file.write(packed_version)
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

        tid, stl, status, ul, dl, el = unpack(TRANS_HDR,h)
        if el < 0: el=t32-el

        if tid <= ltid:
            warn("%s time-stamp reduction at %s", name, pos)
        ltid = tid

        tl=u64(stl)

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

        if tid >= stop: break

        tpos=pos
        tend=tpos+tl

        if status=='u':
            # Undone transaction, skip it
            seek(tend)
            h=read(8)
            if h != stl:
                if recover: return tpos, None, None
                panic('%s has inconsistent transaction length at %s',
                      name, pos)
            pos=tend+8
            continue

        pos=tpos+(TRANS_HDR_LEN+ul+dl+el)
        while pos < tend:
            # Read the data records for this transaction

            seek(pos)
            h=read(DATA_HDR_LEN)
            oid,serial,sprev,stloc,vlen,splen = unpack(DATA_HDR, h)
            prev=u64(sprev)
            tloc=u64(stloc)
            plen=u64(splen)

            dlen=DATA_HDR_LEN+(plen or 8)
            tindex[oid]=pos

            if vlen:
                dlen=dlen+(16+vlen)
                read(16)
                pv=u64(read(8))
                version=read(vlen)
                # Jim says: "It's just not worth the bother."
                #if vndexpos(version, 0) != pv:
                #    panic("%s incorrect previous version pointer at %s",
                #          name, pos)
                vindex[version]=pos

            if pos+dlen > tend or tloc != tpos:
                if recover: return tpos, None, None
                panic("%s data record exceeds transaction record at %s",
                      name, pos)

            if index_get(oid, 0) != prev:
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
        h=read(8)
        if h != stl:
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


def _loadBack_impl(file, oid, back):
    # shared implementation used by various _loadBack methods
    while 1:
        old = u64(back)
        if not old:
            # If the backpointer is 0, the object does not currently exist.
            raise POSKeyError(oid)
        file.seek(old)
        h = file.read(DATA_HDR_LEN)
        doid, serial, prev, tloc, vlen, plen = unpack(DATA_HDR, h)

        if vlen:
            file.read(16)
            version = file.read(vlen)
        if plen != z64:
            return file.read(u64(plen)), serial, old, tloc
        back = file.read(8) # We got a back pointer!

def _loadBack(file, oid, back):
    data, serial, old, tloc = _loadBack_impl(file, oid, back)
    return data, serial

def _loadBackPOS(file, oid, back):
    """Return position of data record for backpointer."""
    data, serial, old, tloc = _loadBack_impl(file, oid, back)
    return old

def _loadBackTxn(file, oid, back):
    """Return data, serial, and txn id for backpointer."""
    data, serial, old, stloc = _loadBack_impl(file, oid, back)
    tloc = u64(stloc)
    file.seek(tloc)
    h = file.read(TRANS_HDR_LEN)
    tid = h[:8]
    return data, serial, tid

def getTxnFromData(file, oid, back):
    """Return transaction id for data at back."""
    file.seek(u64(back))
    h = file.read(DATA_HDR_LEN)
    doid, serial, prev, stloc, vlen, plen = unpack(DATA_HDR, h)
    assert oid == doid
    tloc = u64(stloc)
    file.seek(tloc)
    # seek to transaction header, where tid is first 8 bytes
    return file.read(8)

def _truncate(file, name, pos):
    seek=file.seek
    seek(0,2)
    file_size=file.tell()
    try:
        i=0
        while 1:
            oname='%s.tr%s' % (name, i)
            if os.path.exists(oname):
                i=i+1
            else:
                warn("Writing truncated data from %s to %s", name, oname)
                o=open(oname,'wb')
                seek(pos)
                cp(file, o, file_size-pos)
                o.close()
                break
    except:
        error("couldn\'t write truncated data for %s", name)
        raise POSException.StorageSystemError, (
            "Couldn't save truncated data")

    seek(pos)
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


class FileIterator(Iterator):
    """Iterate over the transactions in a FileStorage file.
    """
    _ltid = z64
    _file = None

    def __init__(self, file, start=None, stop=None):
        if isinstance(file, StringType):
            file = open(file, 'rb')
        self._file = file
        if file.read(4) != packed_version:
            raise FileStorageFormatError, file.name
        file.seek(0,2)
        self._file_size = file.tell()
        self._pos = 4L
        assert start is None or isinstance(start, StringType)
        assert stop is None or isinstance(stop, StringType)
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
        file=self._file
        seek=file.seek
        read=file.read
        pos=self._pos

        while 1:
            # Read the transaction record
            seek(pos)
            h=read(TRANS_HDR_LEN)
            if len(h) < TRANS_HDR_LEN: break


            tid, stl, status, ul, dl, el = unpack(TRANS_HDR,h)
            if el < 0: el=t32-el

            if tid <= self._ltid:
                warn("%s time-stamp reduction at %s", self._file.name, pos)
            self._ltid=tid

            if self._stop is not None and tid > self._stop:
                raise IndexError, index

            if status == 'c':
                # Assume we've hit the last, in-progress transaction
                raise IndexError, index

            tl=u64(stl)

            if pos+(tl+8) > self._file_size:
                # Hm, the data were truncated or the checkpoint flag wasn't
                # cleared.  They may also be corrupted,
                # in which case, we don't want to totally lose the data.
                warn("%s truncated, possibly due to damaged records at %s",
                     self._file.name, pos)
                break

            if status not in ' up':
                warn('%s has invalid status, %s, at %s', self._file.name,
                     status, pos)

            if tl < (TRANS_HDR_LEN+ul+dl+el):
                # We're in trouble. Find out if this is bad data in
                # the middle of the file, or just a turd that Win 9x
                # dropped at the end when the system crashed.  Skip to
                # the end and read what should be the transaction
                # length of the last transaction.
                seek(-8, 2)
                rtl=u64(read(8))
                # Now check to see if the redundant transaction length is
                # reasonable:
                if self._file_size - rtl < pos or rtl < TRANS_HDR_LEN:
                    nearPanic('%s has invalid transaction header at %s',
                              self._file.name, pos)
                    warn("It appears that there is invalid data at the end of "
                         "the file, possibly due to a system crash.  %s "
                         "truncated to recover from bad data at end."
                         % self._file.name)
                    break
                else:
                    warn('%s has invalid transaction header at %s',
                         self._file.name, pos)
                    break

            tpos=pos
            tend=tpos+tl

            if status=='u':
                # Undone transaction, skip it
                seek(tend)
                h=read(8)
                if h != stl:
                    panic('%s has inconsistent transaction length at %s',
                          self._file.name, pos)
                pos=tend+8
                continue

            pos=tpos+(TRANS_HDR_LEN+ul+dl+el)
            user=read(ul)
            description=read(dl)
            if el:
                try: e=loads(read(el))
                except: e={}
            else: e={}

            result = RecordIterator(tid, status, user, description, e, pos,
                                    tend, file, tpos)
            pos = tend

            # Read the (intentionally redundant) transaction length
            seek(pos)
            h = read(8)
            if h != stl:
                warn("%s redundant transaction length check failed at %s",
                     self._file.name, pos)
                break
            self._pos = pos + 8

            return result

        raise IndexError, index

class RecordIterator(Iterator, BaseStorage.TransactionRecord):
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
            self._file.seek(pos)
            h = self._file.read(DATA_HDR_LEN)
            oid, serial, sprev, stloc, vlen, splen = unpack(DATA_HDR, h)
            prev = u64(sprev)
            tloc = u64(stloc)
            plen = u64(splen)
            dlen = DATA_HDR_LEN + (plen or 8)

            if vlen:
                dlen += (16 + vlen)
                tmp = self._file.read(16)
                pv = u64(tmp[8:16])
                version = self._file.read(vlen)
            else:
                version = ''

            datapos = pos + DATA_HDR_LEN
            if vlen:
                datapos += 16 + vlen
            assert self._file.tell() == datapos, (self._file.tell(), datapos)

            if pos + dlen > self._tend or tloc != self._tpos:
                warn("%s data record exceeds transaction record at %s",
                     file.name, pos)
                break

            self._pos = pos + dlen
            prev_txn = None
            if plen:
                data = self._file.read(plen)
            else:
                bp = self._file.read(8)
                if bp == z64:
                    # If the backpointer is 0 (encoded as z64), then
                    # this transaction undoes the object creation.  It
                    # either aborts the version that created the
                    # object or undid the transaction that created it.
                    # Return None instead of a pickle to indicate
                    # this.
                    data = None
                else:
                    data, _s, tid = _loadBackTxn(self._file, oid, bp)
                    prev_txn = getTxnFromData(self._file, oid, bp)

            r = Record(oid, serial, version, data, prev_txn)

            return r

        raise IndexError, index

class Record(BaseStorage.DataRecord):
    """An abstract database record."""
    def __init__(self, *args):
        self.oid, self.serial, self.version, self.data, self.data_txn = args

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


def cleanup(filename):
    """Remove all FileStorage related files."""
    for ext in '', '.old', '.tmp', '.lock', '.index', '.pack':
        try:
            os.remove(filename + ext)
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise
