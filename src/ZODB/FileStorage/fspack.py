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

import binascii
import logging
import os

import ZODB.fsIndex
import ZODB.POSException
from ZODB.FileStorage.format import TRANS_HDR_LEN
from ZODB.FileStorage.format import CorruptedDataError
from ZODB.FileStorage.format import DataHeader
from ZODB.FileStorage.format import FileStorageFormatter
from ZODB.utils import p64
from ZODB.utils import u64
from ZODB.utils import z64


logger = logging.getLogger(__name__)


class PackError(ZODB.POSException.POSError):
    pass


class PackCopier(FileStorageFormatter):

    def __init__(self, f, index, tindex):
        self._file = f
        self._index = index
        self._tindex = tindex
        self._pos = None

    def _txn_find(self, tid, stop_at_pack):
        # _pos always points just past the last transaction
        pos = self._pos
        while pos > 4:
            self._file.seek(pos - 8)
            pos = pos - u64(self._file.read(8)) - 8
            self._file.seek(pos)
            h = self._file.read(TRANS_HDR_LEN)  # XXX bytes
            _tid = h[:8]
            if _tid == tid:
                return pos
            if stop_at_pack:
                if h[16] == 'p':
                    break
        raise PackError("Invalid backpointer transaction id")

    def _data_find(self, tpos, oid, data):
        # Return backpointer for oid.  Must call with the lock held.
        # This is a file offset to oid's data record if found, else 0.
        # The data records in the transaction at tpos are searched for oid.
        # If a data record for oid isn't found, returns 0.
        # Else if oid's data record contains a backpointer, that
        # backpointer is returned.
        # Else oid's data record contains the data, and the file offset of
        # oid's data record is returned.  This data record should contain
        # a pickle identical to the 'data' argument.

        # Unclear:  If the length of the stored data doesn't match len(data),
        # an exception is raised.  If the lengths match but the data isn't
        # the same, 0 is returned.  Why the discrepancy?
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
                    logger.error("Mismatch between data and backpointer at %d",
                                 pos)
                    return 0
                _data = self._file.read(h.plen)
                if data != _data:
                    return 0
                return pos
            pos += h.recordlen()
        return 0

    def copy(self, oid, serial, data, prev_txn, txnpos, datapos):
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
        h = DataHeader(oid, serial, old, txnpos, 0, dlen)

        self._file.write(h.asString())
        # Write the data or a backpointer
        if data is None:
            if prev_pos:
                self._file.write(p64(prev_pos))
            else:
                # Write a zero backpointer, which indicates an
                # un-creation transaction.
                self._file.write(z64)
        else:
            self._file.write(data)

    def setTxnPos(self, pos):
        self._pos = pos

    def _resolve_backpointer(self, prev_txn, oid, data):
        pos = self._file.tell()
        try:
            prev_pos = 0
            if prev_txn is not None:
                prev_txn_pos = self._txn_find(prev_txn, 0)
                if prev_txn_pos:
                    prev_pos = self._data_find(prev_txn_pos, oid, data)
            return prev_pos
        finally:
            self._file.seek(pos)


class GC(FileStorageFormatter):

    def __init__(self, file, eof, packtime, gc, referencesf):
        self._file = file
        self._name = file.name
        self.eof = eof
        self.packtime = packtime
        self.gc = gc
        # packpos: position of first txn header after pack time
        self.packpos = None

        # {oid -> current data record position}:
        self.oid2curpos = ZODB.fsIndex.fsIndex()

        # The set of reachable revisions of each object.
        #
        # This set as managed using two data structures.  The first is
        # an fsIndex mapping oids to one data record pos.  Since only
        # a few objects will have more than one revision, we use this
        # efficient data structure to handle the common case.  The
        # second is a dictionary mapping objects to lists of
        # positions; it is used to handle the same number of objects
        # for which we must keep multiple revisions.
        self.reachable = ZODB.fsIndex.fsIndex()
        self.reach_ex = {}

        # keep ltid for consistency checks during initial scan
        self.ltid = z64

        self.referencesf = referencesf

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
        if self.gc:
            self.findReachableAtPacktime([z64])
            self.findReachableFromFuture()
            # These mappings are no longer needed and may consume a lot of
            # space.
            del self.oid2curpos
        else:
            self.reachable = self.oid2curpos

    def buildPackIndex(self):
        pos = 4
        # We make the initial assumption that the database has been
        # packed before and set unpacked to True only after seeing the
        # first record with a status == " ".  If we get to the packtime
        # and unpacked is still False, we need to watch for a redundant
        # pack.
        unpacked = False
        while pos < self.eof:
            th = self._read_txn_header(pos)
            if th.tid > self.packtime:
                break
            self.checkTxn(th, pos)
            if th.status != "p":
                unpacked = True

            tpos = pos
            end = pos + th.tlen
            pos += th.headerlen()

            while pos < end:
                dh = self._read_data_header(pos)
                self.checkData(th, tpos, dh, pos)
                if dh.plen or dh.back:
                    self.oid2curpos[dh.oid] = pos
                else:
                    if dh.oid in self.oid2curpos:
                        del self.oid2curpos[dh.oid]
                pos += dh.recordlen()

            tlen = self._read_num(pos)
            if tlen != th.tlen:
                self.fail(pos, "redundant transaction length does not "
                          "match initial transaction length: %d != %d",
                          tlen, th.tlen)
            pos += 8

        self.packpos = pos

        if unpacked:
            return
        # check for a redundant pack.  If the first record following
        # the newly computed packpos has status 'p', then it was
        # packed earlier and the current pack is redudant.
        try:
            th = self._read_txn_header(pos)
        except CorruptedDataError as err:
            if err.buf != b"":
                raise
        if th.status == 'p':
            # Delayed import to cope with circular imports.
            # TODO:  put exceptions in a separate module.
            from ZODB.FileStorage.FileStorage import RedundantPackWarning
            raise RedundantPackWarning(
                "The database has already been packed to a later time"
                " or no changes have been made since the last pack")

    def findReachableAtPacktime(self, roots):
        """Mark all objects reachable from the oids in roots as reachable."""
        reachable = self.reachable
        oid2curpos = self.oid2curpos

        todo = list(roots)
        while todo:
            oid = todo.pop()
            if oid in reachable:
                continue

            try:
                pos = oid2curpos[oid]
            except KeyError:
                if oid == z64 and len(oid2curpos) == 0:
                    # special case, pack to before creation time
                    continue
                raise KeyError(oid)

            reachable[oid] = pos
            for oid in self.findrefs(pos):
                if oid not in reachable:
                    todo.append(oid)

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
                    if dh.oid in self.reachable:
                        L = self.reach_ex.setdefault(dh.oid, [])
                        if dh.back not in L:
                            L.append(dh.back)
                            extra_roots.append(dh.back)
                    else:
                        self.reachable[dh.oid] = dh.back

                pos += dh.recordlen()

            tlen = self._read_num(pos)
            if tlen != th.tlen:
                self.fail(pos, "redundant transaction length does not "
                          "match initial transaction length: %d != %d",
                          tlen, th.tlen)
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
            return self.referencesf(self._file.read(dh.plen))
        else:
            return []


class FileStoragePacker(FileStorageFormatter):

    # path is the storage file path.
    # stop is the pack time, as a TimeStamp.
    # current_size is the storage's _pos.  All valid data at the start
    # lives before that offset (there may be a checkpoint transaction in
    # progress after it).

    def __init__(self, storage, referencesf, stop, gc=True):
        self._storage = storage
        if storage.blob_dir:
            self.pack_blobs = True
            self.blob_removed = open(
                os.path.join(storage.blob_dir, '.removed'), 'wb')
        else:
            self.pack_blobs = False
            self.blob_removed = None

        path = storage._file.name
        self._name = path
        # We open our own handle on the storage so that much of pack can
        # proceed in parallel.  It's important to close this file at every
        # return point, else on Windows the caller won't be able to rename
        # or remove the storage file.
        self._file = open(path, "rb")
        self._path = path
        self._stop = stop
        self.locked = False
        self.file_end = storage.getSize()

        self.gc = GC(self._file, self.file_end, self._stop, gc, referencesf)

        # The packer needs to acquire the parent's commit lock
        # during the copying stage, so the two sets of lock acquire
        # and release methods are passed to the constructor.
        self._lock = storage._lock
        self._commit_lock = storage._commit_lock

        # The packer will use several indexes.
        # index: oid -> pos
        # tindex: oid -> pos, for current txn
        # oid2tid: not used by the packer

        self.index = ZODB.fsIndex.fsIndex()
        self.tindex = {}
        self.oid2tid = {}
        self.toid2tid = {}
        self.toid2tid_delete = {}

        self._tfile = None

    def close(self):
        self._file.close()
        if self._tfile is not None:
            self._tfile.close()
        if self.blob_removed is not None:
            self.blob_removed.close()

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

        # TODO:  Should add sanity checking to pack.

        self.gc.findReachable()

        def close_files_remove():
            # blank except: we might be in an IOError situation/handler
            # try our best, but don't fail
            try:
                self._tfile.close()
            except:  # noqa: E722 do not use bare 'except'
                pass
            try:
                self._file.close()
            except:  # noqa: E722 do not use bare 'except'
                pass
            try:
                os.remove(self._name + ".pack")
            except:  # noqa: E722 do not use bare 'except'
                pass
            if self.blob_removed is not None:
                self.blob_removed.close()

        # Setup the destination file and copy the metadata.
        # TODO:  rename from _tfile to something clearer.
        self._tfile = open(self._name + ".pack", "w+b")
        try:
            self._file.seek(0)
            self._tfile.write(self._file.read(self._metadata_size))

            self._copier = PackCopier(self._tfile, self.index, self.tindex)

            ipos, opos = self.copyToPacktime()
        except (OSError, IOError):
            # most probably ran out of disk space or some other IO error
            close_files_remove()
            raise  # don't succeed silently

        assert ipos == self.gc.packpos
        if ipos == opos:
            # pack didn't free any data.  there's no point in continuing.
            close_files_remove()
            return None
        self._commit_lock.acquire()
        self.locked = True
        try:
            with self._lock:
                # Re-open the file in unbuffered mode.

                # The main thread may write new transactions to the
                # file, which creates the possibility that we will
                # read a status 'c' transaction into the pack thread's
                # stdio buffer even though we're acquiring the commit
                # lock.  Transactions can still be in progress
                # throughout much of packing, and are written to the
                # same physical file but via a distinct Python file
                # object.  The code used to leave off the trailing 0
                # argument, and then on every platform except native
                # Windows it was observed that we could read stale
                # data from the tail end of the file.
                self._file.close()  # else self.gc keeps the original
                # alive & open
                self._file = open(self._path, "rb", 0)
                self._file.seek(0, 2)
                self.file_end = self._file.tell()

            if ipos < self.file_end:
                self.copyRest(ipos)

            # OK, we've copied everything. Now we need to wrap things up.
            pos = self._tfile.tell()
            self._tfile.flush()
            self._tfile.close()
            self._file.close()
            if self.blob_removed is not None:
                self.blob_removed.close()

            return pos
        except (OSError, IOError):
            # most probably ran out of disk space or some other IO error
            close_files_remove()
            if self.locked:
                self._commit_lock.release()
            raise  # don't succeed silently
        except:  # noqa: E722 do not use bare 'except'
            if self.locked:
                self._commit_lock.release()
            raise

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
                          tlen, th.tlen)
            pos += 8

        return pos, new_pos

    def copyDataRecords(self, pos, th):
        """Copy any current data records between pos and tend.

        Returns position of txn header in output file and position
        of next record in the input file.

        If any data records are copied, also write txn header (th).
        """
        copy = 0
        new_tpos = 0
        tend = pos + th.tlen
        pos += th.headerlen()
        while pos < tend:
            h = self._read_data_header(pos)
            if not self.gc.isReachable(h.oid, pos):
                if self.pack_blobs:
                    # We need to find out if this is a blob, so get the data:
                    if h.plen:
                        data = self._file.read(h.plen)
                    else:
                        data = self.fetchDataViaBackpointer(h.oid, h.back)
                    if data and self._storage.is_blob_record(data):
                        # We need to remove the blob record. Maybe we
                        # need to remove oid:

                        # But first, we need to make sure the record
                        # we're looking at isn't a dup of the current
                        # record. There's a bug in ZEO blob support that causes
                        # duplicate data records.
                        rpos = self.gc.reachable.get(h.oid)
                        is_dup = (
                            rpos and self._read_data_header(rpos).tid == h.tid)
                        if not is_dup:
                            if h.oid not in self.gc.reachable:
                                self.blob_removed.write(
                                    binascii.hexlify(h.oid)+b'\n')
                            else:
                                self.blob_removed.write(
                                    binascii.hexlify(h.oid+h.tid)+b'\n')

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
                data = self.fetchDataViaBackpointer(h.oid, h.back)

            self.writePackedDataRecord(h, data, new_tpos)

        return new_tpos, pos

    def fetchDataViaBackpointer(self, oid, back):
        """Return the data for oid via backpointer back

        If `back` is 0 or ultimately resolves to 0, return None.
        In this case, the transaction undoes the object
        creation.
        """
        if back == 0:
            return None
        data, tid = self._loadBackTxn(oid, back, 0)
        return data

    def writePackedDataRecord(self, h, data, new_tpos):
        # Update the header to reflect current information, then write
        # it to the output file.
        if data is None:
            data = b''
        h.prev = 0
        h.back = 0
        h.plen = len(data)
        h.tloc = new_tpos
        pos = self._tfile.tell()
        self.index[h.oid] = pos
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

        try:
            while 1:
                ipos = self.copyOne(ipos)
        except CorruptedDataError as err:
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
        # Release commit lock while writing to pack file
        self._commit_lock.release()
        self.locked = False
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
                data = self.fetchDataViaBackpointer(h.oid, h.back)
                if h.back:
                    prev_txn = self.getTxnFromData(h.oid, h.back)

            self._copier.copy(h.oid, h.tid, data, prev_txn,
                              pos, self._tfile.tell())

        tlen = self._tfile.tell() - pos
        assert tlen == th.tlen
        self._tfile.write(p64(tlen))
        ipos += 8

        self.index.update(self.tindex)
        self.tindex.clear()
        self._commit_lock.acquire()
        self.locked = True
        return ipos
