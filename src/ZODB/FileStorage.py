##############################################################################
#
# Copyright (c) 2001 Zope Corporation and Contributors. All Rights Reserved.
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
__version__='$Revision: 1.77 $'[11:-2]

import struct, time, os, string, base64, sys
from struct import pack, unpack
import POSException
from POSException import UndoError
from TimeStamp import TimeStamp
from lock_file import lock_file
from utils import t32, p64, U64, cp
from zLOG import LOG, BLATHER, WARNING, ERROR, PANIC, register_subsystem
register_subsystem('ZODB FS')
import BaseStorage
from cPickle import Pickler, Unpickler, loads
import ConflictResolution

try: from posix import fsync
except: fsync=None

from types import StringType

z64='\0'*8

def warn(message, *data):
    LOG('ZODB FS',WARNING, "%s  warn: %s\n" % (packed_version, (message % data)))

def error(message, *data):
    LOG('ZODB FS',ERROR,"%s ERROR: %s\n" % (packed_version, (message % data)))

def nearPanic(message, *data):
    LOG('ZODB FS',PANIC,"%s ERROR: %s\n" % (packed_version, (message % data)))

def panic(message, *data):
    message=message%data
    LOG('ZODB FS',PANIC,"%s ERROR: %s\n" % (packed_version, message))
    raise CorruptedTransactionError, message

class FileStorageError(POSException.StorageError): pass

class FileStorageFormatError(FileStorageError):
    """Invalid file format

    The format of the given file is not valid
    """

class CorruptedFileStorageError(FileStorageError,
                                POSException.StorageSystemError):
    """Corrupted file storage
    """

class CorruptedTransactionError(CorruptedFileStorageError): pass
class CorruptedDataError(CorruptedFileStorageError): pass

class FileStorageQuotaError(FileStorageError,
                            POSException.StorageSystemError):
    """File storage quota exceeded
    """

packed_version='FS21'

class FileStorage(BaseStorage.BaseStorage,
                  ConflictResolution.ConflictResolvingStorage):
    _packt=z64

    def __init__(self, file_name, create=0, read_only=0, stop=None,
                 quota=None):

        if not os.path.exists(file_name):
            create = 1

        if read_only:
            self._is_read_only = 1
            if create:
                raise ValueError, "can\'t create a read-only file"
        elif stop is not None:
            raise ValueError, "time-travel is only supported in read-only mode"

        if stop is None:
            stop='\377'*8

        # Lock the database and set up the temp file.
        if not read_only:
            try:
                f = open(file_name + '.lock', 'r+')
            except:
                f = open(file_name+'.lock', 'w+')
            lock_file(f)
            try:
                f.write(str(os.getpid()))
                f.flush()
            except:
                pass
            self._lock_file = f # so it stays open

            self._tfile = open(file_name + '.tmp', 'w+b')
        else:
            self._tfile = None

        self._file_name = file_name

        BaseStorage.BaseStorage.__init__(self, file_name)

        index, vindex, tindex, tvindex = self._newIndexes()
        self._initIndex(index, vindex, tindex, tvindex)
        
        # Now open the file
        
        if create:
            if os.path.exists(file_name):
                os.remove(file_name)
            self._file = open(file_name, 'w+b')
            self._file.write(packed_version)
        else:
            self._file = open(file_name, read_only and 'rb' or 'r+b')

        r = self._restore_index()
        if r is not None:
            index, vindex, start, maxoid, ltid = r
            self._initIndex(index, vindex, tindex, tvindex)
            self._pos, self._oid, tid = read_index(
                self._file, file_name, index, vindex, tindex, stop,
                ltid=ltid, start=start, maxoid=maxoid,
                read_only=read_only,
                )
        else:
            self._pos, self._oid, tid = read_index(
                self._file, file_name, index, vindex, tindex, stop,
                read_only=read_only,
                )
        self._ltid = tid

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
        return {}, {}, {}, {}
        
    def abortVersion(self, src, transaction):
        return self.commitVersion(src, '', transaction, abort=1)

    def _save_index(self):
        """Write the database index to a file to support quick startup
        """
        
        index_name=self.__name__+'.index'
        tmp_name=index_name+'.index_tmp'

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

    def _clear_index(self):
        index_name=self.__name__+'.index'
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
        
        if pos < 100: return 0
        file=self._file
        seek=file.seek
        read=file.read
        seek(0,2)
        if file.tell() < pos: return 0
        ltid=None

        while 1:
            seek(pos-8)
            rstl=read(8)
            tl=U64(rstl)
            pos=pos-tl-8
            if pos < 4: return 0
            seek(pos)
            tid, stl, status, ul, dl, el = unpack(">8s8scHHH", read(23))
            if not ltid: ltid=tid
            if stl != rstl: return 0 # inconsistent lengths
            if status == 'u': continue # undone trans, search back
            if status not in ' p': return 0
            if tl < (23+ul+dl+el): return 0
            tend=pos+tl
            opos=pos+(23+ul+dl+el)
            if opos==tend: continue # empty trans

            while opos < tend:
                # Read the data records for this transaction    
                seek(opos)
                h=read(42)
                oid,serial,sprev,stloc,vlen,splen = unpack(">8s8s8s8sH8s", h)
                tloc=U64(stloc)
                plen=U64(splen)
                
                dlen=42+(plen or 8)
                if vlen: dlen=dlen+(16+vlen)
    
                if opos+dlen > tend or tloc != pos: return 0

                if index.get(oid, 0) != opos: return 0
    
                opos=opos+dlen

            return ltid

    def _restore_index(self):
        """Load the database index from a file to support quick startup
        """
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
        index=info.get('index')
        pos=info.get('pos')
        oid=info.get('oid')
        vindex=info.get('vindex')
        if index is None or pos is None or oid is None or vindex is None:
            return None
        pos = long(pos)

        tid=self._sane(index, pos)
        if not tid: return None
        
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
            read=self._file.read
            seek=self._file.seek
            tfile=self._tfile
            write=tfile.write
            tindex=self._tindex
            index=self._index
            index_get=index.get

            srcpos=self._vindex_get(src, 0)
            spos=p64(srcpos)
            middle=struct.pack(">8sH8s", p64(self._pos), len(dest), z64)

            if dest:
                sd=p64(self._vindex_get(dest, 0))
                heredelta=66+len(dest)
            else:
                sd=''
                heredelta=50
                        
            here=self._pos+(tfile.tell()+self._thl)
            oids=[]
            appoids=oids.append
            tvindex=self._tvindex
            current_oids={}
            current=current_oids.has_key
            t=None
            tstatus=' '

            while srcpos:
                seek(srcpos)
                h=read(58) # oid, serial, prev(oid), tloc, vlen, plen, pnv, pv
                oid=h[:8]
                pnv=h[-16:-8]
                if index_get(oid) == srcpos:
                    # This is a current record!
                    tindex[oid]=here
                    appoids(oid)
                    write(h[:16] + spos + middle)
                    if dest:
                        tvindex[dest]=here
                        write(pnv+sd+dest)
                        sd=p64(here)

                    write(abort and pnv or spos) # data backpointer to src data
                    here=here+heredelta

                    current_oids[oid]=1

                else:
                    # Hm.  This is a non-current record.  Is there a
                    # current record for this oid?
                    if not current(oid):
                        # Nope. We're done *if* this transaction wasn't undone.
                        tloc=h[24:32]
                        if t != tloc:
                            # We haven't checked this transaction before,
                            # get it's status.
                            t=tloc
                            seek(U64(t)+16)
                            tstatus=read(1)
                            
                        if tstatus != 'u':
                            # Yee ha! We can quit
                            break

                spos=h[-8:]
                srcpos=U64(spos)

            return oids

        finally: self._lock_release()

    def getSize(self): return self._pos

    def _loada(self, oid, _index, file):
        "Read any version and return the version"
        pos=_index[oid]
        file.seek(pos)
        read=file.read
        h=read(42)
        doid,serial,prev,tloc,vlen,plen = unpack(">8s8s8s8sH8s", h)
        if vlen:
            nv = read(8) != z64
            file.seek(8,1) # Skip previous version record pointer
            version=read(vlen)
        else:
            version=''
            nv=0

        if plen != z64: return read(U64(plen)), version, nv
        return _loadBack(file, oid, read(8))[0], version, nv

    def getSerial(self, oid):
        self._lock_acquire()
        try:
            pos = self._index[oid]
            self._file.seek(pos)
            h = self._file.read(34)
            _oid = h[:8]
            if _oid != oid:
                raise CorruptedData, h
            vlen = unpack(">H", h[-2:])[0]
            if vlen:
                # If there is a version, find out its name and let
                # _load() do all the work.  This is less efficient
                # than possible, because _load() will load the pickle
                # data.  Being more efficient is too complicated.
                self._file.seek(24, 1) # skip plen, pnv, and pv
                version = self._file.read(vlen)
                pickledata, serial = self._load(oid, version,
                                                self._index, self._file)
                return serial
            return h[8:16]
        finally:
            self._lock_release()
        

    def _load(self, oid, version, _index, file):
        pos=_index[oid]
        file.seek(pos)
        read=file.read
        h=read(42)
        doid,serial,prev,tloc,vlen,plen = unpack(">8s8s8s8sH8s", h)
        if doid != oid: raise CorruptedDataError, h
        if vlen:
            pnv=read(8) # Read location of non-version data
            if (not version or len(version) != vlen or
                (read(8) # skip past version link
                 and version != read(vlen))
                ):
                return _loadBack(file, oid, pnv)

        # If we get here, then either this was not a version record,
        # or we've already read past the version data!
        if plen != z64: return read(U64(plen)), serial
        pnv=read(8)
        # We use the current serial, since that is the one that
        # will get checked when we store.
        return _loadBack(file, oid, pnv)[0], serial

    def load(self, oid, version, _stuff=None):
        self._lock_acquire()
        try: return self._load(oid, version, self._index, self._file)
        finally: self._lock_release()

    def loadSerial(self, oid, serial):
        self._lock_acquire()
        try:
            _index=self._index
            file=self._file
            seek=file.seek
            read=file.read
            pos=_index[oid]
            while 1:
                seek(pos)
                h=read(42)
                doid,dserial,prev,tloc,vlen,plen = unpack(">8s8s8s8sH8s", h)
                if doid != oid: raise CorruptedDataError, h
                if dserial == serial: break # Yeee ha!
                # Keep looking for serial
                pos=U64(prev)
                if not pos: raise KeyError, serial
                continue

            if vlen:
                pnv=read(8) # Read location of non-version data
                read(8) # skip past version link
                read(vlen) # skip version

            if plen != z64: return read(U64(plen))

            # We got a backpointer, probably from a commit.
            pnv=read(8)
            return _loadBack(file, oid, pnv)[0]
        finally: self._lock_release()
                    
    def modifiedInVersion(self, oid):
        self._lock_acquire()
        try:
            pos=self._index[oid]
            file=self._file
            seek=file.seek
            seek(pos)
            doid,serial,prev,tloc,vlen = unpack(">8s8s8s8sH", file.read(34))
            if doid != oid:
                raise CorruptedDataError, pos
            if vlen:
                seek(24,1) # skip plen, pnv, and pv
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
                file=self._file
                file.seek(old)
                read=file.read
                h=read(42)
                doid,oserial,sprev,stloc,vlen,splen = unpack(">8s8s8s8sH8s", h)
                if doid != oid: raise CorruptedDataError, h
                if vlen:
                    pnv=read(8) # non-version data pointer
                    read(8) # skip past version link
                    locked_version=read(vlen)
                    if version != locked_version:
                        raise POSException.VersionLockError, (
                            `oid`, locked_version)

                if serial != oserial:
                    data=self.tryToResolveConflict(oid, oserial, serial, data)
                    if not data:
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
            write(pack(">8s8s8s8sH8s",
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
        self._thl=23+len(u)+len(d)+len(e)
        self._nextpos=0

    def tpc_vote(self, transaction):
        self._lock_acquire()
        try:
            if transaction is not self._transaction: return
            tfile=self._tfile
            dlen=tfile.tell()
            if not dlen: return # No data in this trans
            file=self._file
            write=file.write
            tfile.seek(0)
            tid=self._serial
            user, desc, ext = self._ude
            luser=len(user)
            ldesc=len(desc)
            lext=len(ext)

            # We have to check lengths here because struct.pack
            # doesn't raise an exception on overflow!
            if luser > 65535: raise FileStorageError('user name too long')
            if ldesc > 65535: raise FileStorageError('description too long')
            if lext > 65535: raise FileStorageError('too much extension data')

            tlen=self._thl
            pos=self._pos
            file.seek(pos)
            tl=tlen+dlen
            stl=p64(tl)

            try:
                # Note that we use a status of 'c', for checkpoint.
                # If this flag isn't cleared, anything after this is
                # suspect.
                write(pack(
                    ">8s" "8s" "c"  "H"        "H"        "H"
                     ,tid, stl,'c',  luser,     ldesc,     lext,
                    ))
                if user: write(user)
                if desc: write(desc)
                if ext: write(ext)

                cp(tfile, file, dlen)

                write(stl)
                file.flush()
            except:
                # Hm, an error occured writing out the data. Maybe the
                # disk is full. We don't want any turd at the end.
                file.truncate(pos)
                raise
            
            self._nextpos=pos+(tl+8)
            
        finally: self._lock_release()
 
    def _finish(self, tid, u, d, e):
        nextpos=self._nextpos
        if nextpos:
            file=self._file

            # Clear the checkpoint flag
            file.seek(self._pos+16)
            file.write(self._tstatus)        
            file.flush()

            if fsync is not None: fsync(file.fileno())

            self._pos=nextpos

            self._index.update(self._tindex)
            self._vindex.update(self._tvindex)
        self._ltid = tid

    def _abort(self):
        if self._nextpos:
            self._file.truncate(self._pos)
            self._nextpos=0

    def undo(self, transaction_id):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self._lock_acquire()
        try:
            self._clear_index()
            transaction_id=base64.decodestring(transaction_id+'==\n')
            tid, tpos = transaction_id[:8], U64(transaction_id[8:])
            packt=self._packt
            if packt is None or packt > tid:
                raise UndoError, (
                    'Undo is currently disabled for database maintenance.<p>')

            file=self._file
            seek=file.seek
            read=file.read
            index_get=self._index_get
            unpack=struct.unpack
            seek(tpos)
            h=read(23)
            if len(h) != 23 or h[:8] != tid: 
                raise UndoError('Invalid undo transaction id')
            if h[16] == 'u': return
            if h[16] != ' ': raise UndoError
            tl=U64(h[8:16])
            ul,dl,el=unpack(">HHH", h[17:23])
            tend=tpos+tl
            pos=tpos+(23+ul+dl+el)
            t={}
            while pos < tend:
                # Read the data records for this transaction
                seek(pos)
                h=read(42)
                oid,serial,sprev,stloc,vlen,splen = unpack(">8s8s8s8sH8s", h)
                plen=U64(splen)
                prev=U64(sprev)
                dlen=42+(plen or 8)
                if vlen: dlen=dlen+(16+vlen)
                if index_get(oid, 0) != pos: raise UndoError
                pos=pos+dlen
                if pos > tend: raise UndoError
                t[oid]=prev

            seek(tpos+16)
            file.write('u')
            file.flush()
            self._index.update(t)
            return t.keys()            
        finally: self._lock_release()

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
        h=read(42)
        roid,serial,sprev,stloc,vlen,splen = unpack(">8s8s8s8sH8s", h)
        if roid != oid: raise UndoError('Invalid undo transaction id')
        if vlen:
            read(16) # skip nv pointer and version previous pointer
            version=read(vlen)
        else:
            version=''

        plen = U64(splen)
        if plen:
            data = read(plen)
        else:
            data=''
            pos=U64(read(8))

        if tpos: file.seek(tpos) # Restore temp file to end

        return serial, pos, data, version
        
    def _getVersion(self, oid, pos):
        self._file.seek(pos)
        read=self._file.read
        h=read(42)
        doid,serial,sprev,stloc,vlen,splen = unpack(">8s8s8s8sH8s", h)
        if vlen:
            h=read(16)
            return read(vlen), h[:8]
        else:
            return '',''
        
    def _getSerial(self, oid, pos):
        self._file.seek(pos+8)
        return self._file.read(8)


    def _transactionalUndoRecord(self, oid, pos, serial, pre, version):
        """Get the indo information for a data record

        Return a 5-tuple consisting of a pickle, data pointer,
        version, packed non-version data pointer, and current
        position.  If the pickle is true, then the data pointer must
        be 0, but the pickle can be empty *and* the pointer 0.

        """
        
        copy=1 # Can we just copy a data pointer
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
                raise UndoError('Current and undone versions differ')

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
                            copy=0 # we'll try to do conflict resolution
                        else:
                            # We bail if:
                            # - We don't have a previous record, which should
                            #   be impossible.
                            raise UndoError
                except KeyError:
                    # LoadBack gave us a key error. Bail.
                    raise UndoError

        version, snv = self._getVersion(oid, pre)
        if copy:
            # we can just copy our previous-record pointer forward
            return '', pre, version, snv, ipos

        try:
            # returns data, serial tuple
            bdata = _loadBack(self._file, oid, p64(pre))[0]
        except KeyError:
            # couldn't find oid; what's the real explanation for this?
            raise UndoError("_loadBack() failed for %s" % repr(oid))
        data=self.tryToResolveConflict(oid, cserial, serial, bdata, cdata)  

        if data:
            return data, 0, version, snv, ipos

        raise UndoError('Some data were modified by a later transaction')

    def transactionalUndo(self, transaction_id, transaction):
        """Undo a transaction, given by transaction_id.

        Do so by writing new data that reverses tyhe action taken by
        the transaction."""        
        # Usually, we can get by with just copying a data pointer, by
        # writing a file position rather than a pickle. Sometimes, we
        # may do conflict resolution, in which case we actually copy
        # new data that results from resolution.

        if self._is_read_only:
            raise POSException.ReadOnlyError()
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        
        self._lock_acquire()
        try:
            transaction_id=base64.decodestring(transaction_id+'==\n')
            tid, tpos = transaction_id[:8], U64(transaction_id[8:])

            seek=self._file.seek
            read=self._file.read
            unpack=struct.unpack
            write=self._tfile.write

            ostloc = p64(self._pos)
            newserial=self._serial
            here=self._pos+(self._tfile.tell()+self._thl)

            seek(tpos)
            h=read(23)
            if len(h) != 23 or h[:8] != tid: 
                raise UndoError, 'Invalid undo transaction id'
            if h[16] == 'u': return
            if h[16] != ' ':
                raise UndoError, 'non-undoable transaction'
            tl=U64(h[8:16])
            ul,dl,el=unpack(">HHH", h[17:23])
            tend=tpos+tl
            pos=tpos+(23+ul+dl+el)
            tindex={}
            failures={} # keep track of failures, cause we may succeed later
            failed=failures.has_key
            # Read the data records for this transaction
            while pos < tend:
                seek(pos)
                h=read(42)
                oid,serial,sprev,stloc,vlen,splen = unpack(">8s8s8s8sH8s", h)
                if failed(oid): del failures[oid] # second chance! 
                plen=U64(splen)
                prev=U64(sprev)
                if vlen:
                    dlen=58+vlen+(plen or 8)
                    read(16)
                    version=read(vlen)
                else:
                    dlen=42+(plen or 8)
                    version=''

                try:
                    p, prev, v, snv, ipos = self._transactionalUndoRecord(
                        oid, pos, serial, prev, version)
                except UndoError, v:
                    # Don't fail right away. We may be redeemed later!
                    failures[oid]=v
                else:
                    plen=len(p)                
                    write(pack(">8s8s8s8sH8s",
                               oid, newserial, p64(ipos), ostloc,
                               len(v), p64(plen)))
                    if v:
                        vprev=self._tvindex.get(v, 0) or self._vindex.get(v, 0)
                        write(snv+p64(vprev)+v)
                        self._tvindex[v]=here
                        odlen = 58+len(v)+(plen or 8)
                    else:
                        odlen = 42+(plen or 8)

                    if p: write(p)
                    else: write(p64(prev))
                    tindex[oid]=here
                    here=here+odlen

                pos=pos+dlen
                if pos > tend:
                    raise UndoError, 'non-undoable transaction'

            if failures: raise UndoError(failures)
            self._tindex.update(tindex)
            return tindex.keys()            

        finally: self._lock_release()

    def undoLog(self, first=0, last=-20, filter=None):
        if last < 0: last=first-last+1
        self._lock_acquire()
        try:
            packt=self._packt
            if packt is None:
                raise UndoError(
                    'Undo is currently disabled for database maintenance.<p>')
            pos=self._pos
            if pos < 39: return []
            file=self._file
            seek=file.seek
            read=file.read
            unpack=struct.unpack
            strip=string.strip
            encode=base64.encodestring
            r=[]
            append=r.append
            i=0
            while i < last and pos > 39:
                seek(pos-8)
                pos=pos-U64(read(8))-8
                seek(pos)
                h=read(23)
                tid, tl, status, ul, dl, el = unpack(">8s8scHHH", h)
                if tid < packt: break
                if status != ' ': continue
                u=ul and read(ul) or ''
                d=dl and read(dl) or ''
                d={'id': encode(tid+p64(pos))[:22],
                   'time': TimeStamp(tid).timeTime(),
                   'user_name': u, 'description': d}
                if el:
                    try: 
                        e=loads(read(el))
                        d.update(e)
                    except: pass
                if filter is None or filter(d):
                    if i >= first: append(d)
                    i=i+1
                
            return r
        finally: self._lock_release()

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
                    # get it's status.
                    t=tloc
                    seek(U64(t)+16)
                    tstatus=read(1)

                if tstatus != 'u': return 1

                spos=h[-8:]
                srcpos=U64(spos)

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
            pos=self._index[oid]
            wantver=version

            while 1:
                if len(r) >= size: return r
                seek(pos)
                h=read(42)
                doid,serial,prev,tloc,vlen,plen = unpack(">8s8s8s8sH8s", h)
                prev=U64(prev)

                if vlen:
                    nv = read(8) != z64
                    file.seek(8,1) # Skip previous version record pointer
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

                seek(U64(tloc))
                h=read(23)
                tid, stl, status, ul, dl, el = unpack(">8s8scHHH",h)
                user_name=read(ul)
                description=read(dl)
                if el: d=loads(read(el))
                else: d={}

                d['time']=TimeStamp(serial).timeTime()
                d['user_name']=user_name
                d['description']=description
                d['serial']=serial
                d['version']=version
                d['size']=U64(plen)

                if filter is None or filter(d):
                    r.append(d)

                if prev: pos=prev
                else: return r
        finally: self._lock_release()

    def _redundant_pack(self, file, pos):
        assert pos > 8, pos
        file.seek(pos-8)
        p=U64(file.read(8))
        file.seek(pos-p+8)
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
        # Ugh, this seems long
        
        packing=1 # are we in the packing phase (or the copy phase)
        locked=0
        _lock_acquire=self._lock_acquire
        _lock_release=self._lock_release
        _commit_lock_acquire=self._commit_lock_acquire
        _commit_lock_release=self._commit_lock_release
        index, vindex, tindex, tvindex = self._newIndexes()
        name=self.__name__
        file=open(name, 'rb')
        stop=`apply(TimeStamp, time.gmtime(t)[:5]+(t%60,))`
        if stop==z64: raise FileStorageError, 'Invalid pack time'

        # Record pack time so we don't undo while packing
        _lock_acquire()
        try:
            if self._packt != z64:
                # Already packing.
                raise FileStorageError, 'Already packing'
            self._packt=stop
        finally:
            _lock_release()

        try:
            ##################################################################
            # Step 1, get index as of pack time that
            # includes only referenced objects.

            packpos, maxoid, ltid = read_index(
                file, name, index, vindex, tindex, stop,
                read_only=1,
                )

            if packpos == 4:
                return
            if self._redundant_pack(file, packpos):
                raise FileStorageError, (
                    'The database has already been packed to a later time\n'
                    'or no changes have been made since the last pack')
    
            rootl=[z64]
            pop=rootl.pop
            pindex={}
            referenced=pindex.has_key
            _load=self._load
            _loada=self._loada
            v=None
            while rootl:
                oid=pop()
                if referenced(oid): continue
                try:
                    p, v, nv = _loada(oid, index, file)
                    referencesf(p, rootl)
                    if nv:
                        p, serial = _load(oid, '', index, file)
                        referencesf(p, rootl)
    
                    pindex[oid]=index[oid]
                except:
                    pindex[oid]=0
                    error('Bad reference to %s', `(oid,v)`)
    
            spackpos=p64(packpos)
    
            ##################################################################
            # Step 2, copy data and compute new index based on new positions.
            index, vindex, tindex, tvindex = self._newIndexes()
    
            ofile=open(name+'.pack', 'w+b')
    
            # Index for non-version data.  This is a temporary structure
            # to reduce I/O during packing
            nvindex={}
    
            # Cache a bunch of methods
            seek=file.seek
            read=file.read
            oseek=ofile.seek
            write=ofile.write
    
            index_get=index.get
            vindex_get=vindex.get
            pindex_get=pindex.get
    
            # Initialize, 
            pv=z64
            offset=0L  # the amount of space freed by packing
            pos=opos=4L
            oseek(0)
            write(packed_version)

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

                # Check for end of packed records
                if packing and pos >= packpos:
                    # OK, we're done with the old stuff, now we have
                    # to get the lock so we can copy the new stuff!
                    offset=pos-opos
                    if offset <= 0:
                        # we didn't free any space, there's no point in
                        # continuing
                        ofile.close()
                        file.close()
                        os.remove(name+'.pack')
                        return
                    
                    packing=0
                    _commit_lock_acquire()
                    _lock_acquire()
                    locked=1
                    self._packt=None # Prevent undo until we're done

                # Read the transaction record
                seek(pos)
                h=read(23)
                if len(h) < 23: break
                tid, stl, status, ul, dl, el = unpack(">8s8scHHH",h)
                if status=='c':
                    # Oops. we found a checkpoint flag.
                    break
                tl=U64(stl)
                tpos=pos
                tend=tpos+tl

                if status=='u':
                    if not packing:
                        # We rely below on a constant offset for unpacked
                        # records. This assumption holds only if we copy
                        # undone unpacked data. This is lame, but necessary
                        # for now to squash a bug.
                        write(h)
                        tl=tl+8
                        write(read(tl-23))
                        opos=opos+tl
                        
                    # Undone transaction, skip it
                    pos=tend+8
                    continue

                otpos=opos # start pos of output trans

                # write out the transaction record
                status=packing and 'p' or ' '
                write(h[:16]+status+h[17:])
                thl=ul+dl+el
                h=read(thl)
                if len(h) != thl:
                    raise 'Pack Error', opos
                write(h)
                thl=23+thl
                pos=tpos+thl
                opos=otpos+thl

                while pos < tend:
                    # Read the data records for this transaction

                    seek(pos)
                    h=read(42)
                    oid,serial,sprev,stloc,vlen,splen = unpack(
                        ">8s8s8s8sH8s", h)
                    plen=U64(splen)
                    dlen=42+(plen or 8)

                    if vlen:
                        dlen=dlen+(16+vlen)
                        if packing and pindex_get(oid, 0) != pos:
                            # This is not the most current record, or
                            # the oid is no longer referenced so skip it.
                            pos=pos+dlen
                            continue

                        pnv=U64(read(8))
                        # skip position of previous version record
                        seek(8,1)
                        version=read(vlen)
                        pv=p64(vindex_get(version, 0))
                        vindex[version]=opos
                    else:
                        if packing:
                            ppos=pindex_get(oid, 0)
                            if ppos != pos:
                                
                                if not ppos:
                                    # This object is no longer referenced
                                    # so skip it.
                                    pos=pos+dlen
                                    continue
                                
                                # This is not the most current record
                                # But maybe it's the most current committed
                                # record.
                                seek(ppos)
                                ph=read(42)
                                pdoid,ps,pp,pt,pvlen,pplen = unpack(
                                    ">8s8s8s8sH8s", ph)
                                if not pvlen:
                                    # The most current record is committed, so
                                    # we can toss this one
                                    pos=pos+dlen
                                    continue
                                pnv=read(8)
                                pnv=_loadBackPOS(file, oid, pnv)
                                if pnv > pos:
                                    # The current non version data is later,
                                    # so this isn't the current record
                                    pos=pos+dlen
                                    continue

                                # Ok, we've gotten this far, so we have
                                # the current record and we're ready to
                                # read the pickle, but we're in the wrong
                                # place, after wandering around to figure
                                # out is we were current. Seek back
                                # to pickle data:
                                seek(pos+42)

                            nvindex[oid]=opos

                    tindex[oid]=opos
                    
                    opos=opos+dlen
                    pos=pos+dlen

                    if plen:
                        p=read(plen)
                    else:
                        p=read(8)
                        if packing:
                            # When packing we resolve back pointers!
                            p, serial = _loadBack(file, oid, p)
                            plen=len(p)
                            opos=opos+plen-8
                            splen=p64(plen)
                        else:
                            p=U64(p)
                            if p < packpos:
                                # We have a backpointer to a
                                # non-packed record. We have to be
                                # careful.  If we were pointing to a
                                # current record, then we should still
                                # point at one, otherwise, we should
                                # point at the last non-version record.
                                ppos=pindex_get(oid, 0)
                                if ppos:
                                    if ppos==p:
                                        # we were pointing to the
                                        # current record
                                        p=index[oid]
                                    else:
                                        p=nvindex[oid]
                                else:
                                    # Oops, this object was modified
                                    # in a version in which it was deleted.
                                    # Hee hee. It doesn't matter what we
                                    # use cause it's not reachable any more.
                                    p=0
                            else:
                                # This points back to a non-packed record.
                                # Just adjust for the offset
                                p=p-offset
                            p=p64(p)
                            
                    sprev=p64(index_get(oid, 0))
                    write(pack(">8s8s8s8sH8s",
                               oid,serial,sprev,p64(otpos),vlen,splen))
                    if vlen:
                        if not pnv:
                            write(z64)
                        else:
                            if pnv < packpos:
                                # we need to point to the packed
                                # non-version rec
                                pnv=nvindex[oid]
                            else:
                                # we just need to adjust the pointer
                                # with the offset
                                pnv=pnv-offset
                                
                            write(p64(pnv))
                        write(pv)
                        write(version)

                    write(p)

                # skip the (intentionally redundant) transaction length
                pos=pos+8

                if locked:
                    # temporarily release the lock to give other threads
                    # a chance to do some work!
                    _commit_lock_release()
                    _lock_release()
                    locked=0

                index.update(tindex) # Record the position
                tindex.clear()

                # Now, maybe we need to hack or delete the transaction
                otl=opos-otpos
                if otl != tl:
                    # Oops, what came out is not what came in!

                    # Check for empty:
                    if otl==thl:
                        # Empty, slide back over the header:
                        opos=otpos
                        oseek(opos)
                    else:
                        # Not empty, but we need to adjust transaction length
                        # and update the status
                        oseek(otpos+8)
                        otl=p64(otl)
                        write(otl+status)
                        oseek(opos)
                        write(otl)
                        opos=opos+8

                else:
                    write(p64(otl))
                    opos=opos+8


                if not packing:
                    # We are in the copying phase. We need to get the lock
                    # again to avoid someone writing data while we read it.
                    _commit_lock_acquire()
                    _lock_acquire()
                    locked=1


            # OK, we've copied everything. Now we need to wrap things
            # up.

            # Hack the files around.
            name=self.__name__

            ofile.flush()
            ofile.close()
            file.close()
            self._file.close()
            try:
                if os.path.exists(name+'.old'):
                    os.remove(name+'.old')
                os.rename(name, name+'.old')
            except:
                # Waaa
                self._file=open(name,'r+b')
                raise

            # OK, we're beyond the point of no return
            os.rename(name+'.pack', name)
            self._file=open(name,'r+b')
            self._initIndex(index, vindex, tindex, tvindex)
            self._pos=opos
            self._save_index()

        finally:

            if locked:
                _commit_lock_release()
                _lock_release()

            _lock_acquire()
            self._packt=z64
            _lock_release()

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
        self._file.seek(pos)
        # first 8 bytes are oid, second 8 bytes are serialno
        h = self._file.read(16)
        if len(h) < 16:
            raise CorruptedDataError, h
        if h[:8] != oid:
            h = h + self._file.read(26) # get rest of header
            raise CorruptedDataError, h
        return h[8:]

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
    packpos=opos

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
        h=read(23)
        if len(h) < 23: break
        tid, stl, status, ul, dl, el = unpack(">8s8scHHH",h)
        if status=='c': break # Oops. we found a checkpoint flag.            
        tl=U64(stl)
        tpos=pos
        tend=tpos+tl

        otpos=opos # start pos of output trans

        thl=ul+dl+el
        h2=read(thl)
        if len(h2) != thl: raise 'Pack Error', opos

        # write out the transaction record
        seek(opos)
        write(h)
        write(h2)

        thl=23+thl
        pos=tpos+thl
        opos=otpos+thl

        while pos < tend:
            # Read the data records for this transaction
            seek(pos)
            h=read(42)
            oid,serial,sprev,stloc,vlen,splen = unpack(">8s8s8s8sH8s", h)
            plen=U64(splen)
            dlen=42+(plen or 8)

            if vlen:
                dlen=dlen+(16+vlen)
                pnv=U64(read(8))
                # skip position of previous version record
                seek(8,1)
                version=read(vlen)
                pv=p64(vindex_get(version, 0))
                if status != 'u': vindex[version]=opos

            tindex[oid]=opos

            if plen: p=read(plen)
            else:
                p=read(8)
                p=U64(p)
                if p >= p2: p=p-offset
                elif p >= p1:
                    # Ick, we're in trouble. Let's bail
                    # to the index and hope for the best
                    p=index_get(oid, 0)
                p=p64(p)

            # WRITE
            seek(opos)
            sprev=p64(index_get(oid, 0))
            write(pack(">8s8s8s8sH8s",
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
        l=U64(read(8))
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
    vndexpos=vindex.get

    pos=start
    seek(start)
    unpack=struct.unpack
    tid='\0'*7+'\1'

    while 1:
        # Read the transaction record
        h=read(23)
        if not h: break
        if len(h) != 23:
            if not read_only:
                warn('%s truncated at %s', name, pos)
                seek(pos)
                file.truncate()
            break

        tid, stl, status, ul, dl, el = unpack(">8s8scHHH",h)
        if el < 0: el=t32-el

        if tid <= ltid:
            warn("%s time-stamp reduction at %s", name, pos)
        ltid=tid

        tl=U64(stl)

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

        if tl < (23+ul+dl+el):
            # We're in trouble. Find out if this is bad data in the
            # middle of the file, or just a turd that Win 9x dropped
            # at the end when the system crashed.
            # Skip to the end and read what should be the transaction length
            # of the last transaction.
            seek(-8, 2)
            rtl=U64(read(8))
            # Now check to see if the redundant transaction length is
            # reasonable:
            if file_size - rtl < pos or rtl < 23:
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

        pos=tpos+(23+ul+dl+el)
        while pos < tend:
            # Read the data records for this transaction

            seek(pos)
            h=read(42)
            oid,serial,sprev,stloc,vlen,splen = unpack(">8s8s8s8sH8s", h)
            prev=U64(sprev)
            tloc=U64(stloc)
            plen=U64(splen)
            
            dlen=42+(plen or 8)
            tindex[oid]=pos
            
            if vlen:
                dlen=dlen+(16+vlen)
                seek(8,1)
                pv=U64(read(8))
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

        _maxoid = max(tindex.keys()) # in 2.2, just max(tindex)
        maxoid = max(_maxoid, maxoid)
        index.update(tindex)

        tindex.clear()

    return pos, maxoid, ltid


def _loadBack(file, oid, back):
    seek=file.seek
    read=file.read
    
    while 1:
        old=U64(back)
        if not old: raise KeyError, oid
        seek(old)
        h=read(42)
        doid,serial,prev,tloc,vlen,plen = unpack(">8s8s8s8sH8s", h)

        if vlen: seek(vlen+16,1)
        if plen != z64: return read(U64(plen)), serial
        back=read(8) # We got a back pointer!

def _loadBackPOS(file, oid, back):
    """Return the position of the record containing the data used by
    the record at the given position (back)."""
    seek=file.seek
    read=file.read
    
    while 1:
        old=U64(back)
        if not old: raise KeyError, oid
        seek(old)
        h=read(42)
        doid,serial,prev,tloc,vlen,plen = unpack(">8s8s8s8sH8s", h)
        if vlen: seek(vlen+16,1)
        if plen != z64: return old
        back=read(8) # We got a back pointer!

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
    _ltid=z64
    
    def __init__(self, file, start=None, stop=None):
        if isinstance(file, StringType):
            file = open(file, 'rb')
        self._file = file
        if file.read(4) != packed_version:
            raise FileStorageFormatError, name
        file.seek(0,2)
        self._file_size = file.tell()
        self._pos = 4L
        assert start is None or isinstance(start, StringType)
        assert stop is None or isinstance(stop, StringType)
        if start:
            self._skip_to_start(start)
        self._stop = stop

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
            tl = U64(stl)
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
                          self._file.name, pos, U64(rtl), U64(stl))

    def next(self, index=0):
        file=self._file
        seek=file.seek
        read=file.read
        pos=self._pos

        LOG("ZODB FS", BLATHER, "next(%d)" % index)
        while 1:
            # Read the transaction record
            seek(pos)
            h=read(23)
            if len(h) < 23: break

            tid, stl, status, ul, dl, el = unpack(">8s8scHHH",h)
            if el < 0: el=t32-el

            if tid <= self._ltid:
                warn("%s time-stamp reduction at %s", self._file.name, pos)
            self._ltid=tid

            tl=U64(stl)

            if pos+(tl+8) > self._file_size or status=='c':
                # Hm, the data were truncated or the checkpoint flag wasn't
                # cleared.  They may also be corrupted,
                # in which case, we don't want to totally lose the data.
                warn("%s truncated, possibly due to damaged records at %s",
                     self._file.name, pos)
                break

            if status not in ' up':
                warn('%s has invalid status, %s, at %s', self._file.name,
                     status, pos)

            if tl < (23+ul+dl+el):
                # We're in trouble. Find out if this is bad data in
                # the middle of the file, or just a turd that Win 9x
                # dropped at the end when the system crashed.  Skip to
                # the end and read what should be the transaction
                # length of the last transaction.
                seek(-8, 2)
                rtl=U64(read(8))
                # Now check to see if the redundant transaction length is
                # reasonable:
                if self._file_size - rtl < pos or rtl < 23:
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

            if self._stop is not None:
                LOG("ZODB FS", BLATHER,
                    ("tid %x > stop %x ? %d" %
                     (U64(tid), U64(self._stop), tid > self._stop)))
            if self._stop is not None and tid > self._stop:
                raise IndexError, index

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

            pos=tpos+(23+ul+dl+el)
            user=read(ul)
            description=read(dl)
            if el:
                try: e=loads(read(el))
                except: e={}
            else: e={}

            result=RecordIterator(
                tid, status, user, description, e,
                pos, (tend, file, seek, read,
                      tpos,
                      )
                )

            pos=tend

            # Read the (intentionally redundant) transaction length
            seek(pos)
            h=read(8)
            if h != stl:
                warn("%s redundant transaction length check failed at %s",
                     self._file.name, pos)
                break
            self._pos=pos+8

            return result

        raise IndexError, index
    
class RecordIterator(Iterator, BaseStorage.TransactionRecord):
    """Iterate over the transactions in a FileStorage file.
    """
    def __init__(self, tid, status, user, desc, ext, pos, stuff):
        self.tid=tid
        self.status=status
        self.user=user
        self.description=desc
        self._extension=ext
        self._pos=pos
        self._stuff = stuff

    def next(self, index=0):
        name=''
        pos = self._pos
        tend, file, seek, read, tpos = self._stuff
        while pos < tend:
            # Read the data records for this transaction

            seek(pos)
            h=read(42)
            oid,serial,sprev,stloc,vlen,splen = unpack(">8s8s8s8sH8s", h)
            prev=U64(sprev)
            tloc=U64(stloc)
            plen=U64(splen)

            dlen=42+(plen or 8)

            if vlen:
                dlen=dlen+(16+vlen)
                seek(8,1)
                pv=U64(read(8))
                version=read(vlen)
            else:
                version=''

            if pos+dlen > tend or tloc != tpos:
                warn("%s data record exceeds transaction record at %s",
                     name, pos)
                break

            self._pos=pos+dlen
            if plen: p=read(plen)
            else:
                p=read(8)
                p=_loadBack(file, oid, p)[0]
                
            r=Record(oid, serial, version, p)
            
            return r
        
        raise IndexError, index

class Record(BaseStorage.DataRecord):
    """An abstract database record
    """
    def __init__(self, *args):
        self.oid, self.serial, self.version, self.data = args
