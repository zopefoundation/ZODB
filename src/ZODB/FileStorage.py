##############################################################################
# 
# Zope Public License (ZPL) Version 1.0
# -------------------------------------
# 
# Copyright (c) Digital Creations.  All rights reserved.
# 
# This license has been certified as Open Source(tm).
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
# 1. Redistributions in source code must retain the above copyright
#    notice, this list of conditions, and the following disclaimer.
# 
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions, and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
# 
# 3. Digital Creations requests that attribution be given to Zope
#    in any manner possible. Zope includes a "Powered by Zope"
#    button that is installed by default. While it is not a license
#    violation to remove this button, it is requested that the
#    attribution remain. A significant investment has been put
#    into Zope, and this effort will continue if the Zope community
#    continues to grow. This is one way to assure that growth.
# 
# 4. All advertising materials and documentation mentioning
#    features derived from or use of this software must display
#    the following acknowledgement:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    In the event that the product being advertised includes an
#    intact Zope distribution (with copyright and license included)
#    then this clause is waived.
# 
# 5. Names associated with Zope or Digital Creations must not be used to
#    endorse or promote products derived from this software without
#    prior written permission from Digital Creations.
# 
# 6. Modified redistributions of any form whatsoever must retain
#    the following acknowledgment:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    Intact (re-)distributions of any official Zope release do not
#    require an external acknowledgement.
# 
# 7. Modifications are encouraged but must be packaged separately as
#    patches to official Zope releases.  Distributions that do not
#    clearly separate the patches from the original work must be clearly
#    labeled as unofficial distributions.  Modifications which do not
#    carry the name Zope may be packaged in any form, as long as they
#    conform to all of the clauses above.
# 
# 
# Disclaimer
# 
#   THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS ``AS IS'' AND ANY
#   EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
#   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL CREATIONS OR ITS
#   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
#   USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#   ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#   OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
#   OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
#   SUCH DAMAGE.
# 
# 
# This software consists of contributions made by Digital Creations and
# many individuals on behalf of Digital Creations.  Specific
# attributions are listed in the accompanying credits file.
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
__version__='$Revision: 1.37 $'[11:-2]

import struct, time, os, bpthread, string, base64, sys
from struct import pack, unpack
from cPickle import loads
import POSException
from TimeStamp import TimeStamp
from lock_file import lock_file
from utils import t32, p64, u64, cp
from zLOG import LOG, WARNING, ERROR, PANIC, register_subsystem
register_subsystem('ZODB FS')
import BaseStorage
from cPickle import Pickler, Unpickler

try: from posix import fsync
except: fsync=None

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

class FileStorage(BaseStorage.BaseStorage):
    _packt=z64

    def __init__(self, file_name, create=0, read_only=0, stop=None,
                 quota=None):

        if not os.path.exists(file_name): create = 1

        if read_only:
            if create: raise ValueError, "can\'t create a read-only file"
        elif stop is not None:
            raise ValueError, "time-travel is only supported in read-only mode"

        if stop is None: stop='\377'*8

        # Lock the database and set up the temp file.
        if not read_only:
            try: f=open(file_name+'.lock', 'r+')
            except: f=open(file_name+'.lock', 'w+')
            lock_file(f)
            try:
                f.write(str(os.getpid()))
                f.flush()
            except: pass
            self._lock_file=f # so it stays open

            self._tfile=open(file_name+'.tmp','w+b')

        else:

            self._tfile=None

        BaseStorage.BaseStorage.__init__(self, file_name)

        index, vindex, tindex, tvindex = self._newIndexes()

        self._initIndex(index, vindex, tindex, tvindex)
        
        # Now open the file
        
        if create:
            if os.path.exists(file_name): os.remove(file_name)
            file=open(file_name,'w+b')
            file.write(packed_version)
        else:
            file=open(file_name, read_only and 'rb' or 'r+b')

        self._file=file

        r=self._restore_index()
        if r:
            index, vindex, start, maxoid, ltid = r
            self._initIndex(index, vindex, tindex, tvindex)
            self._pos, self._oid, tid = read_index(
                file, file_name, index, vindex, tindex, stop,
                ltid=ltid, start=start, maxoid=maxoid,
                )
        else:        
            self._pos, self._oid, tid = read_index(
                file, file_name, index, vindex, tindex, stop)

        self._ts=tid=TimeStamp(tid)
        t=time.time()
        t=apply(TimeStamp,(time.gmtime(t)[:5]+(t%60,)))
        if tid > t:
            warn("%s Database records in the future", file_name);
            if tid.timeTime() - t.timeTime() > 86400*30:
                # a month in the future? This is bogus, use current time
                self._ts=t

        self._quota=quota
            

    def _initIndex(self, index, vindex, tindex, tvindex):
        self._index=index
        self._vindex=vindex
        self._tindex=tindex
        self._tvindex=tvindex
        self._index_get=index.get
        self._vindex_get=vindex.get
        self._tappend=tindex.append


    def __len__(self): return len(self._index)

    def _newIndexes(self): return {}, {}, [], {}
        
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
            try: os.unlink(index_name)
            except: pass
            os.rename(tmp_name, index_name)
        except: pass

    def _clear_index(self):
        index_name=self.__name__+'.index'
        if os.path.exists(index_name):
            os.unlink(index_name)

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
            tl=u64(rstl)
            pos=pos-tl-8
            if pos < 4: return 0
            seek(pos)
            tid, stl, status, ul, dl, el = unpack(">8s8scHHH", read(23))
            if not ltid: ltid=tid
            if stl != rstl: return 0 # inconsistent lengths
            if status == 'u': continue # undone trans, search back
            if status not in ' p': return 0
            if ul > tl or dl > tl or el > tl or tl < (23+ul+dl+el): return 0
            tend=pos+tl
            opos=pos+23+ul+dl+el
            if opos==tend: continue # empty trans

            while opos < tend:
                # Read the data records for this transaction    
                seek(opos)
                h=read(42)
                oid,serial,sprev,stloc,vlen,splen = unpack(">8s8s8s8sH8s", h)
                tloc=u64(stloc)
                plen=u64(splen)
                
                dlen=42+(plen or 8)
                if vlen: dlen=dlen+16+vlen
    
                if opos+dlen > tend or tloc != pos: return 0

                if index.get(oid,0) != opos: return 0
    
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

        info=p.load()
        index=info.get('index', None)
        pos=info.get('pos', None)
        oid=info.get('oid', None)
        vindex=info.get('vindex', None)
        if index is None or pos is None or oid is None or vindex is None:
            return None

        tid=self._sane(index, pos)
        if not tid: return None
        
        return index, vindex, pos, oid, tid

    def close(self):
        self._file.close()
        self._lock_file.close()
        self._tfile.close()
        self._save_index()
        
    def commitVersion(self, src, dest, transaction, abort=None):
        # We are going to commit by simply storing back pointers.

        if dest and abort:
            raise 'VersionCommitError', (
                'Internal error, can\'t abort to a version')
        
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        
        self._lock_acquire()
        try:
            file=self._file
            read=file.read
            seek=file.seek
            tfile=self._tfile
            write=tfile.write
            tappend=self._tappend
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
                        
            here=tfile.tell()+self._pos+self._thl
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
                if index_get(oid, None) == srcpos:
                    # This is a current record!
                    tappend((oid,here))
                    appoids(oid)
                    write(h[:16] + spos + middle)
                    if dest:
                        tvindex[dest]=here
                        write(pnv+sd+dest)
                        sd=p64(here)

                    write(abort and pnv or spos) # data backpointer to src data
                    here=here+heredelta

                    if h[16:24] != pnv:
                        # This is not the first current record, so mark it
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
                            seek(u64(t)+16)
                            tstatus=read(1)
                            
                        if tstatus != 'u':
                            # Yee ha! We can quit
                            break
                        
                    elif h[16:24] == pnv:
                        # This is the first current record, so unmark it.
                        # Note that we don't need to check if this was
                        # undone.  If it *was* undone, then there must
                        # be a later record that is the first record, or
                        # there isn't a current record.  In either case,
                        # we can't be in this branch. :)
                        del current_oids[oid]
                    
                spos=h[-8:]
                srcpos=u64(spos)

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

        if plen != z64: return read(u64(plen)), version, nv
        return _loadBack(file, oid, read(8))[0], version, nv

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
        if plen != z64: return read(u64(plen)), serial
        pnv=read(8)
        # We use the current serial, since that is the one that
        # will get checked when we store.
        return _loadBack(file, oid, pnv)[0], serial

    def load(self, oid, version, _stuff=None):
        self._lock_acquire()
        try: return self._load(oid, version, self._index, self._file)
        finally: self._lock_release()

    def loadSerial(self, oid, serial):
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
            pos=u64(prev)
            if not pos: raise KeyError, serial
            continue
            
        if vlen:
            pnv=read(8) # Read location of non-version data
            read(8) # skip past version link
            read(vlen) # skip version

        if plen != z64: return read(u64(plen))

        # We got a backpointer, probably from a commit.
        pnv=read(8)
        return _loadBack(file, oid, pnv)[0]
                    
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

                if serial != oserial: raise POSException.ConflictError, (
                    serial, oserial)

            tfile=self._tfile
            write=tfile.write
            pos=self._pos
            here=tfile.tell()+pos+self._thl
            self._tappend((oid, here))
            serial=self._serial
            write(pack(">8s8s8s8sH8s",
                       oid,serial,p64(old),p64(pos),
                       len(version),p64(len(data))
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
            if quota is not None and tfile.tell()+pos+self._thl > quota:
                raise FileStorageQuotaError, (
                    'The storage quota has been exceeded.')

            return serial
        
        finally: self._lock_release()

    def supportsUndo(self): return 1
    def supportsVersions(self): return 1

    def _clear_temp(self):
        del self._tindex[:]
        self._tvindex.clear()
        self._tfile.seek(0)

    def _begin(self, tid, u, d, e):
        self._thl=23+len(u)+len(d)+len(e)

    def _finish(self, tid, u, d, e):
        tfile=self._tfile
        dlen=tfile.tell()
        if not dlen: return # No data in this trans
        file=self._file
        write=file.write
        tfile.seek(0)
        id=self._serial
        user, desc, ext = self._ude

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
                 ,id, stl, 'c', len(user), len(desc), len(ext),
                ))
            if user: write(user)
            if desc: write(desc)
            if ext: write(ext)
    
            cp(tfile, file, dlen)
    
            write(stl)
    
            # OK, not clear the checkpoint flag
            file.seek(pos+16)
            write(' ')        
            file.flush()

            if fsync is not None:
                fsync(file.fileno())

        except:
            # Hm, an error occured writing out the data. Maybe the
            # disk is full. We don't want any turd at the end.
            file.truncate(pos)
        
        self._pos=pos+tl+8

        index=self._index
        for oid, pos in self._tindex: index[oid]=pos

        self._vindex.update(self._tvindex)


    def undo(self, transaction_id):
        self._lock_acquire()
        try:
            self._clear_index()
            transaction_id=base64.decodestring(transaction_id+'==\n')
            tid, tpos = transaction_id[:8], u64(transaction_id[8:])
            packt=self._packt
            if packt is None or packt > tid:
                raise POSException.UndoError, (
                    'Undo is currently disabled for database maintenance.<p>')

            file=self._file
            seek=file.seek
            read=file.read
            index_get=self._index_get
            unpack=struct.unpack
            seek(tpos)
            h=read(23)
            if len(h) != 23 or h[:8] != tid: 
                raise POSException.UndoError, 'Invalid undo transaction id'
            if h[16] == 'u': return
            if h[16] != ' ':
                raise POSException.UndoError, 'Undoable transaction'
            tl=u64(h[8:16])
            ul,dl,el=unpack(">HHH", h[17:23])
            tend=tpos+tl
            pos=tpos+23+ul+dl+el
            t={}
            while pos < tend:
                # Read the data records for this transaction
                seek(pos)
                h=read(42)
                oid,serial,sprev,stloc,vlen,splen = unpack(">8s8s8s8sH8s", h)
                plen=u64(splen)
                prev=u64(sprev)
                dlen=42+(plen or 8)
                if vlen: dlen=dlen+16+vlen
                if index_get(oid,0) != pos:
                    raise POSException.UndoError, 'Undoable transaction'
                pos=pos+dlen
                if pos > tend:
                    raise POSException.UndoError, 'Undoable transaction'
                t[oid]=prev

            seek(tpos+16)
            file.write('u')
            file.flush()
            index=self._index
            for oid, pos in t.items(): index[oid]=pos
            return t.keys()            
        finally: self._lock_release()

    def undoLog(self, first, last, filter=None):
        self._lock_acquire()
        try:
            packt=self._packt
            if packt is None:
                raise POSException.UndoError, (
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
                pos=pos-u64(read(8))-8
                if i < first:
                    i = i+1
                    continue
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
                    append(d)
                    i=i+1
                
            return r
        finally: self._lock_release()

    def versionEmpty(self, version):
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

    def history(self, oid, version=None, length=1, filter=None):
        r=[]
        file=self._file
        seek=file.seek
        read=file.read
        pos=self._index[oid]
        wantver=version

        while 1:
            if len(r) >= length: return r
            seek(pos)
            h=read(42)
            doid,serial,prev,tloc,vlen,plen = unpack(">8s8s8s8sH8s", h)
            prev=u64(prev)

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
            
            seek(u64(tloc))
            h=read(23)
            tid, stl, status, ul, dl, el = unpack(">8s8scHHH",h)
            if el < 0: el=t32-el
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


    def pack(self, t, referencesf):
        """Copy data from the current database file to a packed file
    
        Non-current records from transactions with time-stamp strings less
        than packtss are ommitted. As are all undone records.
    
        Also, data back pointers that point before packtss are resolved and
        the associated data are copied, since the old records are not copied.
        """

        # Ugh, this seems long
        
        packing=1 # are we in the packing phase (or the copy phase)
        locked=0
        _lock_acquire=self._lock_acquire
        _lock_release=self._lock_release
        index, vindex, tindex, tvindex = self._newIndexes()
        name=self.__name__
        file=open(name, 'r+b')
        stop=`apply(TimeStamp, time.gmtime(t)[:5]+(t%60,))`
        if stop==z64: raise FileStorageError, 'Invalid pack time'

        try:
            ##################################################################
            # Step 1, get index as of pack time that
            # includes only referenced objects.

            # Record pack time so we don't undo while packing
            _lock_acquire()
            locked=1
            if self._packt != z64:
                raise FileStorageError, 'Already packing'
            self._packt=stop
            _lock_release()
            locked=0
            
            packpos, maxoid, ltid = read_index(
                file, name, index, vindex, tindex, stop)
    
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
    
            tappend=tindex.append
            index_get=index.get
            vindex_get=vindex.get
            pindex_get=pindex.get
    
            # Initialize, 
            pv=z64
            offset=0  # the amount of spaec freed by packing
            pos=opos=4
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
                if el < 0: el=t32-el
                tl=u64(stl)
                tpos=pos
                tend=tpos+tl

                if status=='u':
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
                    plen=u64(splen)
                    dlen=42+(plen or 8)

                    if vlen:
                        dlen=dlen+16+vlen
                        if packing and pindex_get(oid,0) != pos:
                            # This is not the most current record, or
                            # the oid is no longer referenced so skip it.
                            pos=pos+dlen
                            continue

                        pnv=u64(read(8))
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

                    tappend((oid,opos))
                    
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
                            p=u64(p)
                            if p < packpos:
                                # We have a backpointer to a
                                # non-packed record. We have to be
                                # careful.  If we were pointing to a
                                # current record, then we should still
                                # point at one, otherwise, we should
                                # point at the last non-version record.
                                if pindex[oid]==p:
                                    # we were pointing to the
                                    # current record
                                    p=index[oid]
                                else:
                                    p=nvindex[oid]
                            else:
                                # This points back to a non-packed record.
                                # Just adjust for the offset
                                p=p-offset
                            p=p64(p)
                            
                    sprev=p64(index_get(oid,0))
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
                    _lock_release()
                    locked=0

                for oid, p in tindex:
                    index[oid]=p # Record the position

                del tindex[:]

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
                    # We are in the copying phase.  Lets update the
                    # pack time and release the lock so others can write.
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

            if locked: _lock_release()

            _lock_acquire()
            self._packt=z64
            _lock_release()

def shift_transactions_forward(index, vindex, tindex, file, pos, opos):
    """Copy transactions forward in the data file

    This might be done as part of a recovery effort
    """

    # Cache a bunch of methods
    seek=file.seek
    read=file.read
    write=file.write

    tappend=tindex.append
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
        if el < 0: el=t32-el
        tl=u64(stl)
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
            plen=u64(splen)
            dlen=42+(plen or 8)

            if vlen:
                dlen=dlen+16+vlen
                pnv=u64(read(8))
                # skip position of previous version record
                seek(8,1)
                version=read(vlen)
                pv=p64(vindex_get(version, 0))
                if status != 'u': vindex[version]=opos

            tappend((oid,opos))

            if plen: p=read(plen)
            else:
                p=read(8)
                p=u64(p)
                if p >= p2: p=p-offset
                elif p >= p1:
                    # Ick, we're in trouble. Let's bail
                    # to the index and hope for the best
                    p=index_get(oid,0)
                p=p64(p)

            # WRITE
            seek(opos)
            sprev=p64(index_get(oid,0))
            write(pack(">8s8s8s8sH8s",
                       oid,serial,sprev,p64(otpos),vlen,splen))
            if vlen:
                if not pnv: write(z64)
                else:
                    if pnv >= p2: pnv=pnv-offset
                    elif pnv >= p1:
                        pnv=index_get(oid,0)
                        
                    write(p64(pnv))
                write(pv)
                write(version)

            write(p)
            
            opos=opos+dlen
            pos=pos+dlen

        # skip the (intentionally redundant) transaction length
        pos=pos+8

        if status != 'u': 
            for oid, p in tindex:
                index[oid]=p # Record the position

        del tindex[:]

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
    tindex=[]
    
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
               ltid=z64, start=4, maxoid=z64, recover=0):
    
    read=file.read
    seek=file.seek
    seek(0,2)
    file_size=file.tell()

    if file_size:
        if file_size < start: raise FileStorageFormatError, file.name
        seek(0)
        if read(4) != packed_version: raise FileStorageFormatError, name
    else:
        file.write(packed_version)
        return 4, maxoid, ltid

    index_get=index.get
    vndexpos=vindex.get
    tappend=tindex.append

    pos=start
    seek(start)
    unpack=struct.unpack
    tpos=0
    tid='\0'*7+'\1'

    while 1:
        # Read the transaction record
        h=read(23)
        if not h: break
        if len(h) != 23:
            warn('%s truncated at %s', name, pos)
            seek(pos)
            file.truncate()
            break

        tid, stl, status, ul, dl, el = unpack(">8s8scHHH",h)
        if el < 0: el=t32-el

        if tid <= ltid:
            warn("%s time-stamp reduction at %s", name, pos)
        ltid=tid

        tl=u64(stl)

        if tl+pos+8 > file_size or status=='c':
            # Hm, the data were truncated or the checkpoint flag wasn't
            # cleared.  They may also be corrupted,
            # in which case, we don't want to totally lose the data.
            warn("%s truncated, possibly due to damaged records at %s",
                 name, pos)
            _truncate(file, name, pos)
            break

        if status not in ' up':
            warn('%s has invalid status, %s, at %s', name, status, pos)

        if ul > tl or dl > tl or el > tl or tl < (23+ul+dl+el):
            # We're in trouble. Find out if this is bad data in the
            # middle of the file, or just a turd that Win 9x dropped
            # at the end when the system crashed.
            # Skip to the end and read what should be the transaction length
            # of the last transaction.
            seek(-8, 2)
            rtl=u64(read(8))
            # Now check to see if the redundant transaction length is
            # reasonable:
            if file_size - rtl < pos or rtl < 23:
                nearPanic('%s has invalid transaction header at %s', name, pos)
                warn("It appears that there is invalid data at the end of the "
                     "file, possibly due to a system crash.  %s truncated "
                     "to recover from bad data at end."
                     % name)
                _truncate(file, name, pos)
                break
            else:
                if recover: return tpos, None, None
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

        pos=tpos+23+ul+dl+el
        while pos < tend:
            # Read the data records for this transaction

            seek(pos)
            h=read(42)
            oid,serial,sprev,stloc,vlen,splen = unpack(">8s8s8s8sH8s", h)
            prev=u64(sprev)
            tloc=u64(stloc)
            plen=u64(splen)
            
            dlen=42+(plen or 8)
            tappend((oid,pos))
            
            if vlen:
                dlen=dlen+16+vlen
                seek(8,1)
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
                
            if index_get(oid,0) != prev:
                if prev:
                    if recover: return tpos, None, None
                    panic("%s incorrect previous pointer at %s", name, pos)
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
        
        for oid, p in tindex:
            maxoid=max(maxoid,oid)
            index[oid]=p # Record the position

        del tindex[:]

    return pos, maxoid, ltid


def _loadBack(file, oid, back):
    seek=file.seek
    read=file.read
    
    while 1:
        old=u64(back)
        if not old: raise KeyError, oid
        seek(old)
        h=read(42)
        doid,serial,prev,tloc,vlen,plen = unpack(">8s8s8s8sH8s", h)

        if vlen: seek(vlen+16,1)
        if plen != z64: return read(u64(plen)), serial
        back=read(8) # We got a back pointer!

def _loadBackPOS(file, oid, back):
    seek=file.seek
    read=file.read
    
    while 1:
        old=u64(back)
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
