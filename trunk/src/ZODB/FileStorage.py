##############################################################################
#
# Copyright (c) 1996-1998, Digital Creations, Fredericksburg, VA, USA.
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
#   o Redistributions of source code must retain the above copyright
#     notice, this list of conditions, and the disclaimer that follows.
# 
#   o Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions, and the following disclaimer in
#     the documentation and/or other materials provided with the
#     distribution.
# 
#   o Neither the name of Digital Creations nor the names of its
#     contributors may be used to endorse or promote products derived
#     from this software without specific prior written permission.
# 
# 
# THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS AND CONTRIBUTORS *AS IS*
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
# TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
# PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL
# CREATIONS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
# OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
# TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
# USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
# DAMAGE.
#
# 
# If you have questions regarding this software, contact:
#
#   Digital Creations, L.C.
#   910 Princess Ann Street
#   Fredericksburge, Virginia  22401
#
#   info@digicool.com
#
#   (540) 371-6909
#
##############################################################################
"""File-based BoboPOS3 storage
"""
__version__='$Revision: 1.2 $'[11:-2]

import struct, time, os, bpthread
now=time.time
from struct import pack, unpack
import POSException

class FileStorageError: pass

class FileStorageFormatError(FileStorageError, POSException.StorageError):
    """Invalid file format

    The format of the given file is not valid
    """

class CorruptedFileStorageError(FileStorageError,
                                POSException.StorageSystemError):
    """Corrupted file storage
    """

class CorruptedTransactionError(CorruptedFileStorageError): pass
class CorruptedDataError(CorruptedFileStorageError): pass

class FileStorage:
    _packt=0
    _transaction=None

    def __init__(self, file_name, create=0):
        self.__name__=file_name
        self._tfile=open(file_name+'.tmp','w+b')
        index, vindex, tindex = self._newIndexes()

        self._index=index
        self._vindex=vindex
        self._tindex=tindex
        self._indexpos=index.get
        self._vindexpos=vindex.get
        self._tappend=tindex.append

        # Allocate locks:
        l=bpthread.allocate_lock()
        self._a=l.acquire
        self._r=l.release
        l=bpthread.allocate_lock()
        self._ca=l.acquire
        self._cr=l.release

        # Now open the file
        
        if create:
            if os.path.exists(file_name): os.remove(file_name)
            self._file=file=open(file_name,'w+b')
            self._file.write(packed_version)
            self._pos=4
            self._tpos=0
            self._oid='\0\0\0\0\0\0\0\0'
            return

        if os.path.exists(file_name): file=open(file_name,'r+b')
        else:                         file=open(file_name,'w+b')
        self._file=file
        self._pos, self._tpos, self._oid = read_index(
            file, index, vindex, tindex)

    def __len__(self): return len(self._index)

    def _newIndexes(self): return {}, {}, []

    def abortVersion(self, version):
        self._a()
        try:
            pos=self._vindex[version]
            file=self._file
            seek=file.seek
            read=file.read
            file=self._tfile
            write=file.write
            tell=file.tell
            tloc=self._pos
            tappend=self._tappend
            index=self._index
            pack=struct.pack
            unpack=struct.unpack

            while pos:
                seek(pos)
                h=read(30)
                oid=h[:8]
                if index[oid]==pos: 
                    tappend(oid, tell())
                    pc=h[-8:-4]  # Position of committed (non-version) data
                    write(pack(">8siiHi4s", oid,pos,tloc,0,0,pc))
                pos=unpack(">i",h[-4:])[0]
        finally: self._r()

    def close(self):
        self._file.close()
        # Eventuallly, we should save_index
        
    def commitVersion(self, src, dest):
        self._a()
        try:
            pos=self._vindex[version]
            file=self._file
            seek=file.seek
            read=file.read
            file=self._tfile
            write=file.write
            tell=file.tell
            tloc=self._pos
            tappend=self._tappend
            index=self._index
            pack=struct.pack
            unpack=struct.unpack
            destlen=len(dest)

            while pos:
                seek(pos)
                h=read(30)
                oid=h[:8]
                if index[oid]==pos: 
                    tappend(oid, tell())
                    write(pack(">8siiHi4s", oid,pos,tloc,destlen,0,h[-8:-4]))
                    write(dest)
                    write(pack(">i",pos))
                pos=unpack(">i",h[-4:])[0]
        finally: self._r()

    def getName(self): return self.__name__

    def getSize(self): return self._pos
                  
    def history(self, oid, version, length=1):
        self._a()
        try:
            # not done

            index=self._index
            file=self._file
            seek=file.seek
            read=file.read

            hist=[]
            pos=index[oid]
            while length:
                seek(pos)
                h=read(22)
                doid, prev, tloc, vlen, plen = unpack(">8siiHi", h)
                if vlen and not hist:
                    pnc=read(4)
                    if vlen != len(version) or read(vlen) != version:
                        pos=unpack(">i", pnc)
                        contiue
                pos=prev
                seek(tloc)
                h=read(21)
        finally: self._r()
            

    def load(self, oid, version, _stuff=None):
        self._a()
        try:
            pos=self._index[oid]
            file=self._file
            file.seek(pos)
            read=file.read
            h=read(22)
            doid,prev,tloc,vlen,plen = unpack(">8siiHi", h)
            if doid != oid: raise CorruptedDataError, h
            if vlen:
                pnv=read(4)
                if (not version or len(version) != vlen or
                    (read(4) # skip past version link
                     and version != read(vlen))
                    ):
                    return _loadBack(file, oid, pnv)

            # If we get here, then either this was not a version record,
            # or we've already read past the version data!
            if plen: return read(plen)
            return _loadBack(file, oid, pnv)
        finally: self._r()
                    
    def modifiedInVersion(self, oid):
        self._a()
        try:
            pos=self._index[oid]
            file=self._file
            file.seek(pos)
            doid,prev,tloc,vlen = unpack(">8siiH", file.read(18))
            if doid != oid: raise CorruptedDataError, h
            if vlen:
                seek(8,1)
                return read(vlen)
            return ''
        finally: self._r()

    def new_oid(self, last=None):
        if last is None:
            self._a()
            try:
                last=self._oid
                d=ord(last[-1])
                if d < 255: last=last[:-1]+chr(d+1)
                else:       last=self.new_oid(last[:-1])
                self._oid=last
                return last
            finally: self._r()
        else:
            d=ord(last[-1])
            if d < 255: return last[:-1]+chr(d+1)+'\0'*(8-len(last))
            else:       return self.new_oid(last[:-1])
        
    def pack(self, t, rf):
        self._a()
        try:
            # we're going to leave this undone for a while!


            # This is hellacious.  Hold on to your butts!

            # First, prevent undos before t:
            self._packt=t
            index, vindex, tindex = self._newIndexes()

            # Now we know the part of the file containing transactions
            # written before t will not be touched.  We are free to
            # work on it.
            self._sync__lock.release()

            # Phase 1: pack the old records        
            ofile=open(self.__name__,'r+b')
            import Transaction
            stop=Transaction.time2id(t)
            opos, otpos, maxoid = read_index(file, index, vindex, tindex, stop)
            read=ofile.read
            seek=ofile.seek
            pfile=open(self.__name__+'.pk','w+b')
            write=pfile.write
            unpack=struct.unpack

            rootl=['\0'*8]
            rootd={}
            inroot=rootd.has_key
            while rootl:
                oid=rootl[-1]
                del rootl[-1]
                if inroot[oid]: continue
                pos=index[oid]
                seek(pos)
                h=read(22)
                doid,prev,tloc,vlen,plen = unpack(">8siiHi", h)
                if doid != oid: raise CorruptedDataError, h
                if vlen:
                    pnv=read(4)
                    return _loadBack(file, oid, read(4))

                if plen: return read(plen)
                return _loadBack(file, oid, pnv)

            for oid in rootd.keys(): del index[oid]
            del index['\0'*8]

            unreachable=index.has_key



            seek(4)
            pos=4
            tpos=0
            while 1:
                # Read the transaction record
                h=read(21)
                if not h: break
                tid, prev, tl, status, ul, dl = unpack(">8siicHH", h)
                if tid >= stop: break
                tpos=pos
                tend=tpos+tl

                if status=='u':
                    # Undone transaction, skip it
                    pos=tpos+tl+4
                    seek(pos)
                    continue

                user=read(ul)
                desc=read(dl)
                pos=tpos+21+ul+dl
                while pos < tend:
                    # Read the data records for this transaction

                    h=read(22)
                    oid,prev,tloc,vlen,plen = unpack(">8siiHi", h)
                    dlen=22+(plen or 4)+vlen

                    if vlen:
                        dlen=vlen+8
                        seek(8,1)
                        version=read(vlen)
                        vindex[version]=pos

                    pos=pos+dlen

                if pos != tend: 
                    raise CorruptedTransactionError, lastp

                # Read the (intentionally redundant) transaction length
                h=read(4)
                if len(h) != 4: raise CorruptedTransactionError, h
                if unpack(">i",h)[0] != tl:
                    raise CorruptedTransactionError, h
                pos=pos+4

                for oid, p in tindex:
                    index[oid]=p # Record the position

                del tindex[:]






            # Phase 2: copy the new records, adjusting all of the
            # location pointers.  We'll get the commit lock for this part.
        finally: self._r()


    def store(self, oid, data, version, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)

        self._a()
        try:
            old=self._indexpos(oid, 0)
            pnv=None
            if old:
                file=self._file
                file.seek(old)
                h=file.read(22)
                doid,prev,tloc,vlen,plen = unpack(">8siiHi", h)
                if doid != oid: raise CorruptedDataError, h
                if vlen:
                    pnv=read(4)
                    if (len(version) != vlen or
                        (read(4) # skip past version link
                         and version != read(vlen))
                        ):
                        raise POSException.VersionLockError, oid

            tfile=self._tfile
            write=tfile.write
            self._tappend(oid, tfile.tell())
            pos=self._pos
            write(pack(">8siiHi",oid,old,pos,len(version),len(data)))
            if version:
                if pnv: write(pnv)
                else:   write(pack(">i",old))
                # Link to last record for this version:
                vindex=self._vindex
                write(pack(">i",vindex[version]))
                vindex[version]=pos
                write(version)
            write(data)
        finally: self._r()

    def supportsUndo(self): return 0 # for now
    def supportsVersions(self): return 1
        
    def tpc_abort(self, transaction):
        self._a()
        try:
            if transaction is not self._transaction: return
            del self._tindex[:]
            self._transaction=None
            self._cr()
        finally: self._r()

    def tpc_begin(self, transaction):
        self._a()
        try:
            if self._transaction is transaction: return
            self._r()
            self._ca()
            self._a()
            self._transaction=transaction
            del self._tindex[:] # Just to be sure!
            self._tfile.seek(0)
        finally: self._r()

    def tpc_finish(self, transaction, f=None):
        self._a()
        try:
            if transaction is not self._transaction: return
            if f is not None: f()
            file=self._file
            write=file.write
            tfile=self._tfile
            read=tfile.read
            dlen=tfile.tell()
            tfile.seek(0)
            id=transaction.id
            user=transaction.user
            desc=transaction.description
            tlen=21+len(user)+len(desc)
            pos=self._pos
            file.seek(pos)
            tl=tlen+dlen
            write(pack(">8siicHH",
                       id, self._tpos, tl, ' ', len(user), len(desc)))
            write(user)
            write(desc)
            
            assert dlen >= 0
            while dlen > 0:
                d=read(min(dlen,8192))
                write(d)
                d=len(d)
                assert dlen >= d
                dlen=dlen-d
                
            write(pack(">i", tl))
            file.flush()
            self._tpos=pos
            self._pos=pos+tl+4

            index=self._index
            dpos=pos+tlen
            for oid, pos in self._tindex: index[oid]=pos+dpos

            del self._tindex[:]
            self._transaction=None
            self._cr()
        finally: self._r()

    def undo(self, transaction_id):
        pass

    def undoLog(self, version, first, last, path):
        return []

    def versionEmpty(self, version):
        self._a()
        try:
            pos=self._index[oid]
            file=self._file
            file.seek(pos)
            doid,prev,tloc,vlen = unpack(">8siiH", file.read(18))
            if doid != oid: raise CorruptedDataError, h
            if not vlen or vlen != len(version): return 1
            seek(4,1)
            return read(vlen) != version
        finally: self._r()


        

packed_version='FS10'
def read_index(file, index, vindex, tindex, stop='\377'*8):
    indexpos=index.get
    vndexpos=vindex.get
    tappend=tindex.append
    
    read=file.read
    seek=file.seek
    seek(0,2)
    file_size=file.tell()
    seek(0)
    if file_size:
        if file_size < 4: raise FileStorageFormatError, file.name
        if read(4) != packed_version:
            raise FileStorageFormatError, file_name
    else: file.write(packed_version)

    pos=4
    unpack=struct.unpack
    tpos=0
    maxoid='\0\0\0\0\0\0\0\0'

    while 1:
        # Read the transaction record
        h=read(21)
        if not h: break
        if len(h) != 21: raise CorruptedTransactionError, h
        tid, prev, tl, status, ul, dl = unpack(">8siicHH",h)
        if (prev != tpos
            or status not in ' up' or ul > tl or dl > tl
            or tl > file_size or tl+pos >= file_size):
            raise CorruptedTransactionRecordError, h
        if tid >= stop: break
        tpos=pos
        tend=tpos+tl
        
        if status=='u':
            # Undone transaction, skip it
            pos=tpos+tl
            seek(pos)
            h=read(4)
            if len(h) != 4: raise CorruptedTransactionError, h
            if unpack(">i",h)[0] != tl:
                raise CorruptedTransactionError, h
            pos=pos+4
            continue

        pos=tpos+21+ul+dl
        while pos < tend:
            # Read the data records for this transaction

            seek(pos)
            h=read(22)
            oid,prev,tloc,vlen,plen = unpack(">8siiHi", h)
            dlen=22+(plen or 4)+vlen
            if pos+dlen > tend or tloc != tpos:
                raise CorruptedDataError, h
            if indexpos(oid,0) != prev:
                raise CorruptedDataError, h
            tappend((oid,pos))
            
            if vlen:
                dlen=vlen+8
                seek(8,1)
                version=read(vlen)
                vindex[version]=pos
                
            pos=pos+dlen

        if pos != tend: 
            raise CorruptedTransactionError, lastp

        # Read the (intentionally redundant) transaction length
        seek(pos)
        h=read(4)
        if len(h) != 4: raise CorruptedTransactionError, h
        if unpack(">i",h)[0] != tl:
            raise CorruptedTransactionError, h
        pos=pos+4
        
        for oid, p in tindex:
            maxoid=max(maxoid,oid)
            index[oid]=p # Record the position

        del tindex[:]

    return pos, tpos, maxoid


def _loadBack(file, oid, back):
    while 1:
        old=unpack(">i",back)[0]
        if not old: raise KeyError, oid
        file.seek(old)
        h=file.read(22)
        doid,prev,tloc,vlen,plen = unpack(">8siiHi", h)
        if doid != oid or vlen: raise CorruptedDataError, h
        if plen: return read(plen)
        back=read(4) # We got a back pointer!
