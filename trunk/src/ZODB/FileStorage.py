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
"""File-based ZODB storage

Files are arranged as follows.

  - The first 4 bytes are a file identifier.
  
  - The rest of the file consists of a sequence of transaction
    "records".

A transaction record consists of:

  - 8-byte transaction id, which is also a time stamp.
  
  - 8-byte transaction record length - 8.
  
  - 1-byte status code
  
  - 2-byte length of user name
  
  - 2-byte length of description 
  
  - 4-byte length of extension attributes 
  
  -   user name
  
  -   description

  * A sequence of data records
  
  - 8-byte redundant transaction length -8

A data record consists of

  - 8-byte oid.

  - 8-byte serial, which is a type stamp that matches the
    transaction timestamp.

  - 8-byte previous-record file-position.

  - 8-byte beginning of transaction record file position.

  - 2-byte version length

  - 8-byte data length

  ? 8-byte position of non-version data
    (if version length > 0)

  ? 8-byte position of previous record in this version
    (if version length > 0)

  ?   version string 
    (if version length > 0)

  ?   data
    (data length > 0)

  ? 8-byte position of data record containing data
    (data length > 0)


Note that the lengths and positions are all big-endian.
Also, the object ids time stamps are big-endian, so comparisons
are meaningful.

"""
__version__='$Revision: 1.8 $'[11:-2]

import struct, time, os, bpthread, string, base64
now=time.time
from struct import pack, unpack
from cPickle import dumps, loads
import POSException
from TimeStamp import TimeStamp
from lock_file import lock_file
from utils import t32, p64, u64, cp

z64='\0'*8

def warn(log, message, *data):
    log("%s  warn: %s\n" % (packed_version, (message % data)))

def error(log, message, *data):
    log("%s ERROR: %s\n" % (packed_version, (message % data)))

def panic(log, message, *data):
    message=message%data
    log("%s ERROR: %s\n" % (packed_version, message))
    raise CorruptedTransactionError, message
        

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

packed_version='FS21'

class FileStorage:
    _packt=0
    _transaction=None
    _serial=z64

    def __init__(self, file_name, create=0, log=lambda s: None, read_only=0,
                 stop=None):

        if read_only:
            if create:
                raise ValueError, "can\'t create a read-only file"
        elif stop is not None:
            raise ValueError, "time-travel is only supported in read-only mode"

        if stop is None: stop='\377'*8

        # Lock the database
        if not read_only:
            try: f=open(file_name+'.lock', 'r+')
            except: f=open(file_name+'.lock', 'w+')
            lock_file(f)
            try:
                f.write(str(os.getpid()))
                f.flush()
            except: pass
            self._lock_file=f # so it stays open
        
        self.__name__=file_name
        self._tfile=open(file_name+'.tmp','w+b')
        index, vindex, tindex, tvindex = self._newIndexes()

        self._index=index
        self._vindex=vindex
        self._tindex=tindex
        self._tvindex=tvindex
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
            self._oid='\0\0\0\0\0\0\0\0'
            return

        if os.path.exists(file_name):
            file=open(file_name, read_only and 'rb' or 'r+b')
        else:
            if read_only:
                raise ValueError, "can\'t create a read-only file"
            file=open(file_name,'w+b')

        self._file=file
        self._pos, self._oid, tid = read_index(
            file, file_name, index, vindex, tindex, stop, log)

        self._ts=tid=TimeStamp(tid)
        t=time.time()
        t=apply(TimeStamp,(time.gmtime(t)[:5]+(t%60,)))
        if tid > t:
            warn(log, "%s Database records in the future", file_name);
            

    def __len__(self): return len(self._index)

    def _newIndexes(self): return {}, {}, [], {}
        
    def abortVersion(self, src, transaction):
        # We are going to abort by simply storing back pointers.

        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        
        self._a()
        try:
            file=self._file
            read=file.read
            seek=file.seek
            tfile=self._tfile
            write=tfile.write
            tappend=self._tappend
            index=self._index

            srcpos=self._vindex.get(src, 0)
            spos=p64(srcpos)

            middle=p64(self._pos)+'\0'*10
                        
            here=tfile.tell()+self._pos+self._thl
            oids=[]
            appoids=oids.append

            while srcpos:
                seek(srcpos)
                h=read(58) # oid, serial, prev(oid), tloc, vlen, plen, pnv, pv
                oid=h[:8]
                if index[oid]==srcpos:
                    tappend((oid,here))
                    appoids(oid)
                    #     oid,ser  prev   tl,vl,pl pnv
                    write(h[:16] + spos + middle + h[-16:-8])
                    here=here+50
                    
                spos=h[-8:]
                srcpos=u64(spos)
               
            self._tvindex[src]=0

            return oids

        finally: self._r()

    def close(self):
        self._file.close()
        # Eventuallly, we should save_index
        
    def commitVersion(self, src, dest, transaction):
        # We are going to commit by simply storing back pointers.
        
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        
        self._a()
        try:
            file=self._file
            read=file.read
            seek=file.seek
            tfile=self._tfile
            write=tfile.write
            tappend=self._tappend
            index=self._index

            srcpos=self._vindex.get(src, 0)
            spos=p64(srcpos)

            middle=struct.pack(">8sH8s", p64(self._pos), len(dest), z64)

            if dest:
                sd=p64(self._vindex.get(dest, 0))
                heredelta=66+len(dest)
            else:
                sd=''
                heredelta=50
                        
            here=tfile.tell()+self._pos+self._thl
            oids=[]
            appoids=oids.append
            tvindex=self._tvindex
            
            while srcpos:
                seek(srcpos)
                h=read(58) # oid, serial, prev(oid), tloc, vlen, plen, pnv, pv
                oid=h[:8]
                if index[oid]==srcpos:
                    tappend((oid,here))
                    appoids(oid)
                    write(h[:16] + spos + middle)
                    if dest:
                        tvindex[dest]=here
                        write(h[-16:-8]+sd+dest)
                        sd=p64(here)

                    write(spos) # data backpointer to src data
                    here=here+heredelta
                    
                spos=h[-8:]
                srcpos=u64(spos)
               
            tvindex[src]=0

            return oids

        finally: self._r()

    def getName(self): return self.__name__

    def getSize(self): return self._pos
                  
    def history(self, oid, version, length=1):
        # TBD
        pass

    def load(self, oid, version, _stuff=None):
        self._a()
        try:
            pos=self._index[oid]
            file=self._file
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
        finally: self._r()
                    
    def modifiedInVersion(self, oid):
        self._a()
        try:
            pos=self._index[oid]
            file=self._file
            seek=file.seek
            seek(pos)
            doid,serial,prev,tloc,vlen = unpack(">8s8s8s8sH", file.read(34))
            if doid != oid:
                raise CorruptedDataError, h
            if vlen:
                seek(24,1) # skip plen, pnv, and pv
                return file.read(vlen)
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
        # TBD
        pass

    def store(self, oid, serial, data, version, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)

        self._a()
        try:
            old=self._indexpos(oid, 0)
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
                    if (len(version) != vlen or
                        (read(8) # skip past version link
                         and version != read(vlen))
                        ):
                        raise POSException.VersionLockError, oid

                if serial != oserial: raise POSException.ConflictError

            tfile=self._tfile
            write=tfile.write
            pos=self._pos
            here=tfile.tell()+pos+self._thl
            self._tappend(oid, here)
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
                pv=tvindex.get(version, 0) or self._vindexpos(version, 0)
                write(p64(pv))
                tvindex[version]=here
                write(version)

            write(data)

            return serial
        
        finally: self._r()

    def registerDB(self, db, limit): pass # we don't care

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
            del self._tindex[:]   # Just to be sure!
            self._tvindex.clear() #      ''
            self._tfile.seek(0)

            t=time.time()
            t=apply(TimeStamp,(time.gmtime(t)[:5]+(t%60,)))
            self._ts=t=t.laterThan(self._ts)
            self._serial=`t`

            user=transaction.user
            desc=transaction.description
            ext=transaction._extension
            if ext: ext=dumps(ext,1)
            else: ext=""

            # Ugh, we have to record the transaction header length
            # so that we can get version pointers right.
            self._thl=23+len(user)+len(desc)+len(ext)

            # And we have to save the data used to compute the
            # header length. It's unlikely that this stuff would
            # change, but if it did, it would be a disaster.
            self._ude=user, desc, ext
            
        finally: self._r()

    def tpc_finish(self, transaction, f=None):
        self._a()
        try:
            if transaction is not self._transaction: return
            if f is not None: f()
            file=self._file
            write=file.write
            tfile=self._tfile
            dlen=tfile.tell()
            tfile.seek(0)
            id=self._serial
            user, desc, ext = self._ude
            self._ude=None
                        
            tlen=self._thl
            pos=self._pos
            file.seek(pos)
            tl=tlen+dlen
            stl=p64(tl)
            write(pack(
                ">8s" "8s" "c"  "H"        "H"        "H"
                 ,id, stl, ' ', len(user), len(desc), len(ext),
                ))
            if user: write(user)
            if desc: write(desc)
            if ext: write(ext)
            
            cp(tfile, file, dlen)
                
            write(stl)
            file.flush()
            self._pos=pos+tl+8

            tindex=self._tindex
            index=self._index
            for oid, pos in tindex: index[oid]=pos
            del tindex[:]
            
            tvindex=self._tvindex
            self._vindex.update(tvindex)
            tvindex.clear()

            self._transaction=None
            self._cr()
        finally: self._r()

    def undo(self, transaction_id):
        self._a()
        try:
	    file=self._file
	    seek=file.seek
            read=file.read
            indexpos=self._indexpos
	    unpack=struct.unpack
            transaction_id=base64.decodestring(transaction_id+'==\n')
	    tid, tpos = transaction_id[:8], u64(transaction_id[8:])
	    seek(tpos)
	    h=read(23)
	    if len(h) != 23 or h[:8] != tid: 
		raise UndoError, 'Invalid undo transaction id'
	    if h[16] == 'u': return
	    if h[16] != ' ': raise UndoError, 'Undoable transaction'
	    tl=u64(h[8:16])
	    ul,dl,el=unpack(">HHH", h[17:23])
	    tend=tpos+tl
	    pos=tpos+23+ul+dl+el
	    t=[]
	    tappend=t.append
	    while pos < tend:
		# Read the data records for this transaction
		seek(pos)
		h=read(42)
		oid,serial,sprev,stloc,vlen,splen = unpack(">8s8s8s8sH8s", h)
		plen=u64(splen)
		prev=u64(sprev)
		dlen=42+(plen or 8)
		if vlen: dlen=dlen+16+vlen
		if indexpos(oid,0) != pos:
                    raise UndoError, 'Undoable transaction'
		pos=pos+dlen
		if pos > tend: raise UndoError, 'Undoable transaction'
		tappend((oid,prev))

	    seek(tpos+16)
	    file.write('u')
	    index=self._index
	    for oid, pos in t: index[oid]=pos
        finally: self._r()

    def undoLog(self, first, last, filter=None):
        self._a()
        try:
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
		if i < first: continue
		seek(pos)
		h=read(23)
		tid, tl, status, ul, dl, el = unpack(">8s8scHHH", h)
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
        finally: self._r()

    def versionEmpty(self, version):
        return not self._vindex.get(version, 0)

    def versions(self, max=None):
        if max: return self._vindex.keys()[:max]
        return self._vindex.keys()


def read_index(file, name, index, vindex, tindex, stop='\377'*8,
               log=lambda s: None):
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
            raise FileStorageFormatError, name
    else: file.write(packed_version)

    pos=4
    unpack=struct.unpack
    tpos=0
    maxoid=ltid=z64
    tid='\0'*7+'\1'

    while 1:
        # Read the transaction record
        h=read(23)
        if not h: break
        if len(h) != 23:
            warn(log, '%s truncated at %s', name, pos)
            seek(pos)
            file.truncate()
            break

        tid, stl, status, ul, dl, el = unpack(">8s8scHHH",h)
        if el < 0: el=t32-el

        if tid <= ltid:
            warn(log, "%s time-stamp reduction at %s", name, pos)
        ltid=tid

        tl=u64(stl)

        if tl+pos+8 > file_size:
            # Hm, the data were truncated.  They may also be corrupted,
            # in which case, we don't want to totally lose the data.
            warn(log, "%s truncated, possibly due to damaged records at %s",
                 name, pos)
            try:
                i=0
                while 1:
                    if os.path.exists('%s.tr%s' % (name, i)):
                        i=i+1
                    else:
                        o=open('%s.tr%s' % (name, i),'wb')
                        seek(pos)
                        cp(file, o, file_size-pos)
                        o.close()
                        break
            except:
                error(log, "couldn\'t write truncated data for %s", name)
                raise POSException.StorageSystemError, (
                    "Couldn't save truncated data")
            
            seek(pos)
            file.truncate()
            break

        if status not in ' up':
            warn(log,'%s has invalid status, %s, at %s', name, status, pos)

        if ul > tl or dl > tl or el > tl:
            panic(log,'%s has invalid transaction header at %s', name, pos)

        if tid >= stop: break

        tpos=pos
        tend=tpos+tl
        
        if status=='u':
            # Undone transaction, skip it
            pos=tpos+tl
            seek(pos)
            h=read(8)
            if h != stl:
                panic(log, '%s has inconsistent transaction length at %s',
                      name, pos)
            pos=pos+8
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
                #    panic(log,"%s incorrect previous version pointer at %s",
                #          name, pos)
                vindex[version]=pos

            if pos+dlen > tend or tloc != tpos:
                panic(log,"%s data record exceeds transaction record at %s",
                      name, pos)
            if indexpos(oid,0) != prev:
                panic(log,"%s incorrect previous pointer at %s",
                      name, pos)

            pos=pos+dlen

        if pos != tend:
            panic(log,"%s data records don't add up at %s",name,tpos)

        # Read the (intentionally redundant) transaction length
        seek(pos)
        h=read(8)
        if h != stl:
            panic(log, "%s redundant transaction length check failed at %s",
                  name, pos)
        pos=pos+8
        
        for oid, p in tindex:
            maxoid=max(maxoid,oid)
            index[oid]=p # Record the position

        del tindex[:]

    _checkVindex(file, index, vindex)

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
        if doid != oid:
            panic(lambda x: None,
                  "%s version record back pointer points to "
                  "invalid record as %s", name, back)

        if vlen: seek(vlen+16,1)
        if plen != z64: return read(u64(plen)), serial
        back=read(8) # We got a back pointer!

def _checkVindex(file, index, vindex):
    seek=file.seek
    read=file.read
    get=index.get
    for version, pos in vindex.items():
        seek(pos)
        oid=read(8)
        if get(oid, None) != pos:
            del vindex[version] # This version is no longer active
