######################################################################
# Digital Creations Options License Version 0.9.0
# -----------------------------------------------
# 
# Copyright (c) 1999, Digital Creations.  All rights reserved.
# 
# This license covers Zope software delivered as "options" by Digital
# Creations.
# 
# Use in source and binary forms, with or without modification, are
# permitted provided that the following conditions are met:
# 
# 1. Redistributions are not permitted in any form.
# 
# 2. This license permits one copy of software to be used by up to five
#    developers in a single company. Use by more than five developers
#    requires additional licenses.
# 
# 3. Software may be used to operate any type of website, including
#    publicly accessible ones.
# 
# 4. Software is not fully documented, and the customer acknowledges
#    that the product can best be utilized by reading the source code.
# 
# 5. Support for software is included for 90 days in email only. Further
#    support can be purchased separately.
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
######################################################################
"""Implement a client cache
 
The cache is managed as two files, var/c0.zec and var/c1.zec.

Each cache file is a sequence of records of the form:

  oid -- 8-byte object id

  status -- 1-byte status v': valid, 'n': non-version valid, 'i': invalid

  tlen -- 4-byte (unsigned) record length

  vlen -- 2-bute (unsigned) version length

  dlen -- 4-byte length of non-version data

  serial -- 8-byte non-version serial (timestamp)

  data -- non-version data

  version -- Version string (if vlen > 0)

  vdlen -- 4-byte length of version data (if vlen > 0)

  vdata -- version data (if vlen > 0)

  vserial -- 8-byte version serial (timestamp)

  tlen -- 4-byte (unsigned) record length (for redundancy and backward
          traversal)

There is a cache size limit.

The cache is managed as follows:

  - Data are written to file 0 until file 0 exceeds limit/2 in size.

  - Data are written to file 1 until file 1 exceeds limit/2 in size.

  - File 0 is truncated to size 0 (or deleted and recreated).

  - Data are written to file 0 until file 0 exceeds limit/2 in size.

  - File 1 is truncated to size 0 (or deleted and recreated).

  - Data are written to file 1 until file 1 exceeds limit/2 in size.

and so on.

On startup, index information is read from file 0 and file 1.
Current serial numbers are sent to the server for verification.
If any serial numbers are not valid, then the server will send back
invalidation messages and the cache entries will be invalidated.

When a cache record is invalidated, the data length is overwritten
with '\0\0\0\0'.

If var is not writable, then temporary files are used for
file 0 and file 1.

"""

__version__ = "$Revision: 1.4 $"[11:-2]

import os, tempfile
from struct import pack, unpack

magic='ZEC0'

class ClientCache:

    def __init__(self, storage='', size=20000000, client=None):
        var=os.path.join(INSTANCE_HOME,'var')
        if client:
            self._p=p=map(lambda i, p=storage, var=var, c=client:
                          os.path.join(var,'c%s-%s-%s.zec' % (p, c, i)),
                          (0,1))
            self._f=f=[None, None]
            s=['\0\0\0\0\0\0\0\0', '\0\0\0\0\0\0\0\0']
            for i in 0,1:
                if os.path.exists(p[i]):
                    fi=open(p[i],'r+b')
                    if fi.read(4)==magic: # Minimal sanity
                        fi.seek(0,2)
                        if fi.tell() > 30:
                            fi.seek(22)
                            s[i]=fi.read(8)
                    if s[i]!='\0\0\0\0\0\0\0\0': f[i]=fi
                    fi=None

            if s[1] > s[0]: current=1
            elif s[0] > s[1]: current=0
            else:
                if f[0] is None:
                    f[0]=open(p[0], 'w+b')
                    f[0].write(magic)
                current=0
                f[1]=None
        else:
            self._p=p=map(
                lambda i, p=storage:
                tempfile.mktemp('.zec'),
                (0,1))
            self._f=f=[open(p[0],'w+b'), None]
            f[0].write(magic)
            current=0

        self._limit=size/2
        self._current=current

    def open(self):
        self._index=index={}
        self._get=index.get
        serial={}
        f=self._f
        current=self._current
        if f[not current] is not None:
            read_index(index, serial, f[not current], not current)
        self._pos=read_index(index, serial, f[current], current)

        return serial.items()

    def invalidate(self, oid, version):
        p=self._get(oid, None)
        if p is None: return None
        f=self._f[p < 0]
        ap=abs(p)
        f.seek(ap)
        h=f.read(8)
        if h != oid: return
        f.write(version and 'n' or 'i')

    def load(self, oid, version):
        p=self._get(oid, None)
        if p is None: return None
        f=self._f[p < 0]
        ap=abs(p)
        seek=f.seek
        read=f.read
        seek(ap)
        h=read(27)
        if len(h)==27 and h[8] in 'nv' and h[:8]==oid:
            tlen, vlen, dlen = unpack(">iHi", h[9:19])
        else: tlen=-1
        if tlen <= 0 or vlen < 0 or dlen <= 0 or vlen+dlen > tlen:
            del self._index[oid]
            return None

        if version and h[8]=='n': return None
        
        if not vlen or not version:
            return read(dlen), h[19:]

        seek(dlen, 1)
        v=read(vlen)
        if version != v: 
            seek(-dlen-vlen, 1)
            return read(dlen), h[19:]

        dlen=unpack(">i", read(4))[0]
        return read(dlen), read(8)

    def update(self, oid, serial, version, data):
        if version:
            # We need to find and include non-version data
            p=self._get(oid, None)
            if p is None: return None
            f=self._f[p < 0]
            ap=abs(p)
            seek=f.seek
            read=f.read
            seek(ap)
            h=read(27)
            if len(h)==27 and h[8] in 'nv' and h[:8]==oid:
                tlen, vlen, dlen = unpack(">iHi", h[9:19])
            else: tlen=-1
            if tlen <= 0 or vlen < 0 or dlen <= 0 or vlen+dlen > tlen:
                del self._index[oid]
                return None

            p=read(dlen)
            s=h[19:]

            self.store(oid, p, s, version, data, serial)
        else:
            # Simple case, just store new data:
            self.store(oid, data, serial, '', None, None)

    def modifiedInVersion(self, oid):
        p=self._get(oid, None)
        if p is None: return None
        f=self._f[p < 0]
        ap=abs(p)
        seek=f.seek
        read=f.read
        seek(ap)
        h=read(27)
        if len(h)==27 and h[8] in 'nv' and h[:8]==oid:
            tlen, vlen, dlen = unpack(">iHi", h[9:19])
        else: tlen=-1
        if tlen <= 0 or vlen < 0 or dlen <= 0 or vlen+dlen > tlen:
            del self._index[oid]
            return None

        if h[8]=='n': return None
        
        if not vlen: return ''
        seek(dlen, 1)
        return read(vlen)

    def store(self, oid, p, s, version, pv, sv):
        tlen=31+len(p)
        if version:
            tlen=tlen+len(version)+12+len(pv)
            vlen=len(version)
        else:
            vlen=0
        
        pos=self._pos
        current=self._current
        if pos+tlen > self._limit:
            current=not current
            self._current=current
            self._f[current]=open(self._p[current],'w+b')
            self._f[current].write(magic)
            self._pos=pos=4

        f=self._f[current]
        f.seek(pos)
        stlen=pack(">I",tlen)
        write=f.write
        write(oid+'v'+stlen+pack(">HI", vlen, len(p))+s)
        write(p)
        if version:
            write(pack(">I", len(pv)))
            write(pv)
            write(sv+stlen)

        if current: self._index[oid]=-pos
        else: self._index[oid]=pos

        self._pos=pos+tlen

def read_index(index, serial, f, current):
    seek=f.seek
    read=f.read
    pos=4
    seek(0,2)
    size=f.tell()

    while 1:
        f.seek(pos)
        h=read(27)
        
        if len(h)==27 and h[8] in 'vni':
            tlen, vlen, dlen = unpack(">iHi", h[9:19])
        else: tlen=-1
        if tlen <= 0 or vlen < 0 or dlen <= 0 or vlen+dlen > tlen:
            break

        oid=h[:8]

        if h[8]=='v' and vlen:
            seek(dlen+vlen, 1)
            vdlen=read(4)
            if len(vdlen) != 4: break
            vdlen=unpack(">i", vdlen)[0]
            if vlen+dlen+42+vdlen > tlen: break
            seek(vdlen, 1)
            vs=read(8)
            if read(4) != h[9:13]: break
        else: vs=None

        if h[8] in 'vn':
            if current: index[oid]=-pos
            else: index[oid]=pos
            serial[oid]=h[-8:], vs
            
        pos=pos+tlen

    f.seek(pos)
    try: f.truncate()
    except: pass
    
    return pos
