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

# XXX TO DO
# use two indices rather than the sign bit of the index??????
# add a shared routine to read + verify a record???
# redesign header to include vdlen???
# rewrite the cache using a different algorithm???

"""Implement a client cache

The cache is managed as two files.

The cache can be persistent (meaning it is survives a process restart)
or temporary.  It is persistent if the client argument is not None.

Persistent cache files live in the var directory and are named
'c<storage>-<client>-<digit>.zec' where <storage> is the storage
argument (default '1'), <client> is the client argument, and <digit> is
0 or 1.  Temporary cache files are unnamed files in the standard
temporary directory as determined by the tempfile module.

The ClientStorage overrides the client name default to the value of
the environment variable ZEO_CLIENT, if it exists.

Each cache file has a 4-byte magic number followed by a sequence of
records of the form:

  offset in record: name -- description

  0: oid -- 8-byte object id

  8: status -- 1-byte status 'v': valid, 'n': non-version valid, 'i': invalid
               ('n' means only the non-version data in the record is valid)

  9: tlen -- 4-byte (unsigned) record length

  13: vlen -- 2-byte (unsigned) version length

  15: dlen -- 4-byte length of non-version data

  19: serial -- 8-byte non-version serial (timestamp)

  27: data -- non-version data

  27+dlen: version -- Version string (if vlen > 0)

  27+dlen+vlen: vdlen -- 4-byte length of version data (if vlen > 0)

  31+dlen+vlen: vdata -- version data (if vlen > 0)

  31+dlen+vlen+vdlen: vserial -- 8-byte version serial (timestamp)
                                 (if vlen > 0)

  39+dlen+vlen+vdlen: tlen -- 4-byte (unsigned) record length (for
                              redundancy and backward traversal)

  43+dlen+vlen+vdlen: -- total record length (equal to tlen)

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

$Id: ClientCache.py,v 1.30 2002/08/28 18:58:35 gvanrossum Exp $
"""

import os
import tempfile
from struct import pack, unpack
from thread import allocate_lock

from ZODB.utils import U64

import zLOG
from ZEO.ICache import ICache

def log(msg, level=zLOG.INFO):
    zLOG.LOG("ZEC", level, msg)

magic='ZEC0'

class ClientCache:

    __implements__ = ICache

    def __init__(self, storage='1', size=20000000, client=None, var=None):
        # Arguments:
        # storage -- storage name (used in persistent cache file names only)
        # size -- size limit in bytes of both files together
        # client -- if not None, use a persistent cache file and use this name
        # var -- directory where to create persistent cache files

        # Allocate locks:
        L = allocate_lock()
        self._acquire = L.acquire
        self._release = L.release

        if client is not None:
            # Create a persistent cache
            # CLIENT_HOME and INSTANCE_HOME are builtins set by App.FindHomes
            if var is None:
                try:
                    var = CLIENT_HOME
                except:
                    try:
                        var = os.path.join(INSTANCE_HOME, 'var')
                    except:
                        var = os.getcwd()

            fmt = os.path.join(var, "c%s-%s-%%s.zec" % (storage, client))
            # Initialize pairs of filenames, file objects, and serialnos.
            self._p = p = [fmt % 0, fmt % 1]
            self._f = f = [None, None]
            s = ['\0\0\0\0\0\0\0\0', '\0\0\0\0\0\0\0\0']
            for i in 0, 1:
                if os.path.exists(p[i]):
                    fi = open(p[i],'r+b')
                    if fi.read(4) == magic: # Minimal sanity
                        fi.seek(0, 2)
                        if fi.tell() > 30:
                            # First serial is at offset 19 + 4 for magic
                            fi.seek(23)
                            s[i] = fi.read(8)
                    # If we found a non-zero serial, then use the file
                    if s[i] != '\0\0\0\0\0\0\0\0':
                        f[i] = fi
                    fi = None

            # Whoever has the larger serial is the current
            if s[1] > s[0]:
                current = 1
            elif s[0] > s[1]:
                current = 0
            else:
                if f[0] is None:
                    # We started, open the first cache file
                    f[0] = open(p[0], 'w+b')
                    f[0].write(magic)
                current = 0
                f[1] = None
        else:
            self._f = f = [tempfile.TemporaryFile(suffix='.zec'), None]
            # self._p file name 'None' signifies an unnamed temp file.
            self._p = p = [None, None]
            f[0].write(magic)
            current = 0

        log("cache opened.  current = %s" % current)

        self._limit = size / 2
        self._current = current

    def open(self):
        # Two tasks:
        # - Set self._index, self._get, and self._pos.
        # - Read and validate both cache files, returning a list of
        #   serials to be used by verify().
        # This may be called more than once (by the cache verification code).
        self._acquire()
        try:
            self._index = index = {}
            self._get = index.get
            serial = {}
            f = self._f
            current = self._current
            if f[not current] is not None:
                read_index(index, serial, f[not current], not current)
            self._pos = read_index(index, serial, f[current], current)

            return serial.items()
        finally:
            self._release()

    def close(self):
        for f in self._f:
            if f is not None:
                # In 2.1 on Windows, the TemporaryFileWrapper doesn't allow
                # closing a file more than once.
                try:
                    f.close()
                except OSError:
                    pass

    def verify(self, verifyFunc):
        """Call the verifyFunc on every object in the cache.

        verifyFunc(oid, serialno, version)
        """
        for oid, (s, vs) in self.open():
            verifyFunc(oid, s, vs)

    def invalidate(self, oid, version):
        self._acquire()
        try:
            p = self._get(oid, None)
            if p is None:
                return None
            f = self._f[p < 0]
            ap = abs(p)
            f.seek(ap)
            h = f.read(27)
            if h[:8] != oid:
                return
            f.seek(p+8) # Switch from reading to writing
            if version and h[15:19] != '\0\0\0\0':
                # There's still relevant non-version data in the cache record
                f.write('n')
            else:
                del self._index[oid]
                f.write('i')
        finally:
            self._release()

    def load(self, oid, version):
        self._acquire()
        try:
            p = self._get(oid, None)
            if p is None:
                return None
            f = self._f[p < 0]
            ap = abs(p)
            seek = f.seek
            read = f.read
            seek(ap)
            h = read(27)
            if len(h)==27 and h[8] in 'nv' and h[:8]==oid:
                tlen, vlen, dlen = unpack(">iHi", h[9:19])
            else:
                tlen = -1
            if tlen <= 0 or vlen < 0 or dlen < 0 or vlen+dlen > tlen:
                log("load: bad record for oid %16x "
                    "at position %d in cache file %d"
                    % (U64(oid), ap, p < 0))
                del self._index[oid]
                return None

            if h[8]=='n':
                if version:
                    return None
                if not dlen:
                    del self._index[oid]
                    return None

            if not vlen or not version:
                if dlen:
                    return read(dlen), h[19:]
                else:
                    return None

            if dlen:
                seek(dlen, 1)
            vheader = read(vlen+4)
            v = vheader[:-4]
            if version != v:
                if dlen:
                    seek(p+27)
                    return read(dlen), h[19:]
                else:
                    return None

            vdlen = unpack(">i", vheader[-4:])[0]
            vdata = read(vdlen)
            vserial = read(8)
            return vdata, vserial
        finally:
            self._release()

    def update(self, oid, serial, version, data):
        self._acquire()
        try:
            if version:
                # We need to find and include non-version data
                p = self._get(oid, None)
                if p is None:
                    return self._store(oid, '', '', version, data, serial)
                f = self._f[p < 0]
                ap = abs(p)
                seek = f.seek
                read = f.read
                seek(ap)
                h = read(27)
                if len(h)==27 and h[8] in 'nv' and h[:8]==oid:
                    tlen, vlen, dlen = unpack(">iHi", h[9:19])
                else:
                    return self._store(oid, '', '', version, data, serial)

                if tlen <= 0 or vlen < 0 or dlen <= 0 or vlen+dlen > tlen:
                    return self._store(oid, '', '', version, data, serial)

                if dlen:
                    nvdata = read(dlen)
                    nvserial = h[19:]
                else:
                    return self._store(oid, '', '', version, data, serial)

                self._store(oid, nvdata, nvserial, version, data, serial)
            else:
                # Simple case, just store new data:
                self._store(oid, data, serial, '', None, None)
        finally:
            self._release()

    def modifiedInVersion(self, oid):
        # This should return:
        # - The version from the record for oid, if there is one.
        # - '', if there is no version in the record and its status is 'v'.
        # - None, if we don't know: no valid record or status is 'n'.
        self._acquire()
        try:
            p = self._get(oid, None)
            if p is None:
                return None
            f = self._f[p < 0]
            ap = abs(p)
            seek = f.seek
            read = f.read
            seek(ap)
            h = read(27)
            if len(h)==27 and h[8] in 'nv' and h[:8]==oid:
                tlen, vlen, dlen = unpack(">iHi", h[9:19])
            else:
                tlen = -1
            if tlen <= 0 or vlen < 0 or dlen < 0 or vlen+dlen > tlen:
                log("modifiedInVersion: bad record for oid %16x "
                    "at position %d in cache file %d"
                    % (U64(oid), ap, p < 0))
                del self._index[oid]
                return None

            if h[8] == 'n':
                return None

            if not vlen:
                return ''
            seek(dlen, 1)
            return read(vlen)
        finally:
            self._release()

    def checkSize(self, size):
        # Make sure we aren't going to exceed the target size.
        # If we are, then flip the cache.
        self._acquire()
        try:
            if self._pos + size > self._limit:
                current = not self._current
                self._current = current
                log("flipping cache files.  new current = %d" % current)
                # Delete the half of the index that's no longer valid
                index = self._index
                for oid in index.keys():
                    if (index[oid] < 0) == current:
                        del index[oid]
                if self._p[current] is not None:
                    # Persistent cache file: remove the old file
                    # before opening the new one, because the old file
                    # may be owned by root (created before setuid()).
                    if self._f[current] is not None:
                        self._f[current].close()
                        try:
                            os.remove(self._p[current])
                        except:
                            pass
                    self._f[current] = open(self._p[current],'w+b')
                else:
                    # Temporary cache file:
                    self._f[current] = tempfile.TemporaryFile(suffix='.zec')
                self._f[current].write(magic)
                self._pos = 4
        finally:
            self._release()


    def store(self, oid, p, s, version, pv, sv):
        self._acquire()
        try:
            self._store(oid, p, s, version, pv, sv)
        finally:
            self._release()

    def _store(self, oid, p, s, version, pv, sv):
        if not s:
            p = ''
            s = '\0\0\0\0\0\0\0\0'
        tlen = 31 + len(p)
        if version:
            tlen = tlen + len(version) + 12 + len(pv)
            vlen = len(version)
        else:
            vlen = 0

        stlen = pack(">I", tlen)
        # accumulate various data to write into a list
        l = [oid, 'v', stlen, pack(">HI", vlen, len(p)), s]
        if p:
            l.append(p)
        if version:
            l.extend([version,
                      pack(">I", len(pv)),
                      pv, sv])
        l.append(stlen)
        f = self._f[self._current]
        f.seek(self._pos)
        f.write("".join(l))

        if self._current:
            self._index[oid] = - self._pos
        else:
            self._index[oid] = self._pos

        self._pos += tlen

def read_index(index, serial, f, fileindex):
    seek = f.seek
    read = f.read
    pos = 4
    count = 0

    while 1:
        f.seek(pos)
        h = read(27)
        if len(h) != 27:
            # An empty read is expected, anything else is suspect
            if h:
                rilog("truncated header", pos, fileindex)
            break

        if h[8] in 'vni':
            tlen, vlen, dlen = unpack(">iHi", h[9:19])
        else:
            tlen = -1
        if tlen <= 0 or vlen < 0 or dlen < 0 or vlen + dlen > tlen:
            rilog("invalid header data", pos, fileindex)
            break

        oid = h[:8]

        if h[8] == 'v' and vlen:
            seek(dlen+vlen, 1)
            vdlen = read(4)
            if len(vdlen) != 4:
                rilog("truncated record", pos, fileindex)
                break
            vdlen = unpack(">i", vdlen)[0]
            if vlen+dlen+43+vdlen != tlen:
                rilog("inconsistent lengths", pos, fileindex)
                break
            seek(vdlen, 1)
            vs = read(8)
            if read(4) != h[9:13]:
                rilog("inconsistent tlen", pos, fileindex)
                break
        else:
            if h[8] in 'vn' and vlen == 0:
                if dlen+31 != tlen:
                    rilog("inconsistent nv lengths", pos, fileindex)
                seek(dlen, 1)
                if read(4) != h[9:13]:
                    rilog("inconsistent nv tlen", pos, fileindex)
                    break
            vs = None

        if h[8] in 'vn':
            if fileindex:
                index[oid] = -pos
            else:
                index[oid] = pos
            serial[oid] = h[-8:], vs
        else:
            if serial.has_key(oid):
                # We have a record for this oid, but it was invalidated!
                del serial[oid]
                del index[oid]


        pos = pos + tlen
        count += 1

    f.seek(pos)
    try:
        f.truncate()
    except:
        pass

    if count:
        log("read_index: cache file %d has %d records and %d bytes"
            % (fileindex, count, pos))

    return pos

def rilog(msg, pos, fileindex):
    # Helper to log messages from read_index
    log("read_index: %s at position %d in cache file %d"
        % (msg, pos, fileindex))
