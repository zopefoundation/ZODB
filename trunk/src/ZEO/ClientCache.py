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

Each cache file has a 12-byte header followed by a sequence of
records.  The header format is as follows:

  offset in header: name -- description

  0: magic -- 4-byte magic number, identifying this as a ZEO cache file

  4: lasttid -- 8-byte last transaction id

Each record has the following form:

  offset in record: name -- description

  0: oidlen -- 2-byte unsigned object id length

  2: reserved (6 bytes)

  8: status -- 1-byte status 'v': valid, 'n': non-version valid, 'i': invalid
               ('n' means only the non-version data in the record is valid)

  9: tlen -- 4-byte (unsigned) record length

  13: vlen -- 2-byte (unsigned) version length

  15: dlen -- 4-byte length of non-version data

  19: serial -- 8-byte non-version serial (timestamp)

  27: oid -- object id

  27+oidlen: data -- non-version data

  27+oidlen+dlen: version -- Version string (if vlen > 0)

  27+oidlen+dlen+vlen: vdlen -- 4-byte length of version data (if vlen > 0)

  31+oidlen+dlen+vlen: vdata -- version data (if vlen > 0)

  31+oidlen+dlen+vlen+vdlen: vserial -- 8-byte version serial (timestamp)
                                 (if vlen > 0)

  27+oidlen+dlen (if vlen == 0) **or**
  39+oidlen+dlen+vlen+vdlen: tlen -- 4-byte (unsigned) record length (for
                                     redundancy and backward traversal)

  31+oidlen+dlen (if vlen == 0) **or**
  43+oidlen+dlen+vlen+vdlen: -- total record length (equal to tlen)

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

import os
import time
import tempfile
from struct import pack, unpack
from thread import allocate_lock

from ZODB.utils import oid_repr, u64, z64

import zLOG
from ZEO.ICache import ICache

magic = 'ZEC2'
headersize = 12

MB = 1024**2

class ClientCache:

    __implements__ = ICache

    def __init__(self, storage='1', size=20*MB, client=None, var=None):
        # Arguments:
        # storage -- storage name (used in filenames and log messages)
        # size -- size limit in bytes of both files together
        # client -- if not None, use a persistent cache file and use this name
        # var -- directory where to create persistent cache files; default cwd

        self._storage = storage
        self._limit = size / 2
        self._client = client
        self._ltid = None # For getLastTid()

        # Allocate locks:
        L = allocate_lock()
        self._acquire = L.acquire
        self._release = L.release

        if client is not None:
            # Create a persistent cache
            if var is None:
                var = os.getcwd()

            fmt = os.path.join(var, "c%s-%s-%%s.zec" % (storage, client))
            # Initialize pairs of filenames, file objects, and serialnos.
            self._p = p = [fmt % 0, fmt % 1]
            self._f = f = [None, None]
            self._current = 0
            s = [z64, z64]
            for i in 0, 1:
                if os.path.exists(p[i]):
                    fi = open(p[i],'r+b')
                    if fi.read(4) == magic: # Minimal sanity
                        # Read the ltid for this file.  If it never
                        # saw a transaction commit, it will get tossed,
                        # even if it has valid data.
                        s[i] = fi.read(8)
                    # If we found a non-zero serial, then use the file
                    if s[i] != z64:
                        f[i] = fi

            # Whoever has the larger serial is the current
            if s[1] > s[0]:
                current = 1
            elif s[0] > s[1]:
                current = 0
            else:
                if f[0] is None:
                    # We started, open the first cache file
                    f[0] = open(p[0], 'w+b')
                    f[0].write(magic + '\0' * (headersize - len(magic)))
                current = 0
                f[1] = None
        else:
            self._f = f = [tempfile.TemporaryFile(suffix='.zec'), None]
            # self._p file name 'None' signifies an unnamed temp file.
            self._p = p = [None, None]
            f[0].write(magic + '\0' * (headersize - len(magic)))
            current = 0
        self._current = current

        if self._ltid:
            ts = "; last txn=%x" % u64(self._ltid)
        else:
            ts = ""
        self.log("%s: storage=%r, size=%r; file[%r]=%r%s" %
                 (self.__class__.__name__, storage, size, current, p[current],
                  ts))

        self._setup_trace()

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
                self.read_index(serial, not current)
            self._pos = self.read_index(serial, current)

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

    def getLastTid(self):
        """Get the last transaction id stored by setLastTid().

        If the cache is persistent, it is read from the current
        cache file; otherwise it's an instance variable.
        """
        if self._client is None:
            return self._ltid
        else:
            self._acquire()
            try:
                return self._getLastTid()
            finally:
                self._release()

    def _getLastTid(self):
        f = self._f[self._current]
        f.seek(4)
        tid = f.read(8)
        if len(tid) < 8 or tid == z64:
            return None
        else:
            return tid

    def setLastTid(self, tid):
        """Store the last transaction id.

        If the cache is persistent, it is written to the current
        cache file; otherwise it's an instance variable.
        """
        if self._client is None:
            if tid == z64:
                tid = None
            self._ltid = tid
        else:
            self._acquire()
            try:
                self._setLastTid(tid)
            finally:
                self._release()

    def _setLastTid(self, tid):
        if tid is None:
            tid = z64
        else:
            tid = str(tid)
            assert len(tid) == 8
        f = self._f[self._current]
        f.seek(4)
        f.write(tid)

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
                self._trace(0x10, oid, version)
                return None
            f = self._f[p < 0]
            ap = abs(p)
            f.seek(ap)
            h = f.read(27)
            if len(h) != 27:
                self.log("invalidate: short record for oid %s "
                         "at position %d in cache file %d"
                         % (oid_repr(oid), ap, p < 0))
                del self._index[oid]
                return None
            oidlen = unpack(">H", h[:2])[0]
            rec_oid = f.read(oidlen)
            if rec_oid != oid:
                self.log("invalidate: oid mismatch: expected %s read %s "
                         "at position %d in cache file %d"
                         % (oid_repr(oid), oid_repr(rec_oid), ap, p < 0))
                del self._index[oid]
                return None
            f.seek(ap+8) # Switch from reading to writing
            if version and h[15:19] != '\0\0\0\0':
                self._trace(0x1A, oid, version)
                # There's still relevant non-version data in the cache record
                f.write('n')
            else:
                self._trace(0x1C, oid, version)
                del self._index[oid]
                f.write('i')
        finally:
            self._release()

    def load(self, oid, version):
        self._acquire()
        try:
            p = self._get(oid, None)
            if p is None:
                self._trace(0x20, oid, version)
                return None
            f = self._f[p < 0]
            ap = abs(p)
            seek = f.seek
            read = f.read
            seek(ap)
            h = read(27)
            oidlen = unpack(">H", h[:2])[0]
            rec_oid = read(oidlen)
            if len(h)==27 and h[8] in 'nv' and rec_oid == oid:
                tlen, vlen, dlen = unpack(">iHi", h[9:19])
            else:
                tlen = -1
            if tlen <= 0 or vlen < 0 or dlen < 0 or vlen+dlen > tlen:
                self.log("load: bad record for oid %s "
                         "at position %d in cache file %d"
                         % (oid_repr(oid), ap, p < 0))
                del self._index[oid]
                return None

            if h[8]=='n':
                if version:
                    self._trace(0x22, oid, version)
                    return None
                if not dlen:
                    # XXX This shouldn't actually happen
                    self._trace(0x24, oid, version)
                    del self._index[oid]
                    return None

            if not vlen or not version:
                if dlen:
                    data = read(dlen)
                    self._trace(0x2A, oid, version, h[19:], dlen)
                    if (p < 0) != self._current:
                        # If the cache read we are copying has version info,
                        # we need to pass the header to copytocurrent().
                        if vlen:
                            vheader = read(vlen + 4)
                        else:
                            vheader = None
                        self._copytocurrent(ap, oidlen, tlen, dlen, vlen, h,
                                            oid, data, vheader)
                    return data, h[19:]
                else:
                    self._trace(0x26, oid, version)
                    return None

            if dlen:
                seek(dlen, 1)
            vheader = read(vlen+4)
            v = vheader[:-4]
            if version != v:
                if dlen:
                    seek(ap+27+oidlen)
                    data = read(dlen)
                    self._trace(0x2C, oid, version, h[19:], dlen)
                    if (p < 0) != self._current:
                        self._copytocurrent(ap, oidlen, tlen, dlen, vlen, h,
                                            oid, data, vheader)
                    return data, h[19:]
                else:
                    self._trace(0x28, oid, version)
                    return None

            vdlen = unpack(">i", vheader[-4:])[0]
            vdata = read(vdlen)
            vserial = read(8)
            self._trace(0x2E, oid, version, vserial, vdlen)
            if (p < 0) != self._current:
                self._copytocurrent(ap, oidlen, tlen, dlen, vlen, h,
                                    oid, None, vheader, vdata, vserial)
            return vdata, vserial
        finally:
            self._release()

    def _copytocurrent(self, pos, oidlen, tlen, dlen, vlen, header, oid,
                       data=None, vheader=None, vdata=None, vserial=None):
        """Copy a cache hit from the non-current file to the current file.

        Arguments are the file position in the non-current file,
        record length, data length, version string length, header, and
        optionally parts of the record that have already been read.
        """
        if self._pos + tlen > self._limit:
            return # Don't let this cause a cache flip
        assert len(header) == 27, len(header)
        if header[8] == 'n':
            # Rewrite the header to drop the version data.
            # This shortens the record.
            tlen = 31 + oidlen + dlen
            vlen = 0
            vheader = None
            # (oidlen:2, reserved:6, status:1, tlen:4,
            #  vlen:2, dlen:4, serial:8)
            header = header[:9] + pack(">IHI", tlen, vlen, dlen) + header[-8:]
        else:
            assert header[8] == 'v'
        f = self._f[not self._current]
        if data is None:
            f.seek(pos+27+oidlen)
            data = f.read(dlen)
            if len(data) != dlen:
                return
        l = [header, oid, data]
        if vlen:
            assert vheader is not None
            l.append(vheader)
            assert (vdata is None) == (vserial is None)
            if vdata is None:
                vdlen = unpack(">I", vheader[-4:])[0]
                f.seek(pos+27+oidlen+dlen+vlen+4)
                vdata = f.read(vdlen)
                if len(vdata) != vdlen:
                    return
                vserial = f.read(8)
                if len(vserial) != 8:
                    return
            l.append(vdata)
            l.append(vserial)
        else:
            assert None is vheader is vdata is vserial, (
                vlen, vheader, vdata, vserial)
        l.append(header[9:13]) # copy of tlen
        g = self._f[self._current]
        g.seek(self._pos)
        g.writelines(l)
        assert g.tell() == self._pos + tlen
        if self._current:
            self._index[oid] = - self._pos
        else:
            self._index[oid] = self._pos
        self._pos += tlen
        self._trace(0x6A, oid, vlen and vheader[:-4] or '',
                    vlen and vserial or header[-8:], dlen)

    def update(self, oid, serial, version, data):
        self._acquire()
        try:
            self._trace(0x3A, oid, version, serial, len(data))
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
                oidlen = unpack(">H", h[:2])[0]
                rec_oid = read(oidlen)
                if len(h) == 27 and h[8] in 'nv' and rec_oid == oid:
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
                self._trace(0x40, oid)
                return None
            f = self._f[p < 0]
            ap = abs(p)
            seek = f.seek
            read = f.read
            seek(ap)
            h = read(27)
            oidlen = unpack(">H", h[:2])[0]
            rec_oid = read(oidlen)
            if len(h) == 27 and h[8] in 'nv' and rec_oid == oid:
                tlen, vlen, dlen = unpack(">iHi", h[9:19])
            else:
                tlen = -1
            if tlen <= 0 or vlen < 0 or dlen < 0 or vlen+dlen > tlen:
                self.log("modifiedInVersion: bad record for oid %s "
                         "at position %d in cache file %d"
                         % (oid_repr(oid), ap, p < 0))
                del self._index[oid]
                return None

            if h[8] == 'n':
                self._trace(0x4A, oid)
                return None

            if not vlen:
                self._trace(0x4C, oid)
                return ''
            seek(dlen, 1)
            version = read(vlen)
            self._trace(0x4E, oid, version)
            return version
        finally:
            self._release()

    def checkSize(self, size):
        # Make sure we aren't going to exceed the target size.
        # If we are, then flip the cache.
        self._acquire()
        try:
            if self._pos + size > self._limit:
                ltid = self._getLastTid()
                current = not self._current
                self._current = current
                self._trace(0x70)
                self.log("flipping cache files.  new current = %d" % current)
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
                header = magic
                if ltid:
                    header += ltid
                self._f[current].write(header +
                                       '\0' * (headersize - len(header)))
                self._pos = headersize
        finally:
            self._release()

    def store(self, oid, p, s, version, pv, sv):
        self._acquire()
        if s:
            self._trace(0x5A, oid, version, s, len(p))
        else:
            self._trace(0x5C, oid, version, sv, len(pv))
        try:
            self._store(oid, p, s, version, pv, sv)
        finally:
            self._release()

    def _store(self, oid, p, s, version, pv, sv):
        if not s:
            p = ''
            s = z64
        tlen = 31 + len(oid) + len(p)
        if version:
            tlen = tlen + len(version) + 12 + len(pv)
            vlen = len(version)
        else:
            vlen = 0

        stlen = pack(">I", tlen)
        # accumulate various data to write into a list
        assert len(oid) < 2**16
        assert vlen < 2**16
        assert tlen < 2L**32
        l = [pack(">H6x", len(oid)), 'v', stlen,
             pack(">HI", vlen, len(p)), s, oid]
        if p:
            l.append(p)
        if version:
            l.extend([version,
                      pack(">I", len(pv)),
                      pv, sv])
        l.append(stlen)
        f = self._f[self._current]
        f.seek(self._pos)
        f.writelines(l) # write all list elements

        if self._current:
            self._index[oid] = - self._pos
        else:
            self._index[oid] = self._pos

        self._pos += tlen

    def _setup_trace(self):
        # See if cache tracing is requested through $ZEO_CACHE_TRACE.
        # A dash and the storage name are appended to get the filename.
        # If not, or if we can't write to the trace file,
        # disable tracing by setting self._trace to a dummy function.
        self._tracefile = None
        tfn = os.environ.get("ZEO_CACHE_TRACE")
        if tfn:
            tfn = tfn + "-" + self._storage
            try:
                self._tracefile = open(tfn, "ab")
                self._trace(0x00)
            except IOError, msg:
                self._tracefile = None
                self.log("cannot write tracefile %s (%s)" % (tfn, msg))
            else:
                self.log("opened tracefile %s" % tfn)
        if self._tracefile is None:
            def notrace(*args):
                pass
            self._trace = notrace

    def _trace(self, code, oid='', version='', serial='', dlen=0,
               # Remaining arguments are speed hacks
               time_time=time.time, struct_pack=pack):
        # The code argument is two hex digits; bits 0 and 7 must be zero.
        # The first hex digit shows the operation, the second the outcome.
        # If the second digit is in "02468" then it is a 'miss'.
        # If it is in "ACE" then it is a 'hit'.
        # This method has been carefully tuned to be as fast as possible.
        # Note: when tracing is disabled, this method is hidden by a dummy.
        if version:
            code |= 0x80
        self._tracefile.write(
            struct_pack(">iiH8s",
                        time_time(),
                        (dlen+255) & 0x7fffff00 | code | self._current,
                        len(oid),
                        serial) + oid)

    def read_index(self, serial, fileindex):
        index = self._index
        f = self._f[fileindex]
        seek = f.seek
        read = f.read
        pos = headersize
        count = 0

        while 1:
            f.seek(pos)
            h = read(27)
            if len(h) != 27:
                # An empty read is expected, anything else is suspect
                if h:
                    self.rilog("truncated header", pos, fileindex)
                break

            if h[8] in 'vni':
                tlen, vlen, dlen = unpack(">iHi", h[9:19])
            else:
                tlen = -1
            if tlen <= 0 or vlen < 0 or dlen < 0 or vlen + dlen > tlen:
                self.rilog("invalid header data", pos, fileindex)
                break

            oidlen = unpack(">H", h[:2])[0]
            oid = read(oidlen)

            if h[8] == 'v' and vlen:
                seek(dlen+vlen, 1)
                vdlen = read(4)
                if len(vdlen) != 4:
                    self.rilog("truncated record", pos, fileindex)
                    break
                vdlen = unpack(">i", vdlen)[0]
                if vlen + oidlen + dlen + 43 + vdlen != tlen:
                    self.rilog("inconsistent lengths", pos, fileindex)
                    break
                seek(vdlen, 1)
                vs = read(8)
                if read(4) != h[9:13]:
                    self.rilog("inconsistent tlen", pos, fileindex)
                    break
            else:
                if h[8] in 'vn' and vlen == 0:
                    if oidlen + dlen + 31 != tlen:
                        self.rilog("inconsistent nv lengths", pos, fileindex)
                    seek(dlen, 1)
                    if read(4) != h[9:13]:
                        self.rilog("inconsistent nv tlen", pos, fileindex)
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
            self.log("read_index: cache file %d has %d records and %d bytes"
                     % (fileindex, count, pos))

        return pos

    def rilog(self, msg, pos, fileindex):
        # Helper to log messages from read_index
        self.log("read_index: %s at position %d in cache file %d"
                 % (msg, pos, fileindex))

    def log(self, msg, level=zLOG.INFO):
        # XXX Using the path of the current file means the tags
        # won't match after a cache flip.  But they'll be very similar.
        zLOG.LOG("ZEC:%s" % self._p[self._current], level, msg)
