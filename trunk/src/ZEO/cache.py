##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Disk-based client cache for ZEO.

ClientCache exposes an API used by the ZEO client storage.  FileCache
stores objects one disk using a 2-tuple of oid and tid as key.

The upper cache's API is similar to a storage API with methods like
load(), store(), and invalidate().  It manages in-memory data
structures that allow it to map this richer API onto the simple
key-based API of the lower-level cache.
"""

import bisect
import logging
import os
import struct
import tempfile
import time

from sets import Set

from ZODB.utils import z64, u64

##
# A disk-based cache for ZEO clients.
# <p>
# This class provides an interface to a persistent, disk-based cache
# used by ZEO clients to store copies of database records from the
# server.
# <p>
# The details of the constructor as unspecified at this point.
# <p>
# Each entry in the cache is valid for a particular range of transaction
# ids.  The lower bound is the transaction that wrote the data.  The
# upper bound is the next transaction that wrote a revision of the
# object.  If the data is current, the upper bound is stored as None;
# the data is considered current until an invalidate() call is made.
# <p>
# It is an error to call store() twice with the same object without an
# intervening invalidate() to set the upper bound on the first cache
# entry.  <em>Perhaps it will be necessary to have a call the removes
# something from the cache outright, without keeping a non-current
# entry.</em>
# <h3>Cache verification</h3>
# <p>
# When the client is connected to the server, it receives
# invalidations every time an object is modified.  Whe the client is
# disconnected, it must perform cache verification to make sure its
# cached data is synchronized with the storage's current state.
# <p>
# quick verification
# full verification
# <p>

class ClientCache:
    """A simple in-memory cache."""

    ##
    # Do we put the constructor here?
    # @param path path of persistent snapshot of cache state
    # @param size maximum size of object data, in bytes

    def __init__(self, path=None, size=None, trace=True):
        self.path = path
        self.size = size
        self.log = logging.getLogger("zeo.cache")

        if trace and path:
            self._setup_trace()
        else:
            self._trace = self._notrace

        # Last transaction seen by the cache, either via setLastTid()
        # or by invalidate().
        self.tid = None

        # The cache stores objects in a dict mapping (oid, tid) pairs
        # to Object() records (see below).  The tid is the transaction
        # id that wrote the object.  An object record includes data,
        # serialno, and end tid.  It has auxillary data structures to
        # compute the appropriate tid, given the oid and a transaction id
        # representing an arbitrary point in history.
        #
        # The serialized form of the cache just stores the Object()
        # records.  The in-memory form can be reconstructed from these
        # records.

        # Maps oid to current tid.  Used to find compute key for objects.
        self.current = {}
        # Maps oid to list of (start_tid, end_tid) pairs in sorted order.
        # Used to find matching key for load of non-current data.
        self.noncurrent = {}
        # Map oid to version, tid pair.  If there is no entry, the object
        # is not modified in a version.
        self.version = {}

        # A double-linked list is used to manage the cache.  It makes
        # decisions about which objects to keep and which to evict.
        self.fc = FileCache(size or 10**6, self.path, self)

    def open(self):
        self.fc.scan(self.install)

    def install(self, f, ent):
        # Called by cache storage layer to insert object
        o = Object.fromFile(f, ent.key, header_only=True)
        if o is None:
            return
        oid = o.key[0]
        if o.version:
            self.version[oid] = o.version, o.start_tid
        elif o.end_tid is None:
            self.current[oid] = o.start_tid
        else:
            L = self.noncurrent.setdefault(oid, [])
            bisect.insort_left(L, (o.start_tid, o.end_tid))

    def close(self):
        self.fc.close()

    ##
    # Set the last transaction seen by the cache.
    # @param tid a transaction id
    # @exception ValueError attempt to set a new tid less than the current tid

    def setLastTid(self, tid):
        self.fc.settid(tid)

    ##
    # Return the last transaction seen by the cache.
    # @return a transaction id
    # @defreturn string

    def getLastTid(self):
        if self.fc.tid == z64:
            return None
        else:
            return self.fc.tid

    ##
    # Return the current data record for oid and version.
    # @param oid object id
    # @param version a version string
    # @return data record, serial number, tid or None if the object is not
    #         in the cache
    # @defreturn 3-tuple: (string, string, string)

    def load(self, oid, version=""):
        tid = None
        if version:
            p = self.version.get(oid)
            if p is None:
                return None
            elif p[0] == version:
                tid = p[1]
            # Otherwise, we know the cache has version data but not
            # for the requested version.  Thus, we know it is safe
            # to return the non-version data from the cache.
        if tid is None:
            tid = self.current.get(oid)
        if tid is None:
            self._trace(0x20, oid, version)
            return None
        o = self.fc.access((oid, tid))
        if o is None:
            return None
        self._trace(0x22, oid, version, o.start_tid, o.end_tid, len(o.data))
        return o.data, tid, o.version

    ##
    # Return a non-current revision of oid that was current before tid.
    # @param oid object id
    # @param tid id of transaction that wrote next revision of oid
    # @return data record, serial number, start tid, and end tid
    # @defreturn 4-tuple: (string, string, string, string)

    def loadBefore(self, oid, tid):
        L = self.noncurrent.get(oid)
        if L is None:
            self._trace(0x24, oid, tid)
            return None
        # A pair with None as the second element will always be less
        # than any pair with the same first tid.
        i = bisect.bisect_left(L, (tid, None))
        # The least element left of tid was written before tid.  If
        # there is no element, the cache doesn't have old enough data.
        if i == 0:
            self._trace(0x24, oid, tid)
            return
        lo, hi = L[i-1]
        # XXX lo should always be less than tid
        if not lo < tid <= hi:
            self._trace(0x24, oid, tid)
            return None
        o = self.fc.access((oid, lo))
        self._trace(0x26, oid, tid)
        return o.data, o.start_tid, o.end_tid

    ##
    # Return the version an object is modified in or None for an
    # object that is not modified in a version.
    # @param oid object id
    # @return name of version in which the object is modified
    # @defreturn string or None

    def modifiedInVersion(self, oid):
        p = self.version.get(oid)
        if p is None:
            return None
        version, tid = p
        return version

    ##
    # Store a new data record in the cache.
    # @param oid object id
    # @param version name of version that oid was modified in.  The cache
    #                only stores current version data, so end_tid should
    #                be None.
    # @param start_tid the id of the transaction that wrote this revision
    # @param end_tid the id of the transaction that created the next
    #                revision of oid.  If end_tid is None, the data is
    #                current.
    # @param data the actual data
    # @exception ValueError tried to store non-current version data

    def store(self, oid, version, start_tid, end_tid, data):
        # It's hard for the client to avoid storing the same object
        # more than once.  One case is whether the client requests
        # version data that doesn't exist.  It checks the cache for
        # the requested version, doesn't find it, then asks the server
        # for that data.  The server returns the non-version data,
        # which may already by in the cache.
        if (oid, start_tid) in self.fc:
            return
        o = Object((oid, start_tid), version, data, start_tid, end_tid)
        if version:
            if end_tid is not None:
                raise ValueError("cache only stores current version data")
            if oid in self.version:
                if self.version[oid] != (version, start_tid):
                    raise ValueError("data already exists for version %r"
                                     % self.version[oid][0])
            self.version[oid] = version, start_tid
            self._trace(0x50, oid, version, start_tid, dlen=len(data))
        else:
            if end_tid is None:
                _cur_start = self.current.get(oid)
                if _cur_start:
                    if _cur_start != start_tid:
                        raise ValueError(
                            "already have current data for oid")
                    else:
                        return
                self.current[oid] = start_tid
                self._trace(0x52, oid, version, start_tid, dlen=len(data))
            else:
                L = self.noncurrent.setdefault(oid, [])
                p = start_tid, end_tid
                if p in L:
                    return # duplicate store
                bisect.insort_left(L, (start_tid, end_tid))
                self._trace(0x54, oid, version, start_tid, end_tid,
                            dlen=len(data))
        self.fc.add(o)

    ##
    # Mark the current data for oid as non-current.  If there is no
    # current data for oid, do nothing.
    # @param oid object id
    # @param version name of version to invalidate.
    # @param tid the id of the transaction that wrote a new revision of oid

    def invalidate(self, oid, version, tid):
        if tid > self.fc.tid:
            self.fc.settid(tid)
        if oid in self.version:
            self._trace(0x1A, oid, version, tid)
            dllversion, dlltid = self.version[oid]
            assert not version or version == dllversion, (version, dllversion)
            # remove() will call unlink() to delete from self.version
            self.fc.remove((oid, dlltid))
            # And continue on, we must also remove any non-version data
            # from the cache.  This is a bit of a failure of the current
            # cache consistency approach as the new tid of the version
            # data gets confused with the old tid of the non-version data.
            # I could sort this out, but it seems simpler to punt and
            # have the cache invalidation too much for versions.

        if oid not in self.current:
            self._trace(0x10, oid, version, tid)
            return
        cur_tid = self.current.pop(oid)
        # XXX Want to fetch object without marking it as accessed
        o = self.fc.access((oid, cur_tid))
        if o is None:
            # XXX is this possible?
            return None
        o.end_tid = tid
        self.fc.update(o)
        self._trace(0x1C, oid, version, tid)
        L = self.noncurrent.setdefault(oid, [])
        bisect.insort_left(L, (cur_tid, tid))

    ##
    # Return the number of object revisions in the cache.

    # XXX just return len(self.cache)?

    def __len__(self):
        n = len(self.current) + len(self.version)
        if self.noncurrent:
            n += sum(map(len, self.noncurrent))
        return n

    ##
    # Generates over, version, serial triples for all objects in the
    # cache.  This generator is used by cache verification.

    def contents(self):
        # XXX May need to materialize list instead of iterating,
        # depends on whether the caller may change the cache.
        for o in self.fc:
            oid, tid = o.key
            if oid in self.version:
                obj = self.fc.access(o.key)
                yield oid, tid, obj.version
            else:
                yield oid, tid, ""

    def dump(self):
        from ZODB.utils import oid_repr
        print "cache size", len(self)
        L = list(self.contents())
        L.sort()
        for oid, tid, version in L:
            print oid_repr(oid), oid_repr(tid), repr(version)
        print "dll contents"
        L = list(self.fc)
        L.sort(lambda x,y:cmp(x.key, y.key))
        for x in L:
            end_tid = x.end_tid or z64
            print oid_repr(x.key[0]), oid_repr(x.key[1]), oid_repr(end_tid)
        print

    def _evicted(self, o):
        # Called by Object o to signal its eviction
        oid, tid = o.key
        if o.end_tid is None:
            if o.version:
                del self.version[oid]
            else:
                del self.current[oid]
        else:
            # XXX Although we use bisect to keep the list sorted,
            # we never expect the list to be very long.  So the
            # brute force approach should normally be fine.
            L = self.noncurrent[oid]
            L.remove((o.start_tid, o.end_tid))

    def _setup_trace(self):
        tfn = self.path + ".trace"
        self.tracefile = None
        try:
            self.tracefile = open(tfn, "ab")
            self._trace(0x00)
        except IOError, msg:
            self.tracefile = None
            self.log.warning("Could not write to trace file %s: %s",
                             tfn, msg)

    def _notrace(self, *arg, **kwargs):
        pass

    def _trace(self,
               code, oid="", version="", tid="", end_tid=z64, dlen=0,
               # The next two are just speed hacks.
               time_time=time.time, struct_pack=struct.pack):
        # The code argument is two hex digits; bits 0 and 7 must be zero.
        # The first hex digit shows the operation, the second the outcome.
        # If the second digit is in "02468" then it is a 'miss'.
        # If it is in "ACE" then it is a 'hit'.
        # This method has been carefully tuned to be as fast as possible.
        # Note: when tracing is disabled, this method is hidden by a dummy.
        if version:
            code |= 0x80
        encoded = (dlen + 255) & 0x7fffff00 | code
        if tid is None:
            tid = z64
        if end_tid is None:
            end_tid = z64
        try:
            self.tracefile.write(
                struct_pack(">iiH8s8s",
                            time_time(),
                            encoded,
                            len(oid),
                            tid, end_tid) + oid)
        except:
            print `tid`, `end_tid`
            raise

##
# An Object stores the cached data for a single object.
# <p>
# The cached data includes the actual object data, the key, and three
# data fields that describe the validity period and version of the
# object.  The key contains the oid and a redundant start_tid.  The
# actual size of an object is variable, depending on the size of the
# data and whether it is in a version.
# <p>
# The serialized format does not include the key, because it is stored
# in the header used by the cache's storage format.

class Object(object):
    __slots__ = (# pair, object id, txn id -- something usable as a dict key
                 # the second part of the part is equal to start_tid below
                 "key",

                 "start_tid", # string, id of txn that wrote the data
                 "end_tid", # string, id of txn that wrote next revision
                            # or None
                 "version", # string, name of version
                 "data", # string, the actual data record for the object

                 "size", # total size of serialized object
                )

    def __init__(self, key, version, data, start_tid, end_tid):
        self.key = key
        self.version = version
        self.data = data
        self.start_tid = start_tid
        self.end_tid = end_tid
        # The size of a the serialized object on disk, include the
        # 14-byte header, the length of data and version, and a
        # copy of the 8-byte oid.
        if data is not None:
            self.size = 22 + len(data) + len(version)

    # The serialization format uses an end tid of "\0" * 8, the least
    # 8-byte string, to represent None.  It isn't possible for an
    # end_tid to be 0, because it must always be strictly greater
    # than the start_tid.

    fmt = ">8shi"

    def serialize(self, f):
        # Write standard form of Object to file, f.
        self.serialize_header(f)
        f.write(self.data)
        f.write(struct.pack(">8s", self.key[0]))

    def serialize_header(self, f):
        s = struct.pack(self.fmt, self.end_tid or "\0" * 8,
                        len(self.version), len(self.data))
        f.write(s)
        f.write(self.version)

    def fromFile(cls, f, key, header_only=False):
        s = f.read(struct.calcsize(cls.fmt))
        if not s:
            return None
        oid, start_tid = key
        end_tid, vlen, dlen = struct.unpack(cls.fmt, s)
        if end_tid == z64:
            end_tid = None
        version = f.read(vlen)
        if vlen != len(version):
            raise ValueError("corrupted record, version")
        if header_only:
            data = None
        else:
            data = f.read(dlen)
            if dlen != len(data):
                raise ValueError("corrupted record, data")
            s = f.read(8)
            if struct.pack(">8s", s) != oid:
                raise ValueError("corrupted record, oid")
        return cls((oid, start_tid), version, data, start_tid, end_tid)

    fromFile = classmethod(fromFile)

def sync(f):
    f.flush()
    if hasattr(os, 'fsync'):
        os.fsync(f.fileno())

class Entry(object):
    __slots__ = (# object key -- something usable as a dict key.
                 'key',

                 # Offset from start of file to the object's data
                 # record; this includes all overhead bytes (status
                 # byte, size bytes, etc).  The size of the data
                 # record is stored in the file near the start of the
                 # record, but for efficiency we also keep size in a
                 # dict (filemap; see later).
                 'offset',
                )

    def __init__(self, key=None, offset=None):
        self.key = key
        self.offset = offset


magic = "ZEC3"

OBJECT_HEADER_SIZE = 1 + 4 + 16

##
# FileCache stores a cache in a single on-disk file.
#
# On-disk cache structure
#
# The file begins with a 12-byte header.  The first four bytes are the
# file's magic number - ZEC3 - indicating zeo cache version 3.  The
# next eight bytes are the last transaction id.
#
# The file is a contiguous sequence of blocks.  All blocks begin with
# a one-byte status indicator:
#
# 'a'
#       Allocated.  The block holds an object; the next 4 bytes are >I
#       format total block size.
#
# 'f'
#       Free.  The block is free; the next 4 bytes are >I format total
#       block size.
#
# '1', '2', '3', '4'
#       The block is free, and consists of 1, 2, 3 or 4 bytes total.
#
# 'Z'
#       File header.  The file starts with a magic number, currently
#       'ZEC3' and an 8-byte transaction id.
#
# "Total" includes the status byte, and size bytes.  There are no
# empty (size 0) blocks.


# XXX This needs a lot more hair.
# The structure of an allocated block is more complicated:
#
#     1 byte allocation status ('a').
#     4 bytes block size, >I format.
#     16 bytes oid + tid, string.
#     size-OBJECT_HEADER_SIZE bytes, the object pickle.

# The cache's currentofs goes around the file, circularly, forever.
# It's always the starting offset of some block.
#
# When a new object is added to the cache, it's stored beginning at
# currentofs, and currentofs moves just beyond it.  As many contiguous
# blocks needed to make enough room for the new object are evicted,
# starting at currentofs.  Exception:  if currentofs is close enough
# to the end of the file that the new object can't fit in one
# contiguous chunk, currentofs is reset to 0 first.

# Do all possible to ensure that the bytes we wrote are really on
# disk.

class FileCache(object):

    def __init__(self, maxsize, fpath, parent, reuse=True):
        # Maximum total of object sizes we keep in cache.
        self.maxsize = maxsize
        # Current total of object sizes in cache.
        self.currentsize = 0
        self.parent = parent
        self.tid = None

        # Map offset in file to pair (data record size, Entry).
        # Entry is None iff the block starting at offset is free.
        # filemap always contains a complete account of what's in the
        # file -- study method _verify_filemap for executable checking
        # of the relevant invariants.  An offset is at the start of a
        # block iff it's a key in filemap.
        self.filemap = {}

        # Map key to Entry.  There's one entry for each object in the
        # cache file.  After
        #     obj = key2entry[key]
        # then
        #     obj.key == key
        # is true.
        self.key2entry = {}

        # Always the offset into the file of the start of a block.
        # New and relocated objects are always written starting at
        # currentofs.
        self.currentofs = 12

        self.fpath = fpath
        if not reuse or not fpath or not os.path.exists(fpath):
            self.new = True
            if fpath:
                self.f = file(fpath, 'wb+')
            else:
                self.f = tempfile.TemporaryFile()
            # Make sure the OS really saves enough bytes for the file.
            self.f.seek(self.maxsize - 1)
            self.f.write('x')
            self.f.truncate()
            # Start with one magic header block
            self.f.seek(0)
            self.f.write(magic)
            self.f.write(z64)
            # and one free block.
            self.f.write('f' + struct.pack(">I", self.maxsize - 12))
            self.sync()
            self.filemap[12] = self.maxsize - 12, None
        else:
            self.new = False
            self.f = None

        # Statistics:  _n_adds, _n_added_bytes,
        #              _n_evicts, _n_evicted_bytes
        self.clearStats()

    # Scan the current contents of the cache file, calling install
    # for each object found in the cache.  This method should only
    # be called once to initialize the cache from disk.

    def scan(self, install):
        if self.new:
            return
        fsize = os.path.getsize(self.fpath)
        self.f = file(self.fpath, 'rb+')
        _magic = self.f.read(4)
        if _magic != magic:
            raise ValueError("unexpected magic number: %r" % _magic)
        self.tid = self.f.read(8)
        # Remember the largest free block.  That seems a
        # decent place to start currentofs.
        max_free_size = max_free_offset = 0
        ofs = 12
        while ofs < fsize:
            self.f.seek(ofs)
            ent = None
            status = self.f.read(1)
            if status == 'a':
                size, rawkey = struct.unpack(">I16s", self.f.read(20))
                key = rawkey[:8], rawkey[8:]
                assert key not in self.key2entry
                self.key2entry[key] = ent = Entry(key, ofs)
                install(self.f, ent)
            elif status == 'f':
                size, = struct.unpack(">I", self.f.read(4))
            elif status in '1234':
                size = int(status)
            else:
                assert 0, hex(ord(status))

            self.filemap[ofs] = size, ent
            if ent is None and size > max_free_size:
                max_free_size, max_free_offset = size, ofs

            ofs += size

        assert ofs == fsize
        if __debug__:
            self._verify_filemap()
        self.currentofs = max_free_offset

    def clearStats(self):
        self._n_adds = self._n_added_bytes = 0
        self._n_evicts = self._n_evicted_bytes = 0
        self._n_removes = self._n_removed_bytes = 0
        self._n_accesses = 0

    def getStats(self):
        return (self._n_adds, self._n_added_bytes,
                self._n_evicts, self._n_evicted_bytes,
                self._n_removes, self._n_removed_bytes,
                self._n_accesses
               )

    def __len__(self):
        return len(self.key2entry)

    def __iter__(self):
        return self.key2entry.itervalues()

    def __contains__(self, key):
        return key in self.key2entry

    def sync(self):
        sync(self.f)

    def close(self):
        if self.f:
            self.sync()
            self.f.close()
            self.f = None

    # Evict objects as necessary to free up at least nbytes bytes,
    # starting at currentofs.  If currentofs is closer than nbytes to
    # the end of the file, currentofs is reset to 0.  The number of
    # bytes actually freed may be (and probably will be) greater than
    # nbytes, and is _makeroom's return value.  The file is not
    # altered by _makeroom.  filemap is updated to reflect the
    # evictions, and it's the caller's responsibilty both to fiddle
    # the file, and to update filemap, to account for all the space
    # freed (starting at currentofs when _makeroom returns, and
    # spanning the number of bytes retured by _makeroom).

    def _makeroom(self, nbytes):
        assert 0 < nbytes <= self.maxsize
        if self.currentofs + nbytes > self.maxsize:
            self.currentofs = 12
        ofs = self.currentofs
        while nbytes > 0:
            size, e = self.filemap.pop(ofs)
            if e is not None:
                self._evictobj(e, size)
            ofs += size
            nbytes -= size
        return ofs - self.currentofs

    # Write Object obj, with data, to file starting at currentofs.
    # nfreebytes are already available for overwriting, and it's
    # guranteed that's enough.  obj.offset is changed to reflect the
    # new data record position, and filemap is updated to match.

    def _writeobj(self, obj, nfreebytes):
        size = OBJECT_HEADER_SIZE + obj.size
        assert size <= nfreebytes
        excess = nfreebytes - size
        # If there's any excess (which is likely), we need to record a
        # free block following the end of the data record.  That isn't
        # expensive -- it's all a contiguous write.
        if excess == 0:
            extra = ''
        elif excess < 5:
            extra = "01234"[excess]
        else:
            extra = 'f' + struct.pack(">I", excess)

        self.f.seek(self.currentofs)
        self.f.writelines(('a',
                           struct.pack(">I8s8s", size,
                                       obj.key[0], obj.key[1])))
        obj.serialize(self.f)
        self.f.write(extra)
        e = Entry(obj.key, self.currentofs)
        self.key2entry[obj.key] = e
        self.filemap[self.currentofs] = size, e
        self.currentofs += size
        if excess:
            # We need to record the free block in filemap, but there's
            # no need to advance currentofs beyond it.  Instead it
            # gives some breathing room for the next object to get
            # written.
            self.filemap[self.currentofs] = excess, None

    def add(self, object):
        size = OBJECT_HEADER_SIZE + object.size
        if size > self.maxsize:
            return
        assert size <= self.maxsize

        assert object.key not in self.key2entry
        assert len(object.key[0]) == 8
        assert len(object.key[1]) == 8

        self._n_adds += 1
        self._n_added_bytes += size

        available = self._makeroom(size)
        self._writeobj(object, available)

    def _verify_filemap(self, display=False):
        a = 12
        f = self.f
        while a < self.maxsize:
            f.seek(a)
            status = f.read(1)
            if status in 'af':
                size, = struct.unpack(">I", f.read(4))
            else:
                size = int(status)
            if display:
                if a == self.currentofs:
                    print '*****',
                print "%c%d" % (status, size),
            size2, obj = self.filemap[a]
            assert size == size2
            assert (obj is not None) == (status == 'a')
            if obj is not None:
                assert obj.offset == a
                assert self.key2entry[obj.key] is obj
            a += size
        if display:
            print
        assert a == self.maxsize

    def _evictobj(self, e, size):
        self._n_evicts += 1
        self._n_evicted_bytes += size
        # Load the object header into memory so we know how to
        # update the parent's in-memory data structures.
        self.f.seek(e.offset + OBJECT_HEADER_SIZE)
        o = Object.fromFile(self.f, e.key, header_only=True)
        self.parent._evicted(o)

    ##
    # Return object for key or None if not in cache.

    def access(self, key):
        self._n_accesses += 1
        e = self.key2entry.get(key)
        if e is None:
            return None
        offset = e.offset
        size, e2 = self.filemap[offset]
        assert e is e2

        self.f.seek(offset + OBJECT_HEADER_SIZE)
        return Object.fromFile(self.f, key)

    ##
    # Remove object for key from cache, if present.

    def remove(self, key):
        # If an object is being explicitly removed, we need to load
        # its header into memory and write a free block marker to the
        # disk where the object was stored.  We need to load the
        # header to update the in-memory data structures held by
        # ClientCache.

        # XXX Or we could just keep the header in memory at all times.

        e = self.key2entry.get(key)
        if e is None:
            return
        offset = e.offset
        size, e2 = self.filemap[offset]
        self.f.seek(offset + OBJECT_HEADER_SIZE)
        o = Object.fromFile(self.f, key, header_only=True)
        self.f.seek(offset + OBJECT_HEADER_SIZE)
        self.f.write('f')
        self.f.flush()
        self.parent._evicted(o)
        self.filemap[offset] = size, None

    ##
    # Update on-disk representation of obj.
    #
    # This method should be called when the object header is modified.

    def update(self, obj):

        e = self.key2entry[obj.key]
        self.f.seek(e.offset + OBJECT_HEADER_SIZE)
        obj.serialize_header(self.f)

    def settid(self, tid):
        if self.tid is not None:
            if tid < self.tid:
                raise ValueError(
                    "new last tid must be greater that previous one")
        self.tid = tid
        self.f.seek(4)
        self.f.write(tid)
        self.f.flush()
