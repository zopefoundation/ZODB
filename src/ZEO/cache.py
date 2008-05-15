##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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
"""Disk-based client cache for ZEO.

ClientCache exposes an API used by the ZEO client storage.  FileCache stores
objects on disk using a 2-tuple of oid and tid as key.

ClientCache's API is similar to a storage API, with methods like load(),
store(), and invalidate().  It manages in-memory data structures that allow
it to map this richer API onto the simple key-based API of the lower-level
FileCache.
"""

from struct import pack, unpack

import bisect
import BTrees.LLBTree
import BTrees.LOBTree
import logging
import os
import tempfile
import time

import ZODB.fsIndex
import ZODB.lock_file
from ZODB.utils import p64, u64, z64

logger = logging.getLogger("ZEO.cache")

# A disk-based cache for ZEO clients.
# 
# This class provides an interface to a persistent, disk-based cache
# used by ZEO clients to store copies of database records from the
# server.
# 
# The details of the constructor as unspecified at this point.
# 
# Each entry in the cache is valid for a particular range of transaction
# ids.  The lower bound is the transaction that wrote the data.  The
# upper bound is the next transaction that wrote a revision of the
# object.  If the data is current, the upper bound is stored as None;
# the data is considered current until an invalidate() call is made.
# 
# It is an error to call store() twice with the same object without an
# intervening invalidate() to set the upper bound on the first cache
# entry.  Perhaps it will be necessary to have a call the removes
# something from the cache outright, without keeping a non-current
# entry.

# Cache verification
# 
# When the client is connected to the server, it receives
# invalidations every time an object is modified.  When the client is
# disconnected then reconnects, it must perform cache verification to make
# sure its cached data is synchronized with the storage's current state.
# 
# quick verification
# full verification
#


# FileCache stores a cache in a single on-disk file.
#
# On-disk cache structure.
#
# The file begins with a 12-byte header.  The first four bytes are the
# file's magic number - ZEC3 - indicating zeo cache version 4.  The
# next eight bytes are the last transaction id.

magic = "ZEC4"
ZEC_HEADER_SIZE = 12

# After the header, the file contains a contiguous sequence of blocks.  All
# blocks begin with a one-byte status indicator:
#
# 'a'
#       Allocated.  The block holds an object; the next 4 bytes are >I
#       format total block size.
#
# 'f'
#       Free.  The block is free; the next 8 bytes are >Q format total
#       block size.
#
# '1', '2', '3', '4'
#       The block is free, and consists of 1, 2, 3 or 4 bytes total.
#
# "Total" includes the status byte, and size bytes.  There are no
# empty (size 0) blocks.


# Allocated blocks have more structure:
#
#     1 byte allocation status ('a').
#     4 bytes block size, >I format.
#     8 byte oid
#     8 byte start_tid
#     8 byte end_tid
#     4 byte data size
#     data

# The cache's currentofs goes around the file, circularly, forever.
# It's always the starting offset of some block.
#
# When a new object is added to the cache, it's stored beginning at
# currentofs, and currentofs moves just beyond it.  As many contiguous
# blocks needed to make enough room for the new object are evicted,
# starting at currentofs.  Exception:  if currentofs is close enough
# to the end of the file that the new object can't fit in one
# contiguous chunk, currentofs is reset to ZEC_HEADER_SIZE first.


class ClientCache(object):
    """A simple in-memory cache."""

    # The default size of 200MB makes a lot more sense than the traditional
    # default of 20MB.  The default here is misleading, though, since
    # ClientStorage is the only user of ClientCache, and it always passes an
    # explicit size of its own choosing.
    def __init__(self, path=None, size=200*1024**2):

        # - `path`:  filepath for the cache file, or None (in which case
        #   a temp file will be created)
        self.path = path

        # - `maxsize`:  total size of the cache file, in bytes; this is
        #   ignored path names an existing file; perhaps we should attempt
        #   to change the cache size in that case
        self.maxsize = size

        # The number of records in the cache.
        self._len = 0

        # {oid -> pos}
        self.current = ZODB.fsIndex.fsIndex()

        # {oid -> {tid->pos}}
        # Note that caches in the wild seem to have very little non-current
        # data, so this would seem to have little impact on memory consumption.
        # I wonder if we even need to store non-current data in the cache.
        self.noncurrent = BTrees.LOBTree.LOBTree()

        # tid for the most recent transaction we know about.  This is also
        # stored near the start of the file.
        self.tid = None

        # Always the offset into the file of the start of a block.
        # New and relocated objects are always written starting at
        # currentofs.
        self.currentofs = ZEC_HEADER_SIZE

        # self.f is the open file object.
        # When we're not reusing an existing file, self.f is left None
        # here -- the scan() method must be called then to open the file
        # (and it sets self.f).

        if path:
            self._lock_file = ZODB.lock_file.LockFile(path + '.lock')
        
        if path and os.path.exists(path):
            # Reuse an existing file.  scan() will open & read it.
            self.f = None
            logger.info("reusing persistent cache file %r", path)
        else:
            if path:
                self.f = open(path, 'wb+')
                logger.info("created persistent cache file %r", path)
            else:
                self.f = tempfile.TemporaryFile()
                logger.info("created temporary cache file %r", self.f.name)
            # Make sure the OS really saves enough bytes for the file.
            self.f.seek(self.maxsize - 1)
            self.f.write('x')
            self.f.truncate()
            # Start with one magic header block
            self.f.seek(0)
            self.f.write(magic)
            self.f.write(z64)
            # and one free block.
            self.f.write('f' + pack(">Q", self.maxsize - ZEC_HEADER_SIZE))
            sync(self.f)

        # Statistics:  _n_adds, _n_added_bytes,
        #              _n_evicts, _n_evicted_bytes,
        #              _n_accesses
        self.clearStats()

        self._setup_trace(path)

    # Backward compatibility. Client code used to have to use the fc
    # attr to get to the file cache to get cache stats.
    @property
    def fc(self):
        return self

    ##
    # Scan the current contents of the cache file, calling `install`
    # for each object found in the cache.  This method should only
    # be called once to initialize the cache from disk.
    def open(self):
        if self.f is not None:  # we're not (re)using a pre-existing file
            return
        fsize = os.path.getsize(self.path)
        if fsize != self.maxsize:
            logger.warning("existing cache file %r has size %d; "
                           "requested size %d ignored", self.path,
                           fsize, self.maxsize)
            self.maxsize = fsize
        self.f = open(self.path, 'rb+')
        read = self.f.read
        seek = self.f.seek
        _magic = read(4)
        if _magic != magic:
            raise ValueError("unexpected magic number: %r" % _magic)
        self.tid = read(8)
        if len(self.tid) != 8:
            raise ValueError("cache file too small -- no tid at start")

        # Populate .filemap and .key2entry to reflect what's currently in the
        # file, and tell our parent about it too (via the `install` callback).
        # Remember the location of the largest free block.  That seems a
        # decent place to start currentofs.
        max_free_size = l = 0
        ofs = max_free_offset = ZEC_HEADER_SIZE
        current = self.current
        while ofs < fsize:
            seek(ofs)
            status = read(1)
            if status == 'a':
                size, oid, start_tid, end_tid = unpack(">I8s8s8s", read(28))

                if end_tid == z64:
                    current[oid] = ofs
                else:
                    assert start_tid < end_tid
                    self._set_noncurrent(oid, start_tid, ofs)
                l += 1
            elif status == 'f':
                size, = unpack(">Q", read(8))
            elif status in '12345678':
                size = int(status)
            else:
                raise ValueError("unknown status byte value %s in client "
                                 "cache file" % 0, hex(ord(status)))
            ofs += size

        if ofs != fsize:
            raise ValueError("final offset %s != file size %s in client "
                             "cache file" % (ofs, fsize))
        self.currentofs = max_free_offset
        self._len = l

    def _set_noncurrent(self, oid, tid, ofs):
        noncurrent_for_oid = self.noncurrent.get(u64(oid))
        if noncurrent_for_oid is None:
            noncurrent_for_oid = BTrees.LLBTree.LLBucket()
            self.noncurrent[u64(oid)] = noncurrent_for_oid
        noncurrent_for_oid[u64(tid)] = ofs

    def _del_noncurrent(self, oid, tid):
        noncurrent_for_oid = self.noncurrent[u64(oid)]
        del noncurrent_for_oid[u64(tid)]
        if not noncurrent_for_oid:
            del self.noncurrent[u64(oid)]

    def clearStats(self):
        self._n_adds = self._n_added_bytes = 0
        self._n_evicts = self._n_evicted_bytes = 0
        self._n_accesses = 0

    def getStats(self):
        return (self._n_adds, self._n_added_bytes,
                self._n_evicts, self._n_evicted_bytes,
                self._n_accesses
               )

    ##
    # The number of objects currently in the cache.
    def __len__(self):
        return self._len

    ##
    # Close the underlying file.  No methods accessing the cache should be
    # used after this.
    def close(self):
        if hasattr(self,'_lock_file'):
            self._lock_file.close()
        if self.f:
            sync(self.f)
            self.f.close()
            self.f = None

    ##
    # Evict objects as necessary to free up at least nbytes bytes,
    # starting at currentofs.  If currentofs is closer than nbytes to
    # the end of the file, currentofs is reset to ZEC_HEADER_SIZE first.
    # The number of bytes actually freed may be (and probably will be)
    # greater than nbytes, and is _makeroom's return value.  The file is not
    # altered by _makeroom.  filemap and key2entry are updated to reflect the
    # evictions, and it's the caller's responsibility both to fiddle
    # the file, and to update filemap, to account for all the space
    # freed (starting at currentofs when _makeroom returns, and
    # spanning the number of bytes retured by _makeroom).
    def _makeroom(self, nbytes):
        assert 0 < nbytes <= self.maxsize - ZEC_HEADER_SIZE
        if self.currentofs + nbytes > self.maxsize:
            self.currentofs = ZEC_HEADER_SIZE
        ofs = self.currentofs
        seek = self.f.seek
        read = self.f.read
        current = self.current
        while nbytes > 0:
            seek(ofs)
            status = read(1)
            if status == 'a':
                size, oid, start_tid, end_tid = unpack(">I8s8s8s", read(28))
                self._n_evicts += 1
                self._n_evicted_bytes += size
                if end_tid == z64:
                    del current[oid]
                else:
                    self._del_noncurrent(oid, start_tid)
                self._len -= 1
            else:
                if status == 'f':
                    size = unpack(">Q", read(8))[0]
                else:
                    assert status in '12345678'
                    size = int(status)
            ofs += size
            nbytes -= size
        return ofs - self.currentofs

    ##
    # Update our idea of the most recent tid.  This is stored in the
    # instance, and also written out near the start of the cache file.  The
    # new tid must be strictly greater than our current idea of the most
    # recent tid.
    def setLastTid(self, tid):
        if self.tid is not None and tid <= self.tid:
            raise ValueError("new last tid (%s) must be greater than "
                             "previous one (%s)" % (u64(tid),
                                                    u64(self.tid)))
        assert isinstance(tid, str) and len(tid) == 8
        self.tid = tid
        self.f.seek(len(magic))
        self.f.write(tid)
        self.f.flush()

    ##
    # Return the last transaction seen by the cache.
    # @return a transaction id
    # @defreturn string, or None if no transaction is yet known
    def getLastTid(self):
        if self.tid == z64:
            return None
        else:
            return self.tid

    ##
    # Return the current data record for oid.
    # @param oid object id
    # @return (data record, serial number, tid), or None if the object is not
    #         in the cache
    # @defreturn 3-tuple: (string, string, string)

    def load(self, oid):
        ofs = self.current.get(oid)
        if ofs is None:
            self._trace(0x20, oid)
            return None
        self.f.seek(ofs)
        read = self.f.read
        assert read(1) == 'a'
        size, saved_oid, tid, end_tid, ldata = unpack(
            ">I8s8s8sI", read(32))
        assert saved_oid == oid

        data = read(ldata)
        assert len(data) == ldata
        assert read(8) == oid

        self._n_accesses += 1
        self._trace(0x22, oid, tid, end_tid, ldata)
        return data, tid

    ##
    # Return a non-current revision of oid that was current before tid.
    # @param oid object id
    # @param tid id of transaction that wrote next revision of oid
    # @return data record, serial number, start tid, and end tid
    # @defreturn 4-tuple: (string, string, string, string)

    def loadBefore(self, oid, before_tid):
        noncurrent_for_oid = self.noncurrent.get(u64(oid))
        if noncurrent_for_oid is None:
            self._trace(0x24, oid, "", before_tid)
            return None

        items = noncurrent_for_oid.items(None, u64(before_tid)-1)
        if not items:
            self._trace(0x24, oid, "", before_tid)
            return None
        tid, ofs = items[-1]

        self.f.seek(ofs)
        read = self.f.read
        assert read(1) == 'a'
        size, saved_oid, saved_tid, end_tid, ldata = unpack(
            ">I8s8s8sI", read(32))
        assert saved_oid == oid
        assert saved_tid == p64(tid)
        assert end_tid != z64
        data = read(ldata)
        assert len(data) == ldata
        assert read(8) == oid
        
        if end_tid < before_tid:
            self._trace(0x24, oid, "", before_tid)
            return None
            
        self._n_accesses += 1
        self._trace(0x26, oid, "", saved_tid)
        return data, saved_tid, end_tid

    ##
    # Store a new data record in the cache.
    # @param oid object id
    # @param start_tid the id of the transaction that wrote this revision
    # @param end_tid the id of the transaction that created the next
    #                revision of oid.  If end_tid is None, the data is
    #                current.
    # @param data the actual data

    def store(self, oid, start_tid, end_tid, data):
        seek = self.f.seek
        if end_tid is None:
            ofs = self.current.get(oid)
            if ofs:
                seek(ofs)
                read = self.f.read
                assert read(1) == 'a'
                size, saved_oid, saved_tid, end_tid = unpack(
                    ">I8s8s8s", read(28))
                assert saved_oid == oid
                assert end_tid == z64
                if saved_tid == start_tid:
                    return
                raise ValueError("already have current data for oid")
        else:
            noncurrent_for_oid = self.noncurrent.get(u64(oid))
            if noncurrent_for_oid and (u64(start_tid) in noncurrent_for_oid):
                return

        size = 41 + len(data)

        # A number of cache simulation experiments all concluded that the
        # 2nd-level ZEO cache got a much higher hit rate if "very large"
        # objects simply weren't cached.  For now, we ignore the request
        # only if the entire cache file is too small to hold the object.
        if size > self.maxsize - ZEC_HEADER_SIZE:
            return

        self._n_adds += 1
        self._n_added_bytes += size
        self._len += 1

        nfreebytes = self._makeroom(size)
        assert size <= nfreebytes
        excess = nfreebytes - size
        # If there's any excess (which is likely), we need to record a
        # free block following the end of the data record.  That isn't
        # expensive -- it's all a contiguous write.
        if excess == 0:
            extra = ''
        elif excess < 9:
            extra = "012345678"[excess]
        else:
            extra = 'f' + pack(">Q", excess)

        ofs = self.currentofs
        seek(ofs)
        write = self.f.write

        # Before writing data, we'll write a free block for the space freed.
        # We'll come back with a last atomic write to rewrite the start of the
        # allocated-block header.
        write('f'+pack(">Q", nfreebytes)+'xxxx')

        # Now write the rest of the allocation block header and object data.
        write(pack(">8s8sI", start_tid, end_tid or z64, len(data)))
        write(data)
        write(oid)
        write(extra)

        # Now, we'll go back and rewrite the beginning of the
        # allocated block header.
        seek(ofs)
        write('a'+pack(">I8s", size, oid))

        if end_tid:
            self._set_noncurrent(oid, start_tid, ofs)
            self._trace(0x54, oid, start_tid, end_tid, dlen=len(data))
        else:
            self.current[oid] = ofs
            self._trace(0x52, oid, start_tid, dlen=len(data))
            
        self.currentofs += size

    ##
    # If `tid` is None,
    # forget all knowledge of `oid`.  (`tid` can be None only for
    # invalidations generated by startup cache verification.)  If `tid`
    # isn't None, and we had current
    # data for `oid`, stop believing we have current data, and mark the
    # data we had as being valid only up to `tid`.  In all other cases, do
    # nothing.
    # @param oid object id
    # @param tid the id of the transaction that wrote a new revision of oid,
    #        or None to forget all cached info about oid.
    def invalidate(self, oid, tid):
        if tid > self.tid and tid is not None:
            self.setLastTid(tid)

        ofs = self.current.get(oid)
        if ofs is None:
            # 0x10 == invalidate (miss)
            self._trace(0x10, oid, tid)
            return

        self.f.seek(ofs)
        read = self.f.read
        assert read(1) == 'a'
        size, saved_oid, saved_tid, end_tid = unpack(">I8s8s8s", read(28))
        assert saved_oid == oid
        assert end_tid == z64
        del self.current[oid]
        if tid is None:
            self.f.seek(ofs)
            self.f.write('f'+pack(">Q", size))
            # 0x1E = invalidate (hit, discarding current or non-current)
            self._trace(0x1E, oid, tid)
            self._len -= 1
        else:
            self.f.seek(ofs+21)
            self.f.write(tid)
            self._set_noncurrent(oid, saved_tid, ofs)
            # 0x1C = invalidate (hit, saving non-current)
            self._trace(0x1C, oid, tid)

    ##
    # Generates (oid, serial) oairs for all objects in the
    # cache.  This generator is used by cache verification.
    def contents(self):
        # May need to materialize list instead of iterating;
        # depends on whether the caller may change the cache.
        seek = self.f.seek
        read = self.f.read
        for oid, ofs in self.current.iteritems():
            seek(ofs)
            assert read(1) == 'a'
            size, saved_oid, tid, end_tid = unpack(">I8s8s8s", read(28))
            assert saved_oid == oid
            assert end_tid == z64
            yield oid, tid

    def dump(self):
        from ZODB.utils import oid_repr
        print "cache size", len(self)
        L = list(self.contents())
        L.sort()
        for oid, tid in L:
            print oid_repr(oid), oid_repr(tid)
        print "dll contents"
        L = list(self)
        L.sort(lambda x, y: cmp(x.key, y.key))
        for x in L:
            end_tid = x.end_tid or z64
            print oid_repr(x.key[0]), oid_repr(x.key[1]), oid_repr(end_tid)
        print

    # If `path` isn't None (== we're using a persistent cache file), and
    # envar ZEO_CACHE_TRACE is set to a non-empty value, try to open
    # path+'.trace' as a trace file, and store the file object in
    # self._tracefile.  If not, or we can't write to the trace file, disable
    # tracing by setting self._trace to a dummy function, and set
    # self._tracefile to None.
    def _setup_trace(self, path):
        _tracefile = None
        if path and os.environ.get("ZEO_CACHE_TRACE"):
            tfn = path + ".trace"
            try:
                _tracefile = open(tfn, "ab")
            except IOError, msg:
                logger.warning("cannot write tracefile %r (%s)", tfn, msg)
            else:
                logger.info("opened tracefile %r", tfn)

        if _tracefile is None:
            self._trace = lambda *a, **k: None
            return

        now = time.time
        def _trace(code, oid="", tid=z64, end_tid=z64, dlen=0):
            # The code argument is two hex digits; bits 0 and 7 must be zero.
            # The first hex digit shows the operation, the second the outcome.
            # This method has been carefully tuned to be as fast as possible.
            # Note: when tracing is disabled, this method is hidden by a dummy.
            encoded = (dlen + 255) & 0x7fffff00 | code
            if tid is None:
                tid = z64
            if end_tid is None:
                end_tid = z64
            try:
                _tracefile.write(
                    pack(">iiH8s8s",
                         now(), encoded, len(oid), tid, end_tid) + oid,
                    )
            except:
                print `tid`, `end_tid`
                raise
                
        self._trace = _trace
        _trace(0x00)


def sync(f):
    f.flush()

if hasattr(os, 'fsync'):
    def sync(f):
        f.flush()
        os.fsync(f.fileno())
