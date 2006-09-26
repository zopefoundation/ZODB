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

import bisect
import logging
import os
import struct
import tempfile
import time

from ZODB.utils import z64, u64

logger = logging.getLogger("ZEO.cache")

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
# invalidations every time an object is modified.  When the client is
# disconnected then reconnects, it must perform cache verification to make
# sure its cached data is synchronized with the storage's current state.
# <p>
# quick verification
# full verification
# <p>

class ClientCache(object):
    """A simple in-memory cache."""

    ##
    # Do we put the constructor here?
    # @param path path of persistent snapshot of cache state (a file path)
    # @param size size of cache file, in bytes

    # The default size of 200MB makes a lot more sense than the traditional
    # default of 20MB.  The default here is misleading, though, since
    # ClientStorage is the only user of ClientCache, and it always passes an
    # explicit size of its own choosing.
    def __init__(self, path=None, size=200*1024**2):
        self.path = path
        self.size = size

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

        # Maps oid to current tid.  Used to compute key for objects.
        self.current = {}

        # Maps oid to list of (start_tid, end_tid) pairs in sorted order.
        # Used to find matching key for load of non-current data.
        self.noncurrent = {}

        # Map oid to (version, tid) pair.  If there is no entry, the object
        # is not modified in a version.
        self.version = {}

        # A FileCache instance does all the low-level work of storing
        # and retrieving objects to/from the cache file.
        self.fc = FileCache(size, self.path, self)

        self._setup_trace(self.path)

    def open(self):
        self.fc.scan(self.install)

    ##
    # Callback for FileCache.scan(), when a pre-existing file cache is
    # used.  For each object in the file, `install()` is invoked.  `f`
    # is the file object, positioned at the start of the serialized Object.
    # `ent` is an Entry giving the object's key ((oid, start_tid) pair).
    def install(self, f, ent):
        # Called by cache storage layer to insert object.
        o = Object.fromFile(f, ent.key, skip_data=True)
        if o is None:
            return
        oid = o.key[0]
        if o.version:
            self.version[oid] = o.version, o.start_tid
        elif o.end_tid is None:
            self.current[oid] = o.start_tid
        else:
            assert o.start_tid < o.end_tid
            this_span = o.start_tid, o.end_tid
            span_list = self.noncurrent.get(oid)
            if span_list:
                bisect.insort_left(span_list, this_span)
            else:
                self.noncurrent[oid] = [this_span]

    def close(self):
        self.fc.close()
        if self._tracefile:
            sync(self._tracefile)
            self._tracefile.close()
            self._tracefile = None

    ##
    # Set the last transaction seen by the cache.
    # @param tid a transaction id
    # @exception ValueError attempt to set a new tid less than the current tid

    def setLastTid(self, tid):
        self.fc.settid(tid)

    ##
    # Return the last transaction seen by the cache.
    # @return a transaction id
    # @defreturn string, or None if no transaction is yet known

    def getLastTid(self):
        if self.fc.tid == z64:
            return None
        else:
            return self.fc.tid

    ##
    # Return the current data record for oid and version.
    # @param oid object id
    # @param version a version string
    # @return (data record, serial number, tid), or None if the object is not
    #         in the cache
    # @defreturn 3-tuple: (string, string, string)

    def load(self, oid, version=""):
        tid = None
        if version:
            p = self.version.get(oid)
            if p is None:
                self._trace(0x20, oid, version)
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
            self._trace(0x20, oid, version)
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
            self._trace(0x24, oid, "", tid)
            return None
        # A pair with None as the second element is less than any pair with
        # the same first tid.  Dubious:  this relies on that None is less
        # than any comparable non-None object in recent Pythons.
        i = bisect.bisect_left(L, (tid, None))
        # Now L[i-1] < (tid, None) < L[i], and the start_tid for everything in
        # L[:i] is < tid, and the start_tid for everything in L[i:] is >= tid.
        # Therefore the largest start_tid < tid must be at L[i-1].  If i is 0,
        # there is no start_tid < tid:  we don't have any data old enougn.
        if i == 0:
            self._trace(0x24, oid, "", tid)
            return
        lo, hi = L[i-1]
        assert lo < tid
        if tid > hi:    # we don't have any data in the right range
            self._trace(0x24, oid, "", tid)
            return None
        o = self.fc.access((oid, lo))
        self._trace(0x26, oid, "", tid)
        return o.data, o.start_tid, o.end_tid

    ##
    # Return the version an object is modified in, or None for an
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
    #                be None if version is not the empty string.
    # @param start_tid the id of the transaction that wrote this revision
    # @param end_tid the id of the transaction that created the next
    #                revision of oid.  If end_tid is None, the data is
    #                current.
    # @param data the actual data
    # @exception ValueError tried to store non-current version data

    def store(self, oid, version, start_tid, end_tid, data):
        # It's hard for the client to avoid storing the same object
        # more than once.  One case is when the client requests
        # version data that doesn't exist.  It checks the cache for
        # the requested version, doesn't find it, then asks the server
        # for that data.  The server returns the non-version data,
        # which may already be in the cache.
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
                bisect.insort_left(L, p)
                self._trace(0x54, oid, version, start_tid, end_tid,
                            dlen=len(data))
        self.fc.add(o)

    ##
    # Remove all knowledge of noncurrent revisions of oid, both in
    # self.noncurrent and in our FileCache.  `version` and `tid` are used
    # only for trace records.
    def _remove_noncurrent_revisions(self, oid, version, tid):
        noncurrent_list = self.noncurrent.get(oid)
        if noncurrent_list:
            # Note:  must iterate over a copy of noncurrent_list.  The
            # FileCache remove() calls our _evicted() method, and that
            # mutates the list.
            for old_tid, dummy in noncurrent_list[:]:
                # 0x1E = invalidate (hit, discarding current or non-current)
                self._trace(0x1E, oid, version, tid)
                self.fc.remove((oid, old_tid))
            # fc.remove() calling back to _evicted() should have removed
            # the list from noncurrent when the last non-current revision
            # was removed.
            assert oid not in self.noncurrent

    ##
    # If `tid` is None, or we have data for `oid` in a (non-empty) version,
    # forget all knowledge of `oid`.  (`tid` can be None only for
    # invalidations generated by startup cache verification.)  If `tid`
    # isn't None, we don't have version data for `oid`, and we had current
    # data for `oid`, stop believing we have current data, and mark the
    # data we had as being valid only up to `tid`.  In all other cases, do
    # nothing.
    # @param oid object id
    # @param version name of version to invalidate.
    # @param tid the id of the transaction that wrote a new revision of oid,
    #        or None to forget all cached info about oid (version, current
    #        revision, and non-current revisions)
    def invalidate(self, oid, version, tid):
        if tid > self.fc.tid and tid is not None:
            self.fc.settid(tid)

        remove_all_knowledge_of_oid = tid is None

        if oid in self.version:
            # Forget we know about the version data.
            # 0x1A = invalidate (hit, version)
            self._trace(0x1A, oid, version, tid)
            dllversion, dlltid = self.version[oid]
            assert not version or version == dllversion, (version, dllversion)
            self.fc.remove((oid, dlltid))
            assert oid not in self.version # .remove() got rid of it
            # And continue:  we must also remove any non-version data from
            # the cache.  Or, at least, I have such a poor understanding of
            # versions that anything less drastic would probably be wrong.
            remove_all_knowledge_of_oid = True

        if remove_all_knowledge_of_oid:
            self._remove_noncurrent_revisions(oid, version, tid)

        # Only current, non-version data remains to be handled.

        cur_tid = self.current.get(oid)
        if not cur_tid:
            # 0x10 == invalidate (miss)
            self._trace(0x10, oid, version, tid)
            return

        # We had current data for oid, but no longer.

        if remove_all_knowledge_of_oid:
            # 0x1E = invalidate (hit, discarding current or non-current)
            self._trace(0x1E, oid, version, tid)
            self.fc.remove((oid, cur_tid))
            assert cur_tid not in self.current  # .remove() got rid of it
            return

        # Add the data we have to the list of non-current data for oid.
        assert tid is not None and cur_tid < tid
        # 0x1C = invalidate (hit, saving non-current)
        self._trace(0x1C, oid, version, tid)
        del self.current[oid]   # because we no longer have current data

        # Update the end_tid half of oid's validity range on disk.
        # TODO: Want to fetch object without marking it as accessed.
        o = self.fc.access((oid, cur_tid))
        assert o is not None
        assert o.end_tid is None  # i.e., o was current
        if o is None:
            # TODO:  Since we asserted o is not None above, this block
            # should be removed; waiting on time to prove it can't happen.
            return
        o.end_tid = tid
        self.fc.update(o)   # record the new end_tid on disk
        # Add to oid's list of non-current data.
        L = self.noncurrent.setdefault(oid, [])
        bisect.insort_left(L, (cur_tid, tid))

    ##
    # Return the number of object revisions in the cache.
    #
    # Or maybe better to just return len(self.cache)?  Needs clearer use case.
    def __len__(self):
        n = len(self.current) + len(self.version)
        if self.noncurrent:
            n += sum(map(len, self.noncurrent))
        return n

    ##
    # Generates (oid, serial, version) triples for all objects in the
    # cache.  This generator is used by cache verification.
    def contents(self):
        # May need to materialize list instead of iterating;
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
        L.sort(lambda x, y: cmp(x.key, y.key))
        for x in L:
            end_tid = x.end_tid or z64
            print oid_repr(x.key[0]), oid_repr(x.key[1]), oid_repr(end_tid)
        print

    def _evicted(self, o):
        # Called by the FileCache to signal that Object o has been evicted.
        oid, tid = o.key
        if o.end_tid is None:
            if o.version:
                del self.version[oid]
            else:
                del self.current[oid]
        else:
            # Although we use bisect to keep the list sorted,
            # we never expect the list to be very long.  So the
            # brute force approach should normally be fine.
            L = self.noncurrent[oid]
            element = (o.start_tid, o.end_tid)
            if len(L) == 1:
                # We don't want to leave an empty list in the dict:  if
                # the oid is never referenced again, it would consume RAM
                # forever more for no purpose.
                assert L[0] == element
                del self.noncurrent[oid]
            else:
                L.remove(element)

    # If `path` isn't None (== we're using a persistent cache file), and
    # envar ZEO_CACHE_TRACE is set to a non-empty value, try to open
    # path+'.trace' as a trace file, and store the file object in
    # self._tracefile.  If not, or we can't write to the trace file, disable
    # tracing by setting self._trace to a dummy function, and set
    # self._tracefile to None.
    def _setup_trace(self, path):
        self._tracefile = None
        if path and os.environ.get("ZEO_CACHE_TRACE"):
            tfn = path + ".trace"
            try:
                self._tracefile = open(tfn, "ab")
                self._trace(0x00)
            except IOError, msg:
                self._tracefile = None
                logger.warning("cannot write tracefile %r (%s)", tfn, msg)
            else:
                logger.info("opened tracefile %r", tfn)

        if self._tracefile is None:
            def notrace(*args, **kws):
                pass
            self._trace = notrace

    def _trace(self,
               code, oid="", version="", tid=z64, end_tid=z64, dlen=0,
               # The next two are just speed hacks.
               time_time=time.time, struct_pack=struct.pack):
        # The code argument is two hex digits; bits 0 and 7 must be zero.
        # The first hex digit shows the operation, the second the outcome.
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
            self._tracefile.write(
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
# in the header used by the cache file's storage format.
# <p>
# Instances of Object are generally short-lived -- they're really a way to
# package data on the way to or from the disk file.

class Object(object):
    __slots__ = (# pair (object id, txn id) -- something usable as a dict key;
                 # the second part of the pair is equal to start_tid
                 "key",

                 # string, tid of txn that wrote the data
                 "start_tid",

                 # string, tid of txn that wrote next revision, or None
                 # if the data is current; if not None, end_tid is strictly
                 # greater than start_tid
                 "end_tid",

                 # string, name of version
                 "version",

                 # string, the actual data record for the object
                 "data",

                 # total size of serialized object; this includes the
                 # data, version, and all overhead (header) bytes.
                 "size",
                )

    # A serialized Object on disk looks like:
    #
    #         offset             # bytes   value
    #         ------             -------   -----
    #              0                   8   end_tid; string
    #              8                   2   len(version); 2-byte signed int
    #             10                   4   len(data); 4-byte signed int
    #             14        len(version)   version; string
    # 14+len(version)          len(data)   the object pickle; string
    # 14+len(version)+
    #       len(data)                  8   oid; string

    # The serialization format uses an end tid of "\0"*8 (z64), the least
    # 8-byte string, to represent None.  It isn't possible for an end_tid
    # to be 0, because it must always be strictly greater than the start_tid.

    fmt = ">8shi"  # end_tid, len(self.version), len(self.data)
    FIXED_HEADER_SIZE = struct.calcsize(fmt)
    assert FIXED_HEADER_SIZE == 14
    TOTAL_FIXED_SIZE = FIXED_HEADER_SIZE + 8  # +8 for the oid at the end

    def __init__(self, key, version, data, start_tid, end_tid):
        self.key = key
        self.version = version
        self.data = data
        self.start_tid = start_tid
        self.end_tid = end_tid
        # The size of the serialized object on disk, including the
        # 14-byte header, the lengths of data and version, and a
        # copy of the 8-byte oid.
        if data is not None:
            self.size = self.TOTAL_FIXED_SIZE + len(data) + len(version)

    ##
    # Return the fixed-sized serialization header as a string:  pack end_tid,
    # and the lengths of the .version and .data members.
    def get_header(self):
        return struct.pack(self.fmt,
                           self.end_tid or z64,
                           len(self.version),
                           len(self.data))

    ##
    # Write the serialized representation of self to file f, at its current
    # position.
    def serialize(self, f):
        f.writelines([self.get_header(),
                      self.version,
                      self.data,
                      self.key[0]])

    ##
    # Write the fixed-size header for self, to file f at its current position.
    # The only real use for this is when the current revision of an object
    # in cache is invalidated.  Then the end_tid field gets set to the tid
    # of the transaction that caused the invalidation.
    def serialize_header(self, f):
        f.write(self.get_header())

    ##
    # fromFile is a class constructor, unserializing an Object from the
    # current position in file f.  Exclusive access to f for the duration
    # is assumed.  The key is a (oid, start_tid) pair, and the oid must
    # match the serialized oid.  If `skip_data` is true, .data is left
    # None in the Object returned, but all the other fields are populated.
    # Else (`skip_data` is false, the default), all fields including .data
    # are populated.  .data can be big, so it's prudent to skip it when it
    # isn't needed.
    def fromFile(cls, f, key, skip_data=False):
        s = f.read(cls.FIXED_HEADER_SIZE)
        if not s:
            return None
        oid, start_tid = key

        end_tid, vlen, dlen = struct.unpack(cls.fmt, s)
        if end_tid == z64:
            end_tid = None

        version = f.read(vlen)
        if vlen != len(version):
            raise ValueError("corrupted record, version")

        if skip_data:
            data = None
            f.seek(dlen, 1)
        else:
            data = f.read(dlen)
            if dlen != len(data):
                raise ValueError("corrupted record, data")

        s = f.read(8)
        if s != oid:
            raise ValueError("corrupted record, oid")

        return cls((oid, start_tid), version, data, start_tid, end_tid)

    fromFile = classmethod(fromFile)


# Entry just associates a key with a file offset.  It's used by FileCache.
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



##
# FileCache stores a cache in a single on-disk file.
#
# On-disk cache structure.
#
# The file begins with a 12-byte header.  The first four bytes are the
# file's magic number - ZEC3 - indicating zeo cache version 3.  The
# next eight bytes are the last transaction id.

magic = "ZEC3"
ZEC3_HEADER_SIZE = 12

# After the header, the file contains a contiguous sequence of blocks.  All
# blocks begin with a one-byte status indicator:
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
# "Total" includes the status byte, and size bytes.  There are no
# empty (size 0) blocks.


# Allocated blocks have more structure:
#
#     1 byte allocation status ('a').
#     4 bytes block size, >I format.
#     16 bytes oid + tid, string.
#     size-OBJECT_HEADER_SIZE bytes, the serialization of an Object (see
#         class Object for details).

OBJECT_HEADER_SIZE = 1 + 4 + 16

# The cache's currentofs goes around the file, circularly, forever.
# It's always the starting offset of some block.
#
# When a new object is added to the cache, it's stored beginning at
# currentofs, and currentofs moves just beyond it.  As many contiguous
# blocks needed to make enough room for the new object are evicted,
# starting at currentofs.  Exception:  if currentofs is close enough
# to the end of the file that the new object can't fit in one
# contiguous chunk, currentofs is reset to ZEC3_HEADER_SIZE first.

# Do all possible to ensure that the bytes we wrote to file f are really on
# disk.
def sync(f):
    f.flush()
    if hasattr(os, 'fsync'):
        os.fsync(f.fileno())

class FileCache(object):

    def __init__(self, maxsize, fpath, parent):
        # - `maxsize`:  total size of the cache file, in bytes; this is
        #   ignored path names an existing file; perhaps we should attempt
        #   to change the cache size in that case
        # - `fpath`:  filepath for the cache file, or None (in which case
        #   a temp file will be created)
        # - `parent`:  the ClientCache instance; its `_evicted()` method
        #   is called whenever we need to evict an object to make room in
        #   the file
        self.maxsize = maxsize
        self.parent = parent

        # tid for the most recent transaction we know about.  This is also
        # stored near the start of the file.
        self.tid = None

        # There's one Entry instance, kept in memory, for each currently
        # allocated block in the file, and there's one allocated block in the
        # file per serialized Object.  filemap retrieves the Entry given the
        # starting offset of a block, and key2entry retrieves the Entry given
        # an object revision's key (an (oid, start_tid) pair).  From an
        # Entry, we can get the Object's key and file offset.

        # Map offset in file to pair (data record size, Entry).
        # Entry is None iff the block starting at offset is free.
        # filemap always contains a complete account of what's in the
        # file -- study method _verify_filemap for executable checking
        # of the relevant invariants.  An offset is at the start of a
        # block iff it's a key in filemap.  The data record size is
        # stored in the file too, so we could just seek to the offset
        # and read it up; keeping it in memory is an optimization.
        self.filemap = {}

        # Map key to Entry.  After
        #     obj = key2entry[key]
        # then
        #     obj.key == key
        # is true.  An object is currently stored on disk iff its key is in
        # key2entry.
        self.key2entry = {}

        # Always the offset into the file of the start of a block.
        # New and relocated objects are always written starting at
        # currentofs.
        self.currentofs = ZEC3_HEADER_SIZE

        # self.f is the open file object.
        # When we're not reusing an existing file, self.f is left None
        # here -- the scan() method must be called then to open the file
        # (and it sets self.f).

        self.fpath = fpath
        if fpath and os.path.exists(fpath):
            # Reuse an existing file.  scan() will open & read it.
            self.f = None
            logger.info("reusing persistent cache file %r", fpath)
        else:
            if fpath:
                self.f = open(fpath, 'wb+')
                logger.info("created persistent cache file %r", fpath)
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
            self.f.write('f' + struct.pack(">I", self.maxsize -
                                                 ZEC3_HEADER_SIZE))
            self.sync()
            self.filemap[ZEC3_HEADER_SIZE] = (self.maxsize - ZEC3_HEADER_SIZE,
                                              None)

        # Statistics:  _n_adds, _n_added_bytes,
        #              _n_evicts, _n_evicted_bytes,
        #              _n_accesses
        self.clearStats()

    ##
    # Scan the current contents of the cache file, calling `install`
    # for each object found in the cache.  This method should only
    # be called once to initialize the cache from disk.
    def scan(self, install):
        if self.f is not None:  # we're not (re)using a pre-existing file
            return
        fsize = os.path.getsize(self.fpath)
        if fsize != self.maxsize:
            logger.warning("existing cache file %r has size %d; "
                           "requested size %d ignored", self.fpath,
                           fsize, self.maxsize)
            self.maxsize = fsize
        self.f = open(self.fpath, 'rb+')
        _magic = self.f.read(4)
        if _magic != magic:
            raise ValueError("unexpected magic number: %r" % _magic)
        self.tid = self.f.read(8)
        if len(self.tid) != 8:
            raise ValueError("cache file too small -- no tid at start")

        # Populate .filemap and .key2entry to reflect what's currently in the
        # file, and tell our parent about it too (via the `install` callback).
        # Remember the location of the largest free block.  That seems a
        # decent place to start currentofs.
        max_free_size = max_free_offset = 0
        ofs = ZEC3_HEADER_SIZE
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
                raise ValueError("unknown status byte value %s in client "
                                 "cache file" % 0, hex(ord(status)))

            self.filemap[ofs] = size, ent
            if ent is None and size > max_free_size:
                max_free_size, max_free_offset = size, ofs

            ofs += size

        if ofs != fsize:
            raise ValueError("final offset %s != file size %s in client "
                             "cache file" % (ofs, fsize))
        if __debug__:
            self._verify_filemap()
        self.currentofs = max_free_offset

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
        return len(self.key2entry)

    ##
    # Iterate over the objects in the cache, producing an Entry for each.
    def __iter__(self):
        return self.key2entry.itervalues()

    ##
    # Test whether an (oid, tid) pair is in the cache.
    def __contains__(self, key):
        return key in self.key2entry

    ##
    # Do all possible to ensure all bytes written to the file so far are
    # actually on disk.
    def sync(self):
        sync(self.f)

    ##
    # Close the underlying file.  No methods accessing the cache should be
    # used after this.
    def close(self):
        if self.f:
            self.sync()
            self.f.close()
            self.f = None

    ##
    # Evict objects as necessary to free up at least nbytes bytes,
    # starting at currentofs.  If currentofs is closer than nbytes to
    # the end of the file, currentofs is reset to ZEC3_HEADER_SIZE first.
    # The number of bytes actually freed may be (and probably will be)
    # greater than nbytes, and is _makeroom's return value.  The file is not
    # altered by _makeroom.  filemap and key2entry are updated to reflect the
    # evictions, and it's the caller's responsibility both to fiddle
    # the file, and to update filemap, to account for all the space
    # freed (starting at currentofs when _makeroom returns, and
    # spanning the number of bytes retured by _makeroom).
    def _makeroom(self, nbytes):
        assert 0 < nbytes <= self.maxsize - ZEC3_HEADER_SIZE
        if self.currentofs + nbytes > self.maxsize:
            self.currentofs = ZEC3_HEADER_SIZE
        ofs = self.currentofs
        while nbytes > 0:
            size, e = self.filemap.pop(ofs)
            if e is not None:
                del self.key2entry[e.key]
                self._evictobj(e, size)
            ofs += size
            nbytes -= size
        return ofs - self.currentofs

    ##
    # Write Object obj, with data, to file starting at currentofs.
    # nfreebytes are already available for overwriting, and it's
    # guranteed that's enough.  obj.offset is changed to reflect the
    # new data record position, and filemap and key2entry are updated to
    # match.
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

    ##
    # Add Object object to the cache.  This may evict existing objects, to
    # make room (and almost certainly will, in steady state once the cache
    # is first full).  The object must not already be in the cache.
    def add(self, object):
        size = OBJECT_HEADER_SIZE + object.size
        # A number of cache simulation experiments all concluded that the
        # 2nd-level ZEO cache got a much higher hit rate if "very large"
        # objects simply weren't cached.  For now, we ignore the request
        # only if the entire cache file is too small to hold the object.
        if size > self.maxsize - ZEC3_HEADER_SIZE:
            return

        assert object.key not in self.key2entry
        assert len(object.key[0]) == 8
        assert len(object.key[1]) == 8

        self._n_adds += 1
        self._n_added_bytes += size

        available = self._makeroom(size)
        self._writeobj(object, available)

    ##
    # Evict the object represented by Entry `e` from the cache, freeing
    # `size` bytes in the file for reuse.  `size` is used only for summary
    # statistics.  This does not alter the file, or self.filemap or
    # self.key2entry (those are the caller's responsibilities).  It does
    # invoke _evicted(Object) on our parent.
    def _evictobj(self, e, size):
        self._n_evicts += 1
        self._n_evicted_bytes += size
        # Load the object header into memory so we know how to
        # update the parent's in-memory data structures.
        self.f.seek(e.offset + OBJECT_HEADER_SIZE)
        o = Object.fromFile(self.f, e.key, skip_data=True)
        self.parent._evicted(o)

    ##
    # Return Object for key, or None if not in cache.
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
    # Remove Object for key from cache, if present.
    def remove(self, key):
        # If an object is being explicitly removed, we need to load
        # its header into memory and write a free block marker to the
        # disk where the object was stored.  We need to load the
        # header to update the in-memory data structures held by
        # ClientCache.

        # We could instead just keep the header in memory at all times.

        e = self.key2entry.pop(key, None)
        if e is None:
            return
        offset = e.offset
        size, e2 = self.filemap[offset]
        assert e is e2
        self.filemap[offset] = size, None
        self.f.seek(offset + OBJECT_HEADER_SIZE)
        o = Object.fromFile(self.f, key, skip_data=True)
        assert size >= 5  # only free blocks are tiny
        # Because `size` >= 5, we can change an allocated block to a free
        # block just by overwriting the 'a' status byte with 'f' -- the
        # size field stays the same.
        self.f.seek(offset)
        self.f.write('f')
        self.f.flush()
        self.parent._evicted(o)

    ##
    # Update on-disk representation of Object obj.
    #
    # This method should be called when the object header is modified.
    # obj must be in the cache.  The only real use for this is during
    # invalidation, to set the end_tid field on a revision that was current
    # (and so had an end_tid of None, but no longer does).
    def update(self, obj):
        e = self.key2entry[obj.key]
        self.f.seek(e.offset + OBJECT_HEADER_SIZE)
        obj.serialize_header(self.f)

    ##
    # Update our idea of the most recent tid.  This is stored in the
    # instance, and also written out near the start of the cache file.  The
    # new tid must be strictly greater than our current idea of the most
    # recent tid.
    def settid(self, tid):
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
    # This debug method marches over the entire cache file, verifying that
    # the current contents match the info in self.filemap and self.key2entry.
    def _verify_filemap(self, display=False):
        a = ZEC3_HEADER_SIZE
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
