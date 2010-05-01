##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""A TransactionBuffer store transaction updates until commit or abort.

A transaction may generate enough data that it is not practical to
always hold pending updates in memory.  Instead, a TransactionBuffer
is used to store the data until a commit or abort.
"""

# A faster implementation might store trans data in memory until it
# reaches a certain size.

from threading import Lock
import os
import cPickle
import tempfile
import ZODB.blob

class TransactionBuffer:

    # Valid call sequences:
    #
    #     ((store | invalidate)* begin_iterate next* clear)* close
    #
    # get_size can be called any time

    # The TransactionBuffer is used by client storage to hold update
    # data until the tpc_finish().  It is normally used by a single
    # thread, because only one thread can be in the two-phase commit
    # at one time.

    # It is possible, however, for one thread to close the storage
    # while another thread is in the two-phase commit.  We must use
    # a lock to guard against this race, because unpredictable things
    # can happen in Python if one thread closes a file that another
    # thread is reading.  In a debug build, an assert() can fail.

    # Caution:  If an operation is performed on a closed TransactionBuffer,
    # it has no effect and does not raise an exception.  The only time
    # this should occur is when a ClientStorage is closed in one
    # thread while another thread is in its tpc_finish().  It's not
    # clear what should happen in this case.  If the tpc_finish()
    # completes without error, the Connection using it could have
    # inconsistent data.  This should have minimal effect, though,
    # because the Connection is connected to a closed storage.

    def __init__(self):
        self.file = tempfile.TemporaryFile(suffix=".tbuf")
        self.lock = Lock()
        self.closed = 0
        self.count = 0
        self.size = 0
        self.blobs = []
        # It's safe to use a fast pickler because the only objects
        # stored are builtin types -- strings or None.
        self.pickler = cPickle.Pickler(self.file, 1)
        self.pickler.fast = 1

    def close(self):
        self.clear()
        self.lock.acquire()
        try:
            self.closed = 1
            try:
                self.file.close()
            except OSError:
                pass
        finally:
            self.lock.release()

    def store(self, oid, data):
        """Store oid, version, data for later retrieval"""
        self.lock.acquire()
        try:
            if self.closed:
                return
            self.pickler.dump((oid, data))
            self.count += 1
            # Estimate per-record cache size
            self.size = self.size + (data and len(data) or 0) + 31
        finally:
            self.lock.release()

    def storeBlob(self, oid, blobfilename):
        self.blobs.append((oid, blobfilename))

    def invalidate(self, oid):
        self.lock.acquire()
        try:
            if self.closed:
                return
            self.pickler.dump((oid, None))
            self.count += 1
        finally:
            self.lock.release()

    def clear(self):
        """Mark the buffer as empty"""
        self.lock.acquire()
        try:
            if self.closed:
                return
            self.file.seek(0)
            self.count = 0
            self.size = 0
            while self.blobs:
                oid, blobfilename = self.blobs.pop()
                if os.path.exists(blobfilename):
                    ZODB.blob.remove_committed(blobfilename)
        finally:
            self.lock.release()

    def __iter__(self):
        self.lock.acquire()
        try:
            if self.closed:
                return
            self.file.flush()
            self.file.seek(0)
            return TBIterator(self.file, self.count)
        finally:
            self.lock.release()

class TBIterator(object):

    def __init__(self, f, count):
        self.file = f
        self.count = count
        self.unpickler = cPickle.Unpickler(f)

    def __iter__(self):
        return self

    def next(self):
        """Return next tuple of data or None if EOF"""
        if self.count == 0:
            self.file.seek(0)
            self.size = 0
            raise StopIteration
        oid_ver_data = self.unpickler.load()
        self.count -= 1
        return oid_ver_data
