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
"""A TransactionBuffer store transaction updates until commit or abort.

A transaction may generate enough data that it is not practical to
always hold pending updates in memory.  Instead, a TransactionBuffer
is used to store the data until a commit or abort.
"""

# A faster implementation might store trans data in memory until it
# reaches a certain size.

import cPickle
import tempfile
from threading import Lock

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

    # XXX If an operation is performed on a closed TransactionBuffer,
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
        # It's safe to use a fast pickler because the only objects
        # stored are builtin types -- strings or None.
        self.pickler = cPickle.Pickler(self.file, 1)
        self.pickler.fast = 1

    def close(self):
        self.lock.acquire()
        try:
            self.closed = 1
            try:
                self.file.close()
            except OSError:
                pass
        finally:
            self.lock.release()

    def store(self, oid, version, data):
        self.lock.acquire()
        try:
            self._store(oid, version, data)
        finally:
            self.lock.release()

    def _store(self, oid, version, data):
        """Store oid, version, data for later retrieval"""
        if self.closed:
            return
        self.pickler.dump((oid, version, data))
        self.count += 1
        # Estimate per-record cache size
        self.size = self.size + len(data) + 31
        if version:
            # Assume version data has same size as non-version data
            self.size = self.size + len(version) + len(data) + 12

    def invalidate(self, oid, version):
        self.lock.acquire()
        try:
            if self.closed:
                return
            self.pickler.dump((oid, version, None))
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
        finally:
            self.lock.release()

    # unchecked constraints:
    # 1. can't call store() after begin_iterate()
    # 2. must call clear() after iteration finishes

    def begin_iterate(self):
        """Move the file pointer in advance of iteration"""
        self.lock.acquire()
        try:
            if self.closed:
                return
            self.file.flush()
            self.file.seek(0)
            self.unpickler = cPickle.Unpickler(self.file)
        finally:
            self.lock.release()

    def next(self):
        self.lock.acquire()
        try:
            return self._next()
        finally:
            self.lock.release()

    def _next(self):
        """Return next tuple of data or None if EOF"""
        if self.closed:
            return None
        if self.count == 0:
            del self.unpickler
            return None
        oid_ver_data = self.unpickler.load()
        self.count -= 1
        return oid_ver_data

    def get_size(self):
        """Return size of data stored in buffer (just a hint)."""

        return self.size
