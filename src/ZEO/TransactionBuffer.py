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

import tempfile
import cPickle

class TransactionBuffer:

    def __init__(self):
        self.file = tempfile.TemporaryFile(suffix=".tbuf")
        self.count = 0
        self.size = 0
        # It's safe to use a fast pickler because the only objects
        # stored are builtin types -- strings or None.
        self.pickler = cPickle.Pickler(self.file, 1)
        self.pickler.fast = 1

    def close(self): 
        try:
            self.file.close()
        except OSError:
            pass
        

    def store(self, oid, version, data):
        """Store oid, version, data for later retrieval"""
        self.pickler.dump((oid, version, data))
        self.count += 1
        # Estimate per-record cache size
        self.size = self.size + len(data) + (27 + 12)
        if version:
            self.size = self.size + len(version) + 4

    def invalidate(self, oid, version):
        self.pickler.dump((oid, version, None))
        self.count += 1

    def clear(self):
        """Mark the buffer as empty"""
        self.file.seek(0)
        self.count = 0
        self.size = 0

    # unchecked constraints:
    # 1. can't call store() after begin_iterate()
    # 2. must call clear() after iteration finishes

    def begin_iterate(self):
        """Move the file pointer in advance of iteration"""
        self.file.flush()
        self.file.seek(0)
        self.unpickler = cPickle.Unpickler(self.file)

    def next(self):
        """Return next tuple of data or None if EOF"""
        if self.count == 0:
            del self.unpickler
            return None
        oid_ver_data = self.unpickler.load()
        self.count -= 1
        return oid_ver_data

    def get_size(self):
        """Return size of data stored in buffer (just a hint)."""

        return self.size
