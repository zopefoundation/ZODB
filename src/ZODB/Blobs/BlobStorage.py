##############################################################################
#
# Copyright (c) 2005 Zope Corporation and Contributors.
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

import os

from zope.interface import implements
from zope.proxy import ProxyBase, getProxiedObject

from ZODB import utils
from ZODB.Blobs.interfaces import IBlobStorage, IBlob

class BlobStorage(ProxyBase):
    """A storage to support blobs."""

    implements(IBlobStorage)

    __slots__ = ('base_directory', 'dirty_oids')

    def __new__(self, base_directory, storage):
        return ProxyBase.__new__(self, storage)

    def __init__(self, base_directory, storage):    
        # TODO Complain if storage is ClientStorage
        ProxyBase.__init__(self, storage)
        self.base_directory = base_directory
        self.dirty_oids = []
        
    def storeBlob(self, oid, oldserial, data, blobfilename, version, transaction):
        """Stores data that has a BLOB attached."""
        serial = self.store(oid, oldserial, data, version, transaction)
        assert isinstance(serial, str) # XXX in theory serials could be 
                                       # something else

        self._lock_acquire()
        try:
            targetname = self._getCleanFilename(oid, serial)
            try:
                os.rename(blobfilename, targetname)
            except OSError:
                target = file(targetname, "wb")
                source = file(blobfilename, "rb")
                utils.cp(blobfile, target)
                target.close()
                source.close()
                os.unlink(blobfilename)

            # XXX if oid already in there, something is really hosed.
            # The underlying storage should have complained anyway
            self.dirty_oids.append((oid, serial))
        finally:
            self._lock_release()
        return self._tid

    def _getDirtyFilename(self, oid):
        """Generate an intermediate filename for two-phase commit.

        XXX Not used right now due to conceptual flux. Please keep it around
        anyway. 
        """
        return self._getCleanFilename(oid, "store")

    def _getCleanFilename(self, oid, tid):
        return "%s/%s-%s.blob" % \
                (self.base_directory, 
                 utils.oid_repr(oid),
                 utils.tid_repr(tid),
                 )

    def _finish(self, tid, u, d, e): 
        ProxyBase._finish(self, tid, u, d, e)
        self.dirty_blobs = []

    def _abort(self):
        ProxyBase._abort(self)

        # Throw away the stuff we'd had committed
        while self.dirty_blobs:
            oid, serial = self.dirty_blobs.pop()
            os.unlink(self._getCleanFilename(oid))
        
    def loadBlob(self, oid, serial, version):
        """Return the filename where the blob file can be found.
        """
        return self._getCleanFilename(oid, serial)

