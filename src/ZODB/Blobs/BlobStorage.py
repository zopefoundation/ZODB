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

from zope.interface import implements
from zope.proxy import ProxyBase

from ZODB.interfaces import \
        IStorageAdapter, IUndoableStorage, IVersioningStorage, IBlobStorage

class BlobStorage(ProxyBase):
    """A storage to support blobs."""

    implements(IBlobStorage)

    __slots__ = ('base_directory',)

    def __init__(self, base_directory, storage):
        ProxyBase.__init__(self, storage)
        self.base_directory = base_directory
        
    def storeBlob(oid, serial, data, blob, version, transaction):
        """Stores data that has a BLOB attached."""
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)

        self._lock_acquire()
        try:
            # 


        finally:
            self._lock_release()
        return self._tid





    def loadBlob(oid, serial, version, blob):
        """Loads the BLOB data for 'oid' into the given blob object.
        """
