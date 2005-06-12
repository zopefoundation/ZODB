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
import shutil
import base64

from zope.interface import implements
from zope.proxy import ProxyBase, getProxiedObject

from ZODB import utils
from ZODB.Blobs.interfaces import IBlobStorage, IBlob
from ZODB.POSException import POSKeyError

BLOB_SUFFIX = ".blob"
BLOB_DIRTY = "store"

class BlobStorage(ProxyBase):
    """A storage to support blobs."""

    implements(IBlobStorage)

    __slots__ = ('base_directory', 'dirty_oids')
    # XXX CM: what is the purpose of specifying __slots__ here?

    def __new__(self, base_directory, storage):
        return ProxyBase.__new__(self, storage)

    def __init__(self, base_directory, storage):    
        # TODO Complain if storage is ClientStorage
        ProxyBase.__init__(self, storage)
        self.base_directory = base_directory
        self.dirty_oids = []
     
    def storeBlob(self, oid, oldserial, data, blobfilename, version,
                  transaction):
        """Stores data that has a BLOB attached."""
        serial = self.store(oid, oldserial, data, version, transaction)
        assert isinstance(serial, str) # XXX in theory serials could be 
                                       # something else

        self._lock_acquire()
        try:
            targetpath = self._getBlobPath(oid)
            if not os.path.exists(targetpath):
                os.makedirs(targetpath, 0700)
                              
            targetname = self._getCleanFilename(oid, serial)
            utils.best_rename(blobfilename, targetname)

            # XXX if oid already in there, something is really hosed.
            # The underlying storage should have complained anyway
            self.dirty_oids.append((oid, serial))
        finally:
            self._lock_release()
        return self._tid

    def _getDirtyFilename(self, oid):
        """Generate an intermediate filename for two-phase commit.
        """
        return self._getCleanFilename(oid, BLOB_DIRTY)

    def _getBlobPath(self, oid):
        return os.path.join(self.base_directory,
                            utils.oid_repr(oid)
                            )

    def _getCleanFilename(self, oid, tid):
        return os.path.join(self._getBlobPath(oid),
                            "%s%s" % (utils.tid_repr(tid), 
                                      BLOB_SUFFIX,)
                            )

    def _finish(self, tid, u, d, e): 
        ProxyBase._finish(self, tid, u, d, e)
        # Move dirty blobs if they are "really" dirty
        
        self.dirty_blobs = []

    def _abort(self):
        ProxyBase._abort(self)

        # Throw away the stuff we'd had committed
        while self.dirty_blobs:
            oid, serial = self.dirty_blobs.pop()
            clean = self._getCleanFilename(oid, serial)
            dirty = self._getDirtyFilename(oid, serial)
            for filename in [clean, dirty]:
                if os.exists(filename):
                    os.unlink(filename) 

    def loadBlob(self, oid, serial, version):
        """Return the filename where the blob file can be found.
        """
        filename = self._getCleanFilename(oid, serial)
        if not os.path.exists(filename):
            raise POSKeyError, "Not an existing blob."
        return filename

    def _packUndoing(self, packtime, referencesf):

        # Walk over all existing revisions of all blob files and check
        # if they are still needed by attempting to load the revision
        # of that object from the database.  This is maybe the slowest
        # possible way to do this, but it's safe.

        # XXX we should be tolerant of "garbage" directories/files in
        # the base_directory here.

        for oid_repr in os.listdir(self.base_directory):
            oid = utils.repr_to_oid(oid_repr)
            oid_path = os.path.join(self.base_directory, oid_repr)
            files = os.listdir(oid_path)
            files.sort()

            for filename in files:
                filepath = os.path.join(oid_path, filename)
                whatever, serial = self._splitBlobFilename(filepath)
                try:
                    fn = self._getCleanFilename(oid, serial)
                    self.loadSerial(oid, serial)
                except POSKeyError:
                    os.unlink(filepath)

            if not os.listdir(oid_path):
                shutil.rmtree(oid_path)

    def _packNonUndoing(self, packtime, referencesf):
        for oid_repr in os.listdir(self.base_directory):
            oid = utils.repr_to_oid(oid_repr)
            oid_path = os.path.join(self.base_directory, oid_repr)
            exists = True

            try:
                self.load(oid, None) # no version support
            except (POSKeyError, KeyError):
                exists = False

            if exists:
                files = os.listdir(oid_path)
                files.sort()
                latest = files[-1] # depends on ever-increasing tids
                files.remove(latest)
                for file in files:
                    os.unlink(os.path.join(oid_path, file))
            else:
                shutil.rmtree(oid_path)
                continue

            if not os.listdir(oid_path):
                shutil.rmtree(oid_path)

    def pack(self, packtime, referencesf):
        """Remove all unused oid/tid combinations."""
        unproxied = getProxiedObject(self)

        # pack the underlying storage, which will allow us to determine
        # which serials are current.
        result = unproxied.pack(packtime, referencesf)

        # perform a pack on blob data
        self._lock_acquire()
        try:
            if unproxied.supportsUndo():
                self._packUndoing(packtime, referencesf)
            else:
                self._packNonUndoing(packtime, referencesf)
        finally:
            self._lock_release()

        return result
    
    def getSize(self):
        """Return the size of the database in bytes."""
        orig_size = getProxiedObject(self).getSize()
        
        blob_size = 0
        for oid in os.listdir(self.base_directory):
            for serial in os.listdir(os.path.join(self.base_directory, oid)):
                if not serial.endswith(BLOB_SUFFIX):
                    continue
                file_path = os.path.join(self.base_directory, oid, serial)
                blob_size += os.stat(file_path).st_size
        
        return orig_size + blob_size

    def _splitBlobFilename(self, filename):
        """Returns OID, TID for a given blob filename.

        If it's not a blob filename, (None, None) is returned.
        """
        if not filename.endswith(BLOB_SUFFIX):
            return None, None
        path, filename = os.path.split(filename)
        oid = os.path.split(path)[1]

        serial = filename[:-len(BLOB_SUFFIX)]
        oid = utils.repr_to_oid(oid)
        if serial != BLOB_DIRTY:
            serial = utils.repr_to_oid(serial)
        else:
            serial = None
        return oid, serial 

    def undo(self, serial_id, transaction):
        serial, keys = getProxiedObject(self).undo(serial_id, transaction)
        self._lock_acquire()
        try:
            # The old serial_id is given in base64 encoding ...
            serial_id = base64.decodestring(serial_id+ '\n')
            for oid in self._getOIDsForSerial(serial_id):
                data, serial_before, serial_after = \
                        self.loadBefore(oid, serial_id) 
                orig = file(self._getCleanFilename(oid, serial_before), "r")
                new = file(self._getCleanFilename(oid, serial), "w")
                utils.cp(orig, new)
                orig.close()
                new.close()
                self.dirty_oids.append((oid, serial))
        finally:
            self._lock_release()
        return serial, keys

    def _getOIDsForSerial(self, search_serial):
        oids = []
        for oidpath in os.listdir(self.base_directory):
            for filename in os.listdir(os.path.join(self.base_directory,
                                     oidpath)):
                blob_path = os.path.join(self.base_directory, oidpath, 
                                         filename)
                oid, serial = self._splitBlobFilename(blob_path)
                if search_serial == serial:
                    oids.append(oid)
        return oids
