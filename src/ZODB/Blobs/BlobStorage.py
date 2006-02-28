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
import logging

from zope.interface import implements
from zope.proxy import ProxyBase, getProxiedObject

from ZODB import utils
from ZODB.Blobs.interfaces import IBlobStorage, IBlob
from ZODB.POSException import POSKeyError
from ZODB.Blobs.Blob import BLOB_SUFFIX
from ZODB.Blobs.Blob import FilesystemHelper

logger = logging.getLogger('ZODB.BlobStorage')

class BlobStorage(ProxyBase):
    """A storage to support blobs."""

    implements(IBlobStorage)

    __slots__ = ('fshelper', 'dirty_oids')
    # Proxies can't have a __dict__ so specifying __slots__ here allows
    # us to have instance attributes explicitly on the proxy.

    def __new__(self, base_directory, storage):
        return ProxyBase.__new__(self, storage)

    def __init__(self, base_directory, storage):    
        # TODO Complain if storage is ClientStorage
        ProxyBase.__init__(self, storage)
        self.fshelper = FilesystemHelper(base_directory)
        self.fshelper.create()
        self.fshelper.checkSecure()
        self.dirty_oids = []

    def __repr__(self):
        normal_storage = getProxiedObject(self)
        return '<BlobStorage proxy for %r at %s>' % (normal_storage,
                                                     hex(id(self)))
     
    def storeBlob(self, oid, oldserial, data, blobfilename, version,
                  transaction):
        """Stores data that has a BLOB attached."""
        serial = self.store(oid, oldserial, data, version, transaction)
        assert isinstance(serial, str) # XXX in theory serials could be 
                                       # something else

        self._lock_acquire()
        # the user may not have called "open" on the blob object,
        # in which case, the blob will not have a filename.
        if blobfilename is not None:
            try:
                targetpath = self.fshelper.getPathForOID(oid)
                if not os.path.exists(targetpath):
                    os.makedirs(targetpath, 0700)

                targetname = self.fshelper.getBlobFilename(oid, serial)
                os.rename(blobfilename, targetname)

                # XXX if oid already in there, something is really hosed.
                # The underlying storage should have complained anyway
                self.dirty_oids.append((oid, serial))
            finally:
                self._lock_release()
            return self._tid

    def tpc_finish(self, *arg, **kw):
        """ We need to override the base storage's tpc_finish instead of
        providing a _finish method because methods found on the proxied object
        aren't rebound to the proxy """
        getProxiedObject(self).tpc_finish(*arg, **kw)
        self.dirty_oids = []

    def tpc_abort(self, *arg, **kw):
        """ We need to override the base storage's abort instead of
        providing an _abort method because methods found on the proxied object
        aren't rebound to the proxy """
        getProxiedObject(self).tpc_abort(*arg, **kw)
        while self.dirty_oids:
            oid, serial = self.dirty_oids.pop()
            clean = self.fshelper.getBlobFilename(oid, serial)
            if os.exists(clean):
                os.unlink(clean) 

    def loadBlob(self, oid, serial, version):
        """Return the filename where the blob file can be found.
        """
        filename = self.fshelper.getBlobFilename(oid, serial)
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

        base_dir = self.fshelper.base_dir
        for oid_repr in os.listdir(base_dir):
            oid = utils.repr_to_oid(oid_repr)
            oid_path = os.path.join(base_dir, oid_repr)
            files = os.listdir(oid_path)
            files.sort()

            for filename in files:
                filepath = os.path.join(oid_path, filename)
                whatever, serial = self.fshelper.splitBlobFilename(filepath)
                try:
                    fn = self.fshelper.getBlobFilename(oid, serial)
                    self.loadSerial(oid, serial)
                except POSKeyError:
                    os.unlink(filepath)

            if not os.listdir(oid_path):
                shutil.rmtree(oid_path)

    def _packNonUndoing(self, packtime, referencesf):
        base_dir = self.fshelper.base_dir
        for oid_repr in os.listdir(base_dir):
            oid = utils.repr_to_oid(oid_repr)
            oid_path = os.path.join(base_dir, oid_repr)
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
        base_dir = self.fshelper.base_dir
        for oid in os.listdir(base_dir):
            for serial in os.listdir(os.path.join(base_dir, oid)):
                if not serial.endswith(BLOB_SUFFIX):
                    continue
                file_path = os.path.join(base_dir, oid, serial)
                blob_size += os.stat(file_path).st_size
        
        return orig_size + blob_size

    def undo(self, serial_id, transaction):
        undo_serial, keys = getProxiedObject(self).undo(serial_id, transaction)
        # serial_id is the transaction id of the txn that we wish to undo.
        # "undo_serial" is the transaction id of txn in which the undo is
        # performed.  "keys" is the list of oids that are involved in the
        # undo transaction.

        # The serial_id is assumed to be given to us base-64 encoded
        # (belying the web UI legacy of the ZODB code :-()
        serial_id = base64.decodestring(serial_id+'\n')

        self._lock_acquire()

        try:
            # we get all the blob oids on the filesystem related to the
            # transaction we want to undo.
            for oid in self.fshelper.getOIDsForSerial(serial_id):

                # we want to find the serial id of the previous revision
                # of this blob object.
                load_result = self.loadBefore(oid, serial_id)
                
                if load_result is None:
                    # There was no previous revision of this blob
                    # object.  The blob was created in the transaction
                    # represented by serial_id.  We copy the blob data
                    # to a new file that references the undo
                    # transaction in case a user wishes to undo this
                    # undo.
                    orig_fn = self.fshelper.getBlobFilename(oid, serial_id)
                    new_fn = self.fshelper.getBlobFilename(oid, undo_serial)
                else:
                    # A previous revision of this blob existed before the
                    # transaction implied by "serial_id".  We copy the blob
                    # data to a new file that references the undo transaction
                    # in case a user wishes to undo this undo.
                    data, serial_before, serial_after = load_result
                    orig_fn = self.fshelper.getBlobFilename(oid, serial_before)
                    new_fn = self.fshelper.getBlobFilename(oid, undo_serial)
                orig = open(orig_fn, "r")
                new = open(new_fn, "wb")
                utils.cp(orig, new)
                orig.close()
                new.close()
                self.dirty_oids.append((oid, undo_serial))

        finally:
            self._lock_release()
        return undo_serial, keys

