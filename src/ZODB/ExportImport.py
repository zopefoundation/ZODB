##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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
"""Support for database export and import."""

import os

from cStringIO import StringIO
from cPickle import Pickler, Unpickler
from tempfile import TemporaryFile
import logging

from ZODB.blob import Blob
from ZODB.interfaces import IBlobStorage
from ZODB.POSException import ExportError, POSKeyError
from ZODB.serialize import referencesf
from ZODB.utils import p64, u64, cp, mktemp

logger = logging.getLogger('ZODB.ExportImport')

class ExportImport:

    def exportFile(self, oid, f=None):
        if f is None:
            f = TemporaryFile()
        elif isinstance(f, str):
            f = open(f,'w+b')
        f.write('ZEXP')
        oids = [oid]
        done_oids = {}
        done=done_oids.has_key
        load=self._storage.load
        supports_blobs = IBlobStorage.providedBy(self._storage)
        while oids:
            oid = oids.pop(0)
            if oid in done_oids:
                continue
            done_oids[oid] = True
            try:
                p, serial = load(oid, self._version)
            except:
                logger.debug("broken reference for oid %s", repr(oid),
                             exc_info=True)
            else:
                referencesf(p, oids)
                f.writelines([oid, p64(len(p)), p])

            if supports_blobs:
                if not isinstance(self._reader.getGhost(p), Blob):
                    continue # not a blob
                
                blobfilename = self._storage.loadBlob(oid, serial)
                f.write(blob_begin_marker)
                f.write(p64(os.stat(blobfilename).st_size))
                blobdata = open(blobfilename, "rb")
                cp(blobdata, f)
                blobdata.close()
            
        f.write(export_end_marker)
        return f

    def importFile(self, f, clue='', customImporters=None):
        # This is tricky, because we need to work in a transaction!

        if isinstance(f, str):
            f = open(f, 'rb')

        magic = f.read(4)
        if magic != 'ZEXP':
            if customImporters and customImporters.has_key(magic):
                f.seek(0)
                return customImporters[magic](self, f, clue)
            raise ExportError("Invalid export header")

        t = self.transaction_manager.get()
        if clue:
            t.note(clue)

        return_oid_list = []
        self._import = f, return_oid_list
        self._register()
        t.savepoint(optimistic=True)
        # Return the root imported object.
        if return_oid_list:
            return self.get(return_oid_list[0])
        else:
            return None

    def _importDuringCommit(self, transaction, f, return_oid_list):
        """Import data during two-phase commit.

        Invoked by the transaction manager mid commit.
        Appends one item, the OID of the first object created,
        to return_oid_list.
        """
        oids = {}

        # IMPORTANT: This code should be consistent with the code in
        # serialize.py. It is currently out of date and doesn't handle
        # weak references.

        def persistent_load(ooid):
            """Remap a persistent id to a new ID and create a ghost for it."""

            klass = None
            if isinstance(ooid, tuple):
                ooid, klass = ooid

            if ooid in oids:
                oid = oids[ooid]
            else:
                if klass is None:
                    oid = self._storage.new_oid()
                else:
                    oid = self._storage.new_oid(), klass
                oids[ooid] = oid

            return Ghost(oid)

        version = self._version

        while 1:
            header = f.read(16)
            if header == export_end_marker:
                break
            if len(header) != 16:
                raise ExportError("Truncated export file")

            # Extract header information
            ooid = header[:8]
            length = u64(header[8:16])
            data = f.read(length)

            if len(data) != length:
                raise ExportError("Truncated export file")

            if oids:
                oid = oids[ooid]
                if isinstance(oid, tuple):
                    oid = oid[0]
            else:
                oids[ooid] = oid = self._storage.new_oid()
                return_oid_list.append(oid)

            # Blob support
            blob_begin = f.read(len(blob_begin_marker))
            if blob_begin == blob_begin_marker:
                # Copy the blob data to a temporary file
                # and remember the name
                blob_len = u64(f.read(8))
                blob_filename = mktemp()
                blob_file = open(blob_filename, "wb")
                cp(f, blob_file, blob_len)
                blob_file.close()
            else:
                f.seek(-len(blob_begin_marker),1)
                blob_filename = None

            pfile = StringIO(data)
            unpickler = Unpickler(pfile)
            unpickler.persistent_load = persistent_load

            newp = StringIO()
            pickler = Pickler(newp, 1)
            pickler.persistent_id = persistent_id

            pickler.dump(unpickler.load())
            pickler.dump(unpickler.load())
            data = newp.getvalue()

            if blob_filename is not None:
                self._storage.storeBlob(oid, None, data, blob_filename, 
                                        version, transaction)
            else:
                self._storage.store(oid, None, data, version, transaction)


export_end_marker = '\377'*16
blob_begin_marker = '\000BLOBSTART'

class Ghost(object):
    __slots__ = ("oid",)
    def __init__(self, oid):
        self.oid = oid

def persistent_id(obj):
    if isinstance(obj, Ghost):
        return obj.oid
