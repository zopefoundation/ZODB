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
"""Support for database export and import."""

from cStringIO import StringIO
from cPickle import Pickler, Unpickler
from tempfile import TemporaryFile
import logging

from ZODB.POSException import ExportError
from ZODB.utils import p64, u64
from ZODB.serialize import referencesf
import sys

logger = logging.getLogger('zodb.ExportImport')

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
        f.write(export_end_marker)
        return f

    def importFile(self, f, clue='', customImporters=None):
        # This is tricky, because we need to work in a transaction!

        if isinstance(f, str):
            f = open(f,'rb')

        magic = f.read(4)
        if magic != 'ZEXP':
            if customImporters and customImporters.has_key(magic):
                f.seek(0)
                return customImporters[magic](self, f, clue)
            raise ExportError("Invalid export header")

        t = self._txn_mgr.get()
        if clue:
            t.note(clue)

        return_oid_list = []
        self._import = f, return_oid_list
        self._register()
        t.commit(1)
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
            h = f.read(16)
            if h == export_end_marker:
                break
            if len(h) != 16:
                raise ExportError("Truncated export file")
            l = u64(h[8:16])
            p = f.read(l)
            if len(p) != l:
                raise ExportError("Truncated export file")

            ooid = h[:8]
            if oids:
                oid = oids[ooid]
                if isinstance(oid, tuple):
                    oid = oid[0]
            else:
                oids[ooid] = oid = self._storage.new_oid()
                return_oid_list.append(oid)

            pfile = StringIO(p)
            unpickler = Unpickler(pfile)
            unpickler.persistent_load = persistent_load

            newp = StringIO()
            pickler = Pickler(newp, 1)
            pickler.persistent_id = persistent_id

            pickler.dump(unpickler.load())
            pickler.dump(unpickler.load())
            p = newp.getvalue()

            self._storage.store(oid, None, p, version, transaction)


export_end_marker = '\377'*16

class Ghost(object):
    __slots__ = ("oid",)
    def __init__(self, oid):
        self.oid = oid

def persistent_id(obj):
    if isinstance(obj, Ghost):
        return obj.oid
