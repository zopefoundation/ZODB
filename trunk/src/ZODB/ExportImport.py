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

"""Support for database export and import.
"""

import POSException, string

from utils import p64, u64
from referencesf import referencesf
from cStringIO import StringIO
from cPickle import Pickler, Unpickler
from types import StringType, TupleType
import zLOG

class ExportImport:

    def exportFile(self, oid, file=None):

        if file is None: file=TemporaryFile()
        elif type(file) is StringType: file=open(file,'w+b')
        write=file.write
        write('ZEXP')
        version=self._version
        ref=referencesf
        oids=[oid]
        done_oids={}
        done=done_oids.has_key
        load=self._storage.load
        while oids:
            oid=oids[0]
            del oids[0]
            if done(oid): continue
            done_oids[oid]=1
            try:
                p, serial = load(oid, version)
            except:
                zLOG.LOG("ZODB", zLOG.DEBUG,
                         "broken reference for oid %s" % `oid`,
                         err=sys.exc_info())
            else:
                ref(p, oids)
                write(oid)
                write(p64(len(p)))
                write(p)
        write(export_end_marker)
        return file

    def importFile(self, file, clue='', customImporters=None):
        # This is tricky, because we need to work in a transaction!

        if type(file) is StringType:
            file_name=file
            file=open(file,'rb')
        else:
            try: file_name=file.name
            except: file_name='(unknown)'
        read=file.read

        magic=read(4)

        if magic != 'ZEXP':
            if customImporters and customImporters.has_key(magic):
                file.seek(0)
                return customImporters[magic](self, file, clue)
            raise POSException.ExportError, 'Invalid export header'

        t = get_transaction()
        if clue: t.note(clue)

        return_oid_list = []
        self.onCommitAction('_importDuringCommit', file, return_oid_list)
        t.commit(1)
        # Return the root imported object.
        if return_oid_list:
            return self[return_oid_list[0]]
        else:
            return None

    def _importDuringCommit(self, transaction, file, return_oid_list):
        '''
        Invoked by the transaction manager mid commit.
        Appends one item, the OID of the first object created,
        to return_oid_list.
        '''
        oids = {}
        storage = self._storage
        new_oid = storage.new_oid
        store = storage.store
        read = file.read

        def persistent_load(ooid,
                            Ghost=Ghost,
                            atoi=string.atoi,
                            oids=oids, wrote_oid=oids.has_key,
                            new_oid=storage.new_oid):

            "Remap a persistent id to a new ID and create a ghost for it."

            if type(ooid) is TupleType: ooid, klass = ooid
            else: klass=None

            if wrote_oid(ooid): oid=oids[ooid]
            else:
                if klass is None: oid=new_oid()
                else: oid=new_oid(), klass
                oids[ooid]=oid

            Ghost=Ghost()
            Ghost.oid=oid
            return Ghost

        version = self._version

        while 1:
            h=read(16)
            if h==export_end_marker: break
            if len(h) != 16:
                raise POSException.ExportError, 'Truncated export file'
            l=u64(h[8:16])
            p=read(l)
            if len(p) != l:
                raise POSException.ExportError, 'Truncated export file'

            ooid=h[:8]
            if oids:
                oid=oids[ooid]
                if type(oid) is TupleType: oid=oid[0]
            else:
                oids[ooid] = oid = storage.new_oid()
                return_oid_list.append(oid)

            pfile=StringIO(p)
            unpickler=Unpickler(pfile)
            unpickler.persistent_load=persistent_load

            newp=StringIO()
            pickler=Pickler(newp,1)
            pickler.persistent_id=persistent_id

            pickler.dump(unpickler.load())
            pickler.dump(unpickler.load())
            p=newp.getvalue()
            plen=len(p)

            store(oid, None, p, version, transaction)


def TemporaryFile():
    # This is sneaky suicide
    global TemporaryFile
    import tempfile
    TemporaryFile=tempfile.TemporaryFile
    return TemporaryFile()

export_end_marker='\377'*16

class Ghost: pass

def persistent_id(object, Ghost=Ghost):
    if getattr(object, '__class__', None) is Ghost:
        return object.oid
