##############################################################################
# 
# Zope Public License (ZPL) Version 1.0
# -------------------------------------
# 
# Copyright (c) Digital Creations.  All rights reserved.
# 
# This license has been certified as Open Source(tm).
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
# 1. Redistributions in source code must retain the above copyright
#    notice, this list of conditions, and the following disclaimer.
# 
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions, and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
# 
# 3. Digital Creations requests that attribution be given to Zope
#    in any manner possible. Zope includes a "Powered by Zope"
#    button that is installed by default. While it is not a license
#    violation to remove this button, it is requested that the
#    attribution remain. A significant investment has been put
#    into Zope, and this effort will continue if the Zope community
#    continues to grow. This is one way to assure that growth.
# 
# 4. All advertising materials and documentation mentioning
#    features derived from or use of this software must display
#    the following acknowledgement:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    In the event that the product being advertised includes an
#    intact Zope distribution (with copyright and license included)
#    then this clause is waived.
# 
# 5. Names associated with Zope or Digital Creations must not be used to
#    endorse or promote products derived from this software without
#    prior written permission from Digital Creations.
# 
# 6. Modified redistributions of any form whatsoever must retain
#    the following acknowledgment:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    Intact (re-)distributions of any official Zope release do not
#    require an external acknowledgement.
# 
# 7. Modifications are encouraged but must be packaged separately as
#    patches to official Zope releases.  Distributions that do not
#    clearly separate the patches from the original work must be clearly
#    labeled as unofficial distributions.  Modifications which do not
#    carry the name Zope may be packaged in any form, as long as they
#    conform to all of the clauses above.
# 
# 
# Disclaimer
# 
#   THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS ``AS IS'' AND ANY
#   EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
#   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL CREATIONS OR ITS
#   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
#   USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#   ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#   OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
#   OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
#   SUCH DAMAGE.
# 
# 
# This software consists of contributions made by Digital Creations and
# many individuals on behalf of Digital Creations.  Specific
# attributions are listed in the accompanying credits file.
# 
##############################################################################

"""Support for database export and import.
"""

import POSException, string

from utils import p64, u64
from referencesf import referencesf
from cStringIO import StringIO
from cPickle import Pickler, Unpickler
TupleType=type(())

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
            try: p, serial = load(oid, version)
            except: pass # Ick, a broken reference
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

        t=get_transaction().sub()

        t.note('import into %s from %s' % (self.db().getName(), file_name))
        if clue: t.note(clue)

        storage=self._storage
        new_oid=storage.new_oid
        oids={}
        wrote_oid=oids.has_key
        new_oid=storage.new_oid
        store=storage.store

        def persistent_load(ooid,
                            Ghost=Ghost, StringType=StringType,
                            atoi=string.atoi, TupleType=type(()),
                            oids=oids, wrote_oid=wrote_oid, new_oid=new_oid):
        
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

        version=self._version
        return_oid=None

        storage.tpc_begin(t)
        try:
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
                    oids[ooid]=return_oid=oid=new_oid()

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

                store(oid, None, p, version, t)
                
        except:
            storage.tpc_abort(t)
            raise
        else:
            storage.tpc_finish(t)
            if return_oid is not None: return self[return_oid]

StringType=type('')

def TemporaryFile():
    # This is sneaky suicide
    global TemporaryFile
    import tempfile
    TemporaryFile=tempfile.TemporaryFile
    return TemporaryFile()

export_end_marker='\377'*16

class Ghost: pass

def persistent_id(object, Ghost=Ghost):
    if hasattr(object, '__class__') and object.__class__ is Ghost:
        return object.oid
    
