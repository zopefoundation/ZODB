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

import POSException
from utils import p64, u64

class TmpStore:
    _transaction=_isCommitting=None

    def __init__(self, base_version, file=None):
        if file is None:
            import tempfile
            file=tempfile.TemporaryFile()

        self._file=file
        self._index={}
        self._pos=self._tpos=0
        self._bver=base_version
        self._tindex=[]
        self._db=None
        self._creating=[]

    def __del__(self): self.close()

    def close(self):
        self._file.close()
        del self._file
        del self._index
        del self._db

    def getName(self): return self._db.getName()
    def getSize(self): return self._pos

    def load(self, oid, version):
        #if version is not self: raise KeyError, oid
        pos=self._index.get(oid, None)
        if pos is None:
            return self._storage.load(oid, self._bver)
        file=self._file
        file.seek(pos)
        h=file.read(24)
        if h[:8] != oid:
            raise POSException.StorageSystemError, 'Bad temporary storage'
        return file.read(u64(h[16:])), h[8:16]
        
    def modifiedInVersion(self, oid):
        if self._index.has_key(oid): return 1
        return self._db._storage.modifiedInVersion(oid)

    def new_oid(self): return self._db._storage.new_oid()

    def registerDB(self, db, limit):
        self._db=db
        self._storage=db._storage

    def store(self, oid, serial, data, version, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        file=self._file
        pos=self._pos
        file.seek(pos)
        l=len(data)
        if serial is None:
            serial = '\0\0\0\0\0\0\0\0'
        file.write(oid+serial+p64(l))
        file.write(data)
        self._tindex.append((oid,pos))
        self._pos=pos+l+24
        return serial
        
    def tpc_abort(self, transaction):
        if transaction is not self._transaction: return
        del self._tindex[:]
        self._transaction=None
        self._pos=self._tpos

    def tpc_begin(self, transaction):
        if self._transaction is transaction: return
        self._transaction=transaction
        del self._tindex[:]   # Just to be sure!
        self._pos=self._tpos

    def tpc_vote(self, transaction): pass

    def tpc_finish(self, transaction, f=None):
        if transaction is not self._transaction: return
        if f is not None: f()
        index=self._index
        tindex=self._tindex
        for oid, pos in tindex: index[oid]=pos
        del tindex[:]
        self._tpos=self._pos

    def undoLog(self, first, last, filter=None): return ()
    
    def versionEmpty(self, version):
        if version is self: return len(self._index)
