##############################################################################
#
# Copyright (c) 2001 Zope Corporation and Contributors. All Rights Reserved.
# 
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
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
