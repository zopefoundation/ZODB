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

from ZODB import POSException
from ZODB.utils import p64, u64, z64

import tempfile

class TmpStore:
    """A storage to support subtransactions."""

    _bver = ''

    def __init__(self, base_version):
        self._transaction = None
        if base_version:
            self._bver = base_version
        self._file = tempfile.TemporaryFile()
        # _pos: current file position
        # _tpos: file position at last commit point
        self._pos = self._tpos = 0L
        # _index: map oid to pos of last committed version
        self._index = {}
        # _tindex: map oid to pos for new updates
        self._tindex = {}
        self._db = None
        self._creating = []

    def close(self):
        self._file.close()

    def getName(self):
        return self._db.getName()

    def getSize(self):
        return self._pos

    def load(self, oid, version):
        pos = self._index.get(oid)
        if pos is None:
            return self._storage.load(oid, self._bver)
        self._file.seek(pos)
        h = self._file.read(8)
        oidlen = u64(h)
        read_oid = self._file.read(oidlen)
        if read_oid != oid:
            raise POSException.StorageSystemError('Bad temporary storage')
        h = self._file.read(16)
        size = u64(h[8:])
        serial = h[:8]
        return self._file.read(size), serial

    # XXX clarify difference between self._storage & self._db._storage

    def modifiedInVersion(self, oid):
        if self._index.has_key(oid):
            return self._bver
        return self._db._storage.modifiedInVersion(oid)

    def new_oid(self):
        return self._db._storage.new_oid()

    def registerDB(self, db, limit):
        self._db = db
        self._storage = db._storage

    def store(self, oid, serial, data, version, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        self._file.seek(self._pos)
        l = len(data)
        if serial is None:
            serial = z64
        header = p64(len(oid)) + oid + serial + p64(l)
        self._file.write(header)
        self._file.write(data)
        self._tindex[oid] = self._pos
        self._pos += l + len(header)
        return serial

    def tpc_abort(self, transaction):
        if transaction is not self._transaction:
            return
        self._tindex.clear()
        self._transaction = None
        self._pos = self._tpos

    def tpc_begin(self, transaction):
        if self._transaction is transaction:
            return
        self._transaction = transaction
        self._tindex.clear() # Just to be sure!
        self._pos = self._tpos

    def tpc_vote(self, transaction):
        pass

    def tpc_finish(self, transaction, f=None):
        if transaction is not self._transaction:
            return
        if f is not None:
            f()
        self._index.update(self._tindex)
        self._tindex.clear()
        self._tpos = self._pos

    def undoLog(self, first, last, filter=None):
        return ()

    def versionEmpty(self, version):
        # XXX what is this supposed to do?
        if version == self._bver:
            return len(self._index)
