##############################################################################
#
# Copyright (c) 2001, 2002, 2003 Zope Corporation and Contributors.
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
"""Very Simple Mapping ZODB storage

The Mapping storage provides an extremely simple storage implementation that
doesn't provide undo or version support.

It is meant to illustrate the simplest possible storage.

The Mapping storage uses a single data structure to map object ids to data.
"""

from ZODB.utils import u64, z64
from ZODB.BaseStorage import BaseStorage
from ZODB import POSException
from persistent.TimeStamp import TimeStamp


class MappingStorage(BaseStorage):

    def __init__(self, name='Mapping Storage'):
        BaseStorage.__init__(self, name)
        # ._index maps an oid to a string s.  s[:8] is the tid of the
        # transaction that created oid's current state, and s[8:] is oid's
        # current state.
        self._index = {}
        self._clear_temp()
        self._ltid = None
        # Note: If you subclass this and use a persistent mapping facility
        # (e.g. a dbm file), you will need to get the maximum key and save it
        # as self._oid.  See dbmStorage.

    def __len__(self):
        return len(self._index)

    def getSize(self):
        self._lock_acquire()
        try:
            # These constants are for Python object memory overheads.  Heh.
            s = 32
            for p in self._index.itervalues():
                s += 56 + len(p)
            return s
        finally:
            self._lock_release()

    def load(self, oid, version=''):
        self._lock_acquire()
        try:
            p = self._index[oid]
            return p[8:], p[:8] # pickle, serial
        finally:
            self._lock_release()

    def getTid(self, oid):
        self._lock_acquire()
        try:
            # The tid is the first 8 bytes of the buffer.
            return self._index[oid][:8]
        finally:
            self._lock_release()

    def store(self, oid, serial, data, version, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)

        if version:
            raise POSException.Unsupported("Versions aren't supported")

        self._lock_acquire()
        try:
            if oid in self._index:
                oserial = self._index[oid][:8]
                if serial != oserial:
                    raise POSException.ConflictError(oid=oid,
                                                     serials=(oserial, serial),
                                                     data=data)
            self._tindex[oid] = self._tid + data
        finally:
            self._lock_release()
        return self._tid

    def _clear_temp(self):
        # store() saves data in _tindex; if the transaction completes
        # successfully, _finish() merges _tindex into _index.
        self._tindex = {}

    def _finish(self, tid, user, desc, ext):
        self._index.update(self._tindex)
        self._ltid = self._tid

    def lastTransaction(self):
        return self._ltid

    def pack(self, t, referencesf):
        self._lock_acquire()
        try:
            if not self._index:
                return
            # Build an index of *only* those objects reachable from the root.
            rootl = [z64]
            pindex = {}
            while rootl:
                oid = rootl.pop()
                if oid not in pindex:
                    # Scan non-version pickle for references.
                    r = self._index[oid]
                    pindex[oid] = r
                    referencesf(r[8:], rootl)
            self._index = pindex

        finally:
            self._lock_release()

    def _splat(self):
        """Spit out a string showing state."""
        o = ['Index:']
        keys = self._index.keys()
        keys.sort()
        for oid in keys:
            r = self._index[oid]
            o.append('  %s: %s, %s' %
                     (u64(oid), TimeStamp(r[:8]), repr(r[8:])))

        return '\n'.join(o)

    def cleanup(self):
        pass

    def close(self):
        pass
