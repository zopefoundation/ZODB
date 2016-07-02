"""Adapt non-IMultiCommitStorage storages to IMultiCommitStorage
"""

import zope.interface

from .interfaces import IMultiCommitStorage
from .ConflictResolution import ResolvedSerial

@zope.interface.implementer(IMultiCommitStorage)
class MultiCommitAdapter:

    def __init__(self, storage):
        self._storage = storage
        ifaces = zope.interface.providedBy(storage)
        assert IMultiCommitStorage not in ifaces
        zope.interface.alsoProvides(self, ifaces)
        self._resolved = set()

    def __getattr__(self, name):
        v = getattr(self._storage, name)
        self.__dict__[name] = v
        return v

    def tpc_begin(self, *args):
        self._storage.tpc_begin(*args)
        self._resolved = set()

    def store(self, oid, *args):
        if self._storage.store(oid, *args) == ResolvedSerial:
            self._resolved.add(oid)

    def storeBlob(self, oid, *args):
        s = self._storage.storeBlob(oid, *args)
        if s:
            if isinstance(s, bytes):
                s = ((oid, s), )

            for oid, serial in s:
                if s == ResolvedSerial:
                    self._resolved.add(oid)

    def undo(self, transaction_id, transaction):
        r = self._storage.undo(transaction_id, transaction)
        if r:
            self._resolved.update(set(r[1]))

    def tpc_vote(self, *args):
        s = self._storage.tpc_vote(*args)
        for (oid, serial) in (s or ()):
            if serial == ResolvedSerial:
                self._resolved.add(oid)

        return list(self._resolved)

    def tpc_finish(self, transaction, f=lambda tid: None):

        t = []

        def func(tid):
            t.append(tid)
            f(tid)

        self._storage.tpc_finish(transaction, func)

        return t[0]

    def __len__(self):
        return len(self._storage)
