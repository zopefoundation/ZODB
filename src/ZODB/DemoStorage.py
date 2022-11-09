##############################################################################
#
# Copyright (c) Zope Corporation and Contributors.
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
"""Demo ZODB storage

A demo storage supports demos by allowing a volatile changed database
to be layered over a base database.

The base storage must not change.

"""
from __future__ import print_function

import os
import random
import tempfile
import weakref

import zope.interface

import ZODB.BaseStorage
import ZODB.blob
import ZODB.interfaces
import ZODB.MappingStorage
import ZODB.POSException
import ZODB.utils

from .ConflictResolution import ConflictResolvingStorage
from .utils import load_current
from .utils import maxtid


@zope.interface.implementer(
    ZODB.interfaces.IStorage,
    ZODB.interfaces.IStorageIteration,
)
class DemoStorage(ConflictResolvingStorage):
    """A storage that stores changes against a read-only base database

    This storage was originally meant to support distribution of
    application demonstrations with populated read-only databases (on
    CDROM) and writable in-memory databases.

    Demo storages are extemely convenient for testing where setup of a
    base database can be shared by many tests.

    Demo storages are also handy for staging appplications where a
    read-only snapshot of a production database (often accomplished
    using a `beforestorage
    <https://pypi.org/project/zc.beforestorage/>`_) is combined
    with a changes database implemented with a
    :class:`~ZODB.FileStorage.FileStorage.FileStorage`.
    """

    def __init__(self, name=None, base=None, changes=None,
                 close_base_on_close=None, close_changes_on_close=None):
        """Create a demo storage

        :param str name: The storage name used by the
            :meth:`~ZODB.interfaces.IStorage.getName` and
            :meth:`~ZODB.interfaces.IStorage.sortKey` methods.
        :param object base: base storage
        :param object changes: changes storage
        :param bool close_base_on_close: A Flag indicating whether the base
           database should be closed when the demo storage is closed.
        :param bool close_changes_on_close: A Flag indicating whether the
           changes database should be closed when the demo storage is closed.

        If a base database isn't provided, a
        :class:`~ZODB.MappingStorage.MappingStorage` will be
        constructed and used.

        If ``close_base_on_close`` isn't specified, it will be ``True`` if
        a base database was provided and ``False`` otherwise.

        If a changes database isn't provided, a
        :class:`~ZODB.MappingStorage.MappingStorage` will be
        constructed and used and blob support will be provided using a
        temporary blob directory.

        If ``close_changes_on_close`` isn't specified, it will be ``True`` if
        a changes database was provided and ``False`` otherwise.
        """

        if close_base_on_close is None:
            if base is None:
                base = ZODB.MappingStorage.MappingStorage()
                close_base_on_close = False
            else:
                close_base_on_close = True
        elif base is None:
            base = ZODB.MappingStorage.MappingStorage()

        self.base = base
        self.close_base_on_close = close_base_on_close

        if changes is None:
            self._temporary_changes = True
            changes = ZODB.MappingStorage.MappingStorage()
            zope.interface.alsoProvides(self, ZODB.interfaces.IBlobStorage)
            if close_changes_on_close is None:
                close_changes_on_close = False
        else:
            if ZODB.interfaces.IBlobStorage.providedBy(changes):
                zope.interface.alsoProvides(self, ZODB.interfaces.IBlobStorage)
            if close_changes_on_close is None:
                close_changes_on_close = True

        self.changes = changes
        self.close_changes_on_close = close_changes_on_close

        self._issued_oids = set()
        self._stored_oids = set()
        self._resolved = []

        self._commit_lock = ZODB.utils.Lock()
        self._transaction = None

        if name is None:
            name = 'DemoStorage(%r, %r)' % (base.getName(), changes.getName())
        self.__name__ = name

        self._copy_methods_from_changes(changes)

        self._next_oid = random.randint(1, 1 << 62)

    def _blobify(self):
        if (self._temporary_changes and
                isinstance(self.changes, ZODB.MappingStorage.MappingStorage)):
            blob_dir = tempfile.mkdtemp('.demoblobs')
            _temporary_blobdirs[
                weakref.ref(self, cleanup_temporary_blobdir)
            ] = blob_dir
            self.changes = ZODB.blob.BlobStorage(blob_dir, self.changes)
            self._copy_methods_from_changes(self.changes)
            return True

    def cleanup(self):
        self.base.cleanup()
        self.changes.cleanup()

    __opened = True

    def opened(self):
        return self.__opened

    def close(self):
        self.__opened = False
        if self.close_base_on_close:
            self.base.close()
        if self.close_changes_on_close:
            self.changes.close()

    def _copy_methods_from_changes(self, changes):
        for meth in (
            '_lock',
            'getSize', 'isReadOnly',
            'sortKey', 'tpc_transaction',
        ):
            setattr(self, meth, getattr(changes, meth))

        supportsUndo = getattr(changes, 'supportsUndo', None)
        if supportsUndo is not None and supportsUndo():
            for meth in ('supportsUndo', 'undo', 'undoLog', 'undoInfo'):
                setattr(self, meth, getattr(changes, meth))
            zope.interface.alsoProvides(self, ZODB.interfaces.IStorageUndoable)

        lastInvalidations = getattr(changes, 'lastInvalidations', None)
        if lastInvalidations is not None:
            self.lastInvalidations = lastInvalidations

    def getName(self):
        return self.__name__
    __repr__ = getName

    def getTid(self, oid):
        try:
            return self.changes.getTid(oid)
        except ZODB.POSException.POSKeyError:
            return self.base.getTid(oid)

    def history(self, oid, size=1):
        try:
            r = self.changes.history(oid, size)
        except ZODB.POSException.POSKeyError:
            r = []
        size -= len(r)
        if size:
            try:
                r += self.base.history(oid, size)
            except ZODB.POSException.POSKeyError:
                if not r:
                    raise
        return r

    def iterator(self, start=None, end=None):
        for t in self.base.iterator(start, end):
            yield t
        for t in self.changes.iterator(start, end):
            yield t

    def lastTransaction(self):
        t = self.changes.lastTransaction()
        if t == ZODB.utils.z64:
            t = self.base.lastTransaction()
        return t

    def __len__(self):
        return len(self.changes)

    # still want load for old clients (e.g. zeo servers)
    load = load_current

    def loadBefore(self, oid, tid):
        try:
            result = self.changes.loadBefore(oid, tid)
        except ZODB.POSException.POSKeyError:
            # The oid isn't in the changes, so defer to base
            return self.base.loadBefore(oid, tid)

        if result is None:
            # The oid *was* in the changes, but there aren't any
            # earlier records. Maybe there are in the base.
            try:
                result = self.base.loadBefore(oid, tid)
            except ZODB.POSException.POSKeyError:
                # The oid isn't in the base, so None will be the right result
                pass
            else:
                if result and not result[-1]:
                    # The oid is current in the base.  We need to find
                    # the end tid in the base by fining the first tid
                    # in the changes. Unfortunately, there isn't an
                    # api for this, so we have to walk back using
                    # loadBefore.

                    if tid == maxtid:
                        # Special case: we were looking for the
                        # current value. We won't find anything in
                        # changes, so we're done.
                        return result

                    end_tid = maxtid
                    t = self.changes.loadBefore(oid, end_tid)
                    while t:
                        end_tid = t[1]
                        t = self.changes.loadBefore(oid, end_tid)
                    result = result[:2] + (
                        end_tid if end_tid != maxtid else None,
                    )

        return result

    def loadBlob(self, oid, serial):
        try:
            return self.changes.loadBlob(oid, serial)
        except ZODB.POSException.POSKeyError:
            try:
                return self.base.loadBlob(oid, serial)
            except AttributeError:
                if not ZODB.interfaces.IBlobStorage.providedBy(self.base):
                    raise ZODB.POSException.POSKeyError(oid, serial)
                raise
        except AttributeError:
            if self._blobify():
                return self.loadBlob(oid, serial)
            raise

    def openCommittedBlobFile(self, oid, serial, blob=None):
        try:
            return self.changes.openCommittedBlobFile(oid, serial, blob)
        except ZODB.POSException.POSKeyError:
            try:
                return self.base.openCommittedBlobFile(oid, serial, blob)
            except AttributeError:
                if not ZODB.interfaces.IBlobStorage.providedBy(self.base):
                    raise ZODB.POSException.POSKeyError(oid, serial)
                raise
        except AttributeError:
            if self._blobify():
                return self.openCommittedBlobFile(oid, serial, blob)
            raise

    def loadSerial(self, oid, serial):
        try:
            return self.changes.loadSerial(oid, serial)
        except ZODB.POSException.POSKeyError:
            return self.base.loadSerial(oid, serial)

    def new_oid(self):
        with self._lock:
            while 1:
                oid = ZODB.utils.p64(self._next_oid)
                if oid not in self._issued_oids:
                    try:
                        load_current(self.changes, oid)
                    except ZODB.POSException.POSKeyError:
                        try:
                            load_current(self.base, oid)
                        except ZODB.POSException.POSKeyError:
                            self._next_oid += 1
                            self._issued_oids.add(oid)
                            return oid

                self._next_oid = random.randint(1, 1 << 62)

    def pack(self, t, referencesf, gc=None):
        if gc is None:
            if self._temporary_changes:
                return self.changes.pack(t, referencesf)
        elif self._temporary_changes:
            return self.changes.pack(t, referencesf, gc=gc)
        elif gc:
            raise TypeError(
                "Garbage collection isn't supported"
                " when there is a base storage.")

        try:
            self.changes.pack(t, referencesf, gc=False)
        except TypeError as v:
            if 'gc' in str(v):
                pass  # The gc arg isn't supported. Don't pack
            raise

    def pop(self):
        """Close the changes database and return the base.
        """
        self.changes.close()
        return self.base

    def push(self, changes=None):
        """Create a new demo storage using the storage as a base.

        The given changes are used as the changes for the returned
        storage and ``False`` is passed as ``close_base_on_close``.
        """
        return self.__class__(base=self, changes=changes,
                              close_base_on_close=False)

    def store(self, oid, serial, data, version, transaction):
        assert version == '', "versions aren't supported"
        if transaction is not self._transaction:
            raise ZODB.POSException.StorageTransactionError(self, transaction)

        # Since the OID is being used, we don't have to keep up with it any
        # more. Save it now so we can forget it later. :)
        self._stored_oids.add(oid)

        # See if we already have changes for this oid
        try:
            old = load_current(self, oid)[1]
        except ZODB.POSException.POSKeyError:
            old = serial

        if old != serial:
            rdata = self.tryToResolveConflict(oid, old, serial, data)
            self.changes.store(oid, old, rdata, '', transaction)
            self._resolved.append(oid)
        else:
            self.changes.store(oid, serial, data, '', transaction)

    def storeBlob(self, oid, oldserial, data, blobfilename, version,
                  transaction):
        assert version == '', "versions aren't supported"
        if transaction is not self._transaction:
            raise ZODB.POSException.StorageTransactionError(self, transaction)

        # Since the OID is being used, we don't have to keep up with it any
        # more. Save it now so we can forget it later. :)
        self._stored_oids.add(oid)

        try:
            self.changes.storeBlob(
                oid, oldserial, data, blobfilename, '', transaction)
        except AttributeError:
            if not self._blobify():
                raise
            self.changes.storeBlob(
                oid, oldserial, data, blobfilename, '', transaction)

    checkCurrentSerialInTransaction = (
        ZODB.BaseStorage.checkCurrentSerialInTransaction)

    def temporaryDirectory(self):
        try:
            return self.changes.temporaryDirectory()
        except AttributeError:
            if self._blobify():
                return self.changes.temporaryDirectory()
            raise

    def tpc_abort(self, transaction):
        with self._lock:
            if transaction is not self._transaction:
                return
            self._stored_oids = set()
            self._transaction = None
            self.changes.tpc_abort(transaction)
            self._commit_lock.release()

    def tpc_begin(self, transaction, *a, **k):
        with self._lock:
            # The tid argument exists to support testing.
            if transaction is self._transaction:
                raise ZODB.POSException.StorageTransactionError(
                    "Duplicate tpc_begin calls for same transaction")

        self._commit_lock.acquire()

        with self._lock:
            self.changes.tpc_begin(transaction, *a, **k)
            self._transaction = transaction
            self._stored_oids = set()
            del self._resolved[:]

    def tpc_vote(self, *a, **k):
        if self.changes.tpc_vote(*a, **k):
            raise ZODB.POSException.StorageTransactionError(
                "Unexpected resolved conflicts")
        return self._resolved

    def tpc_finish(self, transaction, func=lambda tid: None):
        with self._lock:
            if (transaction is not self._transaction):
                raise ZODB.POSException.StorageTransactionError(
                    "tpc_finish called with wrong transaction")
            self._issued_oids.difference_update(self._stored_oids)
            self._stored_oids = set()
            self._transaction = None
            tid = self.changes.tpc_finish(transaction, func)
            self._commit_lock.release()
        return tid


_temporary_blobdirs = {}


def cleanup_temporary_blobdir(
    ref,
    _temporary_blobdirs=_temporary_blobdirs,  # Make sure it stays around
):
    blob_dir = _temporary_blobdirs.pop(ref, None)
    if blob_dir and os.path.exists(blob_dir):
        ZODB.blob.remove_committed_dir(blob_dir)
