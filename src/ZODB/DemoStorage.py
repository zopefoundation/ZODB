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
import os
import random
import weakref
import tempfile
import threading
import ZODB.blob
import ZODB.interfaces
import ZODB.MappingStorage
import ZODB.POSException
import ZODB.utils
import zope.interface

class DemoStorage(object):

    zope.interface.implements(
        ZODB.interfaces.IStorage,
        ZODB.interfaces.IStorageIteration,
        )

    def __init__(self, name=None, base=None, changes=None):
        if base is None:
            base = ZODB.MappingStorage.MappingStorage()
            self._temporary_base = True
        else:
            self._temporary_base = False
        self.base = base
            
        if changes is None:
            changes = ZODB.MappingStorage.MappingStorage()
            zope.interface.alsoProvides(self, ZODB.interfaces.IBlobStorage)
            self._temporary_changes = True
        else:
            if ZODB.interfaces.IBlobStorage.providedBy(changes):
                zope.interface.alsoProvides(self, ZODB.interfaces.IBlobStorage)
            self._temporary_changes = False

        self.changes = changes

        self._issued_oids = set()
        self._stored_oids = set()

        self._commit_lock = threading.Lock()
        self._transaction = None

        if name is None:
            name = 'DemoStorage(%r, %r)' % (base.getName(), changes.getName())
        self.__name__ = name

        self._copy_methods_from_changes(changes)

        self._next_oid = random.randint(1, 1<<62)

    def _blobify(self):
        if (self._temporary_changes and
            isinstance(self.changes, ZODB.MappingStorage.MappingStorage)
            ):
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

    def close(self):
        if not self._temporary_base:
            self.base.close()
        if not self._temporary_changes:
            self.changes.close()

    def _copy_methods_from_changes(self, changes):
        for meth in (
            '_lock_acquire', '_lock_release', 
            'getSize', 'history', 'isReadOnly', 'registerDB',
            'sortKey', 'tpc_transaction', 'tpc_vote',
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

    def load(self, oid, version=''):
        try:
            return self.changes.load(oid, version)
        except ZODB.POSException.POSKeyError:
            return self.base.load(oid, version)

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
                return self.base.loadBefore(oid, tid)
            except ZODB.POSException.POSKeyError:
                # The oid isn't in the base, so None will be the right result
                pass

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

    @ZODB.utils.locked
    def new_oid(self):
        while 1:
            oid = ZODB.utils.p64(self._next_oid )
            if oid not in self._issued_oids:
                try:
                    self.changes.load(oid, '')
                except ZODB.POSException.POSKeyError:
                    try:
                        self.base.load(oid, '')
                    except ZODB.POSException.POSKeyError:
                        self._next_oid += 1
                        self._issued_oids.add(oid)
                        return oid

            self._next_oid = random.randint(1, 1<<62)

    def pack(self, t, referencesf, gc=None):
        if gc is None:
            if self._temporary_base:
                return self.changes.pack(t, referencesf)
        elif self._temporary_base:
            return self.changes.pack(t, referencesf, gc=gc)
        elif gc:
            raise TypeError(
                "Garbage collection isn't supported"
                " when there is a base storage.")
        
        try:
            self.changes.pack(t, referencesf, gc=False)
        except TypeError, v:
            if 'gc' in str(v):
                pass # The gc arg isn't supported. Don't pack
            raise

    def pop(self):
        self.changes.close()
        return self.base

    def push(self, changes=None):
        return self.__class__(base=self, changes=changes)

    def store(self, oid, serial, data, version, transaction):
        assert version=='', "versions aren't supported"
        if transaction is not self._transaction:
            raise ZODB.POSException.StorageTransactionError(self, transaction)

        # Since the OID is being used, we don't have to keep up with it any
        # more. Save it now so we can forget it later. :)
        self._stored_oids.add(oid)

        # See if we already have changes for this oid
        try:
            old = self.changes.load(oid, '')[1]
        except ZODB.POSException.POSKeyError:
            try:
                old = self.base.load(oid, '')[1]
            except ZODB.POSException.POSKeyError:
                old = serial
                
        if old != serial:
            raise ZODB.POSException.ConflictError(
                oid=oid, serials=(old, serial)) # XXX untested branch

        return self.changes.store(oid, serial, data, '', transaction)

    def storeBlob(self, oid, oldserial, data, blobfilename, version,
                  transaction):
        assert version=='', "versions aren't supported"
        if transaction is not self._transaction:
            raise ZODB.POSException.StorageTransactionError(self, transaction)

        # Since the OID is being used, we don't have to keep up with it any
        # more. Save it now so we can forget it later. :)
        self._stored_oids.add(oid)

        try:
            return self.changes.storeBlob(
                oid, oldserial, data, blobfilename, '', transaction)
        except AttributeError:
            if self._blobify():
                return self.changes.storeBlob(
                    oid, oldserial, data, blobfilename, '', transaction)
            raise

    def temporaryDirectory(self):
        try:
            return self.changes.temporaryDirectory()
        except AttributeError:
            if self._blobify():
                return self.changes.temporaryDirectory()
            raise

    @ZODB.utils.locked
    def tpc_abort(self, transaction):
        if transaction is not self._transaction:
            return
        self._stored_oids = set()
        self._transaction = None
        self.changes.tpc_abort(transaction)
        self._commit_lock.release()

    @ZODB.utils.locked
    def tpc_begin(self, transaction, *a, **k):
        # The tid argument exists to support testing.
        if transaction is self._transaction:
            return
        self._lock_release()
        self._commit_lock.acquire()
        self._lock_acquire()
        self.changes.tpc_begin(transaction, *a, **k)
        self._transaction = transaction
        self._stored_oids = set()

    @ZODB.utils.locked
    def tpc_finish(self, transaction, func = lambda tid: None):
        if (transaction is not self._transaction):
            return
        self._issued_oids.difference_update(self._stored_oids)
        self._stored_oids = set()
        self._transaction = None
        self.changes.tpc_finish(transaction, func)
        self._commit_lock.release()

_temporary_blobdirs = {}
def cleanup_temporary_blobdir(
    ref,
    _temporary_blobdirs=_temporary_blobdirs, # Make sure it stays around 
    ):
    blob_dir = _temporary_blobdirs.pop(ref, None)
    if blob_dir and os.path.exists(blob_dir):
        ZODB.blob.remove_committed_dir(blob_dir)
