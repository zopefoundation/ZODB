"""Adapt IStorage objects to IMVCCStorage

This is a largely internal implementation of ZODB, especially DB and
Connection.  It takes the MVCC implementation involving invalidations
and start time and moves it into a storage adapter.  This allows ZODB
to treat Relstoage and other storages in pretty much the same way and
also simplifies the implementation of the DB and Connection classes.
"""
import zope.interface

from . import interfaces, serialize, POSException
from .utils import p64, u64, Lock

class Base(object):

    _copy_methods = (
        'getName', 'getSize', 'history', 'lastTransaction', 'sortKey',
        'loadBlob', 'openCommittedBlobFile',
        'isReadOnly', 'supportsUndo', 'undoLog', 'undoInfo',
        'temporaryDirectory',
        )

    def __init__(self, storage):
        self._storage = storage
        if interfaces.IBlobStorage.providedBy(storage):
            zope.interface.alsoProvides(self, interfaces.IBlobStorage)

    def __getattr__(self, name):
        if name in self._copy_methods:
            m = getattr(self._storage, name)
            setattr(self, name, m)
            return m

        raise AttributeError(name)

    def __len__(self):
        return len(self._storage)

class MVCCAdapter(Base):

    def __init__(self, storage):
        Base.__init__(self, storage)
        self._instances = set()
        self._lock = Lock()
        if hasattr(storage, 'registerDB'):
            storage.registerDB(self)

    def new_instance(self):
        instance = MVCCAdapterInstance(self)
        with self._lock:
            self._instances.add(instance)
        return instance

    def before_instance(self, before=None):
        return HistoricalStorageAdapter(self._storage, before)

    def undo_instance(self):
        return UndoAdapterInstance(self)

    def _release(self, instance):
        with self._lock:
            self._instances.remove(instance)

    closed = False
    def close(self):
        if not self.closed:
            self.closed = True
            self._storage.close()
            del self._instances
            del self._storage

    def invalidateCache(self):
        with self._lock:
            for instance in self._instances:
                instance._invalidateCache()

    def invalidate(self, transaction_id, oids):
        with self._lock:
            for instance in self._instances:
                instance._invalidate(oids)

    def _invalidate_finish(self, oids, committing_instance):
        with self._lock:
            for instance in self._instances:
                if instance is not committing_instance:
                    instance._invalidate(oids)

    references = serialize.referencesf
    transform_record_data = untransform_record_data = lambda self, data: data

    def pack(self, pack_time, referencesf):
        return self._storage.pack(pack_time, referencesf)

class MVCCAdapterInstance(Base):

    _copy_methods = Base._copy_methods + (
        'loadSerial', 'new_oid', 'tpc_vote',
        'checkCurrentSerialInTransaction', 'tpc_abort',
        )

    def __init__(self, base):
        self._base = base
        Base.__init__(self, base._storage)
        self._lock = Lock()
        self._invalidations = set()
        self._start = None # Transaction start time
        self._sync = getattr(self._storage, 'sync', lambda : None)

    def release(self):
        self._base._release(self)

    close = release

    def _invalidateCache(self):
        with self._lock:
            self._invalidations = None

    def _invalidate(self, oids):
        with self._lock:
            try:
                self._invalidations.update(oids)
            except AttributeError:
                if self._invalidations is not None:
                    raise

    def sync(self, force=True):
        if force:
            self._sync()

    def poll_invalidations(self):
        self._start = p64(u64(self._storage.lastTransaction()) + 1)
        with self._lock:
            if self._invalidations is None:
                self._invalidations = set()
                return None
            else:
                result = list(self._invalidations)
                self._invalidations.clear()
                return result

    def load(self, oid):
        assert self._start is not None
        r = self._storage.loadBefore(oid, self._start)
        if r is None:
            raise POSException.ReadConflictError(repr(oid))
        return r[:2]

    def prefetch(self, oids):
        try:
            self._storage.prefetch(oids, self._start)
        except AttributeError:
            if not hasattr(self._storage, 'prefetch'):
                self.prefetch = lambda *a: None
            else:
                raise

    _modified = None # Used to keep track of oids modified within a
                     # transaction, so we can invalidate them later.

    def tpc_begin(self, transaction):
        self._storage.tpc_begin(transaction)
        self._modified = set()

    def store(self, oid, serial, data, version, transaction):
        self._storage.store(oid, serial, data, version, transaction)
        self._modified.add(oid)

    def storeBlob(self, oid, serial, data, blobfilename, version, transaction):
        self._storage.storeBlob(
            oid, serial, data, blobfilename, '', transaction)
        self._modified.add(oid)

    def tpc_finish(self, transaction, func = lambda tid: None):
        modified = self._modified
        self._modified = None

        def invalidate_finish(tid):
            self._base._invalidate_finish(modified, self)
            func(tid)

        return self._storage.tpc_finish(transaction, invalidate_finish)

def read_only_writer(self, *a, **kw):
    raise POSException.ReadOnlyError

class HistoricalStorageAdapter(Base):
    """Adapt a storage to a historical storage
    """

    _copy_methods = Base._copy_methods + (
        'loadSerial', 'tpc_begin', 'tpc_finish', 'tpc_abort', 'tpc_vote',
        'checkCurrentSerialInTransaction',
        )

    def __init__(self, storage, before=None):
        Base.__init__(self, storage)
        self._before = before

    def isReadOnly(self):
        return True

    def supportsUndo(self):
        return False

    def release(self):
        try:
            release = self._storage.release
        except AttributeError:
            pass
        else:
            release()

    close = release

    def sync(self, force=True):
        pass

    def poll_invalidations(self):
        return []

    new_oid = pack = store = read_only_writer

    def load(self, oid, version=''):
        r = self._storage.loadBefore(oid, self._before)
        if r is None:
            raise POSException.POSKeyError(oid)
        return r[:2]


class UndoAdapterInstance(Base):

    _copy_methods = Base._copy_methods + (
        'tpc_abort',
        )

    def __init__(self, base):
        self._base = base
        Base.__init__(self, base._storage)

    def release(self):
        pass

    close = release

    def tpc_begin(self, transaction):
        self._storage.tpc_begin(transaction)
        self._undone = set()

    def undo(self, transaction_id, transaction):
        result = self._storage.undo(transaction_id, transaction)
        if result:
            self._undone.update(result[1])
        return result

    def tpc_vote(self, transaction):
        result = self._storage.tpc_vote(transaction)
        if result:
            self._undone.update(result)

    def tpc_finish(self, transaction, func = lambda tid: None):

        def invalidate_finish(tid):
            self._base._invalidate_finish(self._undone, None)
            func(tid)

        self._storage.tpc_finish(transaction, invalidate_finish)
