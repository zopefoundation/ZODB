##############################################################################
#
# Copyright (c) 2010 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
import ZODB.blob
import ZODB.interfaces
import zope.interface

class MadIterator(object):

    def __init__(self, it):
        self.it = it

    def __iter__(self):
        return self

    def next(self):
        try:
            return self.it.next()
        except StopIteration:
            raise ZODB.interfaces.StorageStopIteration()

class HexStorage(object):

    zope.interface.implements(ZODB.interfaces.IStorageWrapper)

    def __init__(self, base):
        self.base = base
        base.registerDB(self)

        for name in (
            'close', 'getName', 'getSize', 'history', 'isReadOnly',
            'lastTransaction', 'new_oid', 'sortKey',
            'tpc_abort', 'tpc_begin', 'tpc_finish', 'tpc_vote',
            'loadBlob', 'openCommittedBlobFile', 'temporaryDirectory',
            'supportsUndo', 'undo', 'undoLog', 'undoInfo',
            ):
            v = getattr(base, name, None)
            if v is not None:
                setattr(self, name, v)

        zope.interface.directlyProvides(self, zope.interface.providedBy(base))

    def __getattr__(self, name):
        return getattr(self.base, name)

    def __len__(self):
        return len(self.base)

    def load(self, oid, version=''):
        data, serial = self.base.load(oid, version)
        return data.decode('hex'), serial

    def loadBefore(self, oid, tid):
        r = self.base.loadBefore(oid, tid)
        if r is not None:
            data, serial, after = r
            return data.decode('hex'), serial, after
        else:
            return r

    def loadSerial(self, oid, serial):
        return self.base.loadSerial(oid, serial).decode('hex')

    def pack(self, pack_time, referencesf, gc=True):
        def refs(p, oids=None):
            return referencesf(p.decode('hex'), oids)
        return self.base.pack(pack_time, refs, gc)

    def registerDB(self, db):
        self.db = db
        self._db_transform = db.transform_record_data
        self._db_untransform = db.untransform_record_data

    _db_transform = _db_untransform = lambda self, data: data

    def store(self, oid, serial, data, version, transaction):
        return self.base.store(
            oid, serial, data.encode('hex'), version, transaction)

    def restore(self, oid, serial, data, version, prev_txn, transaction):
        return self.base.restore(
            oid, serial, data and data.encode('hex'), version, prev_txn,
            transaction)

    def iterator(self, start=None, stop=None):
        def it(start, stop):
            for t in self.base.iterator(start, stop):
                yield Transaction(self, t)

        return MadIterator(it(start, stop))


    def storeBlob(self, oid, oldserial, data, blobfilename, version,
                  transaction):
        return self.base.storeBlob(oid, oldserial, data.encode('hex'),
                                   blobfilename, version, transaction)

    def restoreBlob(self, oid, serial, data, blobfilename, prev_txn,
                    transaction):
        return self.base.restoreBlob(oid, serial, data and data.encode('hex'),
                                     blobfilename, prev_txn, transaction)

    def invalidateCache(self):
        return self.db.invalidateCache()

    def invalidate(self, transaction_id, oids, version=''):
        return self.db.invalidate(transaction_id, oids, version)

    def references(self, record, oids=None):
        return self.db.references(record.decode('hex'), oids)

    def transform_record_data(self, data):
        return self._db_transform(data).encode('hex')

    def untransform_record_data(self, data):
        return self._db_untransform(data.decode('hex'))

    def record_iternext(self, next=None):
        oid, tid, data, next = self.base.record_iternext(next)
        return oid, tid, data.decode('hex'), next

    def copyTransactionsFrom(self, other):
        ZODB.blob.copyTransactionsFromTo(other, self)

class Transaction(object):

    def __init__(self, store, trans):
        self.__store = store
        self.__trans = trans

    def __iter__(self):
        def it(self):
            for r in self.__trans:
                if r.data:
                    r.data = self.__store.untransform_record_data(r.data)
                yield r

        return MadIterator(it(self))

    def __getattr__(self, name):
        return getattr(self.__trans, name)

class ZConfig:

    def __init__(self, config):
        self.config = config
        self.name = config.getSectionName()

    def open(self):
        base = self.config.base.open()
        return HexStorage(base)
