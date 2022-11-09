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
from binascii import hexlify
from binascii import unhexlify

import zope.interface

import ZODB.blob
import ZODB.interfaces
import ZODB.utils


@zope.interface.implementer(ZODB.interfaces.IStorageWrapper)
class HexStorage(object):

    copied_methods = (
        'close', 'getName', 'getSize', 'history', 'isReadOnly',
        'lastTransaction', 'new_oid', 'sortKey',
        'tpc_abort', 'tpc_begin', 'tpc_finish', 'tpc_vote',
        'loadBlob', 'openCommittedBlobFile', 'temporaryDirectory',
        'supportsUndo', 'undo', 'undoLog', 'undoInfo',
    )

    def __init__(self, base):
        self.base = base
        base.registerDB(self)

        for name in self.copied_methods:
            v = getattr(base, name, None)
            if v is not None:
                setattr(self, name, v)

        zope.interface.directlyProvides(self, zope.interface.providedBy(base))

    def __getattr__(self, name):
        return getattr(self.base, name)

    def __len__(self):
        return len(self.base)

    load = ZODB.utils.load_current

    def loadBefore(self, oid, tid):
        r = self.base.loadBefore(oid, tid)
        if r is not None:
            data, serial, after = r
            return unhexlify(data[2:]), serial, after
        else:
            return r

    def loadSerial(self, oid, serial):
        return unhexlify(self.base.loadSerial(oid, serial)[2:])

    def pack(self, pack_time, referencesf, gc=True):
        def refs(p, oids=None):
            return referencesf(unhexlify(p[2:]), oids)
        return self.base.pack(pack_time, refs, gc)

    def registerDB(self, db):
        self.db = db
        self._db_transform = db.transform_record_data
        self._db_untransform = db.untransform_record_data

    _db_transform = _db_untransform = lambda self, data: data

    def store(self, oid, serial, data, version, transaction):
        return self.base.store(
            oid, serial, b'.h'+hexlify(data), version, transaction)

    def restore(self, oid, serial, data, version, prev_txn, transaction):
        return self.base.restore(
            oid, serial, data and (b'.h'+hexlify(data)), version, prev_txn,
            transaction)

    def iterator(self, start=None, stop=None):
        it = self.base.iterator(start, stop)
        try:
            for t in it:
                yield Transaction(self, t)
        finally:
            if hasattr(it, 'close'):
                it.close()

    def storeBlob(self, oid, oldserial, data, blobfilename, version,
                  transaction):
        return self.base.storeBlob(oid, oldserial, b'.h'+hexlify(data),
                                   blobfilename, version, transaction)

    def restoreBlob(self, oid, serial, data, blobfilename, prev_txn,
                    transaction):
        return self.base.restoreBlob(oid, serial,
                                     data and (b'.h'+hexlify(data)),
                                     blobfilename, prev_txn, transaction)

    def invalidateCache(self):
        return self.db.invalidateCache()

    def invalidate(self, transaction_id, oids, version=''):
        return self.db.invalidate(transaction_id, oids, version)

    def references(self, record, oids=None):
        return self.db.references(unhexlify(record[2:]), oids)

    def transform_record_data(self, data):
        return b'.h'+hexlify(self._db_transform(data))

    def untransform_record_data(self, data):
        return self._db_untransform(unhexlify(data[2:]))

    def record_iternext(self, next=None):
        oid, tid, data, next = self.base.record_iternext(next)
        return oid, tid, unhexlify(data[2:]), next

    def copyTransactionsFrom(self, other):
        ZODB.blob.copyTransactionsFromTo(other, self)


class ServerHexStorage(HexStorage):
    """Use on ZEO storage server when Hex is used on client

    Don't do conversion as part of load/store, but provide
    pickle decoding.
    """

    copied_methods = HexStorage.copied_methods + (
        'load', 'loadBefore', 'loadSerial', 'store', 'restore',
        'iterator', 'storeBlob', 'restoreBlob', 'record_iternext',
    )


class Transaction(object):

    def __init__(self, store, trans):
        self.__store = store
        self.__trans = trans

    def __iter__(self):
        for r in self.__trans:
            if r.data:
                r.data = self.__store.untransform_record_data(r.data)
            yield r

    def __getattr__(self, name):
        return getattr(self.__trans, name)


class ZConfigHex(object):

    _factory = HexStorage

    def __init__(self, config):
        self.config = config
        self.name = config.getSectionName()

    def open(self):
        base = self.config.base.open()
        return self._factory(base)


class ZConfigServerHex(ZConfigHex):

    _factory = ServerHexStorage
