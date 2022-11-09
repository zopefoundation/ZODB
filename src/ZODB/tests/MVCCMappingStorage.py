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
"""An extension of MappingStorage that depends on polling.

Each Connection has its own view of the database.  Polling updates each
connection's view.
"""

from zope.interface import implementer

import ZODB.POSException
import ZODB.utils
from ZODB.interfaces import IMVCCStorage
from ZODB.MappingStorage import MappingStorage


@implementer(IMVCCStorage)
class MVCCMappingStorage(MappingStorage):

    def __init__(self, name="MVCC Mapping Storage"):
        MappingStorage.__init__(self, name=name)
        # _polled_tid contains the transaction ID at the last poll.
        self._polled_tid = b''
        self._data_snapshot = None  # {oid->(state, tid)}
        self._main_lock = self._lock

    def new_instance(self):
        """Returns a storage instance that is a view of the same data.
        """
        inst = MVCCMappingStorage(name=self.__name__)
        # All instances share the same OID data, transaction log, commit lock,
        # and OID sequence.
        inst._data = self._data
        inst._transactions = self._transactions
        inst._commit_lock = self._commit_lock
        inst.new_oid = self.new_oid
        inst.pack = self.pack
        inst.loadBefore = self.loadBefore
        inst._ltid = self._ltid
        inst._main_lock = self._lock
        return inst

    @ZODB.utils.locked(MappingStorage.opened)
    def sync(self, force=False):
        self._data_snapshot = None

    def release(self):
        pass

    @ZODB.utils.locked(MappingStorage.opened)
    def load(self, oid, version=''):
        assert not version, "Versions are not supported"
        if self._data_snapshot is None:
            self.poll_invalidations()
        info = self._data_snapshot.get(oid)
        if info:
            return info
        raise ZODB.POSException.POSKeyError(oid)

    def poll_invalidations(self):
        """Poll the storage for changes by other connections.
        """
        # prevent changes to _transactions and _data during analysis
        with self._main_lock:
            if self._transactions:
                new_tid = self._transactions.maxKey()
            else:
                new_tid = ZODB.utils.z64

            # Copy the current data into a snapshot. This is obviously
            # very inefficient for large storages, but it's good for
            # tests.
            self._data_snapshot = {}
            for oid, tid_data in self._data.items():
                if tid_data:
                    tid = tid_data.maxKey()
                    self._data_snapshot[oid] = tid_data[tid], tid

            if self._polled_tid:
                if self._polled_tid not in self._transactions:
                    # This connection is so old that we can no longer enumerate
                    # all the changes.
                    self._polled_tid = new_tid
                    return None

            changed_oids = set()
            for tid, txn in self._transactions.items(
                    self._polled_tid, new_tid,
                    excludemin=True, excludemax=False):
                if txn.status == 'p':
                    # This transaction has been packed, so it is no longer
                    # possible to enumerate all changed oids.
                    self._polled_tid = new_tid
                    return None
                if tid == self._ltid:
                    # ignore the transaction committed by this connection
                    continue
                changed_oids.update(txn.data.keys())

        self._polled_tid = self._ltid = new_tid
        return list(changed_oids)

    def tpc_finish(self, transaction, func=lambda tid: None):
        self._data_snapshot = None
        with self._main_lock:
            return MappingStorage.tpc_finish(self, transaction, func)

    def tpc_abort(self, transaction):
        self._data_snapshot = None
        with self._main_lock:
            MappingStorage.tpc_abort(self, transaction)

    def pack(self, t, referencesf, gc=True):
        # prevent all concurrent commits during packing
        with self._commit_lock:
            MappingStorage.pack(self, t, referencesf, gc)
