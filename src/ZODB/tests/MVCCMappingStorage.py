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

import time

import BTrees
from ZODB.interfaces import IMVCCStorage
from ZODB.MappingStorage import MappingStorage
from ZODB.TimeStamp import TimeStamp
from zope.interface import implements


class MVCCMappingStorage(MappingStorage):
    implements(IMVCCStorage)

    def __init__(self, name="MVCC Mapping Storage"):
        MappingStorage.__init__(self, name=name)
        # _polled_tid contains the transaction ID at the last poll.
        self._polled_tid = ''

    def new_instance(self):
        """Returns a storage instance that is a view of the same data.
        """
        res = MVCCMappingStorage(name=self.__name__)
        res._transactions = self._transactions
        return res

    def sync(self, force=False):
        pass

    def release(self):
        pass

    def poll_invalidations(self):
        """Poll the storage for changes by other connections.
        """
        new_tid = self._transactions.maxKey()

        if self._polled_tid:
            if not self._transactions.has_key(self._polled_tid):
                # This connection is so old that we can no longer enumerate
                # all the changes.
                self._polled_tid = new_tid
                return None

        changed_oids = set()
        for tid, txn in self._transactions.items(
                self._polled_tid, new_tid, excludemin=True, excludemax=False):
            if txn.status == 'p':
                # This transaction has been packed, so it is no longer
                # possible to enumerate all changed oids.
                self._polled_tid = new_tid
                return None
            if tid == self._ltid:
                # ignore the transaction committed by this connection
                continue

            changes = txn.data
            # pull in changes from the transaction log
            for oid, value in changes.iteritems():
                tid_data = self._data.get(oid)
                if tid_data is None:
                    tid_data = BTrees.OOBTree.OOBucket()
                    self._data[oid] = tid_data
                tid_data[tid] = changes[oid]
            changed_oids.update(changes.keys())

        self._polled_tid = new_tid
        return list(changed_oids)
