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
from ZODB.interfaces import IStoragePollable
from ZODB.MappingStorage import MappingStorage
from ZODB.TimeStamp import TimeStamp
from zope.interface import implements


class PollableMappingStorage(MappingStorage):
    implements(IStoragePollable)

    propagate_invalidations = False

    def __init__(self, name="Pollable Mapping Storage"):
        MappingStorage.__init__(self, name=name)
        # _polled_tid contains the transaction ID at the last poll.
        self._polled_tid = ''

    def bind_connection(self, connection):
        """Returns a storage instance to be used by the given Connection.
        """
        return BoundStorage(self)

    def connection_closing(self):
        """Notifies the storage that a connection is closing.
        """
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


class BoundStorage(PollableMappingStorage):
    """A PollableMappingStorage used for a specific Connection."""

    def __init__(self, common):
        PollableMappingStorage.__init__(self, name=common.__name__)
        # bound storages use the same transaction log as the common storage.
        self._transactions = common._transactions
