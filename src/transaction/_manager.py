############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
############################################################################
"""A TransactionManager controls transaction boundaries.

It coordinates application code and resource managers, so that they
are associated with the right transaction.
"""

import thread

from transaction._transaction import Transaction

class TransactionManager(object):

    def __init__(self):
        self._txn = None
        self._synchs = []

    def begin(self):
        if self._txn is not None:
            self._txn.abort()
        self._txn = Transaction(self._synchs, self)
        return self._txn

    def get(self):
        if self._txn is None:
            self._txn = Transaction(self._synchs, self)
        return self._txn

    def free(self, txn):
        assert txn is self._txn
        self._txn = None

    def registerSynch(self, synch):
        self.synchs.append(synch)

    def unregisterSynch(self, synch):
        self._synchs.remove(synch)

class ThreadTransactionManager(object):
    """Thread-aware transaction manager.

    Each thread is associated with a unique transaction.
    """

    def __init__(self):
        # _threads maps thread ids to transactions
        self._txns = {}
        # _synchs maps a thread id to a list of registered synchronizers.
        # The list is passed to the Transaction constructor, because
        # it needs to call the synchronizers when it commits.
        self._synchs = {}

    def begin(self):
        tid = thread.get_ident()
        txn = self._txns.get(tid)
        if txn is not None:
            txn.abort()
        txn = self._txns[tid] = Transaction(self._synchs.get(tid), self)
        return txn

    def get(self):
        tid = thread.get_ident()
        txn = self._txns.get(tid)
        if txn is None:
            txn = self._txns[tid] = Transaction(self._synchs.get(tid), self)
        return txn

    def free(self, txn):
        tid = thread.get_ident()
        assert txn is self._txns.get(tid)
        del self._txns[tid]

    def registerSynch(self, synch):
        tid = thread.get_ident()
        L = self._synchs.setdefault(tid, [])
        L.append(synch)

    def unregisterSynch(self, synch):
        tid = thread.get_ident()
        L = self._synchs.get(tid)
        L.remove(synch)

