############################################################################
#
# Copyright (c) 2004 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
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

from ZODB.utils import WeakSet, deprecated37

from transaction._transaction import Transaction

# Used for deprecated arguments.  ZODB.utils.DEPRECATED_ARGUMENT was
# too hard to use here, due to the convoluted import dance across
# __init__.py files.
_marker = object()

# We have to remember sets of synch objects, especially Connections.
# But we don't want mere registration with a transaction manager to
# keep a synch object alive forever; in particular, it's common
# practice not to explicitly close Connection objects, and keeping
# a Connection alive keeps a potentially huge number of other objects
# alive (e.g., the cache, and everything reachable from it too).
# Therefore we use "weak sets" internally.
#

# Call the ISynchronizer newTransaction() method on every element of
# WeakSet synchs.
# A transaction manager needs to do this whenever begin() is called.
# Since it would be good if tm.get() returned the new transaction while
# newTransaction() is running, calling this has to be delayed until after
# the transaction manager has done whatever it needs to do to make its
# get() return the new txn.
def _new_transaction(txn, synchs):
    if synchs:
        synchs.map(lambda s: s.newTransaction(txn))

# Important:  we must always pass a WeakSet (even if empty) to the Transaction
# constructor:  synchronizers are registered with the TM, but the
# ISynchronizer xyzCompletion() methods are called by Transactions without
# consulting the TM, so we need to pass a mutable collection of synchronizers
# so that Transactions "see" synchronizers that get registered after the
# Transaction object is constructed.

class TransactionManager(object):

    def __init__(self):
        self._txn = None
        self._synchs = WeakSet()

    def begin(self):
        if self._txn is not None:
            self._txn.abort()
        txn = self._txn = Transaction(self._synchs, self)
        _new_transaction(txn, self._synchs)
        return txn

    def get(self):
        if self._txn is None:
            self._txn = Transaction(self._synchs, self)
        return self._txn

    def free(self, txn):
        assert txn is self._txn
        self._txn = None

    def registerSynch(self, synch):
        self._synchs.add(synch)

    def unregisterSynch(self, synch):
        self._synchs.remove(synch)

    def isDoomed(self):
        return self.get().isDoomed()

    def doom(self):
        return self.get().doom()

    def commit(self):
        return self.get().commit()

    def abort(self):
        return self.get().abort()

    def savepoint(self, optimistic=False):
        return self.get().savepoint(optimistic)

class ThreadTransactionManager(TransactionManager):
    """Thread-aware transaction manager.

    Each thread is associated with a unique transaction.
    """

    def __init__(self):
        # _threads maps thread ids to transactions
        self._txns = {}

        # _synchs maps a thread id to a WeakSet of registered synchronizers.
        # The WeakSet is passed to the Transaction constructor, because the
        # latter needs to call the synchronizers when it commits.
        self._synchs = {}

    def begin(self):
        tid = thread.get_ident()
        txn = self._txns.get(tid)
        if txn is not None:
            txn.abort()

        synchs = self._synchs.get(tid)
        if synchs is None:
            synchs = self._synchs[tid] = WeakSet()

        txn = self._txns[tid] = Transaction(synchs, self)
        _new_transaction(txn, synchs)
        return txn

    def get(self):
        tid = thread.get_ident()
        txn = self._txns.get(tid)
        if txn is None:
            synchs = self._synchs.get(tid)
            if synchs is None:
                synchs = self._synchs[tid] = WeakSet()
            txn = self._txns[tid] = Transaction(synchs, self)
        return txn

    def free(self, txn):
        tid = thread.get_ident()
        assert txn is self._txns.get(tid)
        del self._txns[tid]

    def registerSynch(self, synch):
        tid = thread.get_ident()
        ws = self._synchs.get(tid)
        if ws is None:
            ws = self._synchs[tid] = WeakSet()
        ws.add(synch)

    def unregisterSynch(self, synch):
        tid = thread.get_ident()
        ws = self._synchs[tid]
        ws.remove(synch)
