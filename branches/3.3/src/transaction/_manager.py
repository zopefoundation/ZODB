############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
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
import weakref

from transaction._transaction import Transaction

# We have to remember sets of synch objects, especially Connections.
# But we don't want mere registration with a transaction manager to
# keep a synch object alive forever; in particular, it's common
# practice not to explicitly close Connection objects, and keeping
# a Connection alive keeps a potentially huge number of other objects
# alive (e.g., the cache, and everything reachable from it too).
#
# Therefore we use "weak sets" internally.  The implementation here
# implements just enough of Python's sets.Set interface for our needs.

class WeakSet(object):
    """A set of objects that doesn't keep its elements alive.

    The objects in the set must be weakly referencable.
    The objects need not be hashable, and need not support comparison.
    Two objects are considered to be the same iff their id()s are equal.

    When the only references to an object are weak references (including
    those from WeakSets), the object can be garbage-collected, and
    will vanish from any WeakSets it may be a member of at that time.
    """

    def __init__(self):
        # Map id(obj) to obj.  By using ids as keys, we avoid requiring
        # that the elements be hashable or comparable.
        self.data = weakref.WeakValueDictionary()

    # Same as a Set, add obj to the collection.
    def add(self, obj):
        self.data[id(obj)] = obj

    # Same as a Set, remove obj from the collection, and raise
    # KeyError if obj not in the collection.
    def remove(self, obj):
        del self.data[id(obj)]

    # Return a list of all the objects in the collection.
    # Because a weak dict is used internally, iteration
    # is dicey (the underlying dict may change size during
    # iteration, due to gc or activity from other threads).
    # as_list() attempts to be safe.
    def as_list(self):
        return self.data.values()


class TransactionManager(object):

    def __init__(self):
        self._txn = None
        self._synchs = WeakSet()

    def begin(self):
        if self._txn is not None:
            self._txn.abort()
        self._txn = Transaction(self._synchs.as_list(), self)
        return self._txn

    def get(self):
        if self._txn is None:
            self._txn = Transaction(self._synchs.as_list(), self)
        return self._txn

    def free(self, txn):
        assert txn is self._txn
        self._txn = None

    def registerSynch(self, synch):
        self._synchs.add(synch)

    def unregisterSynch(self, synch):
        self._synchs.remove(synch)

class ThreadTransactionManager(object):
    """Thread-aware transaction manager.

    Each thread is associated with a unique transaction.
    """

    def __init__(self):
        # _threads maps thread ids to transactions
        self._txns = {}
        # _synchs maps a thread id to a WeakSet of registered synchronizers.
        # The set elements are passed to the Transaction constructor,
        # because the latter needs to call the synchronizers when it commits.
        self._synchs = {}

    def begin(self):
        tid = thread.get_ident()
        txn = self._txns.get(tid)
        if txn is not None:
            txn.abort()
        synchs = self._synchs.get(tid)
        if synchs is not None:
            synchs = synchs.as_list()
        txn = self._txns[tid] = Transaction(synchs, self)
        return txn

    def get(self):
        tid = thread.get_ident()
        txn = self._txns.get(tid)
        if txn is None:
            synchs = self._synchs.get(tid)
            if synchs is not None:
                synchs = synchs.as_list()
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
