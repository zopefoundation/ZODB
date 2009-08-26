##############################################################################
#
# Copyright (c) 2008 Zope Corporation and Contributors.
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
"""ZEO iterator protocol tests."""

import transaction


class IterationTests:

    def checkIteratorGCProtocol(self):
        # Test garbage collection on protocol level.
        server = self._storage._server

        iid = server.iterator_start(None, None)
        # None signals the end of iteration.
        self.assertEquals(None, server.iterator_next(iid))
        # The server has disposed the iterator already.
        self.assertRaises(KeyError, server.iterator_next, iid)

        iid = server.iterator_start(None, None)
        # This time, we tell the server to throw the iterator away.
        server.iterator_gc([iid])
        self.assertRaises(KeyError, server.iterator_next, iid)

    def checkIteratorExhaustionStorage(self):
        # Test the storage's garbage collection mechanism.
        iterator = self._storage.iterator()
        self.assertEquals(1, len(self._storage._iterator_ids))
        iid = list(self._storage._iterator_ids)[0]
        self.assertEquals([], list(iterator))
        self.assertEquals(0, len(self._storage._iterator_ids))

        # The iterator has run through, so the server has already disposed it.
        self.assertRaises(KeyError, self._storage._server.iterator_next, iid)

    def checkIteratorGCSpanTransactions(self):
        iterator = self._storage.iterator()
        t = transaction.Transaction()
        self._storage.tpc_begin(t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        self.assertEquals([], list(iterator))

    def checkIteratorGCStorageCommitting(self):
        # We want the iterator to be garbage-collected, so we don't keep any
        # hard references to it. The storage tracks its ID, though.
        self._storage.iterator()

        self.assertEquals(1, len(self._storage._iterator_ids))
        iid = list(self._storage._iterator_ids)[0]

        # GC happens at the transaction boundary. After that, both the storage
        # and the server have forgotten the iterator.
        self._dostore()
        self.assertEquals(0, len(self._storage._iterator_ids))
        self.assertRaises(KeyError, self._storage._server.iterator_next, iid)

    def checkIteratorGCStorageTPCAborting(self):
        self._storage.iterator()
        iid = list(self._storage._iterator_ids)[0]
        t = transaction.Transaction()
        self._storage.tpc_begin(t)
        self._storage.tpc_abort(t)
        self.assertEquals(0, len(self._storage._iterator_ids))
        self.assertRaises(KeyError, self._storage._server.iterator_next, iid)

    def checkIteratorGCStorageDisconnect(self):
        self._storage.iterator()
        iid = list(self._storage._iterator_ids)[0]
        t = transaction.Transaction()
        self._storage.tpc_begin(t)
        # Show that after disconnecting, the client side GCs the iterators
        # as well. I'm calling this directly to avoid accidentally
        # calling tpc_abort implicitly.
        self._storage.notifyDisconnected()
        self.assertEquals(0, len(self._storage._iterator_ids))

    def checkIteratorParallel(self):
        self._dostore()
        self._dostore()
        iter1 = self._storage.iterator()
        iter2 = self._storage.iterator()
        txn_info1 = iter1.next()
        txn_info2 = iter2.next()
        self.assertEquals(txn_info1.tid, txn_info2.tid)
        txn_info1 = iter1.next()
        txn_info2 = iter2.next()
        self.assertEquals(txn_info1.tid, txn_info2.tid)
        self.assertRaises(StopIteration, iter1.next)
        self.assertRaises(StopIteration, iter2.next)

def iterator_sane_after_reconnect():
    r"""Make sure that iterators are invalidated on disconnect.

Start a server:

    >>> addr, adminaddr = start_server(
    ...     '<filestorage>\npath fs\n</filestorage>', keep=1)

Open a client storage to it and commit a some transactions:

    >>> import ZEO, transaction
    >>> db = ZEO.DB(addr)
    >>> conn = db.open()
    >>> for i in range(10):
    ...     conn.root().i = i
    ...     transaction.commit()

Create an iterator:

    >>> it = conn._storage.iterator()
    >>> tid1 = it.next().tid

Restart the storage:

    >>> stop_server(adminaddr)
    >>> wait_disconnected(conn._storage)
    >>> _ = start_server('<filestorage>\npath fs\n</filestorage>', addr=addr)
    >>> wait_connected(conn._storage)

Now, we'll create a second iterator:

    >>> it2 = conn._storage.iterator()

If we try to advance the first iterator, we should get an error:

    >>> it.next().tid > tid1
    Traceback (most recent call last):
    ...
    ClientDisconnected: Disconnected iterator

The second iterator should be peachy:

    >>> it2.next().tid == tid1
    True

Cleanup:

    >>> db.close()
    """
