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
from ZODB.tests.StorageTestBase import zodb_pickle, MinPO


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
        self._dostore()
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
