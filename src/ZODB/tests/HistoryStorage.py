##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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
"""Run the history() related tests for a storage.

Any storage that supports the history() method should be able to pass
all these tests.
"""

import sys
from time import sleep
from time import time

from ZODB.tests.MinPO import MinPO


class HistoryStorage(object):
    def checkSimpleHistory(self):
        self._checkHistory((11, 12, 13))

    def _checkHistory(self, data):
        start = time()
        # Store a couple of revisions of the object
        oid = self._storage.new_oid()
        self.assertRaises(KeyError, self._storage.history, oid)
        revids = [None]
        for data in data:
            if sys.platform == 'win32':
                # time.time() has a precision of 1ms on Windows.
                sleep(0.002)
            revids.append(self._dostore(oid, revids[-1], MinPO(data)))
        revids.reverse()
        del revids[-1]
        # Now get various snapshots of the object's history
        for i in range(1, 1 + len(revids)):
            h = self._storage.history(oid, size=i)
            self.assertEqual([d['tid'] for d in h], revids[:i])
        # Check results are sorted by timestamp, in descending order.
        if sys.platform == 'win32':
            # Same as above. This is also required in case this method is
            # called several times for the same storage.
            sleep(0.002)
        a = time()
        for d in h:
            b = a
            a = d['time']
            self.assertLess(a, b)
        self.assertLess(start, a)
