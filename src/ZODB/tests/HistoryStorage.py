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

from ZODB.tests.MinPO import MinPO
from transaction import Transaction

class HistoryStorage:
    def checkSimpleHistory(self):
        eq = self.assertEqual
        # Store a couple of revisions of the object
        oid = self._storage.new_oid()
        self.assertRaises(KeyError,self._storage.history,oid)
        revid1 = self._dostore(oid, data=MinPO(11))
        revid2 = self._dostore(oid, revid=revid1, data=MinPO(12))
        revid3 = self._dostore(oid, revid=revid2, data=MinPO(13))
        # Now get various snapshots of the object's history
        h = self._storage.history(oid, size=1)
        eq(len(h), 1)
        d = h[0]
        eq(d['tid'], revid3)
        # Try to get 2 historical revisions
        h = self._storage.history(oid, size=2)
        eq(len(h), 2)
        d = h[0]
        eq(d['tid'], revid3)
        d = h[1]
        eq(d['tid'], revid2)
        # Try to get all 3 historical revisions
        h = self._storage.history(oid, size=3)
        eq(len(h), 3)
        d = h[0]
        eq(d['tid'], revid3)
        d = h[1]
        eq(d['tid'], revid2)
        d = h[2]
        eq(d['tid'], revid1)
        # There should be no more than 3 revisions
        h = self._storage.history(oid, size=4)
        eq(len(h), 3)
        d = h[0]
        eq(d['tid'], revid3)
        d = h[1]
        eq(d['tid'], revid2)
        d = h[2]
        eq(d['tid'], revid1)
        
