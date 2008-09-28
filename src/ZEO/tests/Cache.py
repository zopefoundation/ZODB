##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
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
"""Tests of the ZEO cache"""

from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle

from transaction import Transaction

class TransUndoStorageWithCache:

    def checkUndoInvalidation(self):
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=MinPO(23))
        revid = self._dostore(oid, revid=revid, data=MinPO(24))
        revid = self._dostore(oid, revid=revid, data=MinPO(25))

        info = self._storage.undoInfo()
        if not info:
            # Preserved this comment, but don't understand it:
            # "Perhaps we have an old storage implementation that
            #  does do the negative nonsense."
            info = self._storage.undoInfo(0, 20)
        tid = info[0]['id']

        # Now start an undo transaction
        t = Transaction()
        t.note('undo1')
        self._storage.tpc_begin(t)

        tid, oids = self._storage.undo(tid, t)

        # Make sure this doesn't load invalid data into the cache
        self._storage.load(oid, '')

        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)

        assert len(oids) == 1
        assert oids[0] == oid
        data, revid = self._storage.load(oid, '')
        obj = zodb_unpickle(data)
        assert obj == MinPO(24)
