##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
# 
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
# 
##############################################################################
"""Run the basic tests for a storage as described in the official storage API

The most complete and most out-of-date description of the interface is:
http://www.zope.org/Documentation/Developer/Models/ZODB/ZODB_Architecture_Storage_Interface_Info.html

All storages should be able to pass these tests.
"""

from ZODB.Transaction import Transaction
from ZODB import POSException

from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase \
     import zodb_unpickle, zodb_pickle, handle_serials

ZERO = '\0'*8



class BasicStorage:
    def checkBasics(self):
        t = Transaction()
        self._storage.tpc_begin(t)
        # This should simply return
        self._storage.tpc_begin(t)
        # Aborting is easy
        self._storage.tpc_abort(t)
        # Test a few expected exceptions when we're doing operations giving a
        # different Transaction object than the one we've begun on.
        self._storage.tpc_begin(t)
        self.assertRaises(
            POSException.StorageTransactionError,
            self._storage.store,
            0, 0, 0, 0, Transaction())

        #JF# The following will fail two ways. UnitTest doesn't
        #JF# help us here:
        #JF# self.assertRaises(
        #JF#     POSException.StorageTransactionError,
        #JF#     self._storage.abortVersion,
        #JF#     0, Transaction())

        #JF# but we can do it another way:
        try:
            self._storage.abortVersion('dummy', Transaction())
        except (POSException.StorageTransactionError,
                POSException.VersionCommitError):
            pass # test passed ;)
        else:
            assert 0, "Should have failed, invalid transaction."

        #JF# ditto
        #JF# self.assertRaises(
        #JF#     POSException.StorageTransactionError,
        #JF#     self._storage.commitVersion,
        #JF#     0, 1, Transaction())
        try:
            self._storage.commitVersion('dummy', 'dummer', Transaction())
        except (POSException.StorageTransactionError,
                POSException.VersionCommitError):
            pass # test passed ;)
        else:
            assert 0, "Should have failed, invalid transaction."

        self.assertRaises(
            POSException.StorageTransactionError,
            self._storage.store,
            0, 1, 2, 3, Transaction())
        self._storage.tpc_abort(t)

    def checkSerialIsNoneForInitialRevision(self):
        eq = self.assertEqual
        oid = self._storage.new_oid()
        txn = Transaction()
        self._storage.tpc_begin(txn)
        # Use None for serial.  Don't use _dostore() here because that coerces
        # serial=None to serial=ZERO.
        r1 = self._storage.store(oid, None, zodb_pickle(MinPO(11)),
                                       '', txn)
        r2 = self._storage.tpc_vote(txn)
        self._storage.tpc_finish(txn)
        newrevid = handle_serials(oid, r1, r2)
        data, revid = self._storage.load(oid, '')
        value = zodb_unpickle(data)
        eq(value, MinPO(11))
        eq(revid, newrevid)

    def checkNonVersionStore(self, oid=None, revid=None, version=None):
        revid = ZERO
        newrevid = self._dostore(revid=revid)
        # Finish the transaction.
        self.assertNotEqual(newrevid, revid)

    def checkNonVersionStoreAndLoad(self):
        eq = self.assertEqual
        oid = self._storage.new_oid()
        self._dostore(oid=oid, data=MinPO(7))
        data, revid = self._storage.load(oid, '')
        value = zodb_unpickle(data)
        eq(value, MinPO(7))
        # Now do a bunch of updates to an object
        for i in range(13, 22):
            revid = self._dostore(oid, revid=revid, data=MinPO(i))
        # Now get the latest revision of the object
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(21))

    def checkNonVersionModifiedInVersion(self):
        oid = self._storage.new_oid()
        self._dostore(oid=oid)
        self.assertEqual(self._storage.modifiedInVersion(oid), '')

    def checkConflicts(self):
        oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=MinPO(11))
        revid2 = self._dostore(oid, revid=revid1, data=MinPO(12))
        self.assertRaises(POSException.ConflictError,
                          self._dostore,
                          oid, revid=revid1, data=MinPO(13))

    def checkWriteAfterAbort(self):
        oid = self._storage.new_oid()
        t = Transaction()
        self._storage.tpc_begin(t)
        self._storage.store(oid, ZERO, zodb_pickle(MinPO(5)), '', t)
        # Now abort this transaction
        self._storage.tpc_abort(t)
        # Now start all over again
        oid = self._storage.new_oid()
        self._dostore(oid=oid, data=MinPO(6))

    def checkAbortAfterVote(self):
        oid1 = self._storage.new_oid()
        revid1 = self._dostore(oid=oid1, data=MinPO(-2))
        oid = self._storage.new_oid()
        t = Transaction()
        self._storage.tpc_begin(t)
        self._storage.store(oid, ZERO, zodb_pickle(MinPO(5)), '', t)
        # Now abort this transaction
        self._storage.tpc_vote(t)
        self._storage.tpc_abort(t)
        # Now start all over again
        oid = self._storage.new_oid()
        revid = self._dostore(oid=oid, data=MinPO(6))

        for oid, revid in [(oid1, revid1), (oid, revid)]:
            data, _revid = self._storage.load(oid, '')
            self.assertEqual(revid, _revid)

    def checkStoreTwoObjects(self):
        noteq = self.assertNotEqual
        p31, p32, p51, p52 = map(MinPO, (31, 32, 51, 52))
        oid1 = self._storage.new_oid()
        oid2 = self._storage.new_oid()
        noteq(oid1, oid2)
        revid1 = self._dostore(oid1, data=p31)
        revid2 = self._dostore(oid2, data=p51)
        noteq(revid1, revid2)
        revid3 = self._dostore(oid1, revid=revid1, data=p32)
        revid4 = self._dostore(oid2, revid=revid2, data=p52)
        noteq(revid3, revid4)

    def checkGetSerial(self):
        if not hasattr(self._storage, 'getSerial'):
            return
        eq = self.assertEqual
        p41, p42 = map(MinPO, (41, 42))
        oid = self._storage.new_oid()
        self.assertRaises(KeyError, self._storage.getSerial, oid)
        # Now store a revision
        revid1 = self._dostore(oid, data=p41)
        eq(revid1, self._storage.getSerial(oid))
        # And another one
        revid2 = self._dostore(oid, revid=revid1, data=p42)
        eq(revid2, self._storage.getSerial(oid))

    def checkTwoArgBegin(self):
        # XXX how standard is three-argument tpc_begin()?
        t = Transaction()
        tid = '\0\0\0\0\0psu'
        self._storage.tpc_begin(t, tid)
        oid = self._storage.new_oid()
        data = zodb_pickle(MinPO(8))
        self._storage.store(oid, None, data, '', t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
