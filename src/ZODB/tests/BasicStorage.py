# Run the basic tests for a storage as described in the official storage API:
#
# http://www.zope.org/Documentation/Developer/Models/ZODB/ZODB_Architecture_Storage_Interface_Info.html
#
# All storages should be able to pass these tests

from ZODB.Transaction import Transaction
from ZODB import POSException

from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle, zodb_pickle

ZERO = '\0'*8



class BasicStorage:
    def checkBasics(self):
        self._storage.tpc_begin(self._transaction)
        # This should simply return
        self._storage.tpc_begin(self._transaction)
        # Aborting is easy
        self._storage.tpc_abort(self._transaction)
        # Test a few expected exceptions when we're doing operations giving a
        # different Transaction object than the one we've begun on.
        self._storage.tpc_begin(self._transaction)
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
        self._storage.tpc_abort(self._transaction)

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

    def checkLoadSerial(self):
        oid = self._storage.new_oid()
        revid = ZERO
        revisions = {}
        for i in range(31, 38):
            revid = self._dostore(oid, revid=revid, data=MinPO(i))
            revisions[revid] = MinPO(i)
        # Now make sure all the revisions have the correct value
        for revid, value in revisions.items():
            data = self._storage.loadSerial(oid, revid)
            self.assertEqual(zodb_unpickle(data), value)
    
    def checkConflicts(self):
        oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=MinPO(11))
        revid2 = self._dostore(oid, revid=revid1, data=MinPO(12))
        self.assertRaises(POSException.ConflictError,
                          self._dostore,
                          oid, revid=revid1, data=MinPO(13))

    def checkWriteAfterAbort(self):
        oid = self._storage.new_oid()
        self._storage.tpc_begin(self._transaction)
        revid = self._storage.store(oid, ZERO, zodb_pickle(MinPO(5)),
                                    '', self._transaction)
        # Now abort this transaction
        self._storage.tpc_abort(self._transaction)
        # Now start all over again
        self._transaction = Transaction()
        oid = self._storage.new_oid()
        revid = self._dostore(oid=oid, data=MinPO(6))

    def checkAbortAfterVote(self):
        oid1 = self._storage.new_oid()
        revid1 = self._dostore(oid=oid1, data=MinPO(-2))
        oid = self._storage.new_oid()
        self._storage.tpc_begin(self._transaction)
        revid = self._storage.store(oid, ZERO, zodb_pickle(MinPO(5)),
                                    '', self._transaction)
        # Now abort this transaction
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_abort(self._transaction)
        # Now start all over again
        self._transaction = Transaction()
        oid = self._storage.new_oid()
        revid = self._dostore(oid=oid, data=MinPO(6))

        for oid, revid in [(oid1, revid1), (oid, revid)]:
            data, _revid = self._storage.load(oid, '')
            self.assertEqual(revid, _revid)
