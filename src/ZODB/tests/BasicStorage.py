from ZODB.Transaction import Transaction
ZERO = '\0'*8
import pickle
from ZODB import POSException

class BasicStorage:

    def setUp(self):
        # You need to override this with a setUp that creates self._storage
        self._transaction = Transaction()

    def _close(self):
        self._transaction.abort()
        self._storage.close()

    def tearDown(self):
        self._close()

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
        self.assertRaises(
            POSException.StorageTransactionError,
            self._storage.abortVersion,
            0, Transaction())
        self.assertRaises(
            POSException.StorageTransactionError,
            self._storage.commitVersion,
            0, 1, Transaction())
        self.assertRaises(
            POSException.StorageTransactionError,
            self._storage.store,
            0, 1, 2, 3, Transaction())
        self._storage.tpc_abort(self._transaction)

    def _dostore(self, oid=None, revid=None, data=None, version=None):
        # Defaults
        if oid is None:
            oid = self._storage.new_oid()
        if revid is None:
            revid = ZERO
        if data is None:
            data = pickle.dumps(7)
        else:
            data = pickle.dumps(data)
        if version is None:
            version = ''
        # Begin the transaction
        self._storage.tpc_begin(self._transaction)
        # Store an object
        newrevid = self._storage.store(oid, revid, data, version,
                                       self._transaction)
        # Finish the transaction
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        return newrevid
        
    def checkNonVersionStore(self, oid=None, revid=None, version=None):
        revid = ZERO
        newrevid = self._dostore(revid=revid)
        # Finish the transaction.
        assert newrevid <> revid

    def checkLen(self):
        # The length of the database ought to grow by one each time
        assert len(self._storage) == 0
        self._dostore()
        assert len(self._storage) == 1
        self._dostore()
        assert len(self._storage) == 2

    def checkNonVersionStoreAndLoad(self):
        oid = self._storage.new_oid()
        self._dostore(oid=oid, data=7)
        data, revid = self._storage.load(oid, '')
        value = pickle.loads(data)
        assert value == 7
        # Now do a bunch of updates to an object
        for i in range(13, 22):
            revid = self._dostore(oid, revid=revid, data=i)
        # Now get the latest revision of the object
        data, revid = self._storage.load(oid, '')
        assert pickle.loads(data) == 21

    def checkNonVersionModifiedInVersion(self):
        oid = self._storage.new_oid()
        self._dostore(oid=oid)
        assert self._storage.modifiedInVersion(oid) == ''

    def checkLoadSerial(self):
        oid = self._storage.new_oid()
        revid = ZERO
        revisions = {}
        for i in range(31, 38):
            revid = self._dostore(oid, revid=revid, data=i)
            revisions[revid] = i
        # Now make sure all the revisions have the correct value
        for revid, value in revisions.items():
            data = self._storage.loadSerial(oid, revid)
            assert pickle.loads(data) == value
    

    def checkConflicts(self):
        oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=11)
        revid2 = self._dostore(oid, revid=revid1, data=12)
        self.assertRaises(POSException.ConflictError,
                          self._dostore,
                          oid, revid=revid1, data=13)
