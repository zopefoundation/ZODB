# This class must be one of the mixin base class for your storage test.  It
# provides basic setUp() and tearDown() semantics (which you can override),
# and it also provides a helper method _dostore() which performs a complete
# store transaction for a single object revision.

import pickle
import unittest
from ZODB.Transaction import Transaction

ZERO = '\0'*8



class StorageTestBase(unittest.TestCase):
    def setUp(self):
        # You need to override this with a setUp that creates self._storage
        self._transaction = Transaction()

    def _close(self):
        # You should override this if closing your storage requires additional
        # shutdown operations.
        self._transaction.abort()
        self._storage.close()

    def tearDown(self):
        self._close()

    def _dostore(self, oid=None, revid=None, data=None, version=None):
        # Do a complete storage transaction.  The defaults are:
        # - oid=None, ask the storage for a new oid
        # - revid=None, use a revid of ZERO
        # - data=None, pickle up some arbitrary data (the integer 7)
        # - version=None, use the empty string version
        #
        # Returns the object's new revision id
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
        
