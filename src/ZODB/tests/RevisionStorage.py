"""Check loadSerial() on storages that support historical revisions."""

from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle, zodb_pickle

ZERO = '\0'*8

class RevisionStorage:
    
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
    
