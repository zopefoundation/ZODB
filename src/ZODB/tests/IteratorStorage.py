# Run tests against the iterator() interface for storages.  Any storage that
# supports the iterator() method should be able to pass all these tests.

from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle



class IteratorStorage:
    def checkSimpleIteration(self):
        eq = self.assertEqual
        # Store a bunch of revisions of a single object
        oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=MinPO(11))
        revid2 = self._dostore(oid, revid=revid1, data=MinPO(12))
        revid3 = self._dostore(oid, revid=revid2, data=MinPO(13))
        # Now iterate over all the transactions
        val = 11
        txniter = self._storage.iterator()
        for reciter, revid in zip(txniter, (revid1, revid2, revid3)):
            eq(reciter.tid, revid)
            for rec in reciter:
                eq(rec.oid, oid)
                eq(rec.serial, revid)
                eq(rec.version, '')
                eq(zodb_unpickle(rec.data), MinPO(val))
                val = val + 1
