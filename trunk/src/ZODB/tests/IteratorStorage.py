"""Run tests against the iterator() interface for storages.

Any storage that supports the iterator() method should be able to pass
all these tests.
"""

from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle
from ZODB.utils import U64, p64
from ZODB.Transaction import Transaction


class IteratorCompare:

    def iter_verify(self, txniter, revids, val0):
        eq = self.assertEqual
        oid = self._oid
        val = val0
        for reciter, revid in zip(txniter, revids + [None]):
            eq(reciter.tid, revid)
            for rec in reciter:
                eq(rec.oid, oid)
                eq(rec.serial, revid)
                eq(rec.version, '')
                eq(zodb_unpickle(rec.data), MinPO(val))
                val = val + 1
        eq(val, val0 + len(revids))

class IteratorStorage(IteratorCompare):

    def checkSimpleIteration(self):
        # Store a bunch of revisions of a single object
        self._oid = oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=MinPO(11))
        revid2 = self._dostore(oid, revid=revid1, data=MinPO(12))
        revid3 = self._dostore(oid, revid=revid2, data=MinPO(13))
        # Now iterate over all the transactions and compare carefully
        txniter = self._storage.iterator()
        self.iter_verify(txniter, [revid1, revid2, revid3], 11)

    def checkVersionIterator(self):
        if not self._storage.supportsVersions():
            return
        self._dostore()
        self._dostore(version='abort')
        self._dostore()
        self._dostore(version='abort')
        t = Transaction()
        self._storage.tpc_begin(t)
        self._storage.abortVersion('abort', t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)

        self._dostore(version='commit')
        self._dostore()
        self._dostore(version='commit')
        t = Transaction()
        self._storage.tpc_begin(t)
        self._storage.commitVersion('commit', '', t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)

        # XXX extend these checks.  right now, just iterating with CVS
        # FS or Berkeley will fail here, but once fixed we should
        # check that the right data is returned.
        txniter = self._storage.iterator()
        for trans in txniter:
            for data in trans:
                pass

    def checkTransactionalUndoIterator(self):
        if not hasattr(self._storage, 'supportsTransactionalUndo'):
            return
        if not self._storage.supportsTransactionalUndo():
            return

        oid = self._storage.new_oid()
        revid = self._dostore(oid)
        self._dostore(oid, revid)

        self.undoLastTrans()
        self.undoLastTrans()

        # XXX extend these checks.  right now, just iterating with CVS
        # FS or Berkeley will fail here, but once fixed we should
        # check that the right data is returned.
        txniter = self._storage.iterator()
        for trans in txniter:
            for data in trans:
                pass

    def undoLastTrans(self):
        info = self._storage.undoInfo()
        tid = info[0]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        

class ExtendedIteratorStorage(IteratorCompare):

    def checkExtendedIteration(self):
        # Store a bunch of revisions of a single object
        self._oid = oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=MinPO(11))
        revid2 = self._dostore(oid, revid=revid1, data=MinPO(12))
        revid3 = self._dostore(oid, revid=revid2, data=MinPO(13))
        revid4 = self._dostore(oid, revid=revid3, data=MinPO(14))
        # Note that the end points are included
        # Iterate over all of the transactions with explicit start/stop
        txniter = self._storage.iterator(revid1, revid4)
        self.iter_verify(txniter, [revid1, revid2, revid3, revid4], 11)
        # Iterate over some of the transactions with explicit start
        txniter = self._storage.iterator(revid3)
        self.iter_verify(txniter, [revid3, revid4], 13)
        # Iterate over some of the transactions with explicit stop
        txniter = self._storage.iterator(None, revid2)
        self.iter_verify(txniter, [revid1, revid2], 11)
        # Iterate over some of the transactions with explicit start+stop
        txniter = self._storage.iterator(revid2, revid3)
        self.iter_verify(txniter, [revid2, revid3], 12)
        # Specify an upper bound somewhere in between values
        revid3a = p64((U64(revid3) + U64(revid4)) / 2)
        txniter = self._storage.iterator(revid2, revid3a)
        self.iter_verify(txniter, [revid2, revid3], 12)
        # Specify a lower bound somewhere in between values
        revid1a = p64((U64(revid1) + U64(revid2)) / 2)
        txniter = self._storage.iterator(revid1a, revid3a)
        self.iter_verify(txniter, [revid2, revid3], 12)
        # Specify an empty range
        txniter = self._storage.iterator(revid3, revid2)
        self.iter_verify(txniter, [], 13)
        # Specify a singleton range
        txniter = self._storage.iterator(revid3, revid3)
        self.iter_verify(txniter, [revid3], 13)
        
