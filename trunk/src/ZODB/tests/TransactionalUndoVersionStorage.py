# Check interactions between transactionalUndo() and versions.  Any storage
# that supports both transactionalUndo() and versions must pass these tests.

from ZODB import POSException
from ZODB.Transaction import Transaction
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle


class TransactionalUndoVersionStorage:
    def checkUndoInVersion(self):
        oid = self._storage.new_oid()
        version = 'one'
        revid_a = self._dostore(oid, data=MinPO(91))
        revid_b = self._dostore(oid, revid=revid_a, data=MinPO(92),
                                version=version)
        revid_c = self._dostore(oid, revid=revid_b, data=MinPO(93),
                                version=version)
        info=self._storage.undoInfo()
        tid=info[0]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        assert len(oids) == 1
        assert oids[0] == oid
        data, revid = self._storage.load(oid, '')
        assert revid == revid_a
        assert zodb_unpickle(data) == MinPO(91)
        data, revid = self._storage.load(oid, version)
        assert revid > revid_b and revid > revid_c
        assert zodb_unpickle(data) == MinPO(92)
        # Now commit the version...
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.commitVersion(version, '', t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        assert len(oids) == 1
        assert oids[0] == oid

        #JF# No, because we fall back to non-version data.
        #JF# self.assertRaises(POSException.VersionError,
        #JF#                   self._storage.load,
        #JF#                   oid, version)
        data, revid = self._storage.load(oid, version)
        assert zodb_unpickle(data) == MinPO(92)
        data, revid = self._storage.load(oid, '')
        assert zodb_unpickle(data) == MinPO(92)
        # ...and undo the commit
        info=self._storage.undoInfo()
        tid=info[0]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        assert len(oids) == 1
        assert oids[0] == oid
        data, revid = self._storage.load(oid, version)
        assert zodb_unpickle(data) == MinPO(92)
        data, revid = self._storage.load(oid, '')
        assert zodb_unpickle(data) == MinPO(91)
        # Now abort the version
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.abortVersion(version, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        assert len(oids) == 1
        assert oids[0] == oid
        # The object should not exist in the version now, but it should exist
        # in the non-version
        #JF# No, because we fall back
        #JF# self.assertRaises(POSException.VersionError,
        #JF#                   self._storage.load,
        #JF#                   oid, version)
        data, revid = self._storage.load(oid, version)
        assert zodb_unpickle(data) == MinPO(91)
        data, revid = self._storage.load(oid, '')
        assert zodb_unpickle(data) == MinPO(91)
        # Now undo the abort
        info=self._storage.undoInfo()
        tid=info[0]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        assert len(oids) == 1
        assert oids[0] == oid
        # And the object should be back in versions 'one' and ''
        data, revid = self._storage.load(oid, version)
        assert zodb_unpickle(data) == MinPO(92)
        data, revid = self._storage.load(oid, '')
        assert zodb_unpickle(data) == MinPO(91)
