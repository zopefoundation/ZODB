import UndoStorage, VersionStorage

class UndoVersionStorage(UndoStorage.UndoStorage,
                         VersionStorage.VersionStorage):

    
    def checkUndoInVersion(self):
        oid = self._storage.new_oid()
        version = 'one'
        revid_a = self._dostore(oid, data=91)
        revid_b = self._dostore(oid, revid=revid_a, data=92, version=version)
        revid_c = self._dostore(oid, revid=revid_b, data=93, version=version)
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.transactionalUndo(revid_c, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        assert len(oids) == 1
        assert oids[0] == oid
        data, revid = self._storage.load(oid, '')
        assert revid == revid_a
        assert pickle.loads(data) == 91
        data, revid = self._storage.load(oid, version)
        assert revid > revid_b and revid > revid_c
        assert pickle.loads(data) == 92
        # Now commit the version...
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.commitVersion(version, '', self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        assert len(oids) == 1
        assert oids[0] == oid
        self.assertRaises(POSException.VersionError,
                          self._storage.load,
                          oid, version)
        data, revid = self._storage.load(oid, '')
        assert pickle.loads(data) == 92
        # ...and undo the commit
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.transactionalUndo(revid, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        assert len(oids) == 1
        assert oids[0] == oid
        data, revid = self._storage.load(oid, version)
        assert pickle.loads(data) == 92
        data, revid = self._storage.load(oid, '')
        assert pickle.loads(data) == 91
        # Now abort the version
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.abortVersion(version, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        assert len(oids) == 1
        assert oids[0] == oid
        # The object should not exist in the version now, but it should exist
        # in the non-version
        self.assertRaises(POSException.VersionError,
                          self._storage.load,
                          oid, version)
        data, revid = self._storage.load(oid, '')
        assert pickle.loads(data) == 91
        # Now undo the abort
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.transactionalUndo(revid, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        assert len(oids) == 1
        assert oids[0] == oid
        # And the object should be back in versions 'one' and ''
        data, revid = self._storage.load(oid, version)
        assert pickle.loads(data) == 92
        data, revid = self._storage.load(oid, '')
        assert pickle.loads(data) == 91
