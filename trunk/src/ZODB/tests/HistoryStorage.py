# Run the history() related tests for a storage.  Any storage that supports
# the history() method should be able to pass all these tests.

from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle



class HistoryStorage:
    def checkSimpleHistory(self):
        # Store a couple of non-version revisions of the object
        oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=MinPO(11))
        revid2 = self._dostore(oid, revid=revid1, data=MinPO(12))
        revid3 = self._dostore(oid, revid=revid2, data=MinPO(13))
        # Now get various snapshots of the object's history
        h = self._storage.history(oid, size=1)
        assert len(h) == 1
        d = h[0]
        assert d['serial'] == revid3 and d['version'] == ''
        # Try to get 2 historical revisions
        h = self._storage.history(oid, size=2)
        assert len(h) == 2
        d = h[0]
        assert d['serial'] == revid3 and d['version'] == ''
        d = h[1]
        assert d['serial'] == revid2 and d['version'] == ''
        # Try to get all 3 historical revisions
        h = self._storage.history(oid, size=3)
        assert len(h) == 3
        d = h[0]
        assert d['serial'] == revid3 and d['version'] == ''
        d = h[1]
        assert d['serial'] == revid2 and d['version'] == ''
        d = h[2]
        assert d['serial'] == revid1 and d['version'] == ''
        # There should be no more than 3 revisions
        h = self._storage.history(oid, size=4)
        assert len(h) == 3
        d = h[0]
        assert d['serial'] == revid3 and d['version'] == ''
        d = h[1]
        assert d['serial'] == revid2 and d['version'] == ''
        d = h[2]
        assert d['serial'] == revid1 and d['version'] == ''

    def checkVersionHistory(self):
        # Store a couple of non-version revisions
        oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=MinPO(11))
        revid2 = self._dostore(oid, revid=revid1, data=MinPO(12))
        revid3 = self._dostore(oid, revid=revid2, data=MinPO(13))
        # Now store some new revisions in a version
        version = 'test-version'
        revid4 = self._dostore(oid, revid=revid3, data=MinPO(14),
                               version=version)
        revid5 = self._dostore(oid, revid=revid4, data=MinPO(15),
                               version=version)
        revid6 = self._dostore(oid, revid=revid5, data=MinPO(16),
                               version=version)
        # Now, try to get the six historical revisions (first three are in
        # 'test-version', followed by the non-version revisions).
        h = self._storage.history(oid, version, 100)
        assert len(h) == 6
        d = h[0]
        assert d['serial'] == revid6 and d['version'] == version
        d = h[1]
        assert d['serial'] == revid5 and d['version'] == version
        d = h[2]
        assert d['serial'] == revid4 and d['version'] == version
        d = h[3]
        assert d['serial'] == revid3 and d['version'] == ''
        d = h[4]
        assert d['serial'] == revid2 and d['version'] == ''
        d = h[5]
        assert d['serial'] == revid1 and d['version'] == ''

    def checkHistoryAfterVersionCommit(self):
        # Store a couple of non-version revisions
        oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=MinPO(11))
        revid2 = self._dostore(oid, revid=revid1, data=MinPO(12))
        revid3 = self._dostore(oid, revid=revid2, data=MinPO(13))
        # Now store some new revisions in a version
        version = 'test-version'
        revid4 = self._dostore(oid, revid=revid3, data=MinPO(14),
                               version=version)
        revid5 = self._dostore(oid, revid=revid4, data=MinPO(15),
                               version=version)
        revid6 = self._dostore(oid, revid=revid5, data=MinPO(16),
                               version=version)
        # Now commit the version
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.commitVersion(version, '', self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        # It is not guaranteed that the revision id (a.k.a. serial number) for
        # the version-committed object is the same as the last in-version
        # modification.  We need to suck that out of the API a different way,
        # just to be sure.
        ign, revid7 = self._storage.load(oid, '')
        # Now, try to get the six historical revisions (first three are in
        # 'test-version', followed by the non-version revisions).
        h = self._storage.history(oid, version, 100)
        assert len(h) == 7
        d = h[0]
        assert d['serial'] == revid7 and d['version'] == ''
        d = h[1]
        assert d['serial'] == revid6 and d['version'] == version
        d = h[2]
        assert d['serial'] == revid5 and d['version'] == version
        d = h[3]
        assert d['serial'] == revid4 and d['version'] == version
        d = h[4]
        assert d['serial'] == revid3 and d['version'] == ''
        d = h[5]
        assert d['serial'] == revid2 and d['version'] == ''
        d = h[6]
        assert d['serial'] == revid1 and d['version'] == ''

    def checkHistoryAfterVersionAbort(self):
        # Store a couple of non-version revisions
        oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=MinPO(11))
        revid2 = self._dostore(oid, revid=revid1, data=MinPO(12))
        revid3 = self._dostore(oid, revid=revid2, data=MinPO(13))
        # Now store some new revisions in a version
        version = 'test-version'
        revid4 = self._dostore(oid, revid=revid3, data=MinPO(14),
                               version=version)
        revid5 = self._dostore(oid, revid=revid4, data=MinPO(15),
                               version=version)
        revid6 = self._dostore(oid, revid=revid5, data=MinPO(16),
                               version=version)
        # Now commit the version
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.abortVersion(version, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        # It is not guaranteed that the revision id (a.k.a. serial number) for
        # the version-committed object is the same as the last in-version
        # modification.  We need to suck that out of the API a different way,
        # just to be sure.
        ign, revid7 = self._storage.load(oid, '')
        # Now, try to get the six historical revisions (first three are in
        # 'test-version', followed by the non-version revisions).
        h = self._storage.history(oid, version, 100)
        assert len(h) == 7
        d = h[0]
        assert d['serial'] == revid7 and d['version'] == ''
        d = h[1]
        assert d['serial'] == revid6 and d['version'] == version
        d = h[2]
        assert d['serial'] == revid5 and d['version'] == version
        d = h[3]
        assert d['serial'] == revid4 and d['version'] == version
        d = h[4]
        assert d['serial'] == revid3 and d['version'] == ''
        d = h[5]
        assert d['serial'] == revid2 and d['version'] == ''
        d = h[6]
        assert d['serial'] == revid1 and d['version'] == ''
