"""Run the history() related tests for a storage.

Any storage that supports the history() method should be able to pass
all these tests.
"""

from ZODB.Transaction import Transaction
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle



class HistoryStorage:
    def checkSimpleHistory(self):
        eq = self.assertEqual
        # Store a couple of non-version revisions of the object
        oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=MinPO(11))
        revid2 = self._dostore(oid, revid=revid1, data=MinPO(12))
        revid3 = self._dostore(oid, revid=revid2, data=MinPO(13))
        # Now get various snapshots of the object's history
        h = self._storage.history(oid, size=1)
        eq(len(h), 1)
        d = h[0]
        eq(d['serial'], revid3)
        eq(d['version'], '')
        # Try to get 2 historical revisions
        h = self._storage.history(oid, size=2)
        eq(len(h), 2)
        d = h[0]
        eq(d['serial'], revid3)
        eq(d['version'], '')
        d = h[1]
        eq(d['serial'], revid2)
        eq(d['version'], '')
        # Try to get all 3 historical revisions
        h = self._storage.history(oid, size=3)
        eq(len(h), 3)
        d = h[0]
        eq(d['serial'], revid3)
        eq(d['version'], '')
        d = h[1]
        eq(d['serial'], revid2)
        eq(d['version'], '')
        d = h[2]
        eq(d['serial'], revid1)
        eq(d['version'], '')
        # There should be no more than 3 revisions
        h = self._storage.history(oid, size=4)
        eq(len(h), 3)
        d = h[0]
        eq(d['serial'], revid3)
        eq(d['version'], '')
        d = h[1]
        eq(d['serial'], revid2)
        eq(d['version'], '')
        d = h[2]
        eq(d['serial'], revid1)
        eq(d['version'], '')

    def checkVersionHistory(self):
        eq = self.assertEqual
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
        eq(len(h), 6)
        d = h[0]
        eq(d['serial'], revid6)
        eq(d['version'], version)
        d = h[1]
        eq(d['serial'], revid5)
        eq(d['version'], version)
        d = h[2]
        eq(d['serial'], revid4)
        eq(d['version'], version)
        d = h[3]
        eq(d['serial'], revid3)
        eq(d['version'], '')
        d = h[4]
        eq(d['serial'], revid2)
        eq(d['version'], '')
        d = h[5]
        eq(d['serial'], revid1)
        eq(d['version'], '')

    def checkHistoryAfterVersionCommit(self):
        eq = self.assertEqual
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
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.commitVersion(version, '', t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        # After consultation with Jim, we agreed that the semantics of
        # revision id's after a version commit is that the committed object
        # gets a new serial number (a.k.a. revision id).  Note that
        # FileStorage is broken here; the serial number in the post-commit
        # non-version revision will be the same as the serial number of the
        # previous in-version revision.
        #
        # BAW: Using load() is the only way to get the serial number of the
        # current revision of the object.  But at least this works for both
        # broken and working storages.
        ign, revid7 = self._storage.load(oid, '')
        # Now, try to get the six historical revisions (first three are in
        # 'test-version', followed by the non-version revisions).
        h = self._storage.history(oid, version, 100)
        eq(len(h), 7)
        d = h[0]
        eq(d['serial'], revid7)
        eq(d['version'], '')
        d = h[1]
        eq(d['serial'], revid6)
        eq(d['version'], version)
        d = h[2]
        eq(d['serial'], revid5)
        eq(d['version'], version)
        d = h[3]
        eq(d['serial'], revid4)
        eq(d['version'], version)
        d = h[4]
        eq(d['serial'], revid3)
        eq(d['version'], '')
        d = h[5]
        eq(d['serial'], revid2)
        eq(d['version'], '')
        d = h[6]
        eq(d['serial'], revid1)
        eq(d['version'], '')

    def checkHistoryAfterVersionAbort(self):
        eq = self.assertEqual
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
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.abortVersion(version, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        # After consultation with Jim, we agreed that the semantics of
        # revision id's after a version commit is that the committed object
        # gets a new serial number (a.k.a. revision id).  Note that
        # FileStorage is broken here; the serial number in the post-commit
        # non-version revision will be the same as the serial number of the
        # previous in-version revision.
        #
        # BAW: Using load() is the only way to get the serial number of the
        # current revision of the object.  But at least this works for both
        # broken and working storages.
        ign, revid7 = self._storage.load(oid, '')
        # Now, try to get the six historical revisions (first three are in
        # 'test-version', followed by the non-version revisions).
        h = self._storage.history(oid, version, 100)
        eq(len(h), 7)
        d = h[0]
        eq(d['serial'], revid7)
        eq(d['version'], '')
        d = h[1]
        eq(d['serial'], revid6)
        eq(d['version'], version)
        d = h[2]
        eq(d['serial'], revid5)
        eq(d['version'], version)
        d = h[3]
        eq(d['serial'], revid4)
        eq(d['version'], version)
        d = h[4]
        eq(d['serial'], revid3)
        eq(d['version'], '')
        d = h[5]
        eq(d['serial'], revid2)
        eq(d['version'], '')
        d = h[6]
        eq(d['serial'], revid1)
        eq(d['version'], '')
