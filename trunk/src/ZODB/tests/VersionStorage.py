# Run the version related tests for a storage.  Any storage that supports
# versions should be able to pass all these tests.

import pickle
from ZODB import POSException



class VersionStorage:
    def checkVersionedStoreAndLoad(self):
        # Store a couple of non-version revisions of the object
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=11)
        revid = self._dostore(oid, revid=revid, data=12)
        # And now store some new revisions in a version
        version = 'test-version'
        revid = self._dostore(oid, revid=revid, data=13, version=version)
        revid = self._dostore(oid, revid=revid, data=14, version=version)
        revid = self._dostore(oid, revid=revid, data=15, version=version)
        # Now read back the object in both the non-version and version and
        # make sure the values jive.
        data, revid = self._storage.load(oid, '')
        assert pickle.loads(data) == 12
        data, revid = self._storage.load(oid, version)
        assert pickle.loads(data) == 15

    def checkVersionedLoadErrors(self):
        oid = self._storage.new_oid()
        version = 'test-version'
        revid = self._dostore(oid, data=11)
        revid = self._dostore(oid, revid=revid, data=12, version=version)
        # Try to load a bogus oid
        self.assertRaises(KeyError,
                          self._storage.load,
                          self._storage.new_oid(), '')
        # Try to load a bogus version string
        #JF# Nope, fall back to non-version
        #JF# self.assertRaises(KeyError,
        #JF#                   self._storage.load,
        #JF#                   oid, 'bogus')
        data, revid = self._storage.load(oid, 'bogus')
        assert pickle.loads(data) == 11


    def checkVersionLock(self):
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=11)
        version = 'test-version'
        revid = self._dostore(oid, revid=revid, data=12, version=version)
        self.assertRaises(POSException.VersionLockError,
                          self._dostore,
                          oid, revid=revid, data=14,
                          version='another-version')

    def checkVersionEmpty(self):
        # Before we store anything, these versions ought to be empty
        version = 'test-version'
        #JF# The empty string is not a valid version. I think that this should
        #JF# be an error. Let's punt for now.
        #JF# assert self._storage.versionEmpty('')
        assert self._storage.versionEmpty(version)
        # Now store some objects
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=11)
        revid = self._dostore(oid, revid=revid, data=12)
        revid = self._dostore(oid, revid=revid, data=13, version=version)
        revid = self._dostore(oid, revid=revid, data=14, version=version)
        # The blank version should not be empty
        #JF# The empty string is not a valid version. I think that this should
        #JF# be an error. Let's punt for now.
        #JF# assert not self._storage.versionEmpty('')

        # Neither should 'test-version'
        assert not self._storage.versionEmpty(version)
        # But this non-existant version should be empty
        assert self._storage.versionEmpty('bogus')

    def checkVersions(self):
        # Store some objects in the non-version
        oid1 = self._storage.new_oid()
        oid2 = self._storage.new_oid()
        oid3 = self._storage.new_oid()
        revid1 = self._dostore(oid1, data=11)
        revid2 = self._dostore(oid2, data=12)
        revid3 = self._dostore(oid3, data=13)
        # Now create some new versions
        revid1 = self._dostore(oid1, revid=revid1, data=14, version='one')
        revid2 = self._dostore(oid2, revid=revid2, data=15, version='two')
        revid3 = self._dostore(oid3, revid=revid3, data=16, version='three')
        # Ask for the versions
        versions = self._storage.versions()
        assert 'one' in versions
        assert 'two' in versions
        assert 'three' in versions
        # Now flex the `max' argument
        versions = self._storage.versions(1)
        assert len(versions) == 1
        assert 'one' in versions or 'two' in versions or 'three' in versions

    def _setup_version(self, version='test-version'):
        # Store some revisions in the non-version
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=49)
        revid = self._dostore(oid, revid=revid, data=50)
        nvrevid = revid = self._dostore(oid, revid=revid, data=51)
        # Now do some stores in a version
        revid = self._dostore(oid, revid=revid, data=52, version=version)
        revid = self._dostore(oid, revid=revid, data=53, version=version)
        revid = self._dostore(oid, revid=revid, data=54, version=version)
        return oid, version

    def checkAbortVersion(self):
        oid, version = self._setup_version()
        # Now abort the version -- must be done in a transaction
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.abortVersion(version, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        assert len(oids) == 1
        assert oids[0] == oid
        data, revid = self._storage.load(oid, '')
        assert pickle.loads(data) == 51

    def checkAbortVersionErrors(self):
        oid, version = self._setup_version()
        # Now abort a bogus version
        self._storage.tpc_begin(self._transaction)

        #JF# The spec is silent on what happens if you abort or commit
        #JF# a non-existent version. FileStorage consideres this a noop.
        #JF# We can change the spec, but until we do ....
        #JF# self.assertRaises(POSException.VersionError,
        #JF#                   self._storage.abortVersion,
        #JF#                   'bogus', self._transaction)

        # And try to abort the empty version
        self.assertRaises(POSException.VersionError,
                          self._storage.abortVersion,
                          '', self._transaction)
        # But now we really try to abort the version
        oids = self._storage.abortVersion(version, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        assert len(oids) == 1
        assert oids[0] == oid
        data, revid = self._storage.load(oid, '')
        assert pickle.loads(data) == 51

    def checkModifyAfterAbortVersion(self):
        oid, version = self._setup_version()
        # Now abort the version
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.abortVersion(version, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        # Load the object's current state (which gets us the revid)
        data, revid = self._storage.load(oid, '')
        # And modify it a few times
        revid = self._dostore(oid, revid=revid, data=52)
        revid = self._dostore(oid, revid=revid, data=53)
        revid = self._dostore(oid, revid=revid, data=54)
        data, newrevid = self._storage.load(oid, '')
        assert newrevid == revid
        assert pickle.loads(data) == 54

    def checkCommitToNonVersion(self):
        oid, version = self._setup_version()
        data, revid = self._storage.load(oid, version)
        assert pickle.loads(data) == 54
        data, revid = self._storage.load(oid, '')
        assert pickle.loads(data) == 51
        # Try committing this version to the empty version
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.commitVersion(version, '', self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        data, revid = self._storage.load(oid, '')
        assert pickle.loads(data) == 54

    def checkCommitToOtherVersion(self):
        oid1, version1 = self._setup_version('one')
        data, revid1 = self._storage.load(oid1, version1)
        assert pickle.loads(data) == 54
        oid2, version2 = self._setup_version('two')
        data, revid2 = self._storage.load(oid2, version2)
        assert pickle.loads(data) == 54
        # Let's make sure we can't get object1 in version2
        #JF# This won't fail because we fall back to non-version data.
        #JF# In fact, it must succed and give us 51
        #JF# self.assertRaises(POSException.VersionError,
        #JF#                   self._storage.load, oid1, version2)
        data, revid2 = self._storage.load(oid1, version2)
        assert pickle.loads(data) == 51
        
        # Okay, now let's commit object1 to version2
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.commitVersion(version1, version2,
                                           self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        assert len(oids) == 1
        assert oids[0] == oid1
        data, revid = self._storage.load(oid1, version2)
        assert pickle.loads(data) == 54
        data, revid = self._storage.load(oid2, version2)
        assert pickle.loads(data) == 54
        #JF# Ditto, sort of
        #JF# self.assertRaises(POSException.VersionError,
        #JF#                   self._storage.load, oid1, version1)
        data, revid2 = self._storage.load(oid1, version1)
        assert pickle.loads(data) == 51

    def checkAbortOneVersionCommitTheOther(self):
        oid1, version1 = self._setup_version('one')
        data, revid1 = self._storage.load(oid1, version1)
        assert pickle.loads(data) == 54
        oid2, version2 = self._setup_version('two')
        data, revid2 = self._storage.load(oid2, version2)
        assert pickle.loads(data) == 54
        # Let's make sure we can't get object1 in version2

        #JF# It's not an error to load data in a different version when data
        #JF# are stored in non-version. See above
        #JF#
        #JF# self.assertRaises(POSException.VersionError,
        #JF#                   self._storage.load, oid1, version2)
        data, revid2 = self._storage.load(oid1, version2)
        assert pickle.loads(data) == 51
        
        # First, let's abort version1
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.abortVersion(version1, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        assert len(oids) == 1
        assert oids[0] == oid1
        data, revid = self._storage.load(oid1, '')
        assert pickle.loads(data) == 51

        #JF# Ditto
        #JF# self.assertRaises(POSException.VersionError,
        #JF#                   self._storage.load, oid1, version1)
        data, revid = self._storage.load(oid1, '')
        assert pickle.loads(data) == 51
        #JF# self.assertRaises(POSException.VersionError,
        #JF#                   self._storage.load, oid1, version2)
        data, revid = self._storage.load(oid1, '')
        assert pickle.loads(data) == 51

        data, revid = self._storage.load(oid2, '')
        assert pickle.loads(data) == 51
        data, revid = self._storage.load(oid2, version2)
        assert pickle.loads(data) == 54
        # Okay, now let's commit version2 back to the trunk
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.commitVersion(version2, '', self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        assert len(oids) == 1
        assert oids[0] == oid2
        # These objects should not be found in version 2
        #JF# Ditto
        #JF# self.assertRaises(POSException.VersionError,
        #JF#                   self._storage.load, oid1, version2)
        data, revid = self._storage.load(oid1, '')
        assert pickle.loads(data) == 51
        #JF# self.assertRaises(POSException.VersionError,
        #JF#                   self._storage.load, oid2, version2)
        # But the trunk should be up to date now
        data, revid = self._storage.load(oid2, version2)
        assert pickle.loads(data) == 54
        data, revid = self._storage.load(oid2, '')
        assert pickle.loads(data) == 54


        #JF# To do a test like you want, you have to add the data in a version
        oid = self._storage.new_oid()
        revid = self._dostore(oid, revid=revid, data=54, version='one')
        self.assertRaises(KeyError,
                          self._storage.load, oid, '')
        self.assertRaises(KeyError,
                          self._storage.load, oid, 'two')
