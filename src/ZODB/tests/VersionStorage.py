"""Run the version related tests for a storage.

Any storage that supports versions should be able to pass all these tests.
"""

# XXX we should clean this code up to get rid of the #JF# comments.
# They were introduced when Jim reviewed the original version of the
# code.  Barry and Jeremy didn't understand versions then.

from ZODB import POSException
from ZODB.Transaction import Transaction
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle

class VersionStorage:

    def _commitVersion(self, src, dst):
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.commitVersion(src, dst, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        return oids

    def _abortVersion(self, ver):
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.abortVersion(ver, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        return oids

    def checkCommitVersionSerialno(self):
        oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=MinPO(12))
        revid2 = self._dostore(oid, revid=revid1, data=MinPO(13),
                               version="version")
        oids = self._commitVersion("version", "")
        self.assertEqual([oid], oids)
        data, revid3 = self._storage.load(oid, "")
        # use repr() to avoid getting binary data in a traceback on error
        self.assertNotEqual(`revid1`, `revid3`)
        self.assertNotEqual(`revid2`, `revid3`)

    def checkAbortVersionSerialno(self):
        oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=MinPO(12))
        revid2 = self._dostore(oid, revid=revid1, data=MinPO(13),
                               version="version")
        oids = self._abortVersion("version")
        self.assertEqual([oid], oids)
        data, revid3 = self._storage.load(oid, "")
        # use repr() to avoid getting binary data in a traceback on error
        self.assertEqual(`revid1`, `revid3`)
        self.assertNotEqual(`revid2`, `revid3`)
    
    def checkVersionedStoreAndLoad(self):
        eq = self.assertEqual
        # Store a couple of non-version revisions of the object
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=MinPO(11))
        revid = self._dostore(oid, revid=revid, data=MinPO(12))
        # And now store some new revisions in a version
        version = 'test-version'
        revid = self._dostore(oid, revid=revid, data=MinPO(13),
                              version=version)
        revid = self._dostore(oid, revid=revid, data=MinPO(14),
                              version=version)
        revid = self._dostore(oid, revid=revid, data=MinPO(15),
                              version=version)
        # Now read back the object in both the non-version and version and
        # make sure the values jive.
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(12))
        data, vrevid = self._storage.load(oid, version)
        eq(zodb_unpickle(data), MinPO(15))
        if hasattr(self._storage, 'getSerial'):
            s = self._storage.getSerial(oid)
            eq(s, max(revid, vrevid))

    def checkVersionedLoadErrors(self):
        oid = self._storage.new_oid()
        version = 'test-version'
        revid = self._dostore(oid, data=MinPO(11))
        revid = self._dostore(oid, revid=revid, data=MinPO(12),
                              version=version)
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
        self.assertEqual(zodb_unpickle(data), MinPO(11))


    def checkVersionLock(self):
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=MinPO(11))
        version = 'test-version'
        revid = self._dostore(oid, revid=revid, data=MinPO(12),
                              version=version)
        self.assertRaises(POSException.VersionLockError,
                          self._dostore,
                          oid, revid=revid, data=MinPO(14),
                          version='another-version')

    def checkVersionEmpty(self):
        # Before we store anything, these versions ought to be empty
        version = 'test-version'
        #JF# The empty string is not a valid version. I think that this should
        #JF# be an error. Let's punt for now.
        #JF# assert self._storage.versionEmpty('')
        self.failUnless(self._storage.versionEmpty(version))
        # Now store some objects
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=MinPO(11))
        revid = self._dostore(oid, revid=revid, data=MinPO(12))
        revid = self._dostore(oid, revid=revid, data=MinPO(13),
                              version=version)
        revid = self._dostore(oid, revid=revid, data=MinPO(14),
                              version=version)
        # The blank version should not be empty
        #JF# The empty string is not a valid version. I think that this should
        #JF# be an error. Let's punt for now.
        #JF# assert not self._storage.versionEmpty('')

        # Neither should 'test-version'
        self.failUnless(not self._storage.versionEmpty(version))
        # But this non-existant version should be empty
        self.failUnless(self._storage.versionEmpty('bogus'))

    def checkVersions(self):
        unless = self.failUnless
        # Store some objects in the non-version
        oid1 = self._storage.new_oid()
        oid2 = self._storage.new_oid()
        oid3 = self._storage.new_oid()
        revid1 = self._dostore(oid1, data=MinPO(11))
        revid2 = self._dostore(oid2, data=MinPO(12))
        revid3 = self._dostore(oid3, data=MinPO(13))
        # Now create some new versions
        revid1 = self._dostore(oid1, revid=revid1, data=MinPO(14),
                               version='one')
        revid2 = self._dostore(oid2, revid=revid2, data=MinPO(15),
                               version='two')
        revid3 = self._dostore(oid3, revid=revid3, data=MinPO(16),
                               version='three')
        # Ask for the versions
        versions = self._storage.versions()
        unless('one' in versions)
        unless('two' in versions)
        unless('three' in versions)
        # Now flex the `max' argument
        versions = self._storage.versions(1)
        self.assertEqual(len(versions), 1)
        unless('one' in versions or 'two' in versions or 'three' in versions)

    def _setup_version(self, version='test-version'):
        # Store some revisions in the non-version
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=MinPO(49))
        revid = self._dostore(oid, revid=revid, data=MinPO(50))
        nvrevid = revid = self._dostore(oid, revid=revid, data=MinPO(51))
        # Now do some stores in a version
        revid = self._dostore(oid, revid=revid, data=MinPO(52),
                              version=version)
        revid = self._dostore(oid, revid=revid, data=MinPO(53),
                              version=version)
        revid = self._dostore(oid, revid=revid, data=MinPO(54),
                              version=version)
        return oid, version

    def checkAbortVersion(self):
        eq = self.assertEqual
        oid, version = self._setup_version()

        # XXX Not sure I can write a test for getSerial() in the
        # presence of aborted versions, because FileStorage and
        # Berkeley storage give a different answer. I think Berkeley
        # is right and FS is wrong.

        oids = self._abortVersion(version)
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(51))

    def checkAbortVersionErrors(self):
        eq = self.assertEqual
        oid, version = self._setup_version()
        # Now abort a bogus version
        t = Transaction()
        self._storage.tpc_begin(t)

        #JF# The spec is silent on what happens if you abort or commit
        #JF# a non-existent version. FileStorage consideres this a noop.
        #JF# We can change the spec, but until we do ....
        #JF# self.assertRaises(POSException.VersionError,
        #JF#                   self._storage.abortVersion,
        #JF#                   'bogus', t)

        # And try to abort the empty version
        if (hasattr(self._storage, 'supportsTransactionalUndo')
            and self._storage.supportsTransactionalUndo()):
            # XXX FileStorage used to be broken on this one
            self.assertRaises(POSException.VersionError,
                              self._storage.abortVersion,
                              '', t)

        # But now we really try to abort the version
        oids = self._storage.abortVersion(version, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(51))

    def checkCommitVersionErrors(self):
        if not (hasattr(self._storage, 'supportsTransactionalUndo')
            and self._storage.supportsTransactionalUndo()):
            # XXX FileStorage used to be broken on this one
            return
        eq = self.assertEqual
        oid1, version1 = self._setup_version('one')
        data, revid1 = self._storage.load(oid1, version1)
        eq(zodb_unpickle(data), MinPO(54))
        t = Transaction()
        self._storage.tpc_begin(t)
        try:
            self.assertRaises(POSException.VersionCommitError,
                              self._storage.commitVersion,
                              'one', 'one', t)
        finally:
            self._storage.tpc_abort(t)


    def checkNewSerialOnAbortVersion(self):
        eq = self.assertEqual
        oid, version = self._setup_version()
        data, vserial = self._storage.load(oid, version)
        data, nserial = self._storage.load(oid, '')

        self._abortVersion(version)
        data, serial = self._storage.load(oid, version)

        self.failUnless(serial != vserial and serial != nserial,
                        "New serial, %r, should be different from the old "
                        "version, %r, and non-version, %r, serials."
                        % (serial, vserial, nserial))

    def checkNewSerialOnCommitVersion(self):
        eq = self.assertEqual
        oid, version = self._setup_version()
        data, vserial = self._storage.load(oid, version)
        data, nserial = self._storage.load(oid, '')

        self._commitVersion(version, '')
        data, serial = self._storage.load(oid, '')

        self.failUnless(serial != vserial and serial != nserial,
                        "New serial, %r, should be different from the old "
                        "version, %r, and non-version, %r, serials."
                        % (serial, vserial, nserial))

    def checkNewSerialOnCommitVersionToVersion(self):
        eq = self.assertEqual
        oid, version = self._setup_version()
        data, vserial = self._storage.load(oid, version)
        data, nserial = self._storage.load(oid, '')

        version2 = 'test version 2'
        self._commitVersion(version, version2)
        data, serial = self._storage.load(oid, version2)

        self.failUnless(serial != vserial and serial != nserial,
                        "New serial, %r, should be different from the old "
                        "version, %r, and non-version, %r, serials."
                        % (serial, vserial, nserial))

    def checkModifyAfterAbortVersion(self):
        eq = self.assertEqual
        oid, version = self._setup_version()
        self._abortVersion(version)
        data, revid = self._storage.load(oid, '')
        # And modify it a few times
        revid = self._dostore(oid, revid=revid, data=MinPO(52))
        revid = self._dostore(oid, revid=revid, data=MinPO(53))
        revid = self._dostore(oid, revid=revid, data=MinPO(54))
        data, newrevid = self._storage.load(oid, '')
        eq(newrevid, revid)
        eq(zodb_unpickle(data), MinPO(54))

    def checkCommitToNonVersion(self):
        eq = self.assertEqual
        oid, version = self._setup_version()
        data, revid = self._storage.load(oid, version)
        eq(zodb_unpickle(data), MinPO(54))
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(51))
        self._commitVersion(version, '')
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(54))

    def checkCommitToOtherVersion(self):
        eq = self.assertEqual
        oid1, version1 = self._setup_version('one')

        data, revid1 = self._storage.load(oid1, version1)
        eq(zodb_unpickle(data), MinPO(54))
        oid2, version2 = self._setup_version('two')
        data, revid2 = self._storage.load(oid2, version2)
        eq(zodb_unpickle(data), MinPO(54))

        # make sure we see the non-version data when appropriate
        data, revid2 = self._storage.load(oid1, version2)
        eq(zodb_unpickle(data), MinPO(51))
        data, revid2 = self._storage.load(oid2, version1)
        eq(zodb_unpickle(data), MinPO(51))
        data, revid2 = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(51))

        # Okay, now let's commit object1 to version2
        oids = self._commitVersion(version1, version2)
        eq(len(oids), 1)
        eq(oids[0], oid1)
        data, revid = self._storage.load(oid1, version2)
        eq(zodb_unpickle(data), MinPO(54))
        data, revid = self._storage.load(oid2, version2)
        eq(zodb_unpickle(data), MinPO(54))

        # an object can only exist in one version, so a load from
        # version1 should now give the non-version data
        data, revid2 = self._storage.load(oid1, version1)
        eq(zodb_unpickle(data), MinPO(51))

        # as should a version that has never been used
        data, revid2 = self._storage.load(oid1, 'bela lugosi')
        eq(zodb_unpickle(data), MinPO(51))

    def checkAbortOneVersionCommitTheOther(self):
        eq = self.assertEqual
        oid1, version1 = self._setup_version('one')
        data, revid1 = self._storage.load(oid1, version1)
        eq(zodb_unpickle(data), MinPO(54))
        oid2, version2 = self._setup_version('two')
        data, revid2 = self._storage.load(oid2, version2)
        eq(zodb_unpickle(data), MinPO(54))

        # Let's make sure we can't get object1 in version2
        data, revid2 = self._storage.load(oid1, version2)
        eq(zodb_unpickle(data), MinPO(51))

        oids = self._abortVersion(version1)
        eq(len(oids), 1)
        eq(oids[0], oid1)
        data, revid = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(51))

        #JF# Ditto
        #JF# self.assertRaises(POSException.VersionError,
        #JF#                   self._storage.load, oid1, version1)
        data, revid = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(51))
        #JF# self.assertRaises(POSException.VersionError,
        #JF#                   self._storage.load, oid1, version2)
        data, revid = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(51))

        data, revid = self._storage.load(oid2, '')
        eq(zodb_unpickle(data), MinPO(51))
        data, revid = self._storage.load(oid2, version2)
        eq(zodb_unpickle(data), MinPO(54))
        # Okay, now let's commit version2 back to the trunk
        oids = self._commitVersion(version2, '')
        eq(len(oids), 1)
        eq(oids[0], oid2)
        data, revid = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(51))

        # But the trunk should be up to date now
        data, revid = self._storage.load(oid2, '')
        eq(zodb_unpickle(data), MinPO(54))
        data, revid = self._storage.load(oid2, version2)
        eq(zodb_unpickle(data), MinPO(54))

        #JF# To do a test like you want, you have to add the data in a version
        oid = self._storage.new_oid()
        revid = self._dostore(oid, revid=revid, data=MinPO(54), version='one')
        self.assertRaises(KeyError,
                          self._storage.load, oid, '')
        self.assertRaises(KeyError,
                          self._storage.load, oid, 'two')
