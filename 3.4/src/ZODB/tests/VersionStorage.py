##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Run the version related tests for a storage.

Any storage that supports versions should be able to pass all these tests.
"""

import time

import transaction
from transaction import Transaction

from ZODB import POSException
from ZODB.serialize import referencesf
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle, snooze
from ZODB import DB

class VersionStorage:

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
        data, tid, ver = self._storage.loadEx(oid, "version")
        self.assertEqual(revid2, tid)
        self.assertEqual(zodb_unpickle(data), MinPO(13))
        oids = self._abortVersion("version")
        self.assertEqual([oid], oids)
        data, revid3 = self._storage.load(oid, "")
        # use repr() to avoid getting binary data in a traceback on error
        self.assertNotEqual(revid1, revid3)
        self.assertNotEqual(revid2, revid3)
        data, tid, ver = self._storage.loadEx(oid, "")
        self.assertEqual(revid3, tid)
        self.assertEqual(zodb_unpickle(data), MinPO(12))
        self.assertEqual(tid, self._storage.lastTransaction())

    def checkVersionedStoreAndLoad(self):
        eq = self.assertEqual
        # Store a couple of non-version revisions of the object
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=MinPO(11))
        revid1 = self._dostore(oid, revid=revid, data=MinPO(12))
        # And now store some new revisions in a version
        version = 'test-version'
        revid = self._dostore(oid, revid=revid1, data=MinPO(13),
                              version=version)
        revid = self._dostore(oid, revid=revid, data=MinPO(14),
                              version=version)
        revid2 = self._dostore(oid, revid=revid, data=MinPO(15),
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
        data, tid, ver = self._storage.loadEx(oid, version)
        eq(zodb_unpickle(data), MinPO(15))
        eq(tid, revid2)
        data, tid, ver = self._storage.loadEx(oid, "other version")
        eq(zodb_unpickle(data), MinPO(12))
        eq(tid, revid2)
        # loadSerial returns non-version data
        try:
            data = self._storage.loadSerial(oid, revid)
            eq(zodb_unpickle(data), MinPO(12))
            data = self._storage.loadSerial(oid, revid2)
            eq(zodb_unpickle(data), MinPO(12))
        except POSException.Unsupported:
            pass

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
        revid = self._dostore(oid, revid=revid, data=MinPO(51))
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

        # Not sure I can write a test for getSerial() in the
        # presence of aborted versions, because FileStorage and
        # Berkeley storage give a different answer. I think Berkeley
        # is right and FS is wrong.

        oids = self._abortVersion(version)
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(51))

    def checkAbortVersionNonCurrent(self):
        # Make sure the non-current serial number is correctly
        # after a version is aborted.
        oid, version = self._setup_version()
        self._abortVersion(version)
        data, tid, ver = self._storage.loadEx(oid, "")
        # write a new revision of oid so that the aborted-version txn
        # is not current
        self._dostore(oid, revid=tid, data=MinPO(17))
        ltid = self._storage.lastTransaction()
        ncdata, ncstart, end = self._storage.loadBefore(oid, ltid)
        self.assertEqual(data, ncdata)
        self.assertEqual(tid, ncstart)

    def checkAbortVersionErrors(self):
        eq = self.assertEqual
        oid, version = self._setup_version()
        # Now abort a bogus version
        t = Transaction()
        self._storage.tpc_begin(t)

        # And try to abort the empty version
        if (hasattr(self._storage, 'supportsTransactionalUndo') and
                self._storage.supportsTransactionalUndo()):
            self.assertRaises(POSException.VersionError,
                              self._storage.abortVersion,
                              '', t)

        # But now we really try to abort the version
        tid, oids = self._storage.abortVersion(version, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(51))

    def checkCommitVersionErrors(self):
        if not (hasattr(self._storage, 'supportsTransactionalUndo') and
                self._storage.supportsTransactionalUndo()):
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

    def checkNewSerialOnCommitVersionToVersion(self):
        oid, version = self._setup_version()
        data, vtid = self._storage.load(oid, version)
        data, ntid = self._storage.load(oid, '')

        version2 = 'test version 2'
        self._commitVersion(version, version2)
        data, tid = self._storage.load(oid, version2)

        self.failUnless(tid != vtid and tid != ntid,
                        "New tid, %r, should be different from the old "
                        "version, %r, and non-version, %r, tids."
                        % (tid, vtid, ntid))

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

        data, revid = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(51))
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

        oid = self._storage.new_oid()
        revid = self._dostore(oid, revid=revid, data=MinPO(54), version='one')
        self.assertRaises(KeyError,
                          self._storage.load, oid, '')
        self.assertRaises(KeyError,
                          self._storage.load, oid, 'two')

    def checkCreateObjectInVersionWithAbort(self):
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=21, version="one")
        revid = self._dostore(oid, revid=revid, data=23, version='one')
        revid = self._dostore(oid, revid=revid, data=34, version='one')
        # Now abort the version and the creation
        t = Transaction()
        self._storage.tpc_begin(t)
        tid, oids = self._storage.abortVersion('one', t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        self.assertEqual(oids, [oid])

    def checkPackVersions(self):
        db = DB(self._storage)
        cn = db.open(version="testversion")
        root = cn.root()

        obj = root["obj"] = MinPO("obj")
        root["obj2"] = MinPO("obj2")
        txn = transaction.get()
        txn.note("create 2 objs in version")
        txn.commit()

        obj.value = "77"
        txn = transaction.get()
        txn.note("modify obj in version")
        txn.commit()

        # undo the modification to generate a mix of backpointers
        # and versions for pack to chase
        info = db.undoInfo()
        db.undo(info[0]["id"])
        txn = transaction.get()
        txn.note("undo modification")
        txn.commit()

        snooze()
        self._storage.pack(time.time(), referencesf)

        db.commitVersion("testversion")
        txn = transaction.get()
        txn.note("commit version")
        txn.commit()

        cn = db.open()
        root = cn.root()
        root["obj"] = "no version"

        txn = transaction.get()
        txn.note("modify obj")
        txn.commit()

        self._storage.pack(time.time(), referencesf)

    def checkPackVersionsInPast(self):
        db = DB(self._storage)
        cn = db.open(version="testversion")
        root = cn.root()

        obj = root["obj"] = MinPO("obj")
        root["obj2"] = MinPO("obj2")
        txn = transaction.get()
        txn.note("create 2 objs in version")
        txn.commit()

        obj.value = "77"
        txn = transaction.get()
        txn.note("modify obj in version")
        txn.commit()

        t0 = time.time()
        snooze()

        # undo the modification to generate a mix of backpointers
        # and versions for pack to chase
        info = db.undoInfo()
        db.undo(info[0]["id"])
        txn = transaction.get()
        txn.note("undo modification")
        txn.commit()

        self._storage.pack(t0, referencesf)

        db.commitVersion("testversion")
        txn = transaction.get()
        txn.note("commit version")
        txn.commit()

        cn = db.open()
        root = cn.root()
        root["obj"] = "no version"

        txn = transaction.get()
        txn.note("modify obj")
        txn.commit()

        self._storage.pack(time.time(), referencesf)

    def checkPackVersionReachable(self):
        db = DB(self._storage)
        cn = db.open()
        root = cn.root()

        names = "a", "b", "c"

        for name in names:
            root[name] = MinPO(name)
            transaction.commit()

        for name in names:
            cn2 = db.open(version=name)
            rt2 = cn2.root()
            obj = rt2[name]
            obj.value = MinPO("version")
            transaction.commit()
            cn2.close()

        root["d"] = MinPO("d")
        transaction.commit()
        snooze()

        self._storage.pack(time.time(), referencesf)
        cn.sync()

        # make sure all the non-version data is there
        for name, obj in root.items():
            self.assertEqual(name, obj.value)

        # make sure all the version-data is there,
        # and create a new revision in the version
        for name in names:
            cn2 = db.open(version=name)
            rt2 = cn2.root()
            obj = rt2[name].value
            self.assertEqual(obj.value, "version")
            obj.value = "still version"
            transaction.commit()
            cn2.close()

        db.abortVersion("b")
        txn = transaction.get()
        txn.note("abort version b")
        txn.commit()

        t = time.time()
        snooze()

        L = db.undoInfo()
        db.undo(L[0]["id"])
        txn = transaction.get()
        txn.note("undo abort")
        txn.commit()

        self._storage.pack(t, referencesf)

        cn2 = db.open(version="b")
        rt2 = cn2.root()
        self.assertEqual(rt2["b"].value.value, "still version")

    def checkLoadBeforeVersion(self):
        eq = self.assertEqual
        oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=1)
        revid2 = self._dostore(oid, data=2, revid=revid1, version="kobe")
        revid3 = self._dostore(oid, data=3, revid=revid2, version="kobe")
        data, start_tid, end_tid = self._storage.loadBefore(oid, revid3)
        eq(zodb_unpickle(data), MinPO(1))
        eq(start_tid, revid1)
        eq(end_tid, None)
