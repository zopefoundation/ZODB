from __future__ import nested_scopes

# Check interactions between transactionalUndo() and versions.  Any storage
# that supports both transactionalUndo() and versions must pass these tests.

import time

from ZODB import POSException
from ZODB.referencesf import referencesf
from ZODB.Transaction import Transaction
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle


class TransactionalUndoVersionStorage:

    def _x_dostore(self, *args, **kwargs):
        # ugh: backwards compatibilty for ZEO 1.0 which runs these
        # tests but has a _dostore() method that does not support the
        # description kwarg.
        try:
            return self._dostore(*args, **kwargs)
        except TypeError:
            # assume that the type error means we've got a _dostore()
            # without the description kwarg
            try:
                del kwargs['description']
            except KeyError:
                pass # not expected
        return self._dostore(*args, **kwargs)

    def checkUndoInVersion(self):
        eq = self.assertEqual
        unless = self.failUnless
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
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, '')
        eq(revid, revid_a)
        eq(zodb_unpickle(data), MinPO(91))
        data, revid = self._storage.load(oid, version)
        unless(revid > revid_b and revid > revid_c)
        eq(zodb_unpickle(data), MinPO(92))
        # Now commit the version...
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.commitVersion(version, '', t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 1)
        eq(oids[0], oid)

        #JF# No, because we fall back to non-version data.
        #JF# self.assertRaises(POSException.VersionError,
        #JF#                   self._storage.load,
        #JF#                   oid, version)
        data, revid = self._storage.load(oid, version)
        eq(zodb_unpickle(data), MinPO(92))
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(92))
        # ...and undo the commit
        info=self._storage.undoInfo()
        tid=info[0]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, version)
        eq(zodb_unpickle(data), MinPO(92))
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(91))
        # Now abort the version
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.abortVersion(version, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 1)
        eq(oids[0], oid)
        # The object should not exist in the version now, but it should exist
        # in the non-version
        #JF# No, because we fall back
        #JF# self.assertRaises(POSException.VersionError,
        #JF#                   self._storage.load,
        #JF#                   oid, version)
        data, revid = self._storage.load(oid, version)
        eq(zodb_unpickle(data), MinPO(91))
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(91))
        # Now undo the abort
        info=self._storage.undoInfo()
        tid=info[0]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        oids = self._storage.transactionalUndo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 1)
        eq(oids[0], oid)
        # And the object should be back in versions 'one' and ''
        data, revid = self._storage.load(oid, version)
        eq(zodb_unpickle(data), MinPO(92))
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(91))

    def checkUndoCommitVersion(self):
        def load_value(oid, version=''):
            data, revid = self._storage.load(oid, version)
            return zodb_unpickle(data).value

        # create a bunch of packable transactions
        oid = self._storage.new_oid()
        revid = '\000' * 8
        for i in range(4):
            revid = self._x_dostore(oid, revid, description='packable%d' % i)
        pt = time.time()
        time.sleep(1)

        oid1 = self._storage.new_oid()
        version = 'version'
        revid1 = self._x_dostore(oid1, data=MinPO(0), description='create1')
        revid2 = self._x_dostore(oid1, data=MinPO(1), revid=revid1,
                               version=version, description='version1')
        revid3 = self._x_dostore(oid1, data=MinPO(2), revid=revid2,
                               version=version, description='version2')
        self._x_dostore(description='create2')

        t = Transaction()
        t.description = 'commit version'
        self._storage.tpc_begin(t)
        self._storage.commitVersion(version, '', t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)

        info = self._storage.undoInfo()
        t_id = info[0]['id']

        self.assertEqual(load_value(oid1), 2)
        self.assertEqual(load_value(oid1, version), 2)

        self._storage.pack(pt, referencesf)

        t = Transaction()
        t.description = 'undo commit version'
        self._storage.tpc_begin(t)
        self._storage.transactionalUndo(t_id, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)

        self.assertEqual(load_value(oid1), 0)
        self.assertEqual(load_value(oid1, version), 2)

    def checkUndoAbortVersion(self):
        def load_value(oid, version=''):
            data, revid = self._storage.load(oid, version)
            return zodb_unpickle(data).value

        # create a bunch of packable transactions
        oid = self._storage.new_oid()
        revid = '\000' * 8
        for i in range(3):
            revid = self._x_dostore(oid, revid, description='packable%d' % i)
        pt = time.time()
        time.sleep(1)

        oid1 = self._storage.new_oid()
        version = 'version'
        revid1 = self._x_dostore(oid1, data=MinPO(0), description='create1')
        revid2 = self._x_dostore(oid1, data=MinPO(1), revid=revid1,
                               version=version, description='version1')
        revid3 = self._x_dostore(oid1, data=MinPO(2), revid=revid2,
                               version=version, description='version2')
        self._x_dostore(description='create2')

        t = Transaction()
        t.description = 'abort version'
        self._storage.tpc_begin(t)
        self._storage.abortVersion(version, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)

        info = self._storage.undoInfo()
        t_id = info[0]['id']

        self.assertEqual(load_value(oid1), 0)
        # after abort, we should see non-version data
        self.assertEqual(load_value(oid1, version), 0)

        t = Transaction()
        t.description = 'undo abort version'
        self._storage.tpc_begin(t)
        self._storage.transactionalUndo(t_id, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)

        self.assertEqual(load_value(oid1), 0)
        # t undo will re-create the version
        self.assertEqual(load_value(oid1, version), 2)

        info = self._storage.undoInfo()
        t_id = info[0]['id']

        self._storage.pack(pt, referencesf)

        t = Transaction()
        t.description = 'undo undo'
        self._storage.tpc_begin(t)
        self._storage.transactionalUndo(t_id, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)

        # undo of undo will put as back where we started
        self.assertEqual(load_value(oid1), 0)
        # after abort, we should see non-version data
        self.assertEqual(load_value(oid1, version), 0)
