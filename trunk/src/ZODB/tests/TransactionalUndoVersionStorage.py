##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
# Check interactions between undo() and versions.  Any storage that
# supports both undo() and versions must pass these tests.

import time

from ZODB.serialize import referencesf
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

        def check_objects(nonversiondata, versiondata):
            data, revid = self._storage.load(oid, version)
            self.assertEqual(zodb_unpickle(data), MinPO(versiondata))
            data, revid = self._storage.load(oid, '')
            self.assertEqual(zodb_unpickle(data), MinPO(nonversiondata))

        oid = self._storage.new_oid()
        version = 'one'
        revid_a = self._dostore(oid, data=MinPO(91))
        revid_b = self._dostore(oid, revid=revid_a, data=MinPO(92),
                                version=version)
        revid_c = self._dostore(oid, revid=revid_b, data=MinPO(93),
                                version=version)

        info = self._storage.undoInfo()
        self._undo(info[0]['id'], [oid])

        data, revid = self._storage.load(oid, '')
##        eq(revid, revid_a)
        eq(zodb_unpickle(data), MinPO(91))
        data, revid = self._storage.load(oid, version)
        unless(revid > revid_b and revid > revid_c)
        eq(zodb_unpickle(data), MinPO(92))

        # Now commit the version...
        oids = self._commitVersion(version, "")
        eq(len(oids), 1)
        eq(oids[0], oid)

        check_objects(92, 92)

        # ...and undo the commit
        info = self._storage.undoInfo()
        self._undo(info[0]['id'], [oid])

        check_objects(91, 92)

        oids = self._abortVersion(version)
        assert len(oids) == 1
        assert oids[0] == oid

        check_objects(91, 91)

        # Now undo the abort
        info=self._storage.undoInfo()
        self._undo(info[0]['id'], [oid])

        check_objects(91, 92)

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
        self._x_dostore(oid1, data=MinPO(2), revid=revid2,
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

        self._undo(t_id, note="undo commit version")

        self.assertEqual(load_value(oid1), 0)
        self.assertEqual(load_value(oid1, version), 2)

        data, tid, ver = self._storage.loadEx(oid1, "")
        # After undoing the version commit, the non-version data
        # once again becomes the non-version data from 'create1'.
        self.assertEqual(tid, self._storage.lastTransaction())
        self.assertEqual(ver, "")

        # The current version data comes from an undo record, which
        # means that it gets data via the backpointer but tid from the
        # current txn.
        data, tid, ver = self._storage.loadEx(oid1, version)
        self.assertEqual(ver, version)
        self.assertEqual(tid, self._storage.lastTransaction())

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
        self._x_dostore(oid1, data=MinPO(2), revid=revid2,
                        version=version, description='version2')
        self._x_dostore(description='create2')

        self._abortVersion(version)

        info = self._storage.undoInfo()
        t_id = info[0]['id']

        self.assertEqual(load_value(oid1), 0)
        # after abort, we should see non-version data
        self.assertEqual(load_value(oid1, version), 0)

        self._undo(t_id, note="undo abort version")

        self.assertEqual(load_value(oid1), 0)
        # t undo will re-create the version
        self.assertEqual(load_value(oid1, version), 2)

        info = self._storage.undoInfo()
        t_id = info[0]['id']

        self._storage.pack(pt, referencesf)

        self._undo(t_id, note="undo undo")

        # undo of undo will put as back where we started
        self.assertEqual(load_value(oid1), 0)
        # after abort, we should see non-version data
        self.assertEqual(load_value(oid1, version), 0)
