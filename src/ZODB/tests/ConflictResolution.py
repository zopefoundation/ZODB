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
"""Tests for application-level conflict resolution."""

from ZODB.Transaction import Transaction
from ZODB.POSException import ConflictError, UndoError
from persistent import Persistent

from ZODB.tests.StorageTestBase import zodb_unpickle, zodb_pickle

class PCounter(Persistent):

    _value = 0

    def __repr__(self):
        return "<PCounter %d>" % self._value

    def inc(self):
        self._value = self._value + 1

    def _p_resolveConflict(self, oldState, savedState, newState):
        savedDiff = savedState['_value'] - oldState['_value']
        newDiff = newState['_value'] - oldState['_value']

        oldState['_value'] = oldState['_value'] + savedDiff + newDiff

        return oldState

    # XXX What if _p_resolveConflict _thinks_ it resolved the
    # conflict, but did something wrong?

class PCounter2(PCounter):

    def _p_resolveConflict(self, oldState, savedState, newState):
        raise ConflictError

class PCounter3(PCounter):
    def _p_resolveConflict(self, oldState, savedState, newState):
        raise AttributeError, "no attribute (testing conflict resolution)"

class PCounter4(PCounter):
    def _p_resolveConflict(self, oldState, savedState):
        raise RuntimeError, "Can't get here; not enough args"

class ConflictResolvingStorage:

    def checkResolve(self):
        obj = PCounter()
        obj.inc()

        oid = self._storage.new_oid()

        revid1 = self._dostoreNP(oid, data=zodb_pickle(obj))

        obj.inc()
        obj.inc()
        # The effect of committing two transactions with the same
        # pickle is to commit two different transactions relative to
        # revid1 that add two to _value.
        revid2 = self._dostoreNP(oid, revid=revid1, data=zodb_pickle(obj))
        revid3 = self._dostoreNP(oid, revid=revid1, data=zodb_pickle(obj))

        data, serialno = self._storage.load(oid, '')
        inst = zodb_unpickle(data)
        self.assertEqual(inst._value, 5)

    def checkUnresolvable(self):
        obj = PCounter2()
        obj.inc()

        oid = self._storage.new_oid()

        revid1 = self._dostoreNP(oid, data=zodb_pickle(obj))

        obj.inc()
        obj.inc()
        # The effect of committing two transactions with the same
        # pickle is to commit two different transactions relative to
        # revid1 that add two to _value.
        revid2 = self._dostoreNP(oid, revid=revid1, data=zodb_pickle(obj))
        try:
            self._dostoreNP(oid, revid=revid1, data=zodb_pickle(obj))
        except ConflictError, err:
            self.assert_("PCounter2" in str(err))
        else:
            self.fail("Expected ConflictError")

    def checkZClassesArentResolved(self):
        from ZODB.ConflictResolution import find_global, BadClassName
        dummy_class_tuple = ('*foobar', ())
        self.assertRaises(BadClassName, find_global, '*foobar', ())

    def checkBuggyResolve1(self):
        obj = PCounter3()
        obj.inc()

        oid = self._storage.new_oid()

        revid1 = self._dostoreNP(oid, data=zodb_pickle(obj))

        obj.inc()
        obj.inc()
        # The effect of committing two transactions with the same
        # pickle is to commit two different transactions relative to
        # revid1 that add two to _value.
        revid2 = self._dostoreNP(oid, revid=revid1, data=zodb_pickle(obj))
        self.assertRaises(ConflictError,
                          self._dostoreNP,
                          oid, revid=revid1, data=zodb_pickle(obj))

    def checkBuggyResolve2(self):
        obj = PCounter4()
        obj.inc()

        oid = self._storage.new_oid()

        revid1 = self._dostoreNP(oid, data=zodb_pickle(obj))

        obj.inc()
        obj.inc()
        # The effect of committing two transactions with the same
        # pickle is to commit two different transactions relative to
        # revid1 that add two to _value.
        revid2 = self._dostoreNP(oid, revid=revid1, data=zodb_pickle(obj))
        self.assertRaises(ConflictError,
                          self._dostoreNP,
                          oid, revid=revid1, data=zodb_pickle(obj))

class ConflictResolvingTransUndoStorage:

    def checkUndoConflictResolution(self):
        # This test is based on checkNotUndoable in the
        # TransactionalUndoStorage test suite.  Except here, conflict
        # resolution should allow us to undo the transaction anyway.

        obj = PCounter()
        obj.inc()
        oid = self._storage.new_oid()
        revid_a = self._dostore(oid, data=obj)
        obj.inc()
        revid_b = self._dostore(oid, revid=revid_a, data=obj)
        obj.inc()
        revid_c = self._dostore(oid, revid=revid_b, data=obj)
        # Start the undo
        info = self._storage.undoInfo()
        tid = info[1]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        self._storage.undo(tid, t)
        self._storage.tpc_finish(t)

    def checkUndoUnresolvable(self):
        # This test is based on checkNotUndoable in the
        # TransactionalUndoStorage test suite.  Except here, conflict
        # resolution should allow us to undo the transaction anyway.

        obj = PCounter2()
        obj.inc()
        oid = self._storage.new_oid()
        revid_a = self._dostore(oid, data=obj)
        obj.inc()
        revid_b = self._dostore(oid, revid=revid_a, data=obj)
        obj.inc()
        revid_c = self._dostore(oid, revid=revid_b, data=obj)
        # Start the undo
        info = self._storage.undoInfo()
        tid = info[1]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        self.assertRaises(UndoError, self._storage.undo,
                          tid, t)
        self._storage.tpc_abort(t)
