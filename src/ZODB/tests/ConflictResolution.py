##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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
"""Tests for application-level conflict resolution."""

from persistent import Persistent
from transaction import TransactionManager

from ZODB import DB
from ZODB.Connection import TransactionMetaData
from ZODB.POSException import ConflictError
from ZODB.POSException import UndoError
from ZODB.tests.StorageTestBase import zodb_pickle


class PCounter(Persistent):

    _value = 0

    def __repr__(self):
        return "<PCounter %d>" % self._value

    def inc(self, n=1):
        self._value = self._value + n

    def _p_resolveConflict(self, oldState, savedState, newState):
        savedDiff = savedState['_value'] - oldState['_value']
        newDiff = newState['_value'] - oldState['_value']

        oldState['_value'] = oldState['_value'] + savedDiff + newDiff

        return oldState

    # Insecurity:  What if _p_resolveConflict _thinks_ it resolved the
    # conflict, but did something wrong?


class PCounter2(PCounter):

    def _p_resolveConflict(self, oldState, savedState, newState):
        raise ConflictError


class PCounter3(PCounter):
    def _p_resolveConflict(self, oldState, savedState, newState):
        raise AttributeError("no attribute (testing conflict resolution)")


class PCounter4(PCounter):
    def _p_resolveConflict(self, oldState, savedState):
        raise RuntimeError("Can't get here; not enough args")


class ConflictResolvingStorage(object):

    def checkResolve(self, resolvable=True):
        db = DB(self._storage)

        t1 = TransactionManager()
        c1 = db.open(t1)
        o1 = c1.root()['p'] = (PCounter if resolvable else PCounter2)()
        o1.inc()
        t1.commit()

        t2 = TransactionManager()
        c2 = db.open(t2)
        o2 = c2.root()['p']
        o2.inc(2)
        t2.commit()

        o1.inc(3)
        try:
            t1.commit()
        except ConflictError as err:
            self.assertIn(".PCounter2,", str(err))
            self.assertEqual(o1._value, 3)
        else:
            self.assertTrue(resolvable, "Expected ConflictError")
            self.assertEqual(o1._value, 6)

        t2.begin()
        self.assertEqual(o2._value, o1._value)

        db.close()

    def checkUnresolvable(self):
        self.checkResolve(False)

    def checkZClassesArentResolved(self):
        from ZODB.ConflictResolution import BadClassName
        from ZODB.ConflictResolution import find_global
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
        self._dostoreNP(oid, revid=revid1, data=zodb_pickle(obj))
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
        self._dostoreNP(oid, revid=revid1, data=zodb_pickle(obj))
        self.assertRaises(ConflictError,
                          self._dostoreNP,
                          oid, revid=revid1, data=zodb_pickle(obj))


class ConflictResolvingTransUndoStorage(object):

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
        self._dostore(oid, revid=revid_b, data=obj)
        # Start the undo
        info = self._storage.undoInfo()
        tid = info[1]['id']
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        self._storage.undo(tid, t)
        self._storage.tpc_vote(t)
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
        self._dostore(oid, revid=revid_b, data=obj)
        # Start the undo
        info = self._storage.undoInfo()
        tid = info[1]['id']
        t = TransactionMetaData()
        self.assertRaises(UndoError, self._begin_undos_vote, t, tid)
        self._storage.tpc_abort(t)
