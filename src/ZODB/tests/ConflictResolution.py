"""Tests for application-level conflict resolution."""

from ZODB.Transaction import Transaction
from ZODB.POSException import ConflictError
from Persistence import Persistent

from ZODB.tests.StorageTestBase import zodb_unpickle, zodb_pickle

import sys
import types
from cPickle import Pickler, Unpickler
from cStringIO import StringIO

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
        raise AttributeError, "no attribute"

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
        self.assert_(inst._value == 5)

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
        self.assertRaises(ConflictError,
                          self._dostoreNP,
                          oid, revid=revid1, data=zodb_pickle(obj))

    
    def checkBuggyResolve(self):
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
        self.assertRaises(AttributeError,
                          self._dostoreNP,
                          oid, revid=revid1, data=zodb_pickle(obj))

    # XXX test conflict error raised during undo
