# Check transactionalUndo().  Any storage that supports transactionalUndo()
# must pass these tests.

import types
from ZODB import POSException

from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_pickle, zodb_unpickle

ZERO = '\0'*8


class TransactionalUndoStorage:

    def _transaction_begin(self):
        self.__serials = {}

    def _transaction_store(self, oid, rev, data, vers, trans):
        r = self._storage.store(oid, rev, data, vers, trans)
        if r:
            if type(r) == types.StringType:
                self.__serials[oid] = r
            else:
                for oid, serial in r:
                    self.__serials[oid] = serial

    def _transaction_vote(self, trans):
        r = self._storage.tpc_vote(trans)
        if r:
            for oid, serial in r:
                self.__serials[oid] = serial

    def _transaction_newserial(self, oid):
        return self.__serials[oid]

    def _multi_obj_transaction(self, objs):
        newrevs = {}
        self._storage.tpc_begin(self._transaction)
        self._transaction_begin()
        for oid, rev, data in objs:
            self._transaction_store(oid, rev, data, '', self._transaction)
            newrevs[oid] = None
        self._transaction_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        for oid in newrevs.keys():
            newrevs[oid] = self._transaction_newserial(oid)
        return newrevs
    
    def checkSimpleTransactionalUndo(self):
        eq = self.assertEqual
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=MinPO(23))
        revid = self._dostore(oid, revid=revid, data=MinPO(24))
        revid = self._dostore(oid, revid=revid, data=MinPO(25))

        info = self._storage.undoInfo()
        tid = info[0]['id']
        # Now start an undo transaction
        self._transaction.note('undo1')
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.transactionalUndo(tid, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(24))
        # Do another one
        info = self._storage.undoInfo()
        tid = info[2]['id']
        self._transaction.note('undo2')
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.transactionalUndo(tid, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(23))
        # Try to undo the first record
        info = self._storage.undoInfo()
        tid = info[4]['id']
        self._transaction.note('undo3')
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.transactionalUndo(tid, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)

        eq(len(oids), 1)
        eq(oids[0], oid)
        # This should fail since we've undone the object's creation
        self.assertRaises(KeyError,
                          self._storage.load, oid, '')
        # And now let's try to redo the object's creation
        info = self._storage.undoInfo()
        tid = info[0]['id']
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.transactionalUndo(tid, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(23))

    def checkUndoCreationBranch1(self):
        eq = self.assertEqual
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=MinPO(11))
        revid = self._dostore(oid, revid=revid, data=MinPO(12))
        # Undo the last transaction
        info = self._storage.undoInfo()
        tid = info[0]['id']
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.transactionalUndo(tid, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(11))
        # Now from here, we can either redo the last undo, or undo the object
        # creation.  Let's undo the object creation.
        info = self._storage.undoInfo()
        tid = info[2]['id']
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.transactionalUndo(tid, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        eq(len(oids), 1)
        eq(oids[0], oid)
        self.assertRaises(KeyError, self._storage.load, oid, '')

    def checkUndoCreationBranch2(self):
        eq = self.assertEqual
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=MinPO(11))
        revid = self._dostore(oid, revid=revid, data=MinPO(12))
        # Undo the last transaction
        info = self._storage.undoInfo()
        tid = info[0]['id']
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.transactionalUndo(tid, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(11))
        # Now from here, we can either redo the last undo, or undo the object
        # creation.  Let's redo the last undo
        info = self._storage.undoInfo()
        tid = info[0]['id']
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.transactionalUndo(tid, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, '')
        eq(zodb_unpickle(data), MinPO(12))

    def checkTwoObjectUndo(self):
        eq = self.assertEqual
        # Convenience
        p31, p32, p51, p52 = map(zodb_pickle,
                                 map(MinPO, (31, 32, 51, 52)))
        oid1 = self._storage.new_oid()
        oid2 = self._storage.new_oid()
        revid1 = revid2 = ZERO
        # Store two objects in the same transaction
        self._storage.tpc_begin(self._transaction)
        self._transaction_begin()
        self._transaction_store(oid1, revid1, p31, '', self._transaction)
        self._transaction_store(oid2, revid2, p51, '', self._transaction)
        # Finish the transaction
        self._transaction_vote(self._transaction)
        revid1 = self._transaction_newserial(oid1)
        revid2 = self._transaction_newserial(oid2)
        self._storage.tpc_finish(self._transaction)
        eq(revid1, revid2)
        # Update those same two objects
        self._storage.tpc_begin(self._transaction)
        self._transaction_begin()
        self._transaction_store(oid1, revid1, p32, '', self._transaction)
        self._transaction_store(oid2, revid2, p52, '', self._transaction)
        # Finish the transaction
        self._transaction_vote(self._transaction)
        revid1 = self._transaction_newserial(oid1)
        revid2 = self._transaction_newserial(oid2)
        self._storage.tpc_finish(self._transaction)
        eq(revid1, revid2)
        # Make sure the objects have the current value
        data, revid1 = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(32))
        data, revid2 = self._storage.load(oid2, '')
        eq(zodb_unpickle(data), MinPO(52))
        # Now attempt to undo the transaction containing two objects
        info = self._storage.undoInfo()
        tid = info[0]['id']
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.transactionalUndo(tid, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        eq(len(oids), 2)
        self.failUnless(oid1 in oids)
        self.failUnless(oid2 in oids)
        data, revid1 = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(31))
        data, revid2 = self._storage.load(oid2, '')
        eq(zodb_unpickle(data), MinPO(51))

    def checkTwoObjectUndoAtOnce(self):
        # Convenience
        eq = self.assertEqual
        unless = self.failUnless
        p30, p31, p32, p50, p51, p52 = map(zodb_pickle,
                                           map(MinPO,
                                               (30, 31, 32, 50, 51, 52)))
        oid1 = self._storage.new_oid()
        oid2 = self._storage.new_oid()
        revid1 = revid2 = ZERO
        # Store two objects in the same transaction
        d = self._multi_obj_transaction([(oid1, revid1, p30),
                                         (oid2, revid2, p50),
                                         ])
        eq(d[oid1], d[oid2])
        # Update those same two objects
        d = self._multi_obj_transaction([(oid1, d[oid1], p31),
                                         (oid2, d[oid2], p51),
                                         ])
        eq(d[oid1], d[oid2])
        # Update those same two objects
        d = self._multi_obj_transaction([(oid1, d[oid1], p32),
                                         (oid2, d[oid2], p52),
                                         ])
        eq(d[oid1], d[oid2])
        revid1 = self._transaction_newserial(oid1)
        revid2 = self._transaction_newserial(oid2)
        eq(revid1, revid2)
        # Make sure the objects have the current value
        data, revid1 = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(32))
        data, revid2 = self._storage.load(oid2, '')
        eq(zodb_unpickle(data), MinPO(52))
        # Now attempt to undo the transaction containing two objects
        info = self._storage.undoInfo()
        tid = info[0]['id']
        tid1 = info[1]['id']
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.transactionalUndo(tid, self._transaction)
        oids1 = self._storage.transactionalUndo(tid1, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        # We get the finalization stuff called an extra time:
##        self._storage.tpc_vote(self._transaction)
##        self._storage.tpc_finish(self._transaction)
        eq(len(oids), 2)
        eq(len(oids1), 2)
        unless(oid1 in oids)
        unless(oid2 in oids)
        data, revid1 = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(30))
        data, revid2 = self._storage.load(oid2, '')
        eq(zodb_unpickle(data), MinPO(50))
        # Now try to undo the one we just did to undo, whew
        info = self._storage.undoInfo()
        tid = info[0]['id']
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.transactionalUndo(tid, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        eq(len(oids), 2)
        unless(oid1 in oids)
        unless(oid2 in oids)
        data, revid1 = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(32))
        data, revid2 = self._storage.load(oid2, '')
        eq(zodb_unpickle(data), MinPO(52))

    def checkTwoObjectUndoAgain(self):
        eq = self.assertEqual
        p31, p32, p33, p51, p52, p53 = map(
            zodb_pickle,
            map(MinPO, (31, 32, 33, 51, 52, 53)))
        # Like the above, but the first revision of the objects are stored in
        # different transactions.
        oid1 = self._storage.new_oid()
        oid2 = self._storage.new_oid()
        revid1 = self._dostore(oid1, data=p31, already_pickled=1)
        revid2 = self._dostore(oid2, data=p51, already_pickled=1)
        # Update those same two objects
        self._storage.tpc_begin(self._transaction)
        self._transaction_begin()
        self._transaction_store(oid1, revid1, p32, '', self._transaction)
        self._transaction_store(oid2, revid2, p52, '', self._transaction)
        # Finish the transaction
        self._transaction_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        revid1 = self._transaction_newserial(oid1)
        revid2 = self._transaction_newserial(oid2)
        eq(revid1, revid2)
        # Now attempt to undo the transaction containing two objects
        info = self._storage.undoInfo()
        tid = info[0]['id']
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.transactionalUndo(tid, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        eq(len(oids), 2)
        self.failUnless(oid1 in oids)
        self.failUnless(oid2 in oids)
        data, revid1 = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(31))
        data, revid2 = self._storage.load(oid2, '')
        eq(zodb_unpickle(data), MinPO(51))
        # Like the above, but this time, the second transaction contains only
        # one object.
        self._storage.tpc_begin(self._transaction)
        self._transaction_begin()
        self._transaction_store(oid1, revid1, p33, '', self._transaction)
        self._transaction_store(oid2, revid2, p53, '', self._transaction)
        # Finish the transaction
        self._transaction_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        revid1 = self._transaction_newserial(oid1)
        revid2 = self._transaction_newserial(oid2)
        eq(revid1, revid2)
        # Update in different transactions
        revid1 = self._dostore(oid1, revid=revid1, data=MinPO(34))
        revid2 = self._dostore(oid2, revid=revid2, data=MinPO(54))
        # Now attempt to undo the transaction containing two objects
        info = self._storage.undoInfo()
        tid = info[1]['id']
        self._storage.tpc_begin(self._transaction)
        oids = self._storage.transactionalUndo(tid, self._transaction)
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        eq(len(oids), 1)
        self.failUnless(oid1 in oids)
        self.failUnless(not oid2 in oids)
        data, revid1 = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(33))
        data, revid2 = self._storage.load(oid2, '')
        eq(zodb_unpickle(data), MinPO(54))
        

    def checkNotUndoable(self):
        eq = self.assertEqual
        # Set things up so we've got a transaction that can't be undone
        oid = self._storage.new_oid()
        revid_a = self._dostore(oid, data=MinPO(51))
        revid_b = self._dostore(oid, revid=revid_a, data=MinPO(52))
        revid_c = self._dostore(oid, revid=revid_b, data=MinPO(53))
        # Start the undo
        info = self._storage.undoInfo()
        tid = info[1]['id']
        self._storage.tpc_begin(self._transaction)
        self.assertRaises(POSException.UndoError,
                          self._storage.transactionalUndo,
                          tid, self._transaction)
        self._storage.tpc_abort(self._transaction)
        # Now have more fun: object1 and object2 are in the same transaction,
        # which we'll try to undo to, but one of them has since modified in
        # different transaction, so the undo should fail.
        oid1 = oid
        revid1 = revid_c
        oid2 = self._storage.new_oid()
        revid2 = ZERO
        p81, p82, p91, p92 = map(zodb_pickle,
                                 map(MinPO, (81, 82, 91, 92)))

        self._storage.tpc_begin(self._transaction)
        self._transaction_begin()
        self._transaction_store(oid1, revid1, p81, '', self._transaction)
        self._transaction_store(oid2, revid2, p91, '', self._transaction)
        self._transaction_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        revid1 = self._transaction_newserial(oid1)
        revid2 = self._transaction_newserial(oid2)
        eq(revid1, revid2)
        # Make sure the objects have the expected values
        data, revid_11 = self._storage.load(oid1, '')
        eq(zodb_unpickle(data), MinPO(81))
        data, revid_22 = self._storage.load(oid2, '')
        eq(zodb_unpickle(data), MinPO(91))
        eq(revid_11, revid1)
        eq(revid_22, revid2)
        # Now modify oid2
        revid2 = self._dostore(oid2, revid=revid2, data=MinPO(92))
        self.assertNotEqual(revid1, revid2)
        self.assertNotEqual(revid2, revid_22)
        info = self._storage.undoInfo()
        tid = info[1]['id']
        self._storage.tpc_begin(self._transaction)
        self.assertRaises(POSException.UndoError,
                          self._storage.transactionalUndo,
                          tid, self._transaction)
        self._storage.tpc_abort(self._transaction)
