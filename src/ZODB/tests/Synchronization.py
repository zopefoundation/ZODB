"""Test the storage's implemenetation of the storage synchronization spec.

The Synchronization spec
    http://www.zope.org/Documentation/Developer/Models/ZODB/
    ZODB_Architecture_Storage_Interface_State_Synchronization_Diag.html

It specifies two states committing and non-committing.  A storage
starts in the non-committing state.  tpc_begin() transfers to the
committting state; tpc_abort() and tpc_finish() transfer back to
non-committing.

Several other methods are only allowed in one state or another.  Many
methods allowed only in the committing state require that they apply
to the currently committing transaction.

The spec is silent on a variety of methods that don't appear to modify
the state, e.g. load(), undoLog(), pack().  It's unclear whether there
is a separate set of synchronization rules that apply to these methods
or if the synchronization is implementation dependent, i.e. only what
is need to guarantee a corrected implementation.

The synchronization spec is also silent on whether there is any
contract implied with the caller.  If the storage can assume that a
single client is single-threaded and that it will not call, e.g., store()
until after it calls tpc_begin(), the implementation can be
substantially simplified.

New and/or unspecified methods:

tpc_vote(): handled like tpc_abort
transactionalUndo(): handled like undo()  (which is how?)

Methods that have nothing to do with committing/non-committing:
load(), loadSerial(), getName(), getSize(), __len__(), history(),
undoLog(), modifiedInVersion(), versionEmpty(), versions(), pack().

Specific questions:

The spec & docs say that undo() takes three arguments, the second
being a transaction.  If the specified arg isn't the current
transaction, the undo() should raise StorageTransactionError.  This
isn't implemented anywhere.  It looks like undo can be called at
anytime.

FileStorage does not allow undo() during a pack.  How should this be
tested?  Is it a general restriction?



"""

from ZODB.Transaction import Transaction
from ZODB.POSException import StorageTransactionError

VERSION = "testversion"
OID = "\000" * 8
SERIALNO = "\000" * 8
TID = "\000" * 8

class SynchronizedStorage:

##    def verifyCommitting(self, callable, *args):
##        self.assertRaises(StorageTransactionError, callable *args)

    def verifyNotCommitting(self, callable, *args):
        self.assertRaises(StorageTransactionError, callable, *args)

    def verifyWrongTrans(self, callable, *args):
        t = Transaction()
        self._storage.tpc_begin(t)
        self.assertRaises(StorageTransactionError, callable, *args)
        self._storage.tpc_abort(t)

    def checkAbortVersionNotCommitting(self):
        self.verifyNotCommitting(self._storage.abortVersion,
                                 VERSION, Transaction())

    def checkAbortVersionWrongTrans(self):
        self.verifyWrongTrans(self._storage.abortVersion,
                              VERSION, Transaction())

    def checkCommitVersionNotCommitting(self):
        self.verifyNotCommitting(self._storage.commitVersion,
                                 VERSION, "", Transaction())

    def checkCommitVersionWrongTrans(self):
        self.verifyWrongTrans(self._storage.commitVersion,
                              VERSION, "", Transaction())


    def checkStoreNotCommitting(self):
        self.verifyNotCommitting(self._storage.store,
                                 OID, SERIALNO, "", "", Transaction())

    def checkStoreWrongTrans(self):
        self.verifyWrongTrans(self._storage.store,
                              OID, SERIALNO, "", "", Transaction())

##    def checkNewOidNotCommitting(self):
##        self.verifyNotCommitting(self._storage.new_oid)

##    def checkNewOidWrongTrans(self):
##        self.verifyWrongTrans(self._storage.new_oid)


    def checkAbortNotCommitting(self):
        self._storage.tpc_abort(Transaction())

    def checkAbortWrongTrans(self):
        t = Transaction()
        self._storage.tpc_begin(t)
        self._storage.tpc_abort(Transaction())
        self._storage.tpc_abort(t)

    def checkFinishNotCommitting(self):
        t = Transaction()
        self._storage.tpc_finish(t)
        self._storage.tpc_abort(t)

    def checkFinishWrongTrans(self):
        t = Transaction()
        self._storage.tpc_begin(t)
        self._storage.tpc_finish(Transaction())
        self._storage.tpc_abort(t)

    def checkBeginCommitting(self):
        t = Transaction()
        self._storage.tpc_begin(t)
        self._storage.tpc_begin(t)
        self._storage.tpc_abort(t)

    # XXX how to check undo?
