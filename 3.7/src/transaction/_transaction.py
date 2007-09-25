############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
############################################################################
"""Transaction objects manage resources for an individual activity.

Compatibility issues
--------------------

The implementation of Transaction objects involves two layers of
backwards compatibility, because this version of transaction supports
both ZODB 3 and ZODB 4.  Zope is evolving towards the ZODB4
interfaces.

Transaction has two methods for a resource manager to call to
participate in a transaction -- register() and join().  join() takes a
resource manager and adds it to the list of resources.  register() is
for backwards compatibility.  It takes a persistent object and
registers its _p_jar attribute.  TODO: explain adapter

Subtransactions
---------------

Note: Subtransactions are deprecated!  Use savepoint/rollback instead.

A subtransaction applies the transaction notion recursively.  It
allows a set of modifications within a transaction to be committed or
aborted as a group.  A subtransaction is a strictly local activity;
its changes are not visible to any other database connection until the
top-level transaction commits.  In addition to its use to organize a
large transaction, subtransactions can be used to optimize memory use.
ZODB must keep modified objects in memory until a transaction commits
and it can write the changes to the storage.  A subtransaction uses a
temporary disk storage for its commits, allowing modified objects to
be flushed from memory when the subtransaction commits.

The commit() and abort() methods take an optional subtransaction
argument that defaults to false.  If it is a true, the operation is
performed on a subtransaction.

Subtransactions add a lot of complexity to the transaction
implementation.  Some resource managers support subtransactions, but
they are not required to.  (ZODB Connection is the only standard
resource manager that supports subtransactions.)  Resource managers
that do support subtransactions implement abort_sub() and commit_sub()
methods and support a second argument to tpc_begin().

The second argument to tpc_begin() indicates that a subtransaction
commit is beginning (if it is true).  In a subtransaction, there is no
tpc_vote() call, because sub-transactions don't need 2-phase commit.
If a sub-transaction abort or commit fails, we can abort the outer
transaction.  The tpc_finish() or tpc_abort() call applies just to
that subtransaction.

Once a resource manager is involved in a subtransaction, all
subsequent transactions will be treated as subtransactions until
abort_sub() or commit_sub() is called.  abort_sub() will undo all the
changes of the subtransactions.  commit_sub() will begin a top-level
transaction and store all the changes from subtransactions.  After
commit_sub(), the transaction must still call tpc_vote() and
tpc_finish().

If the resource manager does not support subtransactions, nothing
happens when the subtransaction commits.  Instead, the resource
manager is put on a list of managers to commit when the actual
top-level transaction commits.  If this happens, it will not be
possible to abort subtransactions.

Two-phase commit
----------------

A transaction commit involves an interaction between the transaction
object and one or more resource managers.  The transaction manager
calls the following four methods on each resource manager; it calls
tpc_begin() on each resource manager before calling commit() on any of
them.

    1. tpc_begin(txn)
    2. commit(txn)
    3. tpc_vote(txn)
    4. tpc_finish(txn)

Subtransaction commit
---------------------

Note: Subtransactions are deprecated!

When a subtransaction commits, the protocol is different.

1. tpc_begin() is passed a second argument, which indicates that a
   subtransaction is being committed.
2. tpc_vote() is not called.

Once a subtransaction has been committed, the top-level transaction
commit will start with a commit_sub() call instead of a tpc_begin()
call.

Before-commit hook
------------------

Sometimes, applications want to execute some code when a transaction is
committed.  For example, one might want to delay object indexing until a
transaction commits, rather than indexing every time an object is changed.
Or someone might want to check invariants only after a set of operations.  A
pre-commit hook is available for such use cases:  use addBeforeCommitHook(),
passing it a callable and arguments.  The callable will be called with its
arguments at the start of the commit (but not for substransaction commits).

After-commit hook
------------------

Sometimes, applications want to execute code after a transaction is
committed or aborted. For example, one might want to launch non
transactional code after a successful commit. Or still someone might
want to launch asynchronous code after.  A post-commit hook is
available for such use cases: use addAfterCommitHook(), passing it a
callable and arguments.  The callable will be called with a Boolean
value representing the status of the commit operation as first
argument (true if successfull or false iff aborted) preceding its
arguments at the start of the commit (but not for substransaction
commits).

Error handling
--------------

When errors occur during two-phase commit, the transaction manager
aborts all the resource managers.  The specific methods it calls
depend on whether the error occurs before or after the call to
tpc_vote() on that transaction manager.

If the resource manager has not voted, then the resource manager will
have one or more uncommitted objects.  There are two cases that lead
to this state; either the transaction manager has not called commit()
for any objects on this resource manager or the call that failed was a
commit() for one of the objects of this resource manager.  For each
uncommitted object, including the object that failed in its commit(),
call abort().

Once uncommitted objects are aborted, tpc_abort() or abort_sub() is
called on each resource manager.

Synchronization
---------------

You can register sychronization objects (synchronizers) with the
tranasction manager.  The synchronizer must implement
beforeCompletion() and afterCompletion() methods.  The transaction
manager calls beforeCompletion() when it starts a top-level two-phase
commit.  It calls afterCompletion() when a top-level transaction is
committed or aborted.  The methods are passed the current Transaction
as their only argument.
"""

import logging
import sys
import thread
import warnings
import weakref
import traceback
from cStringIO import StringIO

from zope import interface
from transaction import interfaces

# Sigh.  In the maze of __init__.py's, ZODB.__init__.py takes 'get'
# out of transaction.__init__.py, in order to stuff the 'get_transaction'
# alias in __builtin__.  So here in _transaction.py, we can't import
# exceptions from ZODB.POSException at top level (we're imported by
# our __init__.py, which is imported by ZODB's __init__, so the ZODB
# package isn't well-formed when we're first imported).
# from ZODB.POSException import TransactionError, TransactionFailedError

_marker = object()

# The point of this is to avoid hiding exceptions (which the builtin
# hasattr() does).
def myhasattr(obj, attr):
    return getattr(obj, attr, _marker) is not _marker

class Status:
    # ACTIVE is the initial state.
    ACTIVE       = "Active"

    COMMITTING   = "Committing"
    COMMITTED    = "Committed"

    # commit() or commit(True) raised an exception.  All further attempts
    # to commit or join this transaction will raise TransactionFailedError.
    COMMITFAILED = "Commit failed"

class Transaction(object):

    interface.implements(interfaces.ITransaction,
                         interfaces.ITransactionDeprecated)


    # Assign an index to each savepoint so we can invalidate later savepoints
    # on rollback.  The first index assigned is 1, and it goes up by 1 each
    # time.
    _savepoint_index = 0

    # If savepoints are used, keep a weak key dict of them.  This maps a
    # savepoint to its index (see above).
    _savepoint2index = None

    # Remember the savepoint for the last subtransaction.
    _subtransaction_savepoint = None

    # Meta data.  ._extension is also metadata, but is initialized to an
    # emtpy dict in __init__.
    user = ""
    description = ""

    def __init__(self, synchronizers=None, manager=None):
        self.status = Status.ACTIVE
        # List of resource managers, e.g. MultiObjectResourceAdapters.
        self._resources = []

        # Weak set of synchronizer objects to call.
        if synchronizers is None:
            from ZODB.utils import WeakSet
            synchronizers = WeakSet()
        self._synchronizers = synchronizers

        self._manager = manager

        # _adapters: Connection/_p_jar -> MultiObjectResourceAdapter[Sub]
        self._adapters = {}
        self._voted = {} # id(Connection) -> boolean, True if voted
        # _voted and other dictionaries use the id() of the resource
        # manager as a key, because we can't guess whether the actual
        # resource managers will be safe to use as dict keys.

        # The user, description, and _extension attributes are accessed
        # directly by storages, leading underscore notwithstanding.
        self._extension = {}

        self.log = logging.getLogger("txn.%d" % thread.get_ident())
        self.log.debug("new transaction")

        # If a commit fails, the traceback is saved in _failure_traceback.
        # If another attempt is made to commit, TransactionFailedError is
        # raised, incorporating this traceback.
        self._failure_traceback = None

        # List of (hook, args, kws) tuples added by addBeforeCommitHook().
        self._before_commit = []

        # List of (hook, args, kws) tuples added by addAfterCommitHook().
        self._after_commit = []

    # Raise TransactionFailedError, due to commit()/join()/register()
    # getting called when the current transaction has already suffered
    # a commit/savepoint failure.
    def _prior_operation_failed(self):
        from ZODB.POSException import TransactionFailedError
        assert self._failure_traceback is not None
        raise TransactionFailedError("An operation previously failed, "
                "with traceback:\n\n%s" %
                self._failure_traceback.getvalue())

    def join(self, resource):
        if self.status is Status.COMMITFAILED:
            self._prior_operation_failed() # doesn't return

        if self.status is not Status.ACTIVE:
            # TODO: Should it be possible to join a committing transaction?
            # I think some users want it.
            raise ValueError("expected txn status %r, but it's %r" % (
                             Status.ACTIVE, self.status))
        # TODO: the prepare check is a bit of a hack, perhaps it would
        # be better to use interfaces.  If this is a ZODB4-style
        # resource manager, it needs to be adapted, too.
        if myhasattr(resource, "prepare"):
            # TODO: deprecate 3.6
            resource = DataManagerAdapter(resource)
        self._resources.append(resource)

        if self._savepoint2index:
            # A data manager has joined a transaction *after* a savepoint
            # was created.  A couple of things are different in this case:
            #
            # 1. We need to add its savepoint to all previous savepoints.
            # so that if they are rolled back, we roll this one back too.
            #
            # 2. We don't actually need to ask the data manager for a
            # savepoint:  because it's just joining, we can just abort it to
            # roll back to the current state, so we simply use an
            # AbortSavepoint.
            datamanager_savepoint = AbortSavepoint(resource, self)
            for transaction_savepoint in self._savepoint2index.keys():
                transaction_savepoint._savepoints.append(
                    datamanager_savepoint)

    def savepoint(self, optimistic=False):
        if self.status is Status.COMMITFAILED:
            self._prior_operation_failed() # doesn't return, it raises

        try:
            savepoint = Savepoint(self, optimistic, *self._resources)
        except:
            self._cleanup(self._resources)
            self._saveAndRaiseCommitishError() # reraises!

        if self._savepoint2index is None:
            self._savepoint2index = weakref.WeakKeyDictionary()
        self._savepoint_index += 1
        self._savepoint2index[savepoint] = self._savepoint_index

        return savepoint

    # Remove and invalidate all savepoints we know about with an index
    # larger than `savepoint`'s.  This is what's needed when a rollback
    # _to_ `savepoint` is done.
    def _remove_and_invalidate_after(self, savepoint):
        savepoint2index = self._savepoint2index
        index = savepoint2index[savepoint]
        # use items() to make copy to avoid mutating while iterating
        for savepoint, i in savepoint2index.items():
            if i > index:
                savepoint.transaction = None # invalidate
                del savepoint2index[savepoint]

    # Invalidate and forget about all savepoints.
    def _invalidate_all_savepoints(self):
        for savepoint in self._savepoint2index.keys():
            savepoint.transaction = None # invalidate
        self._savepoint2index.clear()


    def register(self, obj):
        # The old way of registering transaction participants.
        #
        # register() is passed either a persisent object or a
        # resource manager like the ones defined in ZODB.DB.
        # If it is passed a persistent object, that object should
        # be stored when the transaction commits.  For other
        # objects, the object implements the standard two-phase
        # commit protocol.

        manager = getattr(obj, "_p_jar", obj)
        if manager is None:
            raise ValueError("Register with no manager")
        adapter = self._adapters.get(manager)
        if adapter is None:
            adapter = MultiObjectResourceAdapter(manager)
            adapter.objects.append(obj)
            self._adapters[manager] = adapter
            self.join(adapter)
        else:
            # TODO: comment out this expensive assert later
            # Use id() to guard against proxies.
            assert id(obj) not in map(id, adapter.objects)
            adapter.objects.append(obj)

    def commit(self, subtransaction=_marker, deprecation_wng=True):
        if subtransaction is _marker:
            subtransaction = 0
        elif deprecation_wng:
            from ZODB.utils import deprecated37
            deprecated37("subtransactions are deprecated; instead of "
                         "transaction.commit(1), use "
                         "transaction.savepoint(optimistic=True) in "
                         "contexts where a subtransaction abort will never "
                         "occur, or sp=transaction.savepoint() if later "
                         "rollback is possible and then sp.rollback() "
                         "instead of transaction.abort(1)")

        if self._savepoint2index:
            self._invalidate_all_savepoints()

        if subtransaction:
            # TODO deprecate subtransactions
            self._subtransaction_savepoint = self.savepoint(optimistic=True)
            return

        if self.status is Status.COMMITFAILED:
            self._prior_operation_failed() # doesn't return

        self._callBeforeCommitHooks()

        self._synchronizers.map(lambda s: s.beforeCompletion(self))
        self.status = Status.COMMITTING

        try:
            self._commitResources()
            self.status = Status.COMMITTED
        except:
            t, v, tb = self._saveAndGetCommitishError()
            self._callAfterCommitHooks(status=False)
            raise t, v, tb
        else:
            if self._manager:
                self._manager.free(self)
            self._synchronizers.map(lambda s: s.afterCompletion(self))
            self._callAfterCommitHooks(status=True)
        self.log.debug("commit")

    def _saveAndGetCommitishError(self):
        self.status = Status.COMMITFAILED
        # Save the traceback for TransactionFailedError.
        ft = self._failure_traceback = StringIO()
        t, v, tb = sys.exc_info()
        # Record how we got into commit().
        traceback.print_stack(sys._getframe(1), None, ft)
        # Append the stack entries from here down to the exception.
        traceback.print_tb(tb, None, ft)
        # Append the exception type and value.
        ft.writelines(traceback.format_exception_only(t, v))
        return t, v, tb

    def _saveAndRaiseCommitishError(self):
        t, v, tb = self._saveAndGetCommitishError()
        raise t, v, tb

    def getBeforeCommitHooks(self):
        return iter(self._before_commit)

    def addBeforeCommitHook(self, hook, args=(), kws=None):
        if kws is None:
            kws = {}
        self._before_commit.append((hook, tuple(args), kws))

    def beforeCommitHook(self, hook, *args, **kws):
        from ZODB.utils import deprecated38

        deprecated38("Use addBeforeCommitHook instead of beforeCommitHook.")
        self.addBeforeCommitHook(hook, args, kws)

    def _callBeforeCommitHooks(self):
        # Call all hooks registered, allowing further registrations
        # during processing.  Note that calls to addBeforeCommitHook() may
        # add additional hooks while hooks are running, and iterating over a
        # growing list is well-defined in Python.
        for hook, args, kws in self._before_commit:
            hook(*args, **kws)
        self._before_commit = []

    def getAfterCommitHooks(self):
        return iter(self._after_commit)

    def addAfterCommitHook(self, hook, args=(), kws=None):
        if kws is None:
            kws = {}
        self._after_commit.append((hook, tuple(args), kws))

    def _callAfterCommitHooks(self, status=True):
        # Avoid to abort anything at the end if no hooks are registred.
        if not self._after_commit:
            return
        # Call all hooks registered, allowing further registrations
        # during processing.  Note that calls to addAterCommitHook() may
        # add additional hooks while hooks are running, and iterating over a
        # growing list is well-defined in Python.
        for hook, args, kws in self._after_commit:
            # The first argument passed to the hook is a Boolean value,
            # true if the commit succeeded, or false if the commit aborted.
            try:
                hook(status, *args, **kws)
            except:
                # We need to catch the exceptions if we want all hooks
                # to be called
                self.log.error("Error in after commit hook exec in %s ",
                               hook, exc_info=sys.exc_info())
        # The transaction is already committed. It must not have
        # further effects after the commit.
        for rm in self._resources:
            try:
                rm.abort(self)
            except:
                # XXX should we take further actions here ?
                self.log.error("Error in abort() on manager %s",
                               rm, exc_info=sys.exc_info())
        self._after_commit = []
        self._before_commit = []

    def _commitResources(self):
        # Execute the two-phase commit protocol.

        L = list(self._resources)
        L.sort(rm_cmp)
        try:
            for rm in L:
                rm.tpc_begin(self)
            for rm in L:
                rm.commit(self)
                self.log.debug("commit %r" % rm)
            for rm in L:
                rm.tpc_vote(self)
                self._voted[id(rm)] = True

            try:
                for rm in L:
                    rm.tpc_finish(self)
            except:
                # TODO: do we need to make this warning stronger?
                # TODO: It would be nice if the system could be configured
                # to stop committing transactions at this point.
                self.log.critical("A storage error occurred during the second "
                                  "phase of the two-phase commit.  Resources "
                                  "may be in an inconsistent state.")
                raise
        except:
            # If an error occurs committing a transaction, we try
            # to revert the changes in each of the resource managers.
            t, v, tb = sys.exc_info()
            try:
                self._cleanup(L)
            finally:
                self._synchronizers.map(lambda s: s.afterCompletion(self))
            raise t, v, tb

    def _cleanup(self, L):
        # Called when an exception occurs during tpc_vote or tpc_finish.
        for rm in L:
            if id(rm) not in self._voted:
                try:
                    rm.abort(self)
                except Exception:
                    self.log.error("Error in abort() on manager %s",
                                   rm, exc_info=sys.exc_info())
        for rm in L:
            try:
                rm.tpc_abort(self)
            except Exception:
                self.log.error("Error in tpc_abort() on manager %s",
                               rm, exc_info=sys.exc_info())

    def abort(self, subtransaction=_marker, deprecation_wng=True):
        if subtransaction is _marker:
            subtransaction = 0
        elif deprecation_wng:
            from ZODB.utils import deprecated37
            deprecated37("subtransactions are deprecated; use "
                         "sp.rollback() instead of "
                         "transaction.abort(1), where `sp` is the "
                         "corresponding savepoint captured earlier")

        if subtransaction:
            # TODO deprecate subtransactions.
            if not self._subtransaction_savepoint:
                raise interfaces.InvalidSavepointRollbackError
            if self._subtransaction_savepoint.valid:
                self._subtransaction_savepoint.rollback()
                # We're supposed to be able to call abort(1) multiple
                # times without additional effect, so mark the subtxn
                # savepoint invalid now.
                self._subtransaction_savepoint.transaction = None
                assert not self._subtransaction_savepoint.valid
            return

        if self._savepoint2index:
            self._invalidate_all_savepoints()

        self._synchronizers.map(lambda s: s.beforeCompletion(self))

        tb = None
        for rm in self._resources:
            try:
                rm.abort(self)
            except:
                if tb is None:
                    t, v, tb = sys.exc_info()
                self.log.error("Failed to abort resource manager: %s",
                               rm, exc_info=sys.exc_info())

        if self._manager:
            self._manager.free(self)

        self._synchronizers.map(lambda s: s.afterCompletion(self))

        self.log.debug("abort")

        if tb is not None:
            raise t, v, tb

    def note(self, text):
        text = text.strip()
        if self.description:
            self.description += "\n\n" + text
        else:
            self.description = text

    def setUser(self, user_name, path="/"):
        self.user = "%s %s" % (path, user_name)

    def setExtendedInfo(self, name, value):
        self._extension[name] = value

# TODO: We need a better name for the adapters.

class MultiObjectResourceAdapter(object):
    """Adapt the old-style register() call to the new-style join().

    With join(), a resource mananger like a Connection registers with
    the transaction manager.  With register(), an individual object
    is passed to register().
    """

    def __init__(self, jar):
        self.manager = jar
        self.objects = []
        self.ncommitted = 0

    def __repr__(self):
        return "<%s for %s at %s>" % (self.__class__.__name__,
                                      self.manager, id(self))

    def sortKey(self):
        return self.manager.sortKey()

    def tpc_begin(self, txn):
        self.manager.tpc_begin(txn)

    def tpc_finish(self, txn):
        self.manager.tpc_finish(txn)

    def tpc_abort(self, txn):
        self.manager.tpc_abort(txn)

    def commit(self, txn):
        for o in self.objects:
            self.manager.commit(o, txn)
            self.ncommitted += 1

    def tpc_vote(self, txn):
        self.manager.tpc_vote(txn)

    def abort(self, txn):
        tb = None
        for o in self.objects:
            try:
                self.manager.abort(o, txn)
            except:
                # Capture the first exception and re-raise it after
                # aborting all the other objects.
                if tb is None:
                    t, v, tb = sys.exc_info()
                txn.log.error("Failed to abort object: %s",
                              object_hint(o), exc_info=sys.exc_info())
        if tb is not None:
            raise t, v, tb

def rm_cmp(rm1, rm2):
    return cmp(rm1.sortKey(), rm2.sortKey())

def object_hint(o):
    """Return a string describing the object.

    This function does not raise an exception.
    """

    from ZODB.utils import oid_repr

    # We should always be able to get __class__.
    klass = o.__class__.__name__
    # oid would be great, but may this isn't a persistent object.
    oid = getattr(o, "_p_oid", _marker)
    if oid is not _marker:
        oid = oid_repr(oid)
    return "%s oid=%s" % (klass, oid)

# TODO: deprecate for 3.6.
class DataManagerAdapter(object):
    """Adapt zodb 4-style data managers to zodb3 style

    Adapt transaction.interfaces.IDataManager to
    ZODB.interfaces.IPureDatamanager
    """

    # Note that it is pretty important that this does not have a _p_jar
    # attribute. This object will be registered with a zodb3 TM, which
    # will then try to get a _p_jar from it, using it as the default.
    # (Objects without a _p_jar are their own data managers.)

    def __init__(self, datamanager):
        self._datamanager = datamanager

    # TODO: I'm not sure why commit() doesn't do anything

    def commit(self, transaction):
        # We don't do anything here because ZODB4-style data managers
        # didn't have a separate commit step
        pass

    def abort(self, transaction):
        self._datamanager.abort(transaction)

    def tpc_begin(self, transaction):
        # We don't do anything here because ZODB4-style data managers
        # didn't have a separate tpc_begin step
        pass

    def tpc_abort(self, transaction):
        self._datamanager.abort(transaction)

    def tpc_finish(self, transaction):
        self._datamanager.commit(transaction)

    def tpc_vote(self, transaction):
        self._datamanager.prepare(transaction)

    def sortKey(self):
        return self._datamanager.sortKey()

class Savepoint:
    """Transaction savepoint.

    Transaction savepoints coordinate savepoints for data managers
    participating in a transaction.
    """
    interface.implements(interfaces.ISavepoint)

    valid = property(lambda self: self.transaction is not None)

    def __init__(self, transaction, optimistic, *resources):
        self.transaction = transaction
        self._savepoints = savepoints = []

        for datamanager in resources:
            try:
                savepoint = datamanager.savepoint
            except AttributeError:
                if not optimistic:
                    raise TypeError("Savepoints unsupported", datamanager)
                savepoint = NoRollbackSavepoint(datamanager)
            else:
                savepoint = savepoint()

            savepoints.append(savepoint)

    def rollback(self):
        transaction = self.transaction
        if transaction is None:
            raise interfaces.InvalidSavepointRollbackError
        transaction._remove_and_invalidate_after(self)

        try:
            for savepoint in self._savepoints:
                savepoint.rollback()
        except:
            # Mark the transaction as failed.
            transaction._saveAndRaiseCommitishError() # reraises!

class AbortSavepoint:

    def __init__(self, datamanager, transaction):
        self.datamanager = datamanager
        self.transaction = transaction

    def rollback(self):
        self.datamanager.abort(self.transaction)

class NoRollbackSavepoint:

    def __init__(self, datamanager):
        self.datamanager = datamanager

    def rollback(self):
        raise TypeError("Savepoints unsupported", self.datamanager)
