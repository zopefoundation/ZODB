############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
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
registers its _p_jar attribute.  XXX explain adapter

Subtransactions
---------------

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
tpc_vote() call.  (XXX I don't have any idea why.)  The tpc_finish()
or tpc_abort() call applies just to that subtransaction.

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

    1. tpc_begin()
    2. commit()
    3. tpc_vote()
    4. tpc_finish()

Subtransaction commit
---------------------

When a subtransaction commits, the protocol is different.

1. tpc_begin() is passed a second argument, which indicates that a
   subtransaction is begin committed.
2. tpc_vote() is not called.

Once a subtransaction has been committed, the top-level transaction
commit will start with a commit_sub() called instead of a tpc_begin()
call.

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
called on each resource manager.  abort_sub() is called if the
resource manager was involved in a subtransaction.

Synchronization
---------------

You can register sychronization objects (synchronizers) with the
tranasction manager.  The synchronizer must implement
beforeCompletion() and afterCompletion() methods.  The transaction
manager calls beforeCompletion() when it starts a top-level two-phase
commit.  It calls afterCompletion() when a top-level transaction is
committed or aborted.  The methods are passed the current Transaction
as their only argument.

XXX This code isn't tested.

"""

import logging
import sys
import thread

_marker = object()

# The point of this is to avoid hiding exceptions (which the builtin
# hasattr() does).
def myhasattr(obj, attr):
    return getattr(obj, attr, _marker) is not _marker

class Status:

    ACTIVE = "Active"
    COMMITTING = "Committing"
    COMMITTED = "Committed"
    ABORTING = "Aborting"
    ABORTED = "Aborted"
    FAILED = "Failed"

class Transaction(object):

    def __init__(self, synchronizers=None, manager=None):
        self.status = Status.ACTIVE
        # List of resource managers, e.g. MultiObjectResourceAdapters.
        self._resources = []
        self._synchronizers = synchronizers or []
        self._manager = manager
        # _adapters: Connection/_p_jar -> MultiObjectResourceAdapter[Sub]
        self._adapters = {}
        self._voted = {} # id(Connection) -> boolean, True if voted
        # _voted and other dictionaries use the id() of the resource
        # manager as a key, because we can't guess whether the actual
        # resource managers will be safe to use as dict keys.

        # The user, description, and _extension attributes are accessed
        # directly by storages, leading underscore notwithstanding.
        self.user = ""
        self.description = ""
        self._extension = {}

        self.log = logging.getLogger("txn.%d" % thread.get_ident())
        self.log.debug("new transaction")

        # _sub contains all of the resource managers involved in
        # subtransactions.  It maps id(a resource manager) to the resource
        # manager.
        self._sub = {}
        # _nonsub contains all the resource managers that do not support
        # subtransactions that were involved in subtransaction commits.
        self._nonsub = {}

    def join(self, resource):
        if self.status != Status.ACTIVE:
            # XXX Should it be possible to join a committing transaction?
            # I think some users want it.
            raise ValueError("expected txn status %r, but it's %r" % (
                             Status.ACTIVE, self.status))
        # XXX the prepare check is a bit of a hack, perhaps it would
        # be better to use interfaces.  If this is a ZODB4-style
        # resource manager, it needs to be adapted, too.
        if myhasattr(resource, "prepare"):
            resource = DataManagerAdapter(resource)
        self._resources.append(resource)

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
        adapter = self._adapters.get(manager)
        if adapter is None:
            if myhasattr(manager, "commit_sub"):
                adapter = MultiObjectResourceAdapterSub(manager)
            else:
                adapter = MultiObjectResourceAdapter(manager)
            adapter.objects.append(obj)
            self._adapters[manager] = adapter
            self.join(adapter)
        else:
            # XXX comment out this expensive assert later
            # Use id() to guard against proxies.
            assert id(obj) not in map(id, adapter.objects)
            adapter.objects.append(obj)

            # In the presence of subtransactions, an existing adapter
            # might be in _adapters but not in _resources.
            if adapter not in self._resources:
                self._resources.append(adapter)

    def begin(self):
        # XXX I'm not sure how this should be implemented.  Not doing
        # anything now, but my best guess is: If nothing has happened
        # yet, it's fine.  Otherwise, abort this transaction and let
        # the txn manager create a new one.
        pass

    def commit(self, subtransaction=False):
        if not subtransaction and self._sub and self._resources:
            # This commit is for a top-level transaction that has
            # previously committed subtransactions.  Do one last
            # subtransaction commit to clear out the current objects,
            # then commit all the subjars.
            self.commit(True)

        if not subtransaction:
            for s in self._synchronizers:
                s.beforeCompletion()

        if not subtransaction:
            self.status = Status.COMMITTING

        self._commitResources(subtransaction)

        if subtransaction:
            self._resources = []
        else:
            self.status = Status.COMMITTED
            if self._manager:
                self._manager.free(self)
            for s in self._synchronizers:
                s.afterCompletion()
            self.log.debug("commit")

    def _commitResources(self, subtransaction):
        # Execute the two-phase commit protocol.

        L = self._getResourceManagers(subtransaction)
        try:
            for rm in L:
                # If you pass subtransaction=True to tpc_begin(), it
                # will create a temporary storage for the duration of
                # the transaction.  To signal that the top-level
                # transaction is committing, you must then call
                # commit_sub().
                if not subtransaction and id(rm) in self._sub:
                    del self._sub[id(rm)]
                    rm.commit_sub(self)
                else:
                    rm.tpc_begin(self, subtransaction)
            for rm in L:
                rm.commit(self)
                self.log.debug("commit %r" % rm)
            if not subtransaction:
                # Not sure why, but it is intentional that you do not
                # call tpc_vote() for subtransaction commits.
                for rm in L:
                    rm.tpc_vote(self)
                    self._voted[id(rm)] = True

            try:
                for rm in L:
                    rm.tpc_finish(self)
            except:
                # XXX do we need to make this warning stronger?
                # XXX It would be nice if the system could be configured
                # to stop committing transactions at this point.
                self.log.critical("A storage error occured during the second "
                                  "phase of the two-phase commit.  Resources "
                                  "may be in an inconsistent state.")
                raise
        except:
            # If an error occurs committing a transaction, we try
            # to revert the changes in each of the resource managers.
            # For top-level transactions, it must be freed from the
            # txn manager.
            t, v, tb = sys.exc_info()
            try:
                self._cleanup(L)
            finally:
                if not subtransaction:
                    self.status = Status.FAILED
                    if self._manager:
                        self._manager.free(self)
            raise t, v, tb

    def _cleanup(self, L):
        # Called when an exception occurs during tpc_vote or tpc_finish.
        for rm in L:
            if id(rm) not in self._voted:
                rm.cleanup(self)
        for rm in L:
            if id(rm) in self._sub:
                try:
                    rm.abort_sub(self)
                except Exception:
                    self.log.error("Error in abort_sub() on manager %s",
                                   rm, exc_info=sys.exc_info())
            else:
                try:
                    rm.tpc_abort(self)
                except Exception:
                    self.log.error("Error in tpc_abort() on manager %s",
                                   rm, exc_info=sys.exc_info())

    def _getResourceManagers(self, subtransaction):
        L = []
        if subtransaction:
            # If we are in a subtransaction, make sure all resource
            # managers are placed in either _sub or _nonsub.  When
            # the top-level transaction commits, we need to merge
            # these back into the resource set.

            # If a data manager doesn't support sub-transactions, we
            # don't do anything with it now.  (That's somewhat okay,
            # because subtransactions are mostly just an
            # optimization.)  Save it until the top-level transaction
            # commits.

            for rm in self._resources:
                if myhasattr(rm, "commit_sub"):
                    self._sub[id(rm)] = rm
                    L.append(rm)
                else:
                    self._nonsub[id(rm)] = rm
        else:
            if self._sub or self._nonsub:
                # Merge all of _sub, _nonsub, and _resources.
                d = dict(self._sub)
                d.update(self._nonsub)
                # XXX I think _sub and _nonsub are disjoint, and that
                # XXX _resources is empty.  If so, we can simplify this code.
                assert len(d) == len(self._sub) + len(self._nonsub)
                assert not self._resources
                for rm in self._resources:
                    d[id(rm)] = rm
                L = d.values()
            else:
                L = list(self._resources)

        L.sort(rm_cmp)
        return L

    def abort(self, subtransaction=False):
        if not subtransaction:
            for s in self._synchronizers:
                s.beforeCompletion(self)

        if subtransaction and self._nonsub:
            raise TransactionError("Resource manager does not support "
                                   "subtransaction abort")

        tb = None
        for rm in self._resources + self._nonsub.values():
            try:
                rm.abort(self)
            except:
                if tb is None:
                    t, v, tb = sys.exc_info()
                self.log.error("Failed to abort resource manager: %s",
                               rm, exc_info=sys.exc_info())

        if not subtransaction:
            for rm in self._sub.values():
                try:
                    rm.abort_sub(self)
                except:
                    if tb is None:
                        t, v, tb = sys.exc_info()
                    self.log.error("Failed to abort_sub resource manager: %s",
                                   rm, exc_info=sys.exc_info())

        if not subtransaction:
            if self._manager:
                self._manager.free(self)
            for s in self._synchronizers:
                s.afterCompletion(self)
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

# XXX We need a better name for the adapters.

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

    def tpc_begin(self, txn, sub=False):
        self.manager.tpc_begin(txn, sub)

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

    def cleanup(self, txn):
        self._abort(self.objects[self.ncommitted:], txn)

    def abort(self, txn):
        self._abort(self.objects, txn)

    def _abort(self, objects, txn):
        tb = None
        for o in objects:
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

class MultiObjectResourceAdapterSub(MultiObjectResourceAdapter):
    """Adapt resource managers that participate in subtransactions."""

    def commit_sub(self, txn):
        self.manager.commit_sub(txn)

    def abort_sub(self, txn):
        self.manager.abort_sub(txn)

    def tpc_begin(self, txn, sub=False):
        self.manager.tpc_begin(txn, sub)
        self.sub = sub

    def tpc_finish(self, txn):
        self.manager.tpc_finish(txn)
        if self.sub:
            self.objects = []


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
        self._rollback = None

    # XXX I'm not sure why commit() doesn't do anything

    def commit(self, transaction):
        pass

    def abort(self, transaction):

        # We need to discard any changes since the last save point, or all
        # changes

        if self._rollback is None:
            # No previous savepoint, so just abort
            self._datamanager.abort(transaction)
        else:
            self._rollback()

    def abort_sub(self, transaction):
        self._datamanager.abort(transaction)

    def commit_sub(self, transaction):
        # Nothing to do wrt data, be we begin 2pc for the top-level
        # trans
        self._sub = False

    def tpc_begin(self, transaction, subtransaction=False):
        self._sub = subtransaction

    def tpc_abort(self, transaction):
        if self._sub:
            self.abort(self, transaction)
        else:
            self._datamanager.abort(transaction)

    def tpc_finish(self, transaction):
        if self._sub:
            self._rollback = self._datamanager.savepoint(transaction).rollback
        else:
            self._datamanager.commit(transaction)

    def tpc_vote(self, transaction):
        if not self._sub:
            self._datamanager.prepare(transaction)
