##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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
import logging
import sys

from zope.interface import implements

from transaction.interfaces import *
from transaction.txn import Transaction, Status, Set

# XXX need to change asserts of transaction status into explicit checks
# that raise some exception

# XXX need lots of error checking

class AbstractTransactionManager(object):
    # base class to provide commit logic
    # concrete class must provide logger attribute

    txn_factory = Transaction

    # XXX the methods below use assertions, but perhaps they should
    # check errors.  on the other hand, the transaction instances
    # do raise exceptions.

    def commit(self, txn):
        # commit calls _finishCommit() or abort()
        assert txn._status is Status.ACTIVE
        txn._status = Status.PREPARING
        self.logger.debug("%s: prepare", txn)
        try:
            for r in txn._resources:
                r.prepare(txn)
        except:
            txn._status = Status.FAILED
            raise
        txn._status = Status.PREPARED
        self._finishCommit(txn)

    def _finishCommit(self, txn):
        self.logger.debug("%s: commit", txn)
        try:
            for r in txn._resources:
                r.commit(txn)
            txn._status = Status.COMMITTED
        except:
            # An error occured during the second phase of 2PC.  We can
            # no longer guarantee the system is in a consistent state.
            # The best we can do is abort() all the resource managers
            # that haven't already committed and hope for the best.
            error = sys.exc_info()
            txn._status = Status.FAILED
            self.abort(txn)
            msg = ("Transaction failed during second phase of two-"
                   "phase commit")
            self.logger.critical(msg, exc_info=error)
            raise TransactionError(msg)

    def abort(self, txn):
        self.logger.debug("%s: abort", txn)
        assert txn._status in (Status.ACTIVE, Status.PREPARED, Status.FAILED,
                               Status.ABORTED)
        txn._status = Status.PREPARING
        for r in txn._resources:
            r.abort(txn)
        txn._status = Status.ABORTED

    def savepoint(self, txn):
        assert txn._status == Status.ACTIVE
        self.logger.debug("%s: savepoint", txn)
        return Rollback(txn, [r.savepoint(txn) for r in txn._resources])

class TransactionManager(AbstractTransactionManager):

    implements(ITransactionManager)

    def __init__(self):
        self.logger = logging.getLogger("txn")
        self._current = None
        self._suspended = Set()

    def get(self):
        if self._current is None:
            self._current = self.begin()
        return self._current

    def begin(self):
        if self._current is not None:
            self._current.abort()
        self._current = self.txn_factory(self)
        self.logger.debug("%s: begin", self._current)
        return self._current

    def commit(self, txn):
        super(TransactionManager, self).commit(txn)
        self._current = None

    def abort(self, txn):
        super(TransactionManager, self).abort(txn)
        self._current = None

    def suspend(self, txn):
        if self._current != txn:
            raise TransactionError("Can't suspend transaction because "
                                   "it is not active")
        self._suspended.add(txn)
        self._current = None

    def resume(self, txn):
        if self._current is not None:
            raise TransactionError("Can't resume while other "
                                   "transaction is active")
        self._suspended.remove(txn)
        self._current = txn

class Rollback(object):

    implements(IRollback)

    def __init__(self, txn, resources):
        self._txn = txn
        self._resources = resources

    def rollback(self):
        if self._txn.status() != Status.ACTIVE:
            raise IllegalStateError("rollback", self._txn.status())
        for r in self._resources:
            r.rollback()

# make the transaction manager visible to client code
import thread

class ThreadedTransactionManager(AbstractTransactionManager):

    # XXX Do we need locking on _pool or _suspend?

    # Most methods read and write pool based on the id of the current
    # thread, so they should never interfere with each other.

    # The suspend() and resume() methods modify the _suspend set,
    # but suspend() only adds a new thread.  The resume() method
    # does need a lock to prevent two different threads from resuming
    # the same transaction.

    implements(ITransactionManager)

    def __init__(self):
        self.logger = logging.getLogger("txn")
        self._pool = {}
        self._suspend = Set()
        self._lock = thread.allocate_lock()

    def get(self):
        tid = thread.get_ident()
        txn = self._pool.get(tid)
        if txn is None:
            txn = self.begin()
        return txn

    def begin(self):
        tid = thread.get_ident()
        txn = self._pool.get(tid)
        if txn is not None:
            txn.abort()
        txn = self.txn_factory(self)
        self._pool[tid] = txn
        return txn

    def _finishCommit(self, txn):
        tid = thread.get_ident()
        assert self._pool[tid] is txn
        super(ThreadedTransactionManager, self)._finishCommit(txn)
        del self._pool[tid]

    def abort(self, txn):
        tid = thread.get_ident()
        assert self._pool[tid] is txn
        super(ThreadedTransactionManager, self).abort(txn)
        del self._pool[tid]

    # XXX should we require that the transaction calling suspend()
    # be the one that is using the transaction?

    # XXX need to add locking to suspend() and resume()

    def suspend(self, txn):
        tid = thread.get_ident()
        if self._pool.get(tid) is txn:
            self._suspend.add(txn)
            del self._pool[tid]
        else:
            raise TransactionError("txn %s not owned by thread %s" %
                                   (txn, tid))

    def resume(self, txn):
        tid = thread.get_ident()
        if self._pool.get(tid) is not None:
            raise TransactionError("thread %s already has transaction" %
                                   tid)
        if txn not in self._suspend:
            raise TransactionError("unknown transaction: %s" % txn)
        self._suspend.remove(txn)
        self._pool[tid] = txn
