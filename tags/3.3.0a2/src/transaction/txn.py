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

__metaclass__ = type

from threading import Lock

from transaction.interfaces import *
from zope.interface import implements

try:
    from sets import Set
except ImportError:

    class Set(dict):

        def add(self, k):
            self[k] = 1

        def remove(self, k):
            del self[k]

class Status:

    ACTIVE = "Active"
    PREPARING = "Preparing"
    PREPARED = "Prepared"
    FAILED = "Failed"
    COMMITTED = "Committed"
    ABORTING = "Aborting"
    ABORTED = "Aborted"
    SUSPENDED = "Suspended"

class Transaction:

    implements(ITransaction)

    def __init__(self, manager=None, parent=None):
        self._manager = manager
        self._parent = parent
        self._status = Status.ACTIVE
        self._suspend = None
        self._resources = Set()
        self._lock = Lock()

    def __repr__(self):
        return "<%s %X %s>" % (self.__class__.__name__, id(self), self._status)

    def begin(self, parent=None):
        """Begin a transaction.

        If parent is not None, it is the parent transaction for this one.
        """
        assert self._manager is not None
        if parent is not None:
            t = Transaction(self._manager, self)
            return t

    def commit(self):
        """Commit a transaction."""
        assert self._manager is not None
        if self._status != Status.ACTIVE:
            raise IllegalStateError("commit", self._status)
        self._manager.commit(self)

    def abort(self):
        """Rollback to initial state."""
        assert self._manager is not None
        if self._status == Status.ABORTED:
            return
        if self._status not in (Status.ACTIVE, Status.PREPARED, Status.FAILED):
            raise IllegalStateError("abort", self._status)
        self._manager.abort(self)

    def savepoint(self):
        """Save current progress and return a savepoint."""
        assert self._manager is not None
        if self._status != Status.ACTIVE:
            raise IllegalStateError("create savepoint", self._status)
        return self._manager.savepoint(self)

    def join(self, resource):
        """resource is participating in the transaction."""
        assert self._manager is not None
        if self._status != Status.ACTIVE:
            raise IllegalStateError("join", self._status)
        self._manager.logger.debug("%s join %s" % (self, resource))
        self._resources.add(resource)

    def status(self):
        """Return the status of the transaction."""
        return self._status

    def suspend(self):
        self._lock.acquire()
        try:
            if self._status == Status.SUSPENDED:
                raise IllegalStateError("suspend", self._status)
            self._manager.suspend(self)
            self._suspend = self._status
            self._status = Status.SUSPENDED
        finally:
            self._lock.release()

    def resume(self):
        self._lock.acquire()
        try:
            if self._status != Status.SUSPENDED:
                raise TransactionError("Can only resume suspended transaction")
            self._manager.resume(self)
            self._status = self._suspend
            self._suspend = None
        finally:
            self._lock.release()
