##############################################################################
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
##############################################################################
"""Test backwards compatibility for resource managers using register().

The transaction package supports several different APIs for resource
managers.  The original ZODB3 API was implemented by ZODB.Connection.
The Connection passed persistent objects to a Transaction's register()
method.  It's possible that third-party code also used this API, hence
these tests that the code that adapts the old interface to the current
API works.

These tests use a TestConnection object that implements the old API.
They check that the right methods are called and in roughly the right
order.

Common cases
------------

First, check that a basic transaction commit works.

>>> cn = TestConnection()
>>> cn.register(Object())
>>> cn.register(Object())
>>> cn.register(Object())
>>> transaction.commit()
>>> len(cn.committed)
3
>>> len(cn.aborted)
0
>>> cn.calls
['begin', 'vote', 'finish']

Second, check that a basic transaction abort works.  If the
application calls abort(), then the transaction never gets into the
two-phase commit.  It just aborts each object.

>>> cn = TestConnection()
>>> cn.register(Object())
>>> cn.register(Object())
>>> cn.register(Object())
>>> transaction.abort()
>>> len(cn.committed)
0
>>> len(cn.aborted)
3
>>> cn.calls
[]

Error handling
--------------

The tricky part of the implementation is recovering from an error that
occurs during the two-phase commit.  We override the commit() and
abort() methods of Object to cause errors during commit.

Note that the implementation uses lists internally, so that objects
are committed in the order they are registered.  (In the presence of
multiple resource managers, objects from a single resource manager are
committed in order.  The order of resource managers depends on
sortKey().)  I'm not sure if this is an accident of the implementation
or a feature that should be supported by any implementation.

>>> cn = TestConnection()
>>> cn.register(Object())
>>> cn.register(CommitError())
>>> cn.register(Object())
>>> transaction.commit()
Traceback (most recent call last):
 ...
RuntimeError: commit
>>> len(cn.committed)
1
>>> len(cn.aborted)
2

"""

import transaction

class Object(object):

    def commit(self):
        pass

    def abort(self):
        pass

class CommitError(Object):

    def commit(self):
        raise RuntimeError("commit")

class AbortError(Object):

    def abort(self):
        raise RuntimeError("abort")

class BothError(CommitError, AbortError):
    pass

class TestConnection:

    def __init__(self):
        self.committed = []
        self.aborted = []
        self.calls = []

    def register(self, obj):
        obj._p_jar = self
        transaction.get().register(obj)

    def sortKey(self):
        return str(id(self))

    def tpc_begin(self, txn, sub):
        self.calls.append("begin")

    def tpc_vote(self, txn):
        self.calls.append("vote")

    def tpc_finish(self, txn):
        self.calls.append("finish")

    def tpc_abort(self, txn):
        self.calls.append("abort")

    def commit(self, obj, txn):
        obj.commit()
        self.committed.append(obj)

    def abort(self, obj, txn):
        obj.abort()
        self.aborted.append(obj)

import doctest

def test_suite():
    return doctest.DocTestSuite()
