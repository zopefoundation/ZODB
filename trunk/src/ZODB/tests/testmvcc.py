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
r"""
Multi-version concurrency control tests
=======================================

Multi-version concurrency control (MVCC) exploits storages that store
multiple revisions of an object to avoid read conflicts.  Normally
when an object is read from the storage, its most recent revisions is
read.  Under MVCC, an older revision is read so that the transaction
sees a consistent view of the database.

ZODB guarantees execution-time consistency: A single transaction will
always see a consistent view of the database while it is executing.
If transaction A is running, has already read an object O1, and an
external transaction B modifies object O2, then transaction A can no
longer read the current revision of O2.  It must either read the
version of O2 that is consistent with O1 or raise a ReadConflictError.

This note includes doctests that explain how MVCC is implemented (and
test that the implementation is correct).  The tests use a
MinimalMemoryStorage that implements MVCC support, but not much else.

>>> from ZODB.tests.test_storage import MinimalMemoryStorage
>>> from ZODB import DB
>>> db = DB(MinimalMemoryStorage())

We will use two different connections with the experimental
setLocalTransaction() method to make sure that the connections act
independently, even though they'll be run from a single thread.

>>> cn1 = db.open()
>>> txn1 = cn1.setLocalTransaction()

The test will just use some MinPO objects.  The next few lines just
setup an initial database state.

>>> from ZODB.tests.MinPO import MinPO
>>> r = cn1.root()
>>> r["a"] = MinPO(1)
>>> r["b"] = MinPO(1)
>>> txn1.commit()

Now open a second connection.

>>> cn2 = db.open()
>>> txn2 = cn2.setLocalTransaction()

The ZODB Connection tracks a transaction high-water mark, which
represents the latest transaction id that can be read by the current
transaction and still present a consistent view of the database.  When
a transaction commits, the database sends invalidations to all the
other transactions; the invalidation contains the transaction id and
the oids of modified objects.  The Connection stores the high-water
mark in _txn_time, which is set to None until an invalidation arrives.

>>> cn = db.open()

>>> cn._txn_time
>>> cn.invalidate(1, dict.fromkeys([1, 2]))
>>> cn._txn_time
1
>>> cn.invalidate(2, dict.fromkeys([1, 2]))
>>> cn._txn_time
1

The high-water mark is set to the transaction id of the first
transaction, because transaction ids must be monotonically increasing.
It is reset at transaction boundaries.

XXX We'd like simple abort and commit calls to make txn boundaries,
but that doesn't work unless an object is modified.  sync() will abort
a transaction and process invalidations.

>>> cn.sync()
>>> cn._txn_time

The next bit of code includes a simple MVCC test.  One transaction
will begin and modify "a."  The other transaction will then modify "b"
and commit.

>>> r1 = cn1.root()
>>> r1["a"].value = 2

>>> cn1.getTransaction().commit()
>>> txn = db.lastTransaction()

The second connection has its high-water mark set now.

>>> cn2._txn_time == txn
True

It is safe to read "b," because it was not modified by the concurrent
transaction. 

>>> r2 = cn2.root()
>>> r2["b"]._p_serial < cn2._txn_time
True
>>> r2["b"].value = 2

It is not safe, however, to read the current revision "a," because it
was modified at the high-water mark.  If we read it, we'll get a
non-current version.

>>> r2["a"].value
1
>>> r2["a"]._p_serial < cn2._txn_time
True

We can confirm that we have a non-current revision by asking the
storage.

>>> db._storage.isCurrent(r2["a"]._p_oid, r2["a"]._p_serial)
False

It's possible to modify "a," but we get a conflict error when we
commit the transaction.

>>> r2["a"].value = 3
>>> txn2.commit()
Traceback (most recent call last):
 ...
ConflictError: database conflict error (oid 0000000000000001, class ZODB.tests.MinPO.MinPO)

"""

import doctest

def test_suite():
    return doctest.DocTestSuite()
