##############################################################################
#
# Copyright (c) 2004 Zope Foundation and Contributors.
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
r"""
Multi-version concurrency control tests
=======================================

Multi-version concurrency control (MVCC) exploits storages that store
multiple revisions of an object to avoid read conflicts.  Normally
when an object is read from the storage, its most recent revision is
read.  Under MVCC, an older revision may be read so that the transaction
sees a consistent view of the database.

ZODB guarantees execution-time consistency: A single transaction will
always see a consistent view of the database while it is executing.
If transaction A is running, has already read an object O1, and a
different transaction B modifies object O2, then transaction A can no
longer read the current revision of O2.  It must read the
version of O2 that is consistent with O1.

This note includes doctests that explain how MVCC is implemented (and
test that the implementation is correct).  The tests use a
MinimalMemoryStorage that implements MVCC support, but not much else.

***IMPORTANT***: The MVCC approach has changed since these tests were
originally written. The new approach is much simpler because we no
longer call load to get the current state of an object. We call
loadBefore instead, having gotten a transaction time at the start of a
transaction.  As a result, the rhythm of the tests is a little odd,
because we no longer need to probe a complex dance that doesn't exist any more.

>>> from ZODB.tests.test_storage import MinimalMemoryStorage
>>> from ZODB import DB
>>> st = MinimalMemoryStorage()
>>> db = DB(st)

We will use two different connections with different transaction managers
to make sure that the connections act independently, even though they'll
be run from a single thread.

>>> import transaction
>>> tm1 = transaction.TransactionManager()
>>> cn1 = db.open(transaction_manager=tm1)

The test will just use some MinPO objects.  The next few lines just
setup an initial database state.

>>> from ZODB.tests.MinPO import MinPO
>>> r = cn1.root()
>>> r["a"] = MinPO(1)
>>> tm1.get().commit() # make sure the OIDs get allocated sequentially
>>> r["b"] = MinPO(1)
>>> tm1.get().commit()

Now open a second connection.

>>> tm2 = transaction.TransactionManager()
>>> cn2 = db.open(transaction_manager=tm2)
>>> from ZODB.utils import p64, u64
>>> cn2._storage._start == p64(u64(st.lastTransaction()) + 1)
True
>>> txn_time2  = cn2._storage._start

Connection high-water mark
--------------------------

The ZODB Connection tracks a transaction high-water mark, which
bounds the latest transaction id that can be read by the current
transaction and still present a consistent view of the database.
Transactions with ids up to but not including the high-water mark
are OK to read.  At the beginning of a transaction, a connection
sets the high-water mark to just over the last transaction time the
storage has seen.

>>> cn = db.open()

>>> cn._storage._start == p64(u64(st.lastTransaction()) + 1)
True
>>> cn.db()._mvcc_storage.invalidate(p64(100), dict.fromkeys([1, 2]))
>>> cn._storage._start == p64(u64(st.lastTransaction()) + 1)
True
>>> cn.db()._mvcc_storage.invalidate(p64(200), dict.fromkeys([1, 2]))
>>> cn._storage._start == p64(u64(st.lastTransaction()) + 1)
True

A connection's high-water mark is set to the transaction id taken from
the first invalidation processed by the connection.  Transaction ids are
monotonically increasing, so the first one seen during the current
transaction remains the high-water mark for the duration of the
transaction.

We'd like simple abort and commit calls to make txn boundaries,
but that doesn't work unless an object is modified.  sync() will abort
a transaction and process invalidations.

>>> cn.sync()
>>> cn._storage._start == p64(u64(st.lastTransaction()) + 1)
True

Basic functionality
-------------------

The next bit of code includes a simple MVCC test.  One transaction
will modify "a."  The other transaction will then modify "b" and commit.

>>> r1 = cn1.root()
>>> r1["a"].value = 2
>>> tm1.get().commit()
>>> txn = db.lastTransaction()

The second connection already has its high-water mark set.

>>> cn2._storage._start == txn_time2
True

It is safe to read "b," because it was not modified by the concurrent
transaction.

>>> r2 = cn2.root()
>>> r2["b"]._p_serial < cn2._storage._start
True
>>> r2["b"].value
1
>>> r2["b"].value = 2

It is not safe, however, to read the current revision of "a" because
it was modified at the high-water mark.  If we read it, we'll get a
non-current version.

>>> r2["a"].value
1
>>> r2["a"]._p_serial < cn2._storage._start
True

We can confirm that we have a non-current revision by asking the
storage.

>>> db.storage.isCurrent(r2["a"]._p_oid, r2["a"]._p_serial)
False

It's possible to modify "a", but we get a conflict error when we
commit the transaction.

>>> r2["a"].value = 3
>>> tm2.get().commit() # doctest: +ELLIPSIS
Traceback (most recent call last):
 ...
ConflictError: database conflict error (oid 0x01, class ZODB.tests.MinPO...

>>> tm2.get().abort()

This example will demonstrate that we can commit a transaction if we only
modify current revisions.

>>> cn2._storage._start == p64(u64(st.lastTransaction()) + 1)
True
>>> txn_time2  = cn2._storage._start

>>> r1 = cn1.root()
>>> r1["a"].value = 3
>>> tm1.get().commit()
>>> txn = db.lastTransaction()
>>> cn2._storage._start == txn_time2
True

>>> r2["b"].value = r2["a"].value + 1
>>> r2["b"].value
3
>>> tm2.get().commit()
>>> cn2._storage._start == p64(u64(st.lastTransaction()) + 1)
True

Object cache
------------

A Connection keeps objects in its cache so that multiple database
references will always point to the same Python object.  At
transaction boundaries, objects modified by other transactions are
ghostified so that the next transaction doesn't see stale state.  We
need to be sure the non-current objects loaded by MVCC are always
ghosted.  It should be trivial, because MVCC is only used when an
invalidation has been received for an object.

First get the database back in an initial state.

>>> cn1.sync()
>>> r1["a"].value = 0
>>> r1["b"].value = 0
>>> tm1.get().commit()

>>> cn2.sync()
>>> r2["a"].value
0
>>> r2["b"].value = 1
>>> tm2.get().commit()

>>> r1["b"].value
0
>>> cn1.sync()  # cn2 modified 'b', so cn1 should get a ghost for b
>>> r1["b"]._p_state  # -1 means GHOST
-1

Closing the connection, committing a transaction, and aborting a transaction,
should all have the same effect on non-current objects in cache.

>>> def testit():
...     cn1.sync()
...     r1["a"].value = 0
...     r1["b"].value = 0
...     tm1.get().commit()
...     cn2.sync()
...     r2["b"].value = 1
...     tm2.get().commit()

>>> testit()
>>> r1["b"]._p_state  # 0 means UPTODATE, although note it's an older revision
0
>>> r1["b"].value
0
>>> r1["a"].value = 1
>>> tm1.get().commit()
>>> r1["b"]._p_state
-1

When a connection is closed, it is saved by the database.  It will be
reused by the next open() call (along with its object cache).

>>> testit()
>>> r1["a"].value = 1
>>> tm1.get().abort()
>>> cn1.close()
>>> cn3 = db.open()
>>> cn1 is cn3
True
>>> r1 = cn1.root()

Although "b" is a ghost in cn1 at this point (because closing a connection
has the same effect on non-current objects in the connection's cache as
committing a transaction), not every object is a ghost.  The root was in
the cache and was current, so our first reference to it doesn't return
a ghost.

>>> r1._p_state # UPTODATE
0
>>> r1["b"]._p_state # GHOST
-1


Interaction with Savepoints
---------------------------

Basically, making a savepoint shouldn't have any effect on what a thread
sees.  Before ZODB 3.4.1, the internal TmpStore used when savepoints are
pending didn't delegate all the methods necessary to make this work, so
we'll do a quick test of that here.  First get a clean slate:

>>> cn1.close(); cn2.close()
>>> cn1 = db.open(transaction_manager=tm1)
>>> r1 = cn1.root()
>>> r1["a"].value = 0
>>> r1["b"].value = 1
>>> tm1.commit()

Now modify "a", but not "b", and make a savepoint.

>>> r1["a"].value = 42
>>> sp = cn1.savepoint()

Over in the other connection, modify "b" and commit it.  This makes the
first connection's state for b "old".

>>> cn2 = db.open(transaction_manager=tm2)
>>> r2 = cn2.root()
>>> r2["a"].value, r2["b"].value  # shouldn't see the change to "a"
(0, 1)
>>> r2["b"].value = 43
>>> tm2.commit()
>>> r2["a"].value, r2["b"].value
(0, 43)

Now deactivate "b" in the first connection, and (re)fetch it.  The first
connection should still see 1, due to MVCC, but to get this old state
TmpStore needs to handle the loadBefore() method.

>>> r1["b"]._p_deactivate()

Before 3.4.1, the next line died with
    AttributeError: TmpStore instance has no attribute 'loadBefore'

>>> r1["b"]._p_state  # ghost
-1
>>> r1["b"].value
1

Just for fun, finish the commit and make sure both connections see the
same things now.

>>> tm1.commit()
>>> cn1.sync(); cn2.sync()
>>> r1["a"].value, r1["b"].value
(42, 43)
>>> r2["a"].value, r2["b"].value
(42, 43)

>>> db.close()

Late invalidation
-----------------

The combination of ZEO and MVCC used to add more complexity. That's
why ZODB no-longer calls load. :)

Rather than add all the complexity of ZEO to these tests, the
MinimalMemoryStorage has a hook.  We'll write a subclass that will
deliver an invalidation when it loads (or loadBefore's) an object.
The hook allows us to test the Connection code.

>>> class TestStorage(MinimalMemoryStorage):
...    def __init__(self):
...        self.hooked = {}
...        self.count = 0
...        super(TestStorage, self).__init__()
...    def registerDB(self, db):
...        self.db = db
...    def hook(self, oid, tid, version):
...        if oid in self.hooked:
...            self.db.invalidate(tid, {oid:1})
...            self.count += 1

We can execute this test with a single connection, because we're
synthesizing the invalidation that is normally generated by the second
connection.  We need to create two revisions so that there is a
non-current revision to load.

>>> ts = TestStorage()
>>> db = DB(ts)
>>> cn1 = db.open(transaction_manager=tm1)
>>> r1 = cn1.root()
>>> r1["a"] = MinPO(0)
>>> tm1.get().commit() # make sure the OIDs get allocated sequentially
>>> r1["b"] = MinPO(0)
>>> tm1.get().commit()
>>> r1["b"].value = 1
>>> tm1.get().commit()
>>> cn1.cacheMinimize()  # makes everything in cache a ghost

>>> oid = r1["b"]._p_oid
>>> ts.hooked[oid] = 1

This test is kinda screwy because it depends on an old approach that
has changed.  We'll hack the _txn_time to get the original expected
result, even though what's going on now is much simpler.

>>> cn1._storage._start = ts.lastTransaction()

Once the oid is hooked, an invalidation will be delivered the next
time it is activated.  The code below activates the object, then
confirms that the hook worked and that the old state was retrieved.

>>> oid in cn1._storage._invalidations
False
>>> r1["b"]._p_state
-1
>>> r1["b"]._p_activate()
>>> oid in cn1._storage._invalidations
True
>>> ts.count
1
>>> r1["b"].value
0

>>> db.close()

No earlier revision available
-----------------------------

We'll reuse the code from the example above, except that there will
only be a single revision of "b."  As a result, the attempt to
activate "b" will result in a ReadConflictError.

>>> ts = TestStorage()
>>> db = DB(ts)
>>> cn1 = db.open(transaction_manager=tm1)
>>> r1 = cn1.root()
>>> r1["a"] = MinPO(0)
>>> tm1.get().commit() # make sure the OIDs get allocated sequentially
>>> r1["b"] = MinPO(0)
>>> tm1.get().commit()
>>> cn1.cacheMinimize()  # makes everything in cache a ghost

>>> oid = r1["b"]._p_oid
>>> ts.hooked[oid] = 1

Again, once the oid is hooked, an invalidation will be delivered the next
time it is activated.  The code below activates the object, but unlike the
section above, this is no older state to retrieve.

>>> oid in cn1._storage._invalidations
False
>>> r1["b"]._p_state
-1
>>> cn1._storage._start = ts.lastTransaction()
>>> r1["b"]._p_activate() # doctest: +ELLIPSIS
Traceback (most recent call last):
 ...
ReadConflictError: ...

>>> db.close()
"""
import doctest
import re

from zope.testing import renormalizing


checker = renormalizing.RENormalizing([
    # Python 3 bytes add a "b".
    (re.compile("b('.*?')"), r"\1"),
    # Python 3 adds module name to exceptions.
    (re.compile("ZODB.POSException.ConflictError"), r"ConflictError"),
    (re.compile("ZODB.POSException.ReadConflictError"), r"ReadConflictError"),
])


def test_suite():
    return doctest.DocTestSuite(checker=checker)
