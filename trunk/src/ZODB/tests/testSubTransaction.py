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
ZODB subtransaction tests
=========================

Subtransactions are provided by a generic transaction interface, but
only supported by ZODB.  These tests verify that some of the important
cases work as expected.

>>> import transaction
>>> from ZODB import DB
>>> from ZODB.tests.test_storage import MinimalMemoryStorage
>>> from ZODB.tests.MinPO import MinPO

First create a few objects in the database root with a normal commit.
We're going to make a series of modifications to these objects.

>>> db = DB(MinimalMemoryStorage())
>>> cn = db.open()
>>> rt = cn.root()
>>> def init():
...     global a, b, c
...     a = rt["a"] = MinPO("a0")
...     b = rt["b"] = MinPO("b0")
...     c = rt["c"] = MinPO("c0")
...     transaction.commit()
>>> init()

We'll also open a second database connection and use it to verify that
the intermediate results of subtransactions are not visible to other
connections.

>>> cn2 = db.open(synch=False)
>>> rt2 = cn2.root()
>>> shadow_a = rt2["a"]
>>> shadow_b = rt2["b"]
>>> shadow_c = rt2["c"]

Subtransaction commit
---------------------

We'll make a series of modifications in subtransactions.

>>> a.value = "a1"
>>> b.value = "b1"
>>> transaction.commit(1)
>>> a.value, b.value
('a1', 'b1')
>>> shadow_a.value, shadow_b.value
('a0', 'b0')

>>> a.value = "a2"
>>> c.value = "c1"
>>> transaction.commit(1)
>>> a.value, c.value
('a2', 'c1')
>>> shadow_a.value, shadow_c.value
('a0', 'c0')

>>> a.value = "a3"
>>> transaction.commit(1)
>>> a.value
'a3'
>>> shadow_a.value
'a0'

>>> transaction.commit()

>>> a.value, b.value, c.value
('a3', 'b1', 'c1')

Subtransaction with nested abort
--------------------------------

>>> init()
>>> a.value = "a1"
>>> transaction.commit(1)

>>> b.value = "b1"
>>> transaction.commit(1)

A sub-transaction abort will undo current changes, reverting to the
database state as of the last sub-transaction commit.  There is
(apparently) no way to abort an already-committed subtransaction.

>>> c.value = "c1"
>>> transaction.abort(1)

Multiple aborts have no extra effect.

>>> transaction.abort(1)

>>> a.value, b.value, c.value
('a1', 'b1', 'c0')

>>> transaction.commit()
>>> a.value, b.value, c.value
('a1', 'b1', 'c0')

Subtransaction with top-level abort
-----------------------------------

>>> init()
>>> a.value = "a1"
>>> transaction.commit(1)

>>> b.value = "b1"
>>> transaction.commit(1)

A sub-transaction abort will undo current changes, reverting to the
database state as of the last sub-transaction commit.  There is
(apparently) no way to abort an already-committed subtransaction.

>>> c.value = "c1"
>>> transaction.abort(1)

>>> transaction.abort()
>>> a.value, b.value, c.value
('a0', 'b0', 'c0')

"""

import doctest

def test_suite():
    return doctest.DocTestSuite()
