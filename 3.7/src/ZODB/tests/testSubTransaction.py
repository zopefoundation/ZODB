##############################################################################
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
##############################################################################
"""
ZODB subtransaction tests
=========================

Subtransactions are deprecated.  First we install a hook, to verify that
deprecation warnings are generated.

>>> hook = WarningsHook()
>>> hook.install()

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

The subtransaction commit should have generated a deprecation wng:

>>> len(hook.warnings)
1
>>> message, category, filename, lineno = hook.warnings[0]
>>> print message
This will be removed in ZODB 3.7:
subtransactions are deprecated; use transaction.savepoint() instead of \
transaction.commit(1)
>>> category.__name__
'DeprecationWarning'
>>> hook.clear()

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
>>> a.value, b.value, c.value
('a1', 'b1', 'c0')

The subtxn abort should also have generated a deprecation warning:

>>> len(hook.warnings)
1
>>> message, category, filename, lineno = hook.warnings[0]
>>> print message
This will be removed in ZODB 3.7:
subtransactions are deprecated; use sp.rollback() instead of \
transaction.abort(1), where `sp` is the corresponding savepoint \
captured earlier
>>> category.__name__
'DeprecationWarning'
>>> hook.clear()


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

We have to uninstall the hook so that other warnings don't get lost.

>>> len(hook.warnings)  # we don't expect we captured other warnings
0
>>> hook.uninstall()
"""

from ZODB.tests.warnhook import WarningsHook
from zope.testing import doctest

def test_suite():
    return doctest.DocTestSuite()
