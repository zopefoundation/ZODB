##############################################################################
#
# Copyright (c) 2005 Zope Corporation and Contributors.
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
fsdump test
===========

Let's get a path to work with first.

>>> path = 'Data.fs'

More imports.

>>> import ZODB
>>> from ZODB.FileStorage import FileStorage
>>> import transaction as txn
>>> from BTrees.OOBTree import OOBTree
>>> from ZODB.FileStorage.fsdump import fsdump  # we're testing this

Create an empty FileStorage.

>>> st = FileStorage(path)

For empty DB fsdump() output definitely empty:

>>> fsdump(path)

Create a root object and try again:

>>> db = ZODB.DB(st) # yes, that creates a root object!
>>> fsdump(path) #doctest: +ELLIPSIS
Trans #00000 tid=... time=... offset=52
    status=' ' user='' description='initial database creation'
  data #00000 oid=0000000000000000 size=60 class=persistent.mapping.PersistentMapping

Now we see first transaction with root object.

Let's add a BTree:

>>> root = db.open().root()
>>> root['tree'] = OOBTree()
>>> txn.get().note('added an OOBTree')
>>> txn.get().commit()
>>> fsdump(path) #doctest: +ELLIPSIS
Trans #00000 tid=... time=... offset=52
    status=' ' user='' description='initial database creation'
  data #00000 oid=0000000000000000 size=60 class=persistent.mapping.PersistentMapping
Trans #00001 tid=... time=... offset=201
    status=' ' user='' description='added an OOBTree'
  data #00000 oid=0000000000000000 size=107 class=persistent.mapping.PersistentMapping
  data #00001 oid=0000000000000001 size=29 class=BTrees.OOBTree.OOBTree

Now we see two transactions and two changed objects.

Clean up.

>>> st.close()
"""

import doctest
import zope.testing.setupstack

def test_suite():
    return doctest.DocTestSuite(
        setUp=zope.testing.setupstack.setUpDirectory,
        tearDown=zope.testing.setupstack.tearDown)
