##############################################################################
#
# Copyright (c) 2005 Zope Foundation and Contributors.
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
Trans #00000 tid=... time=... offset=<OFFSET>
    status=' ' user='' description='initial database creation'
  data #00000 oid=0000000000000000 size=<SIZE> class=persistent.mapping.PersistentMapping

Now we see first transaction with root object.

Let's add a BTree:

>>> root = db.open().root()
>>> root['tree'] = OOBTree()
>>> txn.get().note(u'added an OOBTree')
>>> txn.get().commit()
>>> fsdump(path) #doctest: +ELLIPSIS
Trans #00000 tid=... time=... offset=<OFFSET>
    status=' ' user='' description='initial database creation'
  data #00000 oid=0000000000000000 size=<SIZE> class=persistent.mapping.PersistentMapping
Trans #00001 tid=... time=... offset=<OFFSET>
    status=' ' user='' description='added an OOBTree'
  data #00000 oid=0000000000000000 size=<SIZE> class=persistent.mapping.PersistentMapping
  data #00001 oid=0000000000000001 size=<SIZE> class=BTrees.OOBTree.OOBTree...

Now we see two transactions and two changed objects.

Clean up.

>>> db.close()
"""  # noqa: E501 line too long

import doctest
import re

import zope.testing.setupstack
from zope.testing import renormalizing

import ZODB.tests.util


checker = renormalizing.RENormalizing([
    # Normalizing this makes diffs easier to read
    (re.compile(r'\btid=[0-9a-f]+\b'), 'tid=...'),
    (re.compile(r'\b\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d+\b'), '...'),
    # Python 3 produces larger pickles, even when we use zodbpickle :(
    # this changes all the offsets and sizes
    (re.compile(r'\bsize=[0-9]+\b'), 'size=<SIZE>'),
    (re.compile(r'\boffset=[0-9]+\b'), 'offset=<OFFSET>'),
])


def test_suite():
    return doctest.DocTestSuite(
        setUp=zope.testing.setupstack.setUpDirectory,
        tearDown=ZODB.tests.util.tearDown,
        optionflags=doctest.REPORT_NDIFF,
        checker=ZODB.tests.util.checker + checker)
