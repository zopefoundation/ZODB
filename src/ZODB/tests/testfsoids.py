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

r"""
fsoids test, of the workhorse fsoids.Trace class
================================================

Let's get a path to work with first.

>>> path = 'Data.fs'

More imports.

>>> import ZODB
>>> from ZODB.FileStorage import FileStorage
>>> import transaction as txn
>>> from BTrees.OOBTree import OOBTree
>>> from ZODB.FileStorage.fsoids import Tracer  # we're testing this

Create an empty FileStorage.

>>> st = FileStorage(path)

There's not a lot interesting in an empty DB!

>>> t = Tracer(path)
>>> t.register_oids(0x123456)
>>> t.register_oids(1)
>>> t.register_oids(0)
>>> t.run()
>>> t.report()
oid 0x00 <unknown> 0 revisions
    this oid was not defined (no data record for it found)
oid 0x01 <unknown> 0 revisions
    this oid was not defined (no data record for it found)
oid 0x123456 <unknown> 0 revisions
    this oid was not defined (no data record for it found)

That didn't tell us much, but does show that the specified oids are sorted
into increasing order.

Create a root object and try again:

>>> db = ZODB.DB(st) # yes, that creates a root object!
>>> t = Tracer(path)
>>> t.register_oids(0, 1)
>>> t.run(); t.report() #doctest: +ELLIPSIS
oid 0x00 persistent.mapping.PersistentMapping 1 revision
    tid 0x... offset=4 ...
        tid user=''
        tid description='initial database creation'
        new revision persistent.mapping.PersistentMapping at 52
oid 0x01 <unknown> 0 revisions
    this oid was not defined (no data record for it found)

So we see oid 0 has been used in our one transaction, and that it was created
there, and is a PersistentMapping.  4 is the file offset to the start of the
transaction record, and 52 is the file offset to the start of the data record
for oid 0 within this transaction.  Because tids are timestamps too, the
"..." parts vary across runs.  The initial line for a tid actually looks like
this:

    tid 0x035748597843b877 offset=4 2004-08-20 20:41:28.187000

Let's add a BTree and try again:

>>> root = db.open().root()
>>> root['tree'] = OOBTree()
>>> txn.get().note('added an OOBTree')
>>> txn.get().commit()
>>> t = Tracer(path)
>>> t.register_oids(0, 1)
>>> t.run(); t.report() #doctest: +ELLIPSIS
oid 0x00 persistent.mapping.PersistentMapping 2 revisions
    tid 0x... offset=4 ...
        tid user=''
        tid description='initial database creation'
        new revision persistent.mapping.PersistentMapping at 52
    tid 0x... offset=162 ...
        tid user=''
        tid description='added an OOBTree'
        new revision persistent.mapping.PersistentMapping at 201
        references 0x01 BTrees.OOBTree.OOBTree at 201
oid 0x01 BTrees.OOBTree.OOBTree 1 revision
    tid 0x... offset=162 ...
        tid user=''
        tid description='added an OOBTree'
        new revision BTrees.OOBTree.OOBTree at 350
        referenced by 0x00 persistent.mapping.PersistentMapping at 201

So there are two revisions of oid 0 now, and the second references oid 1.

One more, storing a reference in the BTree back to the root object:

>>> tree = root['tree']
>>> tree['root'] = root
>>> txn.get().note('circling back to the root')
>>> txn.get().commit()
>>> t = Tracer(path)
>>> t.register_oids(0, 1, 2)
>>> t.run(); t.report() #doctest: +ELLIPSIS
oid 0x00 persistent.mapping.PersistentMapping 2 revisions
    tid 0x... offset=4 ...
        tid user=''
        tid description='initial database creation'
        new revision persistent.mapping.PersistentMapping at 52
    tid 0x... offset=162 ...
        tid user=''
        tid description='added an OOBTree'
        new revision persistent.mapping.PersistentMapping at 201
        references 0x01 BTrees.OOBTree.OOBTree at 201
    tid 0x... offset=429 ...
        tid user=''
        tid description='circling back to the root'
        referenced by 0x01 BTrees.OOBTree.OOBTree at 477
oid 0x01 BTrees.OOBTree.OOBTree 2 revisions
    tid 0x... offset=162 ...
        tid user=''
        tid description='added an OOBTree'
        new revision BTrees.OOBTree.OOBTree at 350
        referenced by 0x00 persistent.mapping.PersistentMapping at 201
    tid 0x... offset=429 ...
        tid user=''
        tid description='circling back to the root'
        new revision BTrees.OOBTree.OOBTree at 477
        references 0x00 persistent.mapping.PersistentMapping at 477
oid 0x02 <unknown> 0 revisions
    this oid was not defined (no data record for it found)

Note that we didn't create any new object there (oid 2 is still unused), we
just made oid 1 refer to oid 0.  Therefore there's a new "new revision" line
in the output for oid 1.  Note that there's also new output for oid 0, even
though the root object didn't change:  we got new output for oid 0 because
it's a traced oid and the new transaction made a new reference *to* it.

Since the Trace constructor takes only one argument, the only sane thing
you can do to make it fail is to give it a path to a file that doesn't
exist:

>>> Tracer('/eiruowieuu/lsijflfjlsijflsdf/eurowiurowioeuri/908479287.fs')
Traceback (most recent call last):
  ...
ValueError: must specify an existing FileStorage

You get the same kind of exception if you pass it a path to an existing
directory (the path must be to a file, not a directory):

>>> import os
>>> Tracer(os.path.dirname(__file__))
Traceback (most recent call last):
  ...
ValueError: must specify an existing FileStorage


Clean up.
>>> st.close()
>>> st.cleanup() # remove .fs, .index, etc
"""

from zope.testing import doctest

def test_suite():
    return doctest.DocTestSuite()
