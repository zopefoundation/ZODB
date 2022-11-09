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
    tid 0x... offset=<OFFSET> ...
        tid user=''
        tid description='initial database creation'
        new revision persistent.mapping.PersistentMapping at <OFFSET>
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
>>> txn.get().note(u'added an OOBTree')
>>> txn.get().commit()
>>> t = Tracer(path)
>>> t.register_oids(0, 1)
>>> t.run(); t.report() #doctest: +ELLIPSIS
oid 0x00 persistent.mapping.PersistentMapping 2 revisions
    tid 0x... offset=<OFFSET> ...
        tid user=''
        tid description='initial database creation'
        new revision persistent.mapping.PersistentMapping at <OFFSET>
    tid 0x... offset=<OFFSET> ...
        tid user=''
        tid description='added an OOBTree'
        new revision persistent.mapping.PersistentMapping at <OFFSET>
        references 0x01 BTrees.OOBTree.OOBTree... at <OFFSET>
oid 0x01 BTrees.OOBTree.OOBTree... 1 revision
    tid 0x... offset=<OFFSET> ...
        tid user=''
        tid description='added an OOBTree'
        new revision BTrees.OOBTree.OOBTree... at <OFFSET>
        referenced by 0x00 persistent.mapping.PersistentMapping at <OFFSET>

So there are two revisions of oid 0 now, and the second references oid 1.

One more, storing a reference in the BTree back to the root object:

>>> tree = root['tree']
>>> tree['root'] = root
>>> txn.get().note(u'circling back to the root')
>>> txn.get().commit()
>>> t = Tracer(path)
>>> t.register_oids(0, 1, 2)
>>> t.run(); t.report() #doctest: +ELLIPSIS
oid 0x00 persistent.mapping.PersistentMapping 2 revisions
    tid 0x... offset=<OFFSET> ...
        tid user=''
        tid description='initial database creation'
        new revision persistent.mapping.PersistentMapping at <OFFSET>
    tid 0x... offset=<OFFSET> ...
        tid user=''
        tid description='added an OOBTree'
        new revision persistent.mapping.PersistentMapping at <OFFSET>
        references 0x01 BTrees.OOBTree.OOBTree... at <OFFSET>
    tid 0x... offset=<OFFSET> ...
        tid user=''
        tid description='circling back to the root'
        referenced by 0x01 BTrees.OOBTree.OOBTree... at <OFFSET>
oid 0x01 BTrees.OOBTree.OOBTree... 2 revisions
    tid 0x... offset=<OFFSET> ...
        tid user=''
        tid description='added an OOBTree'
        new revision BTrees.OOBTree.OOBTree... at <OFFSET>
        referenced by 0x00 persistent.mapping.PersistentMapping at <OFFSET>
    tid 0x... offset=<OFFSET> ...
        tid user=''
        tid description='circling back to the root'
        new revision BTrees.OOBTree.OOBTree... at <OFFSET>
        references 0x00 persistent.mapping.PersistentMapping at <OFFSET>
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

import doctest
import re

from zope.testing import renormalizing

from .util import checker as util_checker
from .util import setUp
from .util import tearDown


checker = renormalizing.RENormalizing([
    # Normalizing this makes diffs easier to read
    (re.compile(r'\btid 0x[0-9a-f]+\b'), 'tid 0x...'),
    (re.compile(r'\b\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d+\b'), '...'),
    # Python 3 produces larger pickles, even when we use zodbpickle :(
    # this changes all the offsets and sizes
    (re.compile(r'\boffset=[0-9]+\b'), 'offset=<OFFSET>'),
    (re.compile(r'\bat [0-9]+'), 'at <OFFSET>'),
])


def test_suite():
    return doctest.DocTestSuite(setUp=setUp,
                                tearDown=tearDown,
                                checker=util_checker + checker,
                                optionflags=doctest.REPORT_NDIFF)
