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
"""Nitty-gritty conversion of a ZODB 4 FileStorage to a ZODB 3 FileStorage."""

import sys
from cPickle import Pickler, Unpickler
from cStringIO import StringIO

from ZODB.FileStorage import FileStorage
from ZODB.zodb4 import z4iterator

errors = {}
skipped = 0

class Conversion:

    def __init__(self, input_path, output_path):
        """Initialize a ZODB4->ZODB3 FileStorage converter."""
        self.instore = IterableFileIterator(input_path)
        self.outstore = FileStorage(output_path)

    def run(self):
        self.instore._read_metadata()
        self.outstore.copyTransactionsFrom(self.instore)
        self.outstore.close()
        self.instore.close()
        if errors:
            sys.stderr.write(error_explanation)
            sys.stderr.write("%s database records skipped\n" % skipped)


class IterableFileIterator(z4iterator.FileIterator):

    def iterator(self):
        return self

    def __iter__(self):
        baseiter = z4iterator.FileIterator.__iter__(self)
        for txn in baseiter:
            yield DataRecordConvertingTxn(txn)


class DataRecordConvertingTxn(object):

    def __init__(self, txn):
        self._txn = txn
        self.user = str8(txn.user)
        self.description = str8(txn.description)

    def __getattr__(self, name):
        return getattr(self._txn, name)

    def __iter__(self):
        global skipped
        for record in self._txn:
            record.tid = record.serial
            # transform the data record format
            # (including persistent references)
            sio = StringIO(record.data)
            up = Unpickler(sio)
            up.persistent_load = PersistentIdentifier
            try:
                classmeta = up.load()
                state = up.load()
            except Exception, v:
                v = str(v)
                if v not in errors:
                    if not errors:
                        sys.stderr.write("Pickling errors:\n\n")
                    sys.stderr.write('\t'+v+'\n\n')
                    errors[v] = True

                skipped += 1
                continue
                
            sio = StringIO()
            p = Pickler(sio, 1)
            p.persistent_id = get_persistent_id
            p.dump(classmeta)
            p.dump(state)
            record.data = sio.getvalue()
            yield record


error_explanation = """
There were errors while copying data records.

If the errors were import errors, then this is because modules
referenced by the database couldn't be found.  You might be able to
fix this by getting the necessary modules.  It's possible that the
affected objects aren't used any more, in which case, it doesn't
matter whether they were copied.  (We apologise for the lame import
errors that don't show full dotted module names.)

If errors looked something like:

  "('object.__new__(SomeClass) is not safe, use
   persistent.Persistent.__new__()', <function _reconstructor at
   0x4015ccdc>, (<class 'somemodule.SomeClass'>, <type 'object'>,
   None))",

then the error arises from data records for objects whos classes
changed from being non-persistent to being persistent.

If other errors were reported, it would be a good idea to ask about
them on zope3-dev.

In any case, keep your original data file in case you decide to rerun
the conversion.
 
"""


class PersistentIdentifier:
    def __init__(self, ident):
        if isinstance(ident, tuple):
            self._oid, (self._class, args) = ident
            if args:
                # we have args from __getnewargs__(), but can just
                # lose them since they're an optimization to allow
                # ghost construction
                self._class = None
        else:
            assert isinstance(ident, str)
            self._oid = ident
            self._class = None


def get_persistent_id(ob):
    if isinstance(ob, PersistentIdentifier):
        if ob._class is None:
            return ob._oid
        else:
            return ob._oid, ob._class
    else:
        return None


def str8(s):
    # convert unicode strings to 8-bit strings
    if isinstance(s, unicode):
        # Should we use UTF-8 or ASCII?  Not sure.
        return s.encode("ascii")
    else:
        return s
