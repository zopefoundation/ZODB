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

from cPickle import dumps, Pickler, Unpickler
from cStringIO import StringIO

from ZODB.FileStorage import FileStorage
from ZODB.zodb4 import z4iterator


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
        for record in self._txn:
            record.tid = record.serial
            # transform the data record format
            # (including persistent references)
            sio = StringIO(record.data)
            up = Unpickler(sio)
            up.persistent_load = PersistentIdentifier
            classmeta = up.load()
            state = up.load()
            sio = StringIO()
            p = Pickler(sio, 1)
            p.persistent_id = get_persistent_id
            p.dump(classmeta)
            p.dump(state)
            record.data = sio.getvalue()
            yield record


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
