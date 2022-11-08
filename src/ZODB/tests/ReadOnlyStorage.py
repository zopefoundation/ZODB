##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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
from ZODB.Connection import TransactionMetaData
from ZODB.POSException import ReadOnlyError
from ZODB.POSException import Unsupported
from ZODB.utils import load_current


class ReadOnlyStorage(object):

    def _create_data(self):
        # test a read-only storage that already has some data
        self.oids = {}
        for i in range(10):
            oid = self._storage.new_oid()
            revid = self._dostore(oid)
            self.oids[oid] = revid

    def _make_readonly(self):
        self._storage.close()
        self.open(read_only=True)
        self.assertTrue(self._storage.isReadOnly())

    def checkReadMethods(self):
        self._create_data()
        self._make_readonly()
        # Note that this doesn't check _all_ read methods.
        for oid in self.oids.keys():
            data, revid = load_current(self._storage, oid)
            self.assertEqual(revid, self.oids[oid])
            # Storages without revisions may not have loadSerial().
            try:
                _data = self._storage.loadSerial(oid, revid)
                self.assertEqual(data, _data)
            except Unsupported:
                pass

    def checkWriteMethods(self):
        self._make_readonly()
        self.assertRaises(ReadOnlyError, self._storage.new_oid)
        t = TransactionMetaData()
        self.assertRaises(ReadOnlyError, self._storage.tpc_begin, t)

        self.assertRaises(ReadOnlyError, self._storage.store,
                          b'\000' * 8, None, b'', '', t)

        self.assertRaises(ReadOnlyError, self._storage.undo,
                          b'\000' * 8, t)
