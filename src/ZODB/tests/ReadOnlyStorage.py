##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
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
from ZODB.POSException import ReadOnlyError, Unsupported
from ZODB.Transaction import Transaction

class ReadOnlyStorage:

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
        self.assert_(self._storage.isReadOnly())

    def checkReadMethods(self):
        self._create_data()
        self._make_readonly()
        # XXX not going to bother checking all read methods
        for oid in self.oids.keys():
            data, revid = self._storage.load(oid, '')
            self.assertEqual(revid, self.oids[oid])
            self.assert_(not self._storage.modifiedInVersion(oid))
            # Storages without revisions may not have loadSerial().
            try:
                _data = self._storage.loadSerial(oid, revid)
                self.assertEqual(data, _data)
            except Unsupported:
                pass

    def checkWriteMethods(self):
        self._make_readonly()
        self.assertRaises(ReadOnlyError, self._storage.new_oid)
        t = Transaction()
        self.assertRaises(ReadOnlyError, self._storage.tpc_begin, t)

        if self._storage.supportsVersions():
            self.assertRaises(ReadOnlyError, self._storage.abortVersion,
                              '', t)
            self.assertRaises(ReadOnlyError, self._storage.commitVersion,
                              '', '', t)

        self.assertRaises(ReadOnlyError, self._storage.store,
                          '\000' * 8, None, '', '', t)

        if self._storage.supportsTransactionalUndo():
            self.assertRaises(ReadOnlyError, self._storage.transactionalUndo,
                              '\000' * 8, t)
