from ZODB.POSException import ReadOnlyError
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
        self.open(read_only=1)
        self.assert_(self._storage.isReadOnly())

    def checkReadMethods(self):
        self._create_data()
        self._make_readonly()
        # XXX not going to bother checking all read methods
        for oid in self.oids.keys():
            data, revid = self._storage.load(oid, '')
            self.assertEqual(revid, self.oids[oid])
            self.assert_(not self._storage.modifiedInVersion(oid))
            _data = self._storage.loadSerial(oid, revid)
            self.assertEqual(data, _data)

    def checkWriteMethods(self):
        self._make_readonly()
        self.assertRaises(ReadOnlyError, self._storage.new_oid)
        self.assertRaises(ReadOnlyError, self._storage.undo,
                          '\000' * 8)

        t = Transaction()
        self._storage.tpc_begin(t)
        self.assertRaises(ReadOnlyError, self._storage.abortVersion,
                          '', t)
        self._storage.tpc_abort(t)
        
        t = Transaction()
        self._storage.tpc_begin(t)
        self.assertRaises(ReadOnlyError, self._storage.commitVersion,
                          '', '', t)
        self._storage.tpc_abort(t)

        t = Transaction()
        self._storage.tpc_begin(t)
        self.assertRaises(ReadOnlyError, self._storage.store,
                          '\000' * 8, None, '', '', t)
        self._storage.tpc_abort(t)

        if self._storage.supportsTransactionalUndo():
            t = Transaction()
            self._storage.tpc_begin(t)
            self.assertRaises(ReadOnlyError, self._storage.transactionalUndo,
                              '\000' * 8, t)
            self._storage.tpc_abort(t)
            

