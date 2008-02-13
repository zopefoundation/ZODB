##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
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
import os, unittest
import transaction
import ZODB.FileStorage
import ZODB.tests.util
from ZODB import POSException
from ZODB import DB

from ZODB.tests import StorageTestBase, BasicStorage, TransactionalUndoStorage
from ZODB.tests import PackableStorage, Synchronization, ConflictResolution
from ZODB.tests import HistoryStorage, IteratorStorage, Corruption
from ZODB.tests import RevisionStorage, PersistentStorage, MTStorage
from ZODB.tests import ReadOnlyStorage, RecoveryStorage
from ZODB.tests.StorageTestBase import MinPO, zodb_pickle

class BaseFileStorageTests(StorageTestBase.StorageTestBase):

    def open(self, **kwargs):
        self._storage = ZODB.FileStorage.FileStorage('FileStorageTests.fs',
                                                     **kwargs)

    def setUp(self):
        self.open(create=1)

    def tearDown(self):
        self._storage.close()
        self._storage.cleanup()

class FileStorageTests(
    BaseFileStorageTests,
    BasicStorage.BasicStorage,
    TransactionalUndoStorage.TransactionalUndoStorage,
    RevisionStorage.RevisionStorage,
    PackableStorage.PackableStorage,
    PackableStorage.PackableUndoStorage,
    Synchronization.SynchronizedStorage,
    ConflictResolution.ConflictResolvingStorage,
    ConflictResolution.ConflictResolvingTransUndoStorage,
    HistoryStorage.HistoryStorage,
    IteratorStorage.IteratorStorage,
    IteratorStorage.ExtendedIteratorStorage,
    PersistentStorage.PersistentStorage,
    MTStorage.MTStorage,
    ReadOnlyStorage.ReadOnlyStorage
    ):

    def checkLongMetadata(self):
        s = "X" * 75000
        try:
            self._dostore(user=s)
        except POSException.StorageError:
            pass
        else:
            self.fail("expect long user field to raise error")
        try:
            self._dostore(description=s)
        except POSException.StorageError:
            pass
        else:
            self.fail("expect long user field to raise error")

    def check_use_fsIndex(self):
        from ZODB.fsIndex import fsIndex

        self.assertEqual(self._storage._index.__class__, fsIndex)

    # A helper for checking that when an .index contains a dict for the
    # index, it's converted to an fsIndex when the file is opened.
    def convert_index_to_dict(self):
        # Convert the index in the current .index file to a Python dict.
        # Return the index originally found.
        import cPickle as pickle

        f = open('FileStorageTests.fs.index', 'r+b')
        p = pickle.Unpickler(f)
        data = p.load()
        index = data['index']

        newindex = dict(index)
        data['index'] = newindex

        f.seek(0)
        f.truncate()
        p = pickle.Pickler(f, 1)
        p.dump(data)
        f.close()
        return index

    def check_conversion_to_fsIndex(self, read_only=False):
        from ZODB.fsIndex import fsIndex

        # Create some data, and remember the index.
        for i in range(10):
            self._dostore()
        oldindex_as_dict = dict(self._storage._index)

        # Save the index.
        self._storage.close()

        # Convert it to a dict.
        old_index = self.convert_index_to_dict()
        self.assert_(isinstance(old_index, fsIndex))
        new_index = self.convert_index_to_dict()
        self.assert_(isinstance(new_index, dict))

        # Verify it's converted to fsIndex in memory upon open.
        self.open(read_only=read_only)
        self.assert_(isinstance(self._storage._index, fsIndex))

        # Verify it has the right content.
        newindex_as_dict = dict(self._storage._index)
        self.assertEqual(oldindex_as_dict, newindex_as_dict)

        # Check that the type on disk has changed iff read_only is False.
        self._storage.close()
        current_index = self.convert_index_to_dict()
        if read_only:
            self.assert_(isinstance(current_index, dict))
        else:
            self.assert_(isinstance(current_index, fsIndex))

    def check_conversion_to_fsIndex_readonly(self):
        # Same thing, but the disk .index should continue to hold a
        # Python dict.
        self.check_conversion_to_fsIndex(read_only=True)

    def check_conversion_from_dict_to_btree_data_in_fsIndex(self):
        # To support efficient range searches on its keys as part of
        # implementing a record iteration protocol in FileStorage, we
        # converted the fsIndex class from using a dictionary as its
        # self._data attribute to using an OOBTree in its stead.

        from ZODB.fsIndex import fsIndex
        from BTrees.OOBTree import OOBTree

        # Create some data, and remember the index.
        for i in range(10):
            self._dostore()
        data_dict = dict(self._storage._index._data)

        # Replace the OOBTree with a dictionary and commit it.
        self._storage._index._data = data_dict
        transaction.commit()

        # Save the index.
        self._storage.close()

        # Verify it's converted to fsIndex in memory upon open.
        self.open()
        self.assert_(isinstance(self._storage._index, fsIndex))
        self.assert_(isinstance(self._storage._index._data, OOBTree))

        # Verify it has the right content.
        new_data_dict = dict(self._storage._index._data)
        self.assertEqual(len(data_dict), len(new_data_dict))

        for k in data_dict:
            old_tree = data_dict[k]
            new_tree = new_data_dict[k]
            self.assertEqual(list(old_tree.items()), list(new_tree.items()))

    def check_save_after_load_with_no_index(self):
        for i in range(10):
            self._dostore()
        self._storage.close()
        os.remove('FileStorageTests.fs.index')
        self.open()
        self.assertEqual(self._storage._saved, 1)

    # This would make the unit tests too slow
    # check_save_after_load_that_worked_hard(self)

    def check_periodic_save_index(self):

        # Check the basic algorithm
        oldsaved = self._storage._saved
        self._storage._records_before_save = 10
        for i in range(4):
            self._dostore()
        self.assertEqual(self._storage._saved, oldsaved)
        self._dostore()
        self.assertEqual(self._storage._saved, oldsaved+1)

        # Now make sure the parameter changes as we get bigger
        for i in range(20):
            self._dostore()

        self.failUnless(self._storage._records_before_save > 20)

    def checkStoreBumpsOid(self):
        # If .store() is handed an oid bigger than the storage knows
        # about already, it's crucial that the storage bump its notion
        # of the largest oid in use.
        t = transaction.Transaction()
        self._storage.tpc_begin(t)
        giant_oid = '\xee' * 8
        # Store an object.
        # oid, serial, data, version, transaction
        r1 = self._storage.store(giant_oid, '\0'*8, 'data', '', t)
        # Finish the transaction.
        r2 = self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        # Before ZODB 3.2.6, this failed, with ._oid == z64.
        self.assertEqual(self._storage._oid, giant_oid)

    def checkRestoreBumpsOid(self):
        # As above, if .restore() is handed an oid bigger than the storage
        # knows about already, it's crucial that the storage bump its notion
        # of the largest oid in use.  Because copyTransactionsFrom(), and
        # ZRS recovery, use the .restore() method, this is plain critical.
        t = transaction.Transaction()
        self._storage.tpc_begin(t)
        giant_oid = '\xee' * 8
        # Store an object.
        # oid, serial, data, version, prev_txn, transaction
        r1 = self._storage.restore(giant_oid, '\0'*8, 'data', '', None, t)
        # Finish the transaction.
        r2 = self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        # Before ZODB 3.2.6, this failed, with ._oid == z64.
        self.assertEqual(self._storage._oid, giant_oid)

    def checkCorruptionInPack(self):
        # This sets up a corrupt .fs file, with a redundant transaction
        # length mismatch.  The implementation of pack in many releases of
        # ZODB blew up if the .fs file had such damage:  it detected the
        # damage, but the code to raise CorruptedError referenced an undefined
        # global.
        import time

        from ZODB.utils import U64, p64
        from ZODB.FileStorage.format import CorruptedError

        db = DB(self._storage)
        conn = db.open()
        conn.root()['xyz'] = 1
        transaction.commit()

        # Ensure it's all on disk.
        db.close()
        self._storage.close()

        # Reopen before damaging.
        self.open()

        # Open .fs directly, and damage content.
        f = open('FileStorageTests.fs', 'r+b')
        f.seek(0, 2)
        pos2 = f.tell() - 8
        f.seek(pos2)
        tlen2 = U64(f.read(8))  # length-8 of the last transaction
        pos1 = pos2 - tlen2 + 8 # skip over the tid at the start
        f.seek(pos1)
        tlen1 = U64(f.read(8))  # should be redundant length-8
        self.assertEqual(tlen1, tlen2)  # verify that it is redundant

        # Now damage the second copy.
        f.seek(pos2)
        f.write(p64(tlen2 - 1))
        f.close()

        # Try to pack.  This used to yield
        #     NameError: global name 's' is not defined
        try:
            self._storage.pack(time.time(), None)
        except CorruptedError, detail:
            self.assert_("redundant transaction length does not match "
                         "initial transaction length" in str(detail))
        else:
            self.fail("expected CorruptedError")

    def check_record_iternext(self):

        db = DB(self._storage)
        conn = db.open()
        conn.root()['abc'] = MinPO('abc')
        conn.root()['xyz'] = MinPO('xyz')
        transaction.commit()

        # Ensure it's all on disk.
        db.close()
        self._storage.close()

        self.open()

        key = None
        for x in ('\000', '\001', '\002'):
            oid, tid, data, next_oid = self._storage.record_iternext(key)
            self.assertEqual(oid, ('\000' * 7) + x)
            key = next_oid
            expected_data, expected_tid = self._storage.load(oid, '')
            self.assertEqual(expected_data, data)
            self.assertEqual(expected_tid, tid)
            if x == '\002':
                self.assertEqual(next_oid, None)
            else:
                self.assertNotEqual(next_oid, None)


class FileStorageRecoveryTest(
    StorageTestBase.StorageTestBase,
    RecoveryStorage.RecoveryStorage,
    ):

    def setUp(self):
        self._storage = ZODB.FileStorage.FileStorage("Source.fs", create=True)
        self._dst = ZODB.FileStorage.FileStorage("Dest.fs", create=True)

    def tearDown(self):
        self._storage.close()
        self._dst.close()
        self._storage.cleanup()
        self._dst.cleanup()

    def new_dest(self):
        return ZODB.FileStorage.FileStorage('Dest.fs')


class FileStorageNoRestore(ZODB.FileStorage.FileStorage):

    @property
    def restore(self):
        raise Exception


class FileStorageNoRestoreRecoveryTest(
    StorageTestBase.StorageTestBase,
    RecoveryStorage.RecoveryStorage,
    ):
    # This test actually verifies a code path of
    # BaseStorage.copyTransactionsFrom. For simplicity of implementation, we
    # use a FileStorage deprived of its restore method.

    def setUp(self):
        self._storage = FileStorageNoRestore("Source.fs", create=True)
        self._dst = FileStorageNoRestore("Dest.fs", create=True)

    def tearDown(self):
        self._storage.close()
        self._dst.close()
        self._storage.cleanup()
        self._dst.cleanup()

    def new_dest(self):
        return FileStorageNoRestore('Dest.fs')

    def checkRestoreAcrossPack(self):
        # Skip this check as it calls restore directly.
        pass


class SlowFileStorageTest(BaseFileStorageTests):

    level = 2

    def check10Kstores(self):
        # The _get_cached_serial() method has a special case
        # every 8000 calls.  Make sure it gets minimal coverage.
        oids = [[self._storage.new_oid(), None] for i in range(100)]
        for i in range(100):
            t = transaction.Transaction()
            self._storage.tpc_begin(t)
            for j in range(100):
                o = MinPO(j)
                oid, revid = oids[j]
                serial = self._storage.store(oid, revid, zodb_pickle(o), "", t)
                oids[j][1] = serial
            self._storage.tpc_vote(t)
            self._storage.tpc_finish(t)

# Raise an exception if the tids in FileStorage fs aren't
# strictly increasing.
def checkIncreasingTids(fs):
    lasttid = '\0' * 8
    for txn in fs.iterator():
        if lasttid >= txn.tid:
            raise ValueError("tids out of order %r >= %r" % (lasttid, tid))
        lasttid = txn.tid

# Return a TimeStamp object 'minutes' minutes in the future.
def timestamp(minutes):
    import time
    from persistent.TimeStamp import TimeStamp

    t = time.time() + 60 * minutes
    return TimeStamp(*time.gmtime(t)[:5] + (t % 60,))

def testTimeTravelOnOpen():
    """
    >>> from ZODB.FileStorage import FileStorage
    >>> from zope.testing.loggingsupport import InstalledHandler

    Arrange to capture log messages -- they're an important part of
    this test!

    >>> handler = InstalledHandler('ZODB.FileStorage')

    Create a new file storage.

    >>> st = FileStorage('temp.fs', create=True)
    >>> db = DB(st)
    >>> db.close()

    First check the normal case:  transactions are recorded with
    increasing tids, and time doesn't run backwards.

    >>> st = FileStorage('temp.fs')
    >>> db = DB(st)
    >>> conn = db.open()
    >>> conn.root()['xyz'] = 1
    >>> transaction.get().commit()
    >>> checkIncreasingTids(st)
    >>> db.close()
    >>> st.cleanup() # remove .fs, .index, etc files
    >>> handler.records   # i.e., no log messages
    []

    Now force the database to have transaction records with tids from
    the future.

    >>> st = FileStorage('temp.fs', create=True)
    >>> st._ts = timestamp(15)  # 15 minutes in the future
    >>> db = DB(st)
    >>> db.close()

    >>> st = FileStorage('temp.fs') # this should log a warning
    >>> db = DB(st)
    >>> conn = db.open()
    >>> conn.root()['xyz'] = 1
    >>> transaction.get().commit()
    >>> checkIncreasingTids(st)
    >>> db.close()
    >>> st.cleanup()

    >>> [record.levelname for record in handler.records]
    ['WARNING']
    >>> handler.clear()

    And one more time, with transaction records far in the future.
    We expect to log a critical error then, as a time so far in the
    future probably indicates a real problem with the system.  Shorter
    spans may be due to clock drift.

    >>> st = FileStorage('temp.fs', create=True)
    >>> st._ts = timestamp(60)  # an hour in the future
    >>> db = DB(st)
    >>> db.close()

    >>> st = FileStorage('temp.fs') # this should log a critical error
    >>> db = DB(st)
    >>> conn = db.open()
    >>> conn.root()['xyz'] = 1
    >>> transaction.get().commit()
    >>> checkIncreasingTids(st)
    >>> db.close()
    >>> st.cleanup()

    >>> [record.levelname for record in handler.records]
    ['CRITICAL']
    >>> handler.clear()
    >>> handler.uninstall()
    """

def lastInvalidations():
    """

The last invalidations method is used by a storage server to populate
it's data structure of recent invalidations.  The lastInvalidations
method is passed a count and must return up to count number of the
most recent transactions.

We'll create a FileStorage and populate it with some data, keeping
track of the transactions along the way:

    >>> fs = ZODB.FileStorage.FileStorage('t.fs', create=True)
    >>> db = DB(fs)
    >>> conn = db.open()
    >>> from persistent.dict import PersistentDict
    >>> last = []
    >>> for i in range(100):
    ...     conn.root()[i] = PersistentDict()
    ...     transaction.commit()
    ...     last.append(fs.lastTransaction())

Now, we can call lastInvalidations on it:

    >>> invalidations = fs.lastInvalidations(10)
    >>> [t for (t, oids) in invalidations] == last[-10:]
    True

    >>> from ZODB.utils import u64
    >>> [[int(u64(oid)) for (oid, version) in oids]
    ...  for (i, oids) in invalidations]
    ... # doctest: +NORMALIZE_WHITESPACE
    [[0, 91], [0, 92], [0, 93], [0, 94], [0, 95],
     [0, 96], [0, 97], [0, 98], [0, 99], [0, 100]]

If we ask for more transactions than there are, we'll get as many as
there are:

    >>> len(fs.lastInvalidations(1000))
    101

Of course, calling lastInvalidations on an empty storage refturns no data:

    >>> fs.close()
    >>> fs = ZODB.FileStorage.FileStorage('t.fs', create=True)
    >>> list(fs.lastInvalidations(10))
    []

    """

def test_suite():
    from zope.testing import doctest

    suite = unittest.TestSuite()
    for klass in [FileStorageTests, Corruption.FileStorageCorruptTests,
                  FileStorageRecoveryTest, FileStorageNoRestoreRecoveryTest,
                  SlowFileStorageTest]:
        suite.addTest(unittest.makeSuite(klass, "check"))
    suite.addTest(doctest.DocTestSuite(setUp=ZODB.tests.util.setUp,
                                       tearDown=ZODB.tests.util.tearDown))
    return suite

if __name__=='__main__':
    unittest.main()
