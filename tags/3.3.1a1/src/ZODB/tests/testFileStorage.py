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
from ZODB import POSException

from ZODB.tests import StorageTestBase, BasicStorage, \
     TransactionalUndoStorage, VersionStorage, \
     TransactionalUndoVersionStorage, PackableStorage, \
     Synchronization, ConflictResolution, HistoryStorage, \
     IteratorStorage, Corruption, RevisionStorage, PersistentStorage, \
     MTStorage, ReadOnlyStorage, RecoveryStorage
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
    VersionStorage.VersionStorage,
    TransactionalUndoVersionStorage.TransactionalUndoVersionStorage,
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

    # XXX We could really use some tests for sanity checking

    def check_conversion_to_fsIndex_not_if_readonly(self):

        self.tearDown()

        class OldFileStorage(ZODB.FileStorage.FileStorage):
            def _newIndexes(self):
                return {}, {}, {}, {}, {}, {}, {}


        from ZODB.fsIndex import fsIndex

        # Hack FileStorage to create dictionary indexes
        self._storage = OldFileStorage('FileStorageTests.fs')

        self.assertEqual(type(self._storage._index), type({}))
        for i in range(10):
            self._dostore()

        # Should save the index
        self._storage.close()

        self._storage = ZODB.FileStorage.FileStorage(
            'FileStorageTests.fs', read_only=1)
        self.assertEqual(type(self._storage._index), type({}))

    def check_conversion_to_fsIndex(self):

        self.tearDown()

        class OldFileStorage(ZODB.FileStorage.FileStorage):
            def _newIndexes(self):
                return {}, {}, {}, {}, {}, {}, {}


        from ZODB.fsIndex import fsIndex

        # Hack FileStorage to create dictionary indexes
        self._storage = OldFileStorage('FileStorageTests.fs')

        self.assertEqual(type(self._storage._index), type({}))
        for i in range(10):
            self._dostore()

        oldindex = self._storage._index.copy()

        # Should save the index
        self._storage.close()

        self._storage = ZODB.FileStorage.FileStorage('FileStorageTests.fs')
        self.assertEqual(self._storage._index.__class__, fsIndex)
        self.failUnless(self._storage._used_index)

        index = {}
        for k, v in self._storage._index.items():
            index[k] = v

        self.assertEqual(index, oldindex)


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

    def checkCorruptionInPack(self):
        # This sets up a corrupt .fs file, with a redundant transaction
        # length mismatch.  The implementation of pack in many releases of
        # ZODB blew up if the .fs file had such damage:  it detected the
        # damage, but the code to raise CorruptedError referenced an undefined
        # global.
        import time

        from ZODB.DB import DB
        from ZODB.utils import U64, p64
        from ZODB.FileStorage.format import CorruptedError

        db = DB(self._storage)
        conn = db.open()
        conn.root()['xyz'] = 1
        get_transaction().commit()

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
    >>> from ZODB.DB import DB
    >>> import transaction
    >>> from ZODB.tests.loggingsupport import InstalledHandler

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

def test_suite():
    import doctest

    suite = unittest.TestSuite()
    for klass in [FileStorageTests, Corruption.FileStorageCorruptTests,
                  FileStorageRecoveryTest, SlowFileStorageTest]:
        suite.addTest(unittest.makeSuite(klass, "check"))
    suite.addTest(doctest.DocTestSuite())
    return suite

if __name__=='__main__':
    unittest.main()
