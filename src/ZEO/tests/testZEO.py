"""Test suite for ZEO based on ZODB.tests"""

import asyncore
import os
import tempfile
import time
import types
import unittest

import ZEO.ClientStorage, ZEO.StorageServer
import ThreadedAsync, ZEO.trigger
from ZODB.FileStorage import FileStorage

from ZEO.tests import forker, Cache

# Sorry Jim...
from ZODB.tests import StorageTestBase, BasicStorage, VersionStorage, \
     TransactionalUndoStorage, TransactionalUndoVersionStorage, \
     PackableStorage, Synchronization, ConflictResolution
from ZODB.tests.MinPO import MinPO


ZERO = '\0'*8

class DummyDB:
    def invalidate(self, *args):
        pass

class PackWaitWrapper:
    def __init__(self, storage):
        self.storage = storage

    def __getattr__(self, attr):
        return getattr(self.storage, attr)

    def pack(self, t, f):
        self.storage.pack(t, f, wait=1)

class ZEOTestBase(StorageTestBase.StorageTestBase):
    """Version of the storage test class that supports ZEO.
    
    For ZEO, we don't always get the serialno/exception for a
    particular store as the return value from the store.   But we
    will get no later than the return value from vote.
    """
    
    def _dostore(self, oid=None, revid=None, data=None, version=None,
                 already_pickled=0):
        """Do a complete storage transaction.

        The defaults are:
         - oid=None, ask the storage for a new oid
         - revid=None, use a revid of ZERO
         - data=None, pickle up some arbitrary data (the integer 7)
         - version=None, use the empty string version
        
        Returns the object's new revision id.
        """
        if oid is None:
            oid = self._storage.new_oid()
        if revid is None:
            revid = ZERO
        if data is None:
            data = MinPO(7)
        if not already_pickled:
            data = StorageTestBase.zodb_pickle(data)
        if version is None:
            version = ''
        # Begin the transaction
        self._storage.tpc_begin(self._transaction)
        # Store an object
        r1 = self._storage.store(oid, revid, data, version,
                                 self._transaction)
        s1 = self._get_serial(r1)
        # Finish the transaction
        r2 = self._storage.tpc_vote(self._transaction)
        s2 = self._get_serial(r2)
        self._storage.tpc_finish(self._transaction)
        # s1, s2 can be None or dict
        assert not (s1 and s2)
        return s1 and s1[oid] or s2 and s2[oid]

    def _get_serial(self, r):
        """Return oid -> serialno dict from sequence of ZEO replies."""
        d = {}
        if r is None:
            return None
        if type(r) == types.StringType:
            raise RuntimeError, "unexpected ZEO response: no oid"
        else:
            for oid, serial in r:
                if isinstance(serial, Exception):
                    raise serial
                d[oid] = serial
        return d

    def checkLargeUpdate(self):
        obj = MinPO("X" * (10 * 128 * 1024))
        self._dostore(data=obj)
        
class GenericTests(ZEOTestBase,
                   Cache.StorageWithCache,
                   BasicStorage.BasicStorage,
                   VersionStorage.VersionStorage,
                   PackableStorage.PackableStorage,
                   Synchronization.SynchronizedStorage,
                   ConflictResolution.ConflictResolvingStorage,
                   ):
    """An abstract base class for ZEO tests

    A specific ZEO test run depends on having a real storage that the
    StorageServer provides access to.  The GenericTests must be
    subclassed to provide an implementation of getStorage() that
    returns a specific storage, e.g. FileStorage.
    """

    __super_setUp = StorageTestBase.StorageTestBase.setUp
    __super_tearDown = StorageTestBase.StorageTestBase.tearDown

    def setUp(self):
        """Start a ZEO server using a Unix domain socket

        The ZEO server uses the storage object returned by the
        getStorage() method.
        """
        self.running = 1
        client, exit, pid = forker.start_zeo(self.getStorage())
        self._pid = pid
        self._server = exit
        self._storage = PackWaitWrapper(client)
        client.registerDB(DummyDB(), None)
        self.__super_setUp()

    def tearDown(self):
        """Try to cause the tests to halt"""
        self.running = 0
        self._server.close()
        os.waitpid(self._pid, 0)
        self.__super_tearDown()

class ZEOFileStorageTests(GenericTests):
    __super_setUp = GenericTests.setUp
    
    def setUp(self):
        self.__fs_base = tempfile.mktemp()
        self.__super_setUp()

    def getStorage(self):
        return FileStorage(self.__fs_base, create=1)

    def delStorage(self):
        # file storage appears to create three files
        for ext in '', '.index', '.lock', '.tmp':
            path = self.__fs_base + ext
            os.unlink(path)
        
def main():
    import sys, getopt

    name_of_test = ''

    opts, args = getopt.getopt(sys.argv[1:], 'n:')
    for flag, val in opts:
        if flag == '-n':
            name_of_test = val

    if args:
        print >> sys.stderr, "Did not expect arguments.  Got %s" % args
        return 0
    
    tests = unittest.makeSuite(ZEOFileStorageTests, 'check' + name_of_test)
    runner = unittest.TextTestRunner()
    runner.run(tests)

if __name__ == "__main__":
    main()
