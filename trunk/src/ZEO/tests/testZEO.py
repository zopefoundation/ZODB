"""Test suite for ZEO based on ZODB.tests"""

import os
import random
import signal
import tempfile
import time
import types
import unittest

import ZEO.ClientStorage, ZEO.StorageServer
import ThreadedAsync, ZEO.trigger
from ZEO.tests import forker

# XXX The ZODB.tests package contains a grab bad things, including,
# apparently, a collection of modules that define mixin classes
# containing tests cases.

from ZODB.tests import StorageTestBase, BasicStorage, VersionStorage

ZERO = '\0'*8
import pickle

class FakeDB:
    """A ClientStorage must be registered with a DB to function"""

    def invalidate(self, *args):
        pass

class ZEOTestBase(StorageTestBase.StorageTestBase):
    """Version of the storage test class that supports ZEO.
    
    For ZEO, we don't always get the serialno/exception for a
    particular store as the return value from the store.   But we
    will get no later than the return value from vote.
    """
    
    def _dostore(self, oid=None, revid=None, data=None, version=None):
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
            data = pickle.dumps(7)
        else:
            data = pickle.dumps(data)
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
                if type(serial) != types.StringType:
                    raise serial
                else:
                    d[oid] = serial
        return d
        
class GenericTests(ZEOTestBase,
                   BasicStorage.BasicStorage,
                   VersionStorage.VersionStorage,
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
        s = self.__storage = self.getStorage()
        storage, exit, pid = forker.start_zeo(s)
        self._pid = pid
        self._server_exit = exit
        self._storage = storage
        self._storage.registerDB(FakeDB(), None)
        self.__super_setUp()

    def tearDown(self):
        """Try to cause the tests to halt"""
        self.running = 0
        # XXX This only works on Unix
        self._server_exit.close()
        os.waitpid(self._pid, 0)
        self.delStorage()
        self.__super_tearDown()

    def checkFirst(self):
        self._storage.tpc_begin(self._transaction)
        self._storage.tpc_abort(self._transaction)

class ZEOFileStorageTests(GenericTests):
    __super_setUp = GenericTests.setUp
    
    from ZODB.FileStorage import FileStorage

    def setUp(self):
        self.__fs_base = tempfile.mktemp()
        self.__super_setUp()

    def getStorage(self):
        return self.FileStorage(self.__fs_base, create=1)

    def delStorage(self):
        # file storage appears to create three files
        for ext in '', '.index', '.lock', '.tmp':
            path = self.__fs_base + ext
            if os.path.exists(path):
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
