"""Do some minimal tests of data corruption"""

import os
import random
import stat
import tempfile
import unittest

import ZODB, ZODB.FileStorage
from StorageTestBase import StorageTestBase

class FileStorageCorruptTests(StorageTestBase):
    __super_setUp = StorageTestBase.setUp
    __super_tearDown = StorageTestBase.tearDown

    def setUp(self):
        self.path = tempfile.mktemp()
        self._storage = ZODB.FileStorage.FileStorage(self.path, create=1)
        self.__super_setUp()

    def tearDown(self):
        self.__super_tearDown()
        for ext in '', '.old', '.tmp', '.lock', '.index':
            path = self.path + ext
            if os.path.exists(path):
                os.remove(path)

    def _do_stores(self):
        oids = []
        for i in range(5):
            oid = self._storage.new_oid()
            revid = self._dostore(oid)
            oids.append((oid, revid))
        return oids

    def _check_stores(self, oids):
        for oid, revid in oids:
            data, s_revid = self._storage.load(oid, '')
            self.assertEqual(s_revid, revid)

    def checkTruncatedIndex(self):
        oids = self._do_stores()
        self._close()
        
        # truncation the index file
        path = self.path + '.index'
        self.failUnless(os.path.exists(path))
        f = open(path, 'r+')
        f.seek(0, 2)
        size = f.tell()
        f.seek(size / 2)
        f.truncate()
        f.close()

        self._storage = ZODB.FileStorage.FileStorage(self.path)
        self._check_stores(oids)

    def checkCorruptedIndex(self):
        oids = self._do_stores()
        self._close()
        
        # truncation the index file
        path = self.path + '.index'
        self.failUnless(os.path.exists(path))
        size = os.stat(path)[stat.ST_SIZE]
        f = open(path, 'r+')
        while f.tell() < size:
            f.seek(random.randrange(1, size / 10), 1)
            f.write('\000')
        f.close()

        self._storage = ZODB.FileStorage.FileStorage(self.path)
        self._check_stores(oids)
