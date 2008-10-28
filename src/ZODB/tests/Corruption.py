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
"""Do some minimal tests of data corruption"""

import os
import random
import stat

import ZODB, ZODB.FileStorage
from StorageTestBase import StorageTestBase

class FileStorageCorruptTests(StorageTestBase):

    def setUp(self):
        StorageTestBase.setUp(self)
        self._storage = ZODB.FileStorage.FileStorage('Data.fs', create=1)

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
        self.failUnless(os.path.exists('Data.fs.index'))
        f = open('Data.fs.index', 'r+')
        f.seek(0, 2)
        size = f.tell()
        f.seek(size / 2)
        f.truncate()
        f.close()

        self._storage = ZODB.FileStorage.FileStorage('Data.fs')
        self._check_stores(oids)

    def checkCorruptedIndex(self):
        oids = self._do_stores()
        self._close()

        # truncation the index file
        self.failUnless(os.path.exists('Data.fs.index'))
        size = os.stat('Data.fs.index')[stat.ST_SIZE]
        f = open('Data.fs.index', 'r+')
        while f.tell() < size:
            f.seek(random.randrange(1, size / 10), 1)
            f.write('\000')
        f.close()

        self._storage = ZODB.FileStorage.FileStorage('Data.fs')
        self._check_stores(oids)
