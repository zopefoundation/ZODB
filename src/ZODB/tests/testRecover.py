##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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
"""Tests of the file storage recovery script."""

import os
import random
import sys
import tempfile
import unittest
import StringIO

import ZODB
from ZODB.FileStorage import FileStorage
from ZODB.PersistentMapping import PersistentMapping
from ZODB.fsrecover import recover
from ZODB.tests.StorageTestBase import removefs

from ZODB.fsdump import Dumper

class RecoverTest(unittest.TestCase):

    level = 2

    path = None

    def setUp(self):
        self.path = tempfile.mktemp(suffix=".fs")
        self.storage = FileStorage(self.path)
        self.populate()
        self.storage.close()
        self.dest = tempfile.mktemp(suffix=".fs")
        self.recovered = None

    def tearDown(self):
        if self.recovered is not None:
            self.recovered.close()
        removefs(self.path)
        removefs(self.dest)

    def populate(self):
        db = ZODB.DB(self.storage)
        cn = db.open()
        rt = cn.root()

        # create a whole bunch of objects,
        # looks like a Data.fs > 1MB
        for i in range(100):
            d = rt[i] = PersistentMapping()
            get_transaction().commit()
            for j in range(100):
                d[j] = "a" * j
            get_transaction().commit()

    def damage(self, num, size):
        # Drop size null bytes into num random spots.
        for i in range(num):
            offset = random.randint(0, self.storage._pos - size)
            f = open(self.path, "a+b")
            f.seek(offset)
            f.write("\0" * size)
            f.close()

    ITERATIONS = 10

    def recover(self, source, dest):
        orig = sys.stdout
        try:
            sys.stdout = StringIO.StringIO()
            try:
                recover(self.path, self.dest,
                        verbose=0, partial=1, force=0, pack=1)
            except SystemExit:
                raise RuntimeError, "recover tried to exit"
        finally:
            sys.stdout = orig

    def testOneBlock(self):
        for i in range(self.ITERATIONS):
            self.damage(1, 1024)
            self.recover(self.path, self.dest)
            self.recovered = FileStorage(self.dest)
            self.recovered.close()
            os.remove(self.path)
            os.rename(self.dest, self.path)

    def testFourBlocks(self):
        for i in range(self.ITERATIONS):
            self.damage(4, 512)
            self.recover(self.path, self.dest)
            self.recovered = FileStorage(self.dest)
            self.recovered.close()
            os.remove(self.path)
            os.rename(self.dest, self.path)

    def testBigBlock(self):
        for i in range(self.ITERATIONS):
            self.damage(1, 32 * 1024)
            self.recover(self.path, self.dest)
            self.recovered = FileStorage(self.dest)
            self.recovered.close()
            os.remove(self.path)
            os.rename(self.dest, self.path)

def test_suite():
    return unittest.makeSuite(RecoverTest)


