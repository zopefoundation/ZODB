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

import base64
import os
import random
import sys
import tempfile
import unittest
import StringIO

import ZODB
from ZODB.FileStorage import FileStorage
from ZODB.fsrecover import recover

from persistent.mapping import PersistentMapping
import transaction

class RecoverTest(unittest.TestCase):

    level = 2

    path = None

    def setUp(self):
        self.path = tempfile.mktemp(suffix=".fs")
        self.storage = FileStorage(self.path)
        self.populate()
        self.dest = tempfile.mktemp(suffix=".fs")
        self.recovered = None

    def tearDown(self):
        self.storage.close()
        if self.recovered is not None:
            self.recovered.close()
        self.storage.cleanup()
        temp = FileStorage(self.dest)
        temp.close()
        temp.cleanup()

    def populate(self):
        db = ZODB.DB(self.storage)
        cn = db.open()
        rt = cn.root()

        # Create a bunch of objects; the Data.fs is about 100KB.
        for i in range(50):
            d = rt[i] = PersistentMapping()
            transaction.commit()
            for j in range(50):
                d[j] = "a" * j
            transaction.commit()

    def damage(self, num, size):
        self.storage.close()
        # Drop size null bytes into num random spots.
        for i in range(num):
            offset = random.randint(0, self.storage._pos - size)
            f = open(self.path, "a+b")
            f.seek(offset)
            f.write("\0" * size)
            f.close()

    ITERATIONS = 5

    # Run recovery, from self.path to self.dest.  Return whatever
    # recovery printed to stdout, as a string.
    def recover(self):
        orig_stdout = sys.stdout
        faux_stdout = StringIO.StringIO()
        try:
            sys.stdout = faux_stdout
            try:
                recover(self.path, self.dest,
                        verbose=0, partial=True, force=False, pack=1)
            except SystemExit:
                raise RuntimeError, "recover tried to exit"
        finally:
            sys.stdout = orig_stdout
        return faux_stdout.getvalue()

    # Caution:  because recovery is robust against many kinds of damage,
    # it's almost impossible for a call to self.recover() to raise an
    # exception.  As a result, these tests may pass even if fsrecover.py
    # is broken badly.  testNoDamage() tries to ensure that at least
    # recovery doesn't produce any error msgs if the input .fs is in
    # fact not damaged.
    def testNoDamage(self):
        output = self.recover()
        self.assert_('error' not in output, output)
        self.assert_('\n0 bytes removed during recovery' in output, output)

        # Verify that the recovered database is identical to the original.
        before = file(self.path, 'rb')
        before_guts = before.read()
        before.close()

        after = file(self.dest, 'rb')
        after_guts = after.read()
        after.close()

        self.assertEqual(before_guts, after_guts,
                         "recovery changed a non-damaged .fs file")

    def testOneBlock(self):
        for i in range(self.ITERATIONS):
            self.damage(1, 1024)
            output = self.recover()
            self.assert_('error' in output, output)
            self.recovered = FileStorage(self.dest)
            self.recovered.close()
            os.remove(self.path)
            os.rename(self.dest, self.path)

    def testFourBlocks(self):
        for i in range(self.ITERATIONS):
            self.damage(4, 512)
            output = self.recover()
            self.assert_('error' in output, output)
            self.recovered = FileStorage(self.dest)
            self.recovered.close()
            os.remove(self.path)
            os.rename(self.dest, self.path)

    def testBigBlock(self):
        for i in range(self.ITERATIONS):
            self.damage(1, 32 * 1024)
            output = self.recover()
            self.assert_('error' in output, output)
            self.recovered = FileStorage(self.dest)
            self.recovered.close()
            os.remove(self.path)
            os.rename(self.dest, self.path)

    def testBadTransaction(self):
        # Find transaction headers and blast them.

        L = self.storage.undoLog()
        r = L[3]
        tid = base64.decodestring(r["id"] + "\n")
        pos1 = self.storage._txn_find(tid, 0)

        r = L[8]
        tid = base64.decodestring(r["id"] + "\n")
        pos2 = self.storage._txn_find(tid, 0)

        self.storage.close()

        # Overwrite the entire header.
        f = open(self.path, "a+b")
        f.seek(pos1 - 50)
        f.write("\0" * 100)
        f.close()
        output = self.recover()
        self.assert_('error' in output, output)
        self.recovered = FileStorage(self.dest)
        self.recovered.close()
        os.remove(self.path)
        os.rename(self.dest, self.path)

        # Overwrite part of the header.
        f = open(self.path, "a+b")
        f.seek(pos2 + 10)
        f.write("\0" * 100)
        f.close()
        output = self.recover()
        self.assert_('error' in output, output)
        self.recovered = FileStorage(self.dest)
        self.recovered.close()


def test_suite():
    return unittest.makeSuite(RecoverTest)
