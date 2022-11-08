##############################################################################
#
# Copyright (c) 2003 Zope Foundation and Contributors.
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
"""Tests of the file storage recovery script."""

import os
import random
import sys
import unittest

import transaction
from persistent.mapping import PersistentMapping

import ZODB
import ZODB.fsrecover
import ZODB.tests.util
from ZODB._compat import decodebytes
from ZODB.FileStorage import FileStorage


try:
    import StringIO
except ImportError:
    # Py3
    import io as StringIO


class RecoverTest(ZODB.tests.util.TestCase):

    path = None

    def setUp(self):
        ZODB.tests.util.TestCase.setUp(self)
        self.path = 'source.fs'
        self.storage = FileStorage(self.path)
        self.populate()
        self.dest = 'dest.fs'
        self.recovered = None

    def tearDown(self):
        self.storage.close()
        if self.recovered is not None:
            self.recovered.close()
        temp = FileStorage(self.dest)
        temp.close()
        ZODB.tests.util.TestCase.tearDown(self)

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
        for i in range(num - 1):
            offset = random.randint(0, self.storage._pos - size)
            # Note that we open the file as r+, not a+. Seeking a file
            # open in append mode is effectively a no-op *depending on
            # platform*, as the write may simply append to the file. An
            # earlier version of this code opened the file in a+ mode,
            # meaning on some platforms it was only writing to the end of the
            # file, and so the test cases were always finding that bad data.
            # For compatibility with that, we do one write outside the loop
            # at the end.
            with open(self.path, "r+b") as f:
                f.seek(offset)
                f.write(b"\0" * size)

            with open(self.path, 'rb') as f:
                f.seek(offset)
                v = f.read(size)
                self.assertEqual(b"\0" * size, v)

        with open(self.path, 'a+b') as f:
            f.write(b"\0" * size)

    ITERATIONS = 5

    # Run recovery, from self.path to self.dest.  Return whatever
    # recovery printed to stdout, as a string.
    def recover(self):
        orig_stdout = sys.stdout
        faux_stdout = StringIO.StringIO()
        try:
            sys.stdout = faux_stdout
            try:
                ZODB.fsrecover.recover(
                    self.path, self.dest, verbose=0, partial=True, force=False,
                    pack=1)
            except SystemExit:
                raise RuntimeError("recover tried to exit")
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
        self.assertTrue('error' not in output, output)
        self.assertTrue('\n0 bytes removed during recovery' in output, output)

        # Verify that the recovered database is identical to the original.
        with open(self.path, 'rb') as before:
            before_guts = before.read()

        with open(self.dest, 'rb') as after:
            after_guts = after.read()

        self.assertEqual(before_guts, after_guts,
                         "recovery changed a non-damaged .fs file")

    def testOneBlock(self):
        for i in range(self.ITERATIONS):
            self.damage(1, 1024)
            output = self.recover()
            self.assertTrue('error' in output, output)
            self.recovered = FileStorage(self.dest)
            self.recovered.close()
            os.remove(self.path)
            os.rename(self.dest, self.path)

    def testFourBlocks(self):
        for i in range(self.ITERATIONS):
            self.damage(4, 512)
            output = self.recover()
            self.assertTrue('error' in output, output)
            self.recovered = FileStorage(self.dest)
            self.recovered.close()
            os.remove(self.path)
            os.rename(self.dest, self.path)

    def testBigBlock(self):
        for i in range(self.ITERATIONS):
            self.damage(1, 32 * 1024)
            output = self.recover()
            self.assertTrue('error' in output, output)
            self.recovered = FileStorage(self.dest)
            self.recovered.close()
            os.remove(self.path)
            os.rename(self.dest, self.path)

    def testBadTransaction(self):
        # Find transaction headers and blast them.

        L = self.storage.undoLog()
        r = L[3]
        tid = decodebytes(r["id"] + b"\n")
        pos1 = self.storage._txn_find(tid, 0)

        r = L[8]
        tid = decodebytes(r["id"] + b"\n")
        pos2 = self.storage._txn_find(tid, 0)

        self.storage.close()

        # Overwrite the entire header.
        with open(self.path, "a+b") as f:
            f.seek(pos1 - 50)
            f.write(b"\0" * 100)
        output = self.recover()
        self.assertTrue('error' in output, output)
        self.recovered = FileStorage(self.dest)
        self.recovered.close()
        os.remove(self.path)
        os.rename(self.dest, self.path)

        # Overwrite part of the header.
        with open(self.path, "a+b") as f:
            f.seek(pos2 + 10)
            f.write(b"\0" * 100)
        output = self.recover()
        self.assertTrue('error' in output, output)
        self.recovered = FileStorage(self.dest)
        self.recovered.close()

    # Issue 1846:  When a transaction had 'c' status (not yet committed),
    # the attempt to open a temp file to write the trailing bytes fell
    # into an infinite loop.
    def testUncommittedAtEnd(self):
        # Find a transaction near the end.
        L = self.storage.undoLog()
        r = L[1]
        tid = decodebytes(r["id"] + b"\n")
        pos = self.storage._txn_find(tid, 0)

        # Overwrite its status with 'c'.
        with open(self.path, "r+b") as f:
            f.seek(pos + 16)
            current_status = f.read(1)
            self.assertEqual(current_status, b' ')
            f.seek(pos + 16)
            f.write(b'c')

        # Try to recover.  The original bug was that this never completed --
        # infinite loop in fsrecover.py.  Also, in the ZODB 3.2 line,
        # reference to an undefined global masked the infinite loop.
        self.recover()

        # Verify the destination got truncated.
        self.assertEqual(os.path.getsize(self.dest), pos)

        # Get rid of the temp file holding the truncated bytes.
        os.remove(ZODB.fsrecover._trname)


def test_suite():
    return unittest.makeSuite(RecoverTest)
