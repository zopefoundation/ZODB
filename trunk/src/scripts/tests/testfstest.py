"""Verify that fstest.py can find errors.

XXX To run this test script fstest.py must be on your PYTHONPATH.
"""

from cStringIO import StringIO
import os
import re
import struct
import tempfile
import unittest

import fstest
from fstest import FormatError, U64

class TestCorruptedFS(unittest.TestCase):

    # XXX path?
    f = open('test-checker.fs', 'rb')
    datafs = f.read()
    f.close()
    del f

    def setUp(self):
        self._temp = tempfile.mktemp()
        self._file = open(self._temp, 'wb')

    def tearDown(self):
        if not self._file.closed:
            self._file.close()
        if os.path.exists(self._temp):
            try:
                os.remove(self._temp)
            except os.error:
                pass

    def noError(self):
        if not self._file.closed:
            self._file.close()
        fstest.check(self._temp)

    def detectsError(self, rx):
        if not self._file.closed:
            self._file.close()
        try:
            fstest.check(self._temp)
        except FormatError, msg:
            mo = re.search(rx, str(msg))
            self.failIf(mo is None, "unexpected error: %s" % msg)
        else:
            self.fail("fstest did not detect corruption")

    def getHeader(self):
        buf = self._datafs.read(16)
        if not buf:
            return 0, ''
        tl = U64(buf[8:])
        return tl, buf

    def copyTransactions(self, n):
        """Copy at most n transactions from the good data"""
        f = self._datafs = StringIO(self.datafs)
        self._file.write(f.read(4))
        for i in range(n):
            tl, data = self.getHeader()
            if not tl:
                return
            self._file.write(data)
            rec = f.read(tl - 8)
            self._file.write(rec)

    def testGood(self):
        self._file.write(self.datafs)
        self.noError()

    def testTwoTransactions(self):
        self.copyTransactions(2)
        self.noError()

    def testEmptyFile(self):
        self.detectsError("empty file")

    def testInvalidHeader(self):
        self._file.write('SF12')
        self.detectsError("invalid file header")

    def testTruncatedTransaction(self):
        self._file.write(self.datafs[:4+22])
        self.detectsError("truncated")

    def testCheckpointFlag(self):
        self.copyTransactions(2)
        tl, data = self.getHeader()
        assert tl > 0, "ran out of good transaction data"
        self._file.write(data)
        self._file.write('c')
        self._file.write(self._datafs.read(tl - 9))
        self.detectsError("checkpoint flag")

    def testInvalidStatus(self):
        self.copyTransactions(2)
        tl, data = self.getHeader()
        assert tl > 0, "ran out of good transaction data"
        self._file.write(data)
        self._file.write('Z')
        self._file.write(self._datafs.read(tl - 9))
        self.detectsError("invalid status")

    def testTruncatedRecord(self):
        self.copyTransactions(3)
        tl, data = self.getHeader()
        assert tl > 0, "ran out of good transaction data"
        self._file.write(data)
        buf = self._datafs.read(tl / 2)
        self._file.write(buf)
        self.detectsError("truncated possibly")

    def testBadLength(self):
        self.copyTransactions(2)
        tl, data = self.getHeader()
        assert tl > 0, "ran out of good transaction data"
        self._file.write(data)
        buf = self._datafs.read(tl - 8)
        self._file.write(buf[0])
        assert tl <= 1<<16, "can't use this transaction for this test"
        self._file.write("\777\777")
        self._file.write(buf[3:])
        self.detectsError("invalid transaction header")

    def testDecreasingTimestamps(self):
        self.copyTransactions(0)
        tl, data = self.getHeader()
        buf = self._datafs.read(tl - 8)
        t1 = data + buf

        tl, data = self.getHeader()
        buf = self._datafs.read(tl - 8)
        t2 = data + buf

        self._file.write(t2[:8] + t1[8:])
        self._file.write(t1[:8] + t2[8:])
        self.detectsError("time-stamp")

    def testTruncatedData(self):
        # This test must re-write the transaction header length in
        # order to trigger the error in check_drec().  If it doesn't,
        # the truncated data record would also caught a truncated
        # transaction record.
        self.copyTransactions(1)
        tl, data = self.getHeader()
        pos = self._file.tell()
        self._file.write(data)
        buf = self._datafs.read(tl - 8)
        hdr = buf[:15]
        ul, dl, el = struct.unpack(">HHH", hdr[-6:])
        self._file.write(buf[:15 + ul + dl + el])
        data = buf[15 + ul + dl + el:]
        self._file.write(data[:24])
        self._file.seek(pos + 8, 0)
        newlen = struct.pack(">II", 0, tl - (len(data) - 24))
        self._file.write(newlen)
        self.detectsError("truncated at")
        
    def testBadDataLength(self):
        self.copyTransactions(1)
        tl, data = self.getHeader()
        self._file.write(data)
        buf = self._datafs.read(tl - 8)
        hdr = buf[:7]
        # write the transaction meta data
        ul, dl, el = struct.unpack(">HHH", hdr[-6:])
        self._file.write(buf[:7 + ul + dl + el])

        # write the first part of the data header
        data = buf[7 + ul + dl + el:]
        self._file.write(data[:24])
        self._file.write("\000" * 4 + "\077" + "\000" * 3)
        self._file.write(data[32:])
        self.detectsError("record exceeds transaction")

if __name__ == "__main__":
    unittest.main()
    
