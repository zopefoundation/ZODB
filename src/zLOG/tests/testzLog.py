##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
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

import os
import sys
import tempfile
import unittest
import zLOG
import logging

severity_string = {
    -300: 'TRACE',
    -200: 'DEBUG',
    -100: 'BLATHER',
       0: 'INFO',
     100: 'PROBLEM',
     200: 'ERROR',
     300: 'PANIC',
    }

class EventLogTest(unittest.TestCase):
    """Test zLOG with the default implementation."""

    def setUp(self):
        self.path = tempfile.mktemp()
        self._severity = 0
        # Windows cannot remove a file that's open.  The logging code
        # keeps the log file open, and I can't find an advertised API
        # to tell the logger to close a log file.  So here we cheat:
        # tearDown() will close and remove all the handlers that pop
        # into existence after setUp() runs.  This breaks into internals,
        # but I couldn't find a sane way to do it.
        self.handlers = logging._handlers.keys()  # capture current handlers

    def tearDown(self):
        # Close and remove all the handlers that came into existence
        # since setUp ran.
        for h in logging._handlers.keys():
            if h not in self.handlers:
                h.close()
                del logging._handlers[h]
        os.remove(self.path)
        zLOG.initialize()

    def setLog(self, severity=0):
        os.environ['%s_LOG_FILE' % self.prefix] = self.path
        if severity:
            os.environ['%s_LOG_SEVERITY' % self.prefix] = str(severity)
        self._severity = severity
        zLOG.initialize()

    def verifyEntry(self, f, time=None, subsys=None, severity=None,
                    summary=None, detail=None, error=None):
        # skip to the beginning of next entry
        line = f.readline().strip()
        while line != "------":
            if not line:
                self.fail("can't find entry in log file")
            line = f.readline()

        line = f.readline().strip()
        _time, rest = line.split(" ", 1)
        if time is not None:
            self.assertEqual(_time, time)
        if subsys is not None:
            self.assert_(rest.find(subsys) != -1, "subsystem mismatch")
        if severity is not None and severity >= self._severity:
            s = severity_string[severity]
            self.assert_(rest.find(s) != -1, "severity mismatch")
        if summary is not None:
            self.assert_(rest.find(summary) != -1, "summary mismatch")
        if detail is not None:
            line = f.readline()
            self.assert_(line.find(detail) != -1, "missing detail")
        if error is not None:
            line = f.readline().strip()
            self.assert_(line.startswith('Traceback'),
                         "missing traceback")
            last = "%s: %s" % (error[0], error[1])
            if last.startswith("exceptions."):
                last = last[len("exceptions."):]
            while 1:
                line = f.readline().strip()
                if not line:
                    self.fail("couldn't find end of traceback")
                if line == "------":
                    self.fail("couldn't find end of traceback")
                if line == last:
                    break

    def getLogFile(self):
        return open(self.path, 'rb')

    def checkBasics(self):
        self.setLog()
        zLOG.LOG("basic", zLOG.INFO, "summary")
        f = self.getLogFile()
        try:
            self.verifyEntry(f, subsys="basic", summary="summary")
        finally:
            f.close()

    def checkDetail(self):
        self.setLog()
        zLOG.LOG("basic", zLOG.INFO, "xxx", "this is a detail")

        f = self.getLogFile()
        try:
            self.verifyEntry(f, subsys="basic", detail="detail")
        finally:
            f.close()

    def checkError(self):
        self.setLog()
        try:
            1 / 0
        except ZeroDivisionError, err:
            err = sys.exc_info()

        zLOG.LOG("basic", zLOG.INFO, "summary")
        zLOG.LOG("basic", zLOG.ERROR, "raised exception", error=err)

        f = self.getLogFile()
        try:
            self.verifyEntry(f, subsys="basic", summary="summary")
            self.verifyEntry(f, subsys="basic", severity=zLOG.ERROR,
                             error=err)
        finally:
            f.close()


def test_suite():
    return unittest.TestSuite()
    return unittest.makeSuite(EventLogTest, 'check')

if __name__ == "__main__":
    loader = unittest.TestLoader()
    loader.testMethodPrefix = "check"
    unittest.main(testLoader=loader)
