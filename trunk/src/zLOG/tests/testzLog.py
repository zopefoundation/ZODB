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

severity_string = {
    -300: 'TRACE',
    -200: 'DEBUG',
    -100: 'BLATHER',
       0: 'INFO',
     100: 'PROBLEM',
     200: 'ERROR',
     300: 'PANIC',
    }

class StupidLogTest(unittest.TestCase):
    """Test zLOG with the default implementation.

    The default implementation uses the environment variables
    STUPID_LOG_FILE and STUPID_LOG_SEVERITY.
    """
    prefix = 'STUPID'

    def wipeEnvironment(self):
        if os.environ.has_key('STUPID_LOG_FILE'):
            del os.environ['STUPID_LOG_FILE']
        if os.environ.has_key('EVENT_LOG_FILE'):
            del os.environ['EVENT_LOG_FILE']
        if os.environ.has_key('STUPID_LOG_SEVERITY'):
            del os.environ['STUPID_LOG_SEVERITY']
        if os.environ.has_key('EVENT_LOG_SEVERITY'):
            del os.environ['EVENT_LOG_SEVERITY']

    def setUp(self):
        self.wipeEnvironment()
        self.path = tempfile.mktemp()
        self._severity = 0

    def tearDown(self):
        try:
            os.remove(self.path)
        except os.error:
            pass
        self.wipeEnvironment()

    def setLog(self, severity=0):
        os.environ['%s_LOG_FILE' % self.prefix] = self.path
        if severity:
            os.environ['%s_LOG_SEVERITY' % self.prefix] = str(severity)
        self._severity = severity
        zLOG.MinimalLogger._log.initialize()

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
        self.verifyEntry(f, subsys="basic", summary="summary")

    def checkDetail(self):
        self.setLog()
        zLOG.LOG("basic", zLOG.INFO, "xxx", "this is a detail")

        f = self.getLogFile()
        self.verifyEntry(f, subsys="basic", detail="detail")

    def checkError(self):
        self.setLog()
        try:
            1 / 0
        except ZeroDivisionError, err:
            err = sys.exc_info()

        zLOG.LOG("basic", zLOG.INFO, "summary")
        zLOG.LOG("basic", zLOG.ERROR, "raised exception", error=err)

        f = self.getLogFile()
        self.verifyEntry(f, subsys="basic", summary="summary")
        self.verifyEntry(f, subsys="basic", severity=zLOG.ERROR,
                         error=err)

class EventLogTest(StupidLogTest):
    """ Test alternate envvars EVENT_LOG_FILE and EVENT_LOG_SEVERITY """
    prefix = 'EVENT'

def test_suite():
    suite = unittest.makeSuite(StupidLogTest, 'check')
    suite.addTest(unittest.makeSuite(EventLogTest, 'check'))
    return suite

if __name__ == "__main__":
    loader = unittest.TestLoader()
    loader.testMethodPrefix = "check"
    unittest.main(testLoader=loader)
