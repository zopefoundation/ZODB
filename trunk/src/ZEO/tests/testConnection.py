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
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Test setup for ZEO connection logic.

The actual tests are in ConnectionTests.py; this file provides the
platform-dependent scaffolding.
"""

# System imports
import os
import time
import socket
import unittest

# Zope/ZODB3 imports
import zLOG

# ZEO test support
from ZEO.tests import forker

# Import the actual test class
from ZEO.tests.ConnectionTests import ConnectionTests

class UnixConnectionTests(ConnectionTests):

    """Add Unix-specific scaffolding to the generic test suite."""

    def startServer(self, create=1, index=0, read_only=0, ro_svr=0):
        zLOG.LOG("testZEO", zLOG.INFO,
                 "startServer(create=%d, index=%d, read_only=%d)" %
                 (create, index, read_only))
        path = "%s.%d" % (self.file, index)
        addr = self.addr[index]
        pid, server = forker.start_zeo_server(
            'FileStorage', (path, create, read_only), addr, ro_svr)
        self._pids.append(pid)
        self._servers.append(server)

    def shutdownServer(self, index=0):
        zLOG.LOG("testZEO", zLOG.INFO, "shutdownServer(index=%d)" % index)
        self._servers[index].close()
        if self._pids[index] is not None:
            try:
                os.waitpid(self._pids[index], 0)
                self._pids[index] = None
            except os.error, err:
                print err

class WindowsConnectionTests(ConnectionTests):

    """Add Windows-specific scaffolding to the generic test suite."""

    def startServer(self, create=1, index=0, read_only=0, ro_svr=0):
        zLOG.LOG("testZEO", zLOG.INFO,
                 "startServer(create=%d, index=%d, read_only=%d)" %
                 (create, index, read_only))
        path = "%s.%d" % (self.file, index)
        addr = self.addr[index]
        args = (path, '='+str(create), '='+str(read_only))
        _addr, test_addr, test_pid = forker.start_zeo_server(
            'FileStorage', args, addr, ro_svr)
        self._pids.append(test_pid)
        self._servers.append(test_addr)

    def shutdownServer(self, index=0):
        zLOG.LOG("testZEO", zLOG.INFO, "shutdownServer(index=%d)" % index)
        if self._servers[index] is not None:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(self._servers[index])
            s.close()
            self._servers[index] = None
            # XXX waitpid() isn't available until Python 2.3
            time.sleep(0.5)

if os.name == "posix":
    test_classes = [UnixConnectionTests]
elif os.name == "nt":
    test_classes = [WindowsConnectionTests]
else:
    raise RuntimeError, "unsupported os: %s" % os.name

def test_suite():

    # shutup warnings about mktemp
    import warnings
    warnings.filterwarnings("ignore", "mktemp")
    
    suite = unittest.TestSuite()
    for klass in test_classes:
        sub = unittest.makeSuite(klass, 'check')
        suite.addTest(sub)
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest='test_suite')
