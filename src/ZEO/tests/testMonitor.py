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
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Test that the monitor produce sensible results.

$Id: testMonitor.py,v 1.2 2003/01/13 21:43:24 tim_one Exp $
"""

import socket
import time
import unittest

from ZEO.tests.ConnectionTests import CommonSetupTearDown

class MonitorTests(CommonSetupTearDown):

    monitor = 1

    def get_monitor_output(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('localhost', 42000))
        L = []
        while 1:
            buf = s.recv(8192)
            if buf:
                L.append(buf)
            else:
                break
        s.close()
        return "".join(L)

    def getConfig(self, path, create, read_only):
        return """\
        <Storage>
            type MappingStorage
        </Storage>
        """

    def testMonitor(self):
        # just open a client to know that the server is up and running
        # XXX should put this in setUp
        self.storage = self.openClientStorage()
        s = self.get_monitor_output()
        self.storage.close()
        self.assert_(s.find("monitor") != -1)
        

def test_suite():
    return unittest.makeSuite(MonitorTests)
