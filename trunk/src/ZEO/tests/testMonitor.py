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

$Id: testMonitor.py,v 1.6 2003/05/30 19:20:56 jeremy Exp $
"""

import socket
import time
import unittest

from ZEO.tests.ConnectionTests import CommonSetupTearDown
from ZEO.monitor import StorageStats

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

    def parse(self, s):
        # Return a list of StorageStats, one for each storage.
        lines = s.split("\n")
        self.assert_(lines[0].startswith("ZEO monitor server"))
        # lines[1] is a date

        # Break up rest of lines into sections starting with Storage:
        # and ending with a blank line.
        sections = []
        cur = None
        for line in lines[2:]:
            if line.startswith("Storage:"):
                cur = [line]
            elif line:
                cur.append(line)
            else:
                if cur is not None:
                    sections.append(cur)
                    cur = None
        assert cur is None # bug in the test code if this fails

        d = {}
        for sect in sections:
            hdr = sect[0]
            key, value = hdr.split(":")
            storage = int(value)
            s = d[storage] = StorageStats()
            s.parse("\n".join(sect[1:]))

        return d
        
    def getConfig(self, path, create, read_only):
        return """<mappingstorage 1/>"""

    def testMonitor(self):
        # just open a client to know that the server is up and running
        # XXX should put this in setUp
        self.storage = self.openClientStorage()
        s = self.get_monitor_output()
        self.storage.close()
        self.assert_(s.find("monitor") != -1)
        d = self.parse(s)
        stats = d[1]
        self.assertEqual(stats.clients, 1)
        self.assertEqual(stats.commits, 0)

def test_suite():
    return unittest.makeSuite(MonitorTests)
