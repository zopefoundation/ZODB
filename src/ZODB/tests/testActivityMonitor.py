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
"""Tests of the default activity monitor.

See ZODB/ActivityMonitor.py

$Id: testActivityMonitor.py,v 1.3 2002/08/14 22:07:09 mj Exp $
"""

import unittest
import time

from ZODB.ActivityMonitor import ActivityMonitor


class FakeConnection:

    loads = 0
    stores = 0

    def _transferred(self, loads, stores):
        self.loads = self.loads + loads
        self.stores = self.stores + stores

    def getTransferCounts(self, clear=0):
        res = self.loads, self.stores
        if clear:
            self.loads = self.stores = 0
        return res


class Tests(unittest.TestCase):

    def testAddLogEntries(self):
        am = ActivityMonitor(history_length=3600)
        self.assertEqual(len(am.log), 0)
        c = FakeConnection()
        c._transferred(1, 2)
        am.closedConnection(c)
        c._transferred(3, 7)
        am.closedConnection(c)
        self.assertEqual(len(am.log), 2)

    def testTrim(self):
        am = ActivityMonitor(history_length=0.1)
        c = FakeConnection()
        c._transferred(1, 2)
        am.closedConnection(c)
        time.sleep(0.2)
        c._transferred(3, 7)
        am.closedConnection(c)
        self.assert_(len(am.log) <= 1)

    def testSetHistoryLength(self):
        am = ActivityMonitor(history_length=3600)
        c = FakeConnection()
        c._transferred(1, 2)
        am.closedConnection(c)
        time.sleep(0.2)
        c._transferred(3, 7)
        am.closedConnection(c)
        self.assertEqual(len(am.log), 2)
        am.setHistoryLength(0.1)
        self.assertEqual(am.getHistoryLength(), 0.1)
        self.assert_(len(am.log) <= 1)

    def testActivityAnalysis(self):
        am = ActivityMonitor(history_length=3600)
        c = FakeConnection()
        c._transferred(1, 2)
        am.closedConnection(c)
        c._transferred(3, 7)
        am.closedConnection(c)
        res = am.getActivityAnalysis(start=0, end=0, divisions=10)
        lastend = 0
        for n in range(9):
            div = res[n]
            self.assertEqual(div['stores'], 0)
            self.assertEqual(div['loads'], 0)
            self.assert_(div['start'] > 0)
            self.assert_(div['start'] >= lastend)
            self.assert_(div['start'] < div['end'])
            lastend = div['end']
        div = res[9]
        self.assertEqual(div['stores'], 9)
        self.assertEqual(div['loads'], 4)
        self.assert_(div['start'] > 0)
        self.assert_(div['start'] >= lastend)
        self.assert_(div['start'] < div['end'])


def test_suite():
    return unittest.makeSuite(Tests)

if __name__=='__main__':
    unittest.main(defaultTest='test_suite')
