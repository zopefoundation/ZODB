##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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
"""Test the list interface to PersistentList
"""
import unittest


class TestPList(unittest.TestCase):

    def checkBackwardCompat(self):
        # Verify that the sanest of the ZODB 3.2 dotted paths still works.
        from persistent.list import PersistentList
        from ZODB.PersistentList import PersistentList as oldPath
        self.assert_(oldPath is PersistentList)


def test_suite():
    return unittest.makeSuite(TestPList, 'check')
