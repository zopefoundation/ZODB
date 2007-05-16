##############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
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
import os, sys, unittest
from zope.testing import doctest

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(doctest.DocFileSuite(os.path.join('..', 'lock_file.txt')))
    if sys.platform == 'win32':
        suite.addTest(doctest.DocFileSuite(os.path.join('..', 'winlock.txt')))
    return suite
