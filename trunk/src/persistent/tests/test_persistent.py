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
import doctest
import os
import sys
import unittest

import persistent.tests
from persistent import Persistent

class P(Persistent):
    def __init__(self):
        self.x = 0
    def inc(self):
        self.x += 1

def DocFileSuite(path, globs=None):
    # It's not entirely obvious how to connection this single string
    # with unittest.  For now, re-use the _utest() function that comes
    # standard with doctest in Python 2.3.  One problem is that the
    # error indicator doesn't point to the line of the doctest file
    # that failed.
    source = open(path).read()
    if globs is None:
        globs = sys._getframe(1).f_globals
    t = doctest.Tester(globs=globs)
    def runit():
        doctest._utest(t, path, source, path, 0)
    f = unittest.FunctionTestCase(runit, description="doctest from %s" % path)
    suite = unittest.TestSuite()
    suite.addTest(f)
    return suite

def test_suite():
    path = os.path.join(persistent.tests.__path__[0], "persistent.txt")
    return DocFileSuite(path, {"P": P})
