#! /usr/bin/env python
##############################################################################
#
# Copyright (c) 2002, 2003 Zope Corporation and Contributors.
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
"""Script to run all the regression tests for the ZConfig package."""

import os
import sys
import unittest

if __name__ == "__main__":
    __file__ = sys.argv[0]

TESTDIR = os.path.dirname(os.path.abspath(__file__))

PKGDIR = os.path.dirname(TESTDIR) # the ZConfig package directory
TOPDIR = os.path.dirname(PKGDIR)

COMPONENTS = os.path.join(PKGDIR, "components")

TESTDIRS = {
    "ZConfig.tests": TESTDIR,
    "ZConfig.components.basic.tests": os.path.join(COMPONENTS,
                                                   "basic", "tests"),
    "ZConfig.components.logger.tests": os.path.join(COMPONENTS,
                                                    "logger", "tests"),
    }


def load_tests(pkgname, name):
    name = "%s.%s" % (pkgname, name)
    __import__(name)
    mod = sys.modules[name]
    return mod.test_suite()

def test_suite():
    L = []
    for pkgname, path in TESTDIRS.items():
        for fn in os.listdir(path):
            name, ext = os.path.splitext(fn)
            if name[:4] == "test" and ext == ".py":
                L.append(load_tests(pkgname, name))
    if len(L) == 1:
        return L[0]
    else:
        suite = unittest.TestSuite()
        for t in L:
            suite.addTest(t)
        return suite

if __name__ == "__main__":
    if TOPDIR not in sys.path:
        sys.path.insert(0, TOPDIR)
    unittest.main(defaultTest="test_suite")
