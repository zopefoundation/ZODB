##############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
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
"""Utility to create doc tests from readme files

$Id$
"""

import os, doctest, new, unittest

def DocFileSuite(*paths):
    """Utility to create doc tests from readme files

    Eventually, this, or something like it, will be part of doctest
    """
    # It's not entirely obvious how to connection this single string
    # with unittest.  For now, re-use the _utest() function that comes
    # standard with doctest in Python 2.3.  One problem is that the
    # error indicator doesn't point to the line of the doctest file
    # that failed.
    t = doctest.Tester(globs={'__name__': '__main__'})
    suite = unittest.TestSuite()
    dir = os.path.split(__file__)[0]
    for path in paths:
        path = os.path.join(dir, path)
        source = open(path).read()
        def runit(path=path, source=source):
            doctest._utest(t, path, source, path, 0)
        runit = new.function(runit.func_code, runit.func_globals, path,
                             runit.func_defaults, runit.func_closure)
        f = unittest.FunctionTestCase(runit,
                                      description="doctest from %s" % path)
        suite.addTest(f)
    return suite
