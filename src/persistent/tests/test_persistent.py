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
import new
import os
import sys
import unittest

from persistent import Persistent
from persistent.interfaces import IPersistent
import persistent.tests

try:
    import zope.interface
except ImportError:
    interfaces = False
else:
    interfaces = True

oid = "\0\0\0\0\0\0hi"

class P(Persistent):
    def __init__(self):
        self.x = 0
    def inc(self):
        self.x += 1

class P2(P):
    def __getstate__(self):
        return 42
    def __setstate__(self, v):
        self.v = v

class B(Persistent):

    __slots__ = ["x", "_p_serial"]

    def __init__(self):
        self.x = 0

    def inc(self):
        self.x += 1

    def __getstate__(self):
        return {'x': self.x}

    def __setstate__(self, state):
        self.x = state['x']

class DM:
    def __init__(self):
        self.called = 0
    def register(self, ob):
        self.called += 1
    def setstate(self, ob):
        ob.__setstate__({'x': 42})

class BrokenDM(DM):

    def register(self,ob):
        self.called += 1
        raise NotImplementedError

    def setstate(self,ob):
        raise NotImplementedError

class Test(unittest.TestCase):

    # XXX This is the only remaining unittest.  Figure out how to move
    # this into doctest?

    if interfaces:
        def testInterface(self):
            self.assert_(IPersistent.isImplementedByInstancesOf(Persistent),
                         "%s does not implement IPersistent" % Persistent)
            p = Persistent()
            self.assert_(IPersistent.isImplementedBy(p),
                         "%s does not implement IPersistent" % p)

            self.assert_(IPersistent.isImplementedByInstancesOf(P),
                         "%s does not implement IPersistent" % P)
            p = P()
            self.assert_(IPersistent.isImplementedBy(p),
                         "%s does not implement IPersistent" % p)

def DocFileSuite(path):
    # It's not entirely obvious how to connection this single string
    # with unittest.  For now, re-use the _utest() function that comes
    # standard with doctest in Python 2.3.  One problem is that the
    # error indicator doesn't point to the line of the doctest file
    # that failed.
    source = open(path).read()
    t = doctest.Tester(globs=sys._getframe(1).f_globals)
    def runit():
        doctest._utest(t, path, source, path, 0)
    f = unittest.FunctionTestCase(runit, description="doctest from %s" % path)
    suite = unittest.TestSuite()
    suite.addTest(f)
    return suite

def test_suite():
    p = os.path.join(persistent.tests.__path__[0], "persistent.txt")
    s = unittest.makeSuite(Test)
    s.addTest(DocFileSuite(p))
    return s
