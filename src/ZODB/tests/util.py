##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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
"""Conventience function for creating test databases

$Id$
"""

from ZODB.MappingStorage import DB

import atexit
import os
import tempfile
import time
import unittest
import persistent
import transaction
import zope.testing.setupstack

def setUp(test, name='test'):
    transaction.abort()
    d = tempfile.mkdtemp(prefix=name)
    zope.testing.setupstack.register(test, zope.testing.setupstack.rmtree, d)
    zope.testing.setupstack.register(
        test, setattr, tempfile, 'tempdir', tempfile.tempdir)
    tempfile.tempdir = d
    zope.testing.setupstack.register(test, os.chdir, os.getcwd())
    os.chdir(d)
    zope.testing.setupstack.register(test, transaction.abort)

tearDown = zope.testing.setupstack.tearDown

class TestCase(unittest.TestCase):

    def setUp(self):
        self.globs = {}
        name = self.__class__.__name__
        mname = getattr(self, '_TestCase__testMethodName', '')
        if mname:
            name += '-' + mname
        setUp(self, name)

    tearDown = tearDown

def pack(db):
    db.pack(time.time()+1)

class P(persistent.Persistent):

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return 'P(%s)' % self.name

class MininalTestLayer:

    __bases__ = ()
    __module__ = ''
    def __init__(self, name):
        self.__name__ = name

    def setUp(self):
        self.here = os.getcwd()
        self.tmp = tempfile.mkdtemp(self.__name__, dir=os.getcwd())
        os.chdir(self.tmp)

        # sigh. tearDown isn't called when a layer is run in a sub-process.
        atexit.register(clean, self.tmp)

    def tearDown(self):
        os.chdir(self.here)
        zope.testing.setupstack.rmtree(self.tmp)

    testSetUp = testTearDown = lambda self: None

def clean(tmp):
    if os.path.isdir(tmp):
        zope.testing.setupstack.rmtree(tmp)

class AAAA_Test_Runner_Hack(unittest.TestCase):
    """Hack to work around a bug in the test runner.

    The first later (lex sorted) is run first in the foreground
    """

    layer = MininalTestLayer('!no tests here!')

    def testNothing(self):
        pass

