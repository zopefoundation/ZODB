##############################################################################
#
# Copyright (c) 2003 Zope Foundation and Contributors.
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
"""

from __future__ import with_statement

from ZODB.MappingStorage import DB

import atexit
import os
import persistent
import sys
import tempfile
import time
import transaction
import unittest
import warnings
import ZODB.utils
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

    def __init__(self, name=None):
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

def assert_warning(category, func, warning_text=''):
    if sys.version_info < (2, 6):
        return func() # Can't use catch_warnings :(

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('default')
        result = func()
        for warning in w:
            if ((warning.category is category)
                and (warning_text in str(warning.message))):
                return result
        raise AssertionError(w)

def assert_deprecated(func, warning_text=''):
    return assert_warning(DeprecationWarning, func, warning_text)

def wait(func=None, timeout=30):
    if func is None:
        return lambda f: wait(f, timeout)
    for i in xrange(int(timeout*100)):
        if func():
            return
        time.sleep(.01)
    raise AssertionError

def store(storage, oid, value='x', serial=ZODB.utils.z64):
    if not isinstance(oid, str):
        oid = ZODB.utils.p64(oid)
    if not isinstance(serial, str):
        serial = ZODB.utils.p64(serial)
    t = transaction.get()
    storage.tpc_begin(t)
    storage.store(oid, serial, value, '', t)
    storage.tpc_vote(t)
    storage.tpc_finish(t)

def mess_with_time(test=None, globs=None, now=1278864701.5):
    now = [now]
    def faux_time():
        now[0] += 1
        return now[0]

    if test is None and globs is not None:
        # sigh
        faux_time.globs = globs
        test = faux_time

    import time
    zope.testing.setupstack.register(test, setattr, time, 'time', time.time)

    time.time = faux_time

