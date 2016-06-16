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
from ZODB.MappingStorage import DB

import atexit
import os
import persistent
import re
import sys
import tempfile
import time
import transaction
import unittest
import warnings
import ZODB.utils
import zope.testing.setupstack
from zope.testing import renormalizing

checker = renormalizing.RENormalizing([
    (re.compile("<(.*?) object at 0x[0-9a-f]*?>"),
     r"<\1 object at 0x000000000000>"),
    # Python 3 bytes add a "b".
    (re.compile("b('.*?')"),
     r"\1"),
    (re.compile('b(".*?")'),
     r"\1"),
    # Python 3 adds module name to exceptions.
    (re.compile("ZODB.interfaces.BlobError"),
     r"BlobError"),
    (re.compile("ZODB.blob.BlobStorageError"),
     r"BlobStorageError"),
    (re.compile("ZODB.broken.BrokenModified"),
     r"BrokenModified"),
    (re.compile("ZODB.POSException.POSKeyError"),
     r"POSKeyError"),
    (re.compile("ZODB.POSException.ConflictError"),
     r"ConflictError"),
    (re.compile("ZODB.POSException.ReadConflictError"),
     r"ReadConflictError"),
    (re.compile("ZODB.POSException.InvalidObjectReference"),
     r"InvalidObjectReference"),
    (re.compile("ZODB.POSException.ReadOnlyHistoryError"),
     r"ReadOnlyHistoryError"),
    (re.compile("ZODB.POSException.Unsupported"),
     r"Unsupported"),
    (re.compile("ZConfig.ConfigurationSyntaxError"),
     r"ConfigurationSyntaxError"),
    ])

def setUp(test, name='test'):
    clear_transaction_syncs()
    transaction.abort()
    d = tempfile.mkdtemp(prefix=name)
    zope.testing.setupstack.register(test, zope.testing.setupstack.rmtree, d)
    zope.testing.setupstack.register(
        test, setattr, tempfile, 'tempdir', tempfile.tempdir)
    tempfile.tempdir = d
    zope.testing.setupstack.register(test, os.chdir, os.getcwd())
    os.chdir(d)
    zope.testing.setupstack.register(test, transaction.abort)

def tearDown(test):
    clear_transaction_syncs()
    zope.testing.setupstack.tearDown(test)

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
    for i in range(int(timeout*100)):
        if func():
            return
        time.sleep(.01)
    raise AssertionError

def store(storage, oid, value='x', serial=ZODB.utils.z64):
    if not isinstance(oid, bytes):
        oid = ZODB.utils.p64(oid)
    if not isinstance(serial, bytes):
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

    if isinstance(time,type):
        time.time = staticmethod(faux_time) # jython
    else:
        time.time = faux_time

def clear_transaction_syncs():
    """Clear data managers registered with the global transaction manager

    Many tests don't clean up synchronizer's registered with the
    global transaction managers, which can wreak havoc with following
    tests, now that connections interact with their storages at
    transaction boundaries.  We need to make sure that we clear any
    registered data managers.

    For now, we'll use the transaction manager's
    underware. Eventually, an transaction managers need to grow an API
    for this.
    """
    transaction.manager.clearSynchs()
