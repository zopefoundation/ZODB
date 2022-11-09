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
import atexit
import doctest
import os
import pdb
import re
import runpy
import sys
import tempfile
import time
import unittest
import warnings

import persistent
import transaction
import zope.testing.setupstack
from zope.testing import renormalizing

import ZODB.utils
from ZODB.Connection import TransactionMetaData
# BBB
from ZODB.MappingStorage import DB  # noqa: F401 import unused


try:
    from unittest import mock
except ImportError:
    import mock

import functools
from time import gmtime as _real_gmtime
from time import time as _real_time

import six


_current_time = _real_time()


checker = renormalizing.RENormalizing([
    (re.compile("<(.*?) object at 0x[0-9a-f]*?>"),
     r"<\1 object at 0x000000000000>"),
    # Python 3 bytes add a "b".
    (re.compile("b('.*?')"),
     r"\1"),
    (re.compile('b(".*?")'),
     r"\1"),
    # Persistent 4.4 changes the repr of persistent subclasses,
    # and it is slightly different with the C extension and
    # pure-Python module
    (re.compile('ZODB.tests.testcrossdatabasereferences.'),
     ''),
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

    # propagate .level from tested method to TestCase so that e.g. @long_test
    # works
    @property
    def level(self):
        f = getattr(self, self._testMethodName)
        return getattr(f, 'level', 1)


def long_test(f):
    """
    long_test decorates f to be marked as long-running test.

    Use `zope-testrunner --at-level=1` to run tests without the long-ones.
    """
    f.level = 2
    return f


def pack(db):
    db.pack(time.time()+1)


class P(persistent.Persistent):

    def __init__(self, name=None):
        self.name = name

    def __repr__(self):
        return 'P(%s)' % self.name


class MininalTestLayer(object):

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
    for _ in range(int(timeout*100)):
        if func():
            return
        time.sleep(.01)
    raise AssertionError


def store(storage, oid, value='x', serial=ZODB.utils.z64):
    if not isinstance(oid, bytes):
        oid = ZODB.utils.p64(oid)
    if not isinstance(serial, bytes):
        serial = ZODB.utils.p64(serial)
    t = TransactionMetaData()
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

    if isinstance(time, type):
        time.time = staticmethod(faux_time)  # jython
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


class _TimeWrapper(object):

    def __init__(self, granularity=1.0):
        self._granularity = granularity
        self._lock = ZODB.utils.Lock()
        self.fake_gmtime = mock.Mock()
        self.fake_time = mock.Mock()
        self._configure_fakes()

    def _configure_fakes(self):
        def incr():
            global _current_time  # pylint:disable=global-statement
            with self._lock:
                _current_time = max(
                    _real_time(), _current_time + self._granularity)
            return _current_time
        self.fake_time.side_effect = incr

        def incr_gmtime(seconds=None):
            if seconds is not None:
                now = seconds
            else:
                now = incr()
            return _real_gmtime(now)
        self.fake_gmtime.side_effect = incr_gmtime

    def install_fakes(self):
        time.time = self.fake_time
        time.gmtime = self.fake_gmtime

    __enter__ = install_fakes

    def close(self, *args):
        time.time = _real_time
        time.gmtime = _real_gmtime

    __exit__ = close

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        return wrapper


def time_monotonically_increases(func_or_granularity):
    """
    Decorate a unittest method with this function to cause the value
    of :func:`time.time` and :func:`time.gmtime` to monotonically
    increase by one each time it is called. This ensures things like
    last modified dates always increase.

    We make three guarantees about the value of :func:`time.time`
    returned while the decorated function is running:

        1. It is always *at least* the value of the *real*
           :func:`time.time`;

        2. Each call returns a value greater than the previous call;

        3. Those two constraints hold across different invocations of
           functions decorated. This decorator can be applied to a
           method in a test case::

               class TestThing(unittest.TestCase)
                   @time_monotonically_increases
                   def test_method(self):
                     t = time.time()
                      ...

    It can also be applied to a bare function taking any number of
    arguments::

        @time_monotonically_increases
        def utility_function(a, b, c=1):
           t = time.time()
           ...

    By default, the time will be incremented in 1.0 second intervals.
    You can specify a particular granularity as an argument; this is
    useful to keep from running too far ahead of the real clock::

        @time_monotonically_increases(0.1)
        def smaller_increment():
            t1 = time.time()
            t2 = time.time()
            assrt t2 == t1 + 0.1
    """
    if isinstance(func_or_granularity, (six.integer_types, float)):
        # We're being used as a factory.
        wrapper_factory = _TimeWrapper(func_or_granularity)
        return wrapper_factory

    # We're being used bare
    wrapper_factory = _TimeWrapper()
    return wrapper_factory(func_or_granularity)


def reset_monotonic_time(value=0.0):
    """
    Make the monotonic clock return the real time on its next
    call.
    """

    global _current_time  # pylint:disable=global-statement
    _current_time = value


class MonotonicallyIncreasingTimeMinimalTestLayer(MininalTestLayer):

    def testSetUp(self):
        self.time_manager = _TimeWrapper()
        self.time_manager.install_fakes()

    def testTearDown(self):
        self.time_manager.close()
        reset_monotonic_time()


def with_high_concurrency(f):
    """
    with_high_concurrency decorates f to run with high frequency of thread
    context switches.

    It is useful for tests that try to probabilistically reproduce race
    condition scenarios.
    """
    @functools.wraps(f)
    def _(*argv, **kw):
        if six.PY3:
            # Python3, by default, switches every 5ms, which turns threads in
            # intended "high concurrency" scenarios to execute almost serially.
            # Raise the frequency of context switches in order to increase the
            # probability to reproduce interesting/tricky overlapping of
            # threads.
            #
            # See https://github.com/zopefoundation/ZODB/pull/345#issuecomment-822188305 and  # noqa: E501 line too long
            # https://github.com/zopefoundation/ZEO/issues/168#issuecomment-821829116 for details.  # noqa: E501 line too long
            _ = sys.getswitchinterval()

            def restore():
                sys.setswitchinterval(_)
            # ~ 100 simple instructions on modern hardware
            sys.setswitchinterval(5e-6)

        else:
            # Python2, by default, switches threads every "100 instructions".
            # Just make sure we run f with that default.
            _ = sys.getcheckinterval()

            def restore():
                sys.setcheckinterval(_)
            sys.setcheckinterval(100)

        try:
            return f(*argv, **kw)
        finally:
            restore()

    return _


def run_module_as_script(mod, args, stdout="stdout", stderr="stderr"):
    """run module *mod* as script with arguments *arg*.

    stdout and stderr are redirected to files given by the
    correcponding parameters.

    The function is usually called in a ``setUp/tearDown`` frame
    which will remove the created files.
    """
    sargv, sout, serr = sys.argv, sys.stdout, sys.stderr
    s_set_trace = pdb.set_trace
    try:
        sys.argv = [sargv[0]] + args
        sys.stdout = open(stdout, "w")
        sys.stderr = open(stderr, "w")
        # to allow debugging
        pdb.set_trace = doctest._OutputRedirectingPdb(sout)
        runpy.run_module(mod, run_name="__main__", alter_sys=True)
    finally:
        sys.stdout.close()
        sys.stderr.close()
        pdb.set_trace = s_set_trace
        sys.argv, sys.stdout, sys.stderr = sargv, sout, serr
