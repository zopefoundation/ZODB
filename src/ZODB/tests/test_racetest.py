##############################################################################
#
# Copyright (c) 2019 - 2023 Zope Foundation and Contributors.
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
from time import sleep
from unittest import TestCase

from .racetest import TestWorkGroup


class TestWorkGroupTests(TestCase):
    def setUp(self):
        self._failed = failed = []
        case_mockup = SimpleNamespace(fail=failed.append)
        self.tg = TestWorkGroup(case_mockup)

    @property
    def failed(self):
        return "\n\n".join(self._failed)

    def test_success(self):
        tg = self.tg
        tg.go(tg_test_function)
        tg.wait(10)
        self.assertEqual(self.failed, "")

    def test_failure1(self):
        tg = self.tg
        tg.go(tg_test_function, T_FAIL)
        tg.wait(10)
        self.assertEqual(self.failed, "T0 failed")

    def test_failure1_okmany(self):
        tg = self.tg
        tg.go(tg_test_function, T_SUCCESS)
        tg.go(tg_test_function, T_SUCCESS)
        tg.go(tg_test_function, T_SUCCESS)
        tg.go(tg_test_function, T_FAIL)
        tg.wait(10)
        self.assertEqual(self.failed, "T3 failed")

    def test_failure_many(self):
        tg = self.tg
        tg.go(tg_test_function, T_FAIL)
        tg.go(tg_test_function, T_SUCCESS)
        tg.go(tg_test_function, T_FAIL)
        tg.go(tg_test_function, T_SUCCESS)
        tg.go(tg_test_function, T_FAIL)
        tg.wait(10)
        self.assertIn("T0 failed", self.failed)
        self.assertIn("T2 failed", self.failed)
        self.assertIn("T4 failed", self.failed)
        self.assertNotIn("T1 failed", self.failed)
        self.assertNotIn("T3 failed", self.failed)

    def test_exception(self):
        tg = self.tg
        tg.go(tg_test_function, T_EXC)
        tg.wait(10)
        self.assertIn("Unhandled exception", self.failed)
        self.assertIn("in thread T0", self.failed)

    def test_timeout(self):
        tg = self.tg
        tg.go(tg_test_function, T_SLOW)
        tg.wait(0.1)
        self.assertEqual(self.failed,
                         "test did not finish within 0.1 seconds")

    def test_thread_unfinished(self):
        tg = self.tg
        tg.go(tg_test_function, T_SLOW)
        tg.go(tg_test_function, T_SLOW, 2)
        tg.go(tg_test_function, T_SLOW, wait_time=2)
        tg.wait(0.1)
        self.assertEqual(self.failed,
                         "test did not finish within 0.1 seconds\n\n"
                         "threads did not finish: ['T2']")


T_SUCCESS = 0
T_SLOW = 1
T_EXC = 3
T_FAIL = 4


def tg_test_function(tg, tx, mode=T_SUCCESS, waits=1, wait_time=0.2):
    if mode == T_SUCCESS:
        return
    if mode == T_FAIL:
        tg.fail("T%d failed" % tx)
        return
    if mode == T_EXC:
        raise ValueError(str(tx))
    assert mode == T_SLOW
    while waits:
        waits -= 1
        if tg.failed():
            return
        sleep(wait_time)


try:
    from types import SimpleNamespace
except ImportError:
    # PY2
    class SimpleNamespace(object):
        def __init__(self, **kw):
            self.__dict__.update(kw)
