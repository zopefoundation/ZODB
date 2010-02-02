##############################################################################
#
# Copyright (c) 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""A Thread base class for use with unittest."""

import threading
import sys

class TestThread(threading.Thread):
    """Base class for defining threads that run from unittest.

    The subclass should define a testrun() method instead of a run()
    method.

    Call cleanup() when the test is done with the thread, instead of join().
    If the thread exits with an uncaught exception, it's captured and
    re-raised when cleanup() is called.  cleanup() should be called by
    the main thread!  Trying to tell unittest that a test failed from
    another thread creates a nightmare of timing-depending cascading
    failures and missed errors (tracebacks that show up on the screen,
    but don't cause unittest to believe the test failed).

    cleanup() also joins the thread.  If the thread ended without raising
    an uncaught exception, and the join doesn't succeed in the timeout
    period, then the test is made to fail with a "Thread still alive"
    message.
    """

    def __init__(self, testcase):
        threading.Thread.__init__(self)
        # In case this thread hangs, don't stop Python from exiting.
        self.setDaemon(1)
        self._exc_info = None
        self._testcase = testcase

    def run(self):
        try:
            self.testrun()
        except:
            self._exc_info = sys.exc_info()

    def cleanup(self, timeout=15):
        self.join(timeout)
        if self._exc_info:
            raise self._exc_info[0], self._exc_info[1], self._exc_info[2]
        if self.isAlive():
            self._testcase.fail("Thread did not finish: %s" % self)
