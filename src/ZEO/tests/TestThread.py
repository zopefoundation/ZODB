##############################################################################
#
# Copyright (c) 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""A Thread base class for use with unittest."""

from cStringIO import StringIO
import threading
import traceback

class TestThread(threading.Thread):
    __super_init = threading.Thread.__init__
    __super_run = threading.Thread.run

    def __init__(self, testcase, group=None, target=None, name=None,
                 args=(), kwargs={}, verbose=None):
        self.__super_init(group, target, name, args, kwargs, verbose)
        self.setDaemon(1)
        self._testcase = testcase

    def run(self):
        try:
            self.testrun()
        except Exception, err:
            s = StringIO()
            traceback.print_exc(file=s)
            self._testcase.fail("Exception in thread %s:\n%s\n" %
                                (self, s.getvalue()))

    def cleanup(self, timeout=15):
        self.join(timeout)
        if self.isAlive():
            self._testcase.fail("Thread did not finish: %s" % self)
