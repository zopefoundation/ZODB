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
# FOR A PARTICULAR PURPOSE
#
##############################################################################

import os
import signal
import sys
import tempfile
import time
import unittest
import errno

import ZEO.start
from ZEO.ClientStorage import ClientStorage
from ZEO.util import Environment

try:
    from ZODB.tests.StorageTestBase import removefs
except ImportError:
    # for compatibility with Zope 2.5 &c.
    import errno

    def removefs(base):
        """Remove all files created by FileStorage with path base."""
        for ext in '', '.old', '.tmp', '.lock', '.index', '.pack':
            path = base + ext
            try:
                os.remove(path)
            except os.error, err:
                if err[0] != errno.ENOENT:
                    raise


class StartTests(unittest.TestCase):

    def setUp(self):
        startfile = ZEO.start.__file__
        if startfile[-1] == 'c':
            startfile = startfile[:-1]
        self.env = Environment(startfile)
        self.cmd = '%s %s' % (sys.executable, startfile)
        self.pids = {}

    def tearDown(self):
        try:
            self.stop_server()
            self.shutdown()
        finally:
            removefs("Data.fs")
            try:
                os.remove(self.env.zeo_pid)
            except os.error:
                pass

    def getpids(self):
        if not os.path.exists(self.env.zeo_pid):
            # If there's no pid file, assume the server isn't running
            return None, None
        return map(int, open(self.env.zeo_pid).read().split())

    def stop_server(self):
        ppid, pid = self.getpids()
        if ppid is None:
            return
        self.kill(pids=[pid])

    def kill(self, sig=signal.SIGTERM, pids=None):
        if pids is None:
            pids = self.pids.keys()
        for pid in pids:
            try:
                os.kill(pid, sig)
            except os.error, err:
                print err

    def wait(self, flag=0, pids=None):
        if pids is None:
            pids = self.pids.keys()
        alive = []
        for pid in pids:
            try:
                _pid, status = os.waitpid(pid, flag)
            except os.error, err:
                if err[0] == 10:
                    continue
                print err
            else:
                if status == 0:
                    alive.append(pid)
        return alive

    def shutdown(self):
        # XXX This is probably too complicated, but I'm not sure what
        # the right thing to do is.
        alive = self.wait(os.WNOHANG)
        if not alive:
            return
        self.kill(pids=alive)
        alive = self.wait(os.WNOHANG, alive)
        if not alive:
            return
        self.kill(signal.SIGKILL, pids=alive)
        alive = self.wait(pids=alive)

    def fork(self, *args):
        file = tempfile.mktemp()
        pid = os.fork()
        if pid:
            self.pids[pid] = file
            return file
        else:
            try:
                f = os.popen(self.cmd + " " + " ".join(args))
                buf = f.read()
                f.close()
                f = open(file, "wb")
                f.write(buf)
                f.close()
            finally:
                os._exit(0)

    def system(self, *args):
        file = self.fork(*args)
        self.wait()
        f = open(file, "rb")
        buf = f.read()
        f.close()
        return buf

    def connect(self, port=None, wait=1):
        cs = ClientStorage(('', port), wait=wait)
        cs.close()

    def testNoPort(self):
        outp = self.system("-s")
        self.assert_(outp.find("No port specified") != -1)

    def testStart(self):
        port = 9090
        outp = self.fork("-s", "-p", str(port))
        self.connect(port=port)

    def testLogRestart(self):
        port = 9090
        logfile1 = tempfile.mktemp(suffix="log")
        logfile2 = tempfile.mktemp(suffix="log")
        os.environ["STUPID_LOG_FILE"] = logfile1
        os.environ["EVENT_LOG_FILE"] = logfile1

        try:
            outp = self.fork("-s", "-p", str(port))
            self.connect(port=port)
            buf1 = None
            for i in range(10):
                try:
                    buf1 = open(logfile1).read()
                except IOError, e:
                    if e.errno != errno.ENOENT:
                        raise
                    time.sleep(1)
                else:
                    break
            self.assert_(buf1)
            os.rename(logfile1, logfile2)
            ppid, pid = self.getpids()
    ##        os.kill(ppid, signal.SIGHUP)
            os.kill(pid, signal.SIGHUP)
            self.connect(port=port)
            buf2 = open(logfile1).read()
            self.assert_(buf2)
        finally:
            self.shutdown()
            try:
                os.unlink(logfile1)
            except os.error:
                pass
            try:
                os.unlink(logfile2)
            except os.error:
                pass

def test_suite():

    # shutup warnings about mktemp
    import warnings
    warnings.filterwarnings("ignore", "mktemp")

    if os.name == "posix":
        return unittest.makeSuite(StartTests)
    else:
        # Don't even bother with these tests on Windows
        return None
