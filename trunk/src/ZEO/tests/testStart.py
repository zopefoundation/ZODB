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

import ZEO.start
from ZEO.ClientStorage import ClientStorage
from ZEO.util import Environment

class StartTests(unittest.TestCase):

    cmd = "%s %s" % (sys.executable, ZEO.start.__file__)
    if cmd[-1] == "c":
        cmd = cmd[:-1]

    def setUp(self):
        self.pids = {}
        self.env = Environment(self.cmd)

    def tearDown(self):
        try:
            self.stop_server()
            self.shutdown()
        finally:
            for ext in "", ".index", ".tmp", ".lock", ".old":
                f = "Data.fs" + ext
                try:
                    os.remove(f)
                except os.error:
                    pass
            try:
                os.remove(self.env.zeo_pid)
            except os.error:
                pass

    def stop_server(self):
        if not os.path.exists(self.env.zeo_pid):
            # If there's no pid file, assume the server isn't running
            return
        ppid, pid = map(int, open(self.env.zeo_pid).read().split())
        self.kill(pids=[pid])

    def kill(self, sig=signal.SIGTERM, pids=None):
        if pids is None:
            pids = self.pids
        for pid in pids:
            try:
                os.kill(pid, sig)
            except os.error, err:
                print err

    def wait(self, flag=0, pids=None):
        if pids is None:
            pids = self.pids
        alive = []
        for pid in self.pids:
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
        
def test_suite():
    if os.name == "posix":
        return unittest.makeSuite(StartTests)
    else:
        # Don't even bother with these tests on Windows
        return None

