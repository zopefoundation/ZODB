# Some simple tests for zeopack.py
# For this to work, zeopack.py must by on your PATH.

from ZODB.FileStorage import FileStorage
from ZODB.tests.StorageTestBase import StorageTestBase
from ZEO.tests import forker
import ZODB

import os
import socket
import tempfile
import threading
import time
import unittest

# TODO:  The forker interface isn't clearly defined.  It's different on
# different branches of ZEO.  This will break someday.

# TODO:  Only handle the Unix variant of the forker.  Just to give Tim
# something to do.

class PackerTests(StorageTestBase):

    def setUp(self):
        StorageTestBase.setUp(self)
        self.started = 0

    def start(self):
        self.started =1
        self.path = 'Data.fs'
        self._storage = FileStorage(self.path)
        self.db = ZODB.DB(self._storage)
        self.do_updates()
        self.pid, self.exit = forker.start_zeo_server(self._storage, self.addr)

    def do_updates(self):
        for i in range(100):
            self._dostore()

    def tearDown(self):
        if self.started:
            self.db.close()
            self.exit.close()
            try:
                os.kill(self.pid, 9)
            except os.error:
                pass
            try:
                os.waitpid(self.pid, 0)
            except os.error, err:
                ##print "waitpid failed", err
                pass
        StorageTestBase.tearDown(self)

    def set_inet_addr(self):
        self.host = socket.gethostname()
        self.port = forker.get_port()
        self.addr = self.host, self.port

    def testPack(self):
        self.set_inet_addr()
        self.start()
        status = os.system("zeopack.py -h %s -p %s" % (self.host, self.port))
        assert status == 0
        assert os.path.exists(self.path + ".old")

    def testPackDays(self):
        self.set_inet_addr()
        self.start()
        status = os.system("zeopack.py -h %s -p %s -d 1" % (self.host,
                                                            self.port))
        # Since we specified one day, nothing should get packed
        assert status == 0
        assert not os.path.exists(self.path + ".old")

    def testAF_UNIXPack(self):
        self.addr = tempfile.mktemp(suffix=".zeo-socket")
        self.start()
        status = os.system("zeopack.py -U %s" % self.addr)
        assert status == 0
        assert os.path.exists(self.path + ".old")

    def testNoServer(self):
        status = os.system("zeopack.py -p 19")
        assert status != 0

    def testWaitForServer(self):
        self.set_inet_addr()
        def delayed_start():
            time.sleep(11)
            self.start()
        t = threading.Thread(target=delayed_start)
        t.start()
        status = os.system("zeopack.py -h %s -p %s -W" % (self.host,
                                                          self.port))
        t.join()
        assert status == 0
        assert os.path.exists(self.path + ".old")

class UpTest(unittest.TestCase):

    def testUp(self):
        status = os.system("zeoup.py -p 19")
        # There is no ZEO server on port 19, so we should see non-zero
        # exit status.
        assert status != 0

if __name__ == "__main__":
    unittest.main()
