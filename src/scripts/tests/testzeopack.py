# Some simple tests for zeopack.py
# For this to work, zeopack.py must by on your PATH.

from ZODB.FileStorage import FileStorage
from ZODB.tests.StorageTestBase import StorageTestBase
from ZEO.tests import forker
import ZODB

import os
import socket
import tempfile
import unittest

# XXX The forker interface isn't clearly defined.  It's different on
# different branches of ZEO.  This will break someday.

# XXX Only handle the Unix variant of the forker.  Just to give Tim
# something to do.

class PackerTests(StorageTestBase):

    def start(self):
        self.path = tempfile.mktemp(suffix=".fs")
        self._storage = FileStorage(self.path)
        self.db = ZODB.DB(self._storage)
        self.do_updates()
        self.pid, self.exit = forker.start_zeo_server(self._storage, self.addr)

    def do_updates(self):
        for i in range(100):
            self._dostore()

    def tearDown(self):
        self.db.close()
        self._storage.close()
        self.exit.close()
        try:
            os.kill(self.pid, 9)
        except os.error:
            pass
        try:
            os.waitpid(self.pid, 0)
        except os.error, err:
            print err
        for ext in '', '.old', '.lock', '.index', '.tmp':
            path = self.path + ext
            try:
                os.remove(path)
            except os.error:
                pass

    def set_inet_addr(self):
        self.host = socket.gethostname()
        self.port = forker.get_port()
        self.addr = self.host, self.port

    def testPack(self):
        self.set_inet_addr()
        self.start()
        os.system("zeopack.py -h %s -p %s" % (self.host, self.port))
        assert os.path.exists(self.path + ".old")

    def testAF_UNIXPack(self):
        self.addr = tempfile.mktemp(suffix=".zeo-socket")
        self.start()
        os.system("zeopack.py -U %s" % self.addr)
        assert os.path.exists(self.path + ".old")

if __name__ == "__main__":
    unittest.main()
