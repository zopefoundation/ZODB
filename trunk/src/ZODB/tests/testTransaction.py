"""High-level tests of the transaction interface"""

import os
import tempfile
import unittest

import ZODB
from ZODB.DB import DB
from ZODB.FileStorage import FileStorage
from ZODB.tests.MinPO import MinPO

class TransactionTestBase(unittest.TestCase):

    def setUp(self):
        self.fs_path = tempfile.mktemp()
        self.fs = FileStorage(self.fs_path)
        db = DB(self.fs)
        conn = db.open()
        self.root = conn.root()

    def tearDown(self):
        self.fs.close()
        for ext in '', '.index', '.lock', '.tmp':
            path = self.fs_path + ext
            if os.path.exists(path):
                os.unlink(path)

class BasicTests:

    def checkSingleCommit(self, subtrans=None):
        self.root["a"] = MinPO("a")
        get_transaction().commit(subtrans)
        assert self.root["a"].value == "a"

    def checkMultipleCommits(self, subtrans=None):
        a = self.root["a"] = MinPO("a")
        get_transaction().commit(subtrans)
        a.extra_attr = MinPO("b")
        get_transaction().commit(subtrans)
        del a
        assert self.root["a"].value == "a"
        assert self.root["a"].extra_attr == MinPO("b")

    def checkCommitAndAbort(self, subtrans=None):
        a = self.root["a"] = MinPO("a")
        get_transaction().commit(subtrans)
        a.extra_attr = MinPO("b")
        get_transaction().abort()
        del a
        if subtrans:
            assert not self.root.has_key("a")
        else:
            assert self.root["a"].value == "a"
            assert not hasattr(self.root["a"], 'extra_attr')

class SubtransTests:

    def wrap_test(self, klass, meth_name):
        obj = klass()
        obj.root = self.root
        meth = getattr(obj, meth_name)
        meth(1)
        get_transaction().commit()

    checkSubSingleCommit = lambda self:\
                           self.wrap_test(BasicTests, "checkSingleCommit")

    checkSubMultipleCommits = lambda self:\
                              self.wrap_test(BasicTests,
                                             "checkMultipleCommits")

    checkSubCommitAndAbort = lambda self:\
                             self.wrap_test(BasicTests,
                                            "checkCommitAndAbort")

class AllTests(TransactionTestBase, BasicTests, SubtransTests):
    pass

def main():
    tests = unittest.makeSuite(AllTests, 'check')
    runner = unittest.TextTestRunner()
    runner.run(tests)

if __name__ == "__main__":
    main()
