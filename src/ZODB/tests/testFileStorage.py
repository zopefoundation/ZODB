import sys, os

sys.path.insert(0, '.')

import ZODB.FileStorage
import unittest, UndoVersionStorage

class FileStorageTests(UndoVersionStorage.UndoVersionStorage,
                       unittest.TestCase):

    def setUp(self):
        self._storage = ZODB.FileStorage.FileStorage(
            'FileStorageTests.fs', create=1)
        UndoVersionStorage.UndoVersionStorage.setUp(self)

    def tearDown(self):
        UndoVersionStorage.UndoVersionStorage.tearDown(self)
        os.remove('FileStorageTests.fs')

def test_suite():
    return unittest.makeSuite(FileStorageTests, 'check')

def main():
    alltests=test_suite()
    runner = unittest.TextTestRunner()
    runner.run(alltests)

def debug():
   test_suite().debug()

def pdebug():
    import pdb
    pdb.run('debug()')

if __name__=='__main__':
   if len(sys.argv) > 1:
      globals()[sys.argv[1]]()
   else:
      main()
