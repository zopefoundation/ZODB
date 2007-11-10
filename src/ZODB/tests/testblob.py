##############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
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

import base64, os, shutil, tempfile, unittest
import time
from zope.testing import doctest
import ZODB.tests.util

from ZODB import utils
from ZODB.FileStorage import FileStorage
from ZODB.blob import Blob, BlobStorage
import ZODB.blob
from ZODB.DB import DB
import transaction

from ZODB.tests.testConfig import ConfigTestBase
from ZConfig import ConfigurationSyntaxError


def new_time():
    """Create a _new_ time stamp.

    This method also makes sure that after retrieving a timestamp that was
    *before* a transaction was committed, that at least one second passes so
    the packing time actually is before the commit time.

    """
    now = new_time = time.time()
    while new_time <= now:
        new_time = time.time()
    time.sleep(1)
    return new_time


class BlobConfigTestBase(ConfigTestBase):

    def setUp(self):
        super(BlobConfigTestBase, self).setUp()

        self.blob_dir = tempfile.mkdtemp()

    def tearDown(self):
        super(BlobConfigTestBase, self).tearDown()

        shutil.rmtree(self.blob_dir)


class ZODBBlobConfigTest(BlobConfigTestBase):

    def test_map_config1(self):
        self._test(
            """
            <zodb>
              <blobstorage>
                blob-dir %s
                <mappingstorage/>
              </blobstorage>
            </zodb>
            """ % self.blob_dir)

    def test_file_config1(self):
        path = tempfile.mktemp()
        self._test(
            """
            <zodb>
              <blobstorage>
                blob-dir %s
                <filestorage>
                  path %s
                </filestorage>
              </blobstorage>
            </zodb>
            """ %(self.blob_dir, path))
        os.unlink(path)
        os.unlink(path+".index")
        os.unlink(path+".tmp")

    def test_blob_dir_needed(self):
        self.assertRaises(ConfigurationSyntaxError,
                          self._test,
                          """
                          <zodb>
                            <blobstorage>
                              <mappingstorage/>
                            </blobstorage>
                          </zodb>
                          """)


class BlobUndoTests(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.here = os.getcwd()
        os.chdir(self.test_dir)
        self.storagefile = 'Data.fs'
        os.mkdir('blobs')
        self.blob_dir = 'blobs'

    def tearDown(self):
        os.chdir(self.here)
        ZODB.blob.remove_committed_dir(self.test_dir)

    def testUndoWithoutPreviousVersion(self):
        base_storage = FileStorage(self.storagefile)
        blob_storage = BlobStorage(self.blob_dir, base_storage)
        database = DB(blob_storage)
        connection = database.open()
        root = connection.root()
        transaction.begin()
        root['blob'] = Blob()
        transaction.commit()

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        # the blob footprint object should exist no longer
        self.assertRaises(KeyError, root.__getitem__, 'blob')
        database.close()
        
    def testUndo(self):
        base_storage = FileStorage(self.storagefile)
        blob_storage = BlobStorage(self.blob_dir, base_storage)
        database = DB(blob_storage)
        connection = database.open()
        root = connection.root()
        transaction.begin()
        blob = Blob()
        blob.open('w').write('this is state 1')
        root['blob'] = blob
        transaction.commit()

        transaction.begin()
        blob = root['blob']
        blob.open('w').write('this is state 2')
        transaction.commit()


        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()
        self.assertEqual(blob.open('r').read(), 'this is state 1')

        database.close()

    def testUndoAfterConsumption(self):
        base_storage = FileStorage(self.storagefile)
        blob_storage = BlobStorage(self.blob_dir, base_storage)
        database = DB(blob_storage)
        connection = database.open()
        root = connection.root()
        transaction.begin()
        open('consume1', 'w').write('this is state 1')
        blob = Blob()
        blob.consumeFile('consume1')
        root['blob'] = blob
        transaction.commit()
        
        transaction.begin()
        blob = root['blob']
        open('consume2', 'w').write('this is state 2')
        blob.consumeFile('consume2')
        transaction.commit()

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        self.assertEqual(blob.open('r').read(), 'this is state 1')

        database.close()

    def testRedo(self):
        base_storage = FileStorage(self.storagefile)
        blob_storage = BlobStorage(self.blob_dir, base_storage)
        database = DB(blob_storage)
        connection = database.open()
        root = connection.root()
        blob = Blob()

        transaction.begin()
        blob.open('w').write('this is state 1')
        root['blob'] = blob
        transaction.commit()

        transaction.begin()
        blob = root['blob']
        blob.open('w').write('this is state 2')
        transaction.commit()

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        self.assertEqual(blob.open('r').read(), 'this is state 1')

        serial = base64.encodestring(blob_storage._tid)

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        self.assertEqual(blob.open('r').read(), 'this is state 2')

        database.close()

    def testRedoOfCreation(self):
        base_storage = FileStorage(self.storagefile)
        blob_storage = BlobStorage(self.blob_dir, base_storage)
        database = DB(blob_storage)
        connection = database.open()
        root = connection.root()
        blob = Blob()

        transaction.begin()
        blob.open('w').write('this is state 1')
        root['blob'] = blob
        transaction.commit()

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        self.assertRaises(KeyError, root.__getitem__, 'blob')

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        self.assertEqual(blob.open('r').read(), 'this is state 1')

        database.close()

def gc_blob_removes_uncommitted_data():
    """
    >>> from ZODB.blob import Blob
    >>> blob = Blob()
    >>> blob.open('w').write('x')
    >>> fname = blob._p_blob_uncommitted
    >>> os.path.exists(fname)
    True
    >>> blob = None
    >>> os.path.exists(fname)
    False
    """

def commit_from_wrong_partition():
    """
    It should be possible to commit changes even when a blob is on a
    different partition.

    We can simulare this by temporarily breaking os.rename. :)

    >>> def fail(*args):
    ...     raise OSError

    >>> os_rename = os.rename
    >>> os.rename = fail

    >>> import logging, sys
    >>> logger = logging.getLogger('ZODB.blob.copied')
    >>> handler = logging.StreamHandler(sys.stdout)
    >>> logger.propagate = False
    >>> logger.setLevel(logging.DEBUG)
    >>> logger.addHandler(handler)

    >>> import transaction
    >>> from ZODB.MappingStorage import MappingStorage
    >>> from ZODB.blob import BlobStorage
    >>> from ZODB.DB import DB
    >>> from tempfile import mkdtemp
    >>> base_storage = MappingStorage("test")
    >>> blob_dir = mkdtemp()
    >>> blob_storage = BlobStorage(blob_dir, base_storage)
    >>> database = DB(blob_storage)
    >>> connection = database.open()
    >>> root = connection.root()
    >>> from ZODB.blob import Blob
    >>> root['blob'] = Blob()
    >>> root['blob'].open('w').write('test')
    >>> transaction.commit() # doctest: +ELLIPSIS
    Copied blob file ...

    >>> root['blob'].open().read()
    'test'

Works with savepoints too:

    >>> root['blob2'] = Blob()
    >>> root['blob2'].open('w').write('test2')
    >>> _ = transaction.savepoint() # doctest: +ELLIPSIS
    Copied blob file ...

    >>> transaction.commit() # doctest: +ELLIPSIS
    Copied blob file ...
    
    >>> root['blob2'].open().read()
    'test2'

    >>> os.rename = os_rename
    >>> logger.propagate = True
    >>> logger.setLevel(0)
    >>> logger.removeHandler(handler)

    """


def packing_with_uncommitted_data_non_undoing():
    """
    This covers regression for bug #130459.

    When uncommitted data exists it formerly was written to the root of the
    blob_directory and confused our packing strategy. We now use a separate
    temporary directory that is ignored while packing.

    >>> import transaction
    >>> from ZODB.MappingStorage import MappingStorage
    >>> from ZODB.blob import BlobStorage
    >>> from ZODB.DB import DB
    >>> from ZODB.serialize import referencesf
    >>> from tempfile import mkdtemp

    >>> base_storage = MappingStorage("test")
    >>> blob_dir = mkdtemp()
    >>> blob_storage = BlobStorage(blob_dir, base_storage)
    >>> database = DB(blob_storage)
    >>> connection = database.open()
    >>> root = connection.root()
    >>> from ZODB.blob import Blob
    >>> root['blob'] = Blob()
    >>> connection.add(root['blob'])
    >>> root['blob'].open('w').write('test')

    >>> blob_storage.pack(new_time(), referencesf)

    Clean up:

    >>> database.close()
    >>> import shutil
    >>> shutil.rmtree(blob_dir)

    """

def packing_with_uncommitted_data_undoing():
    """
    This covers regression for bug #130459.

    When uncommitted data exists it formerly was written to the root of the
    blob_directory and confused our packing strategy. We now use a separate
    temporary directory that is ignored while packing.

    >>> import transaction
    >>> from ZODB.FileStorage.FileStorage import FileStorage
    >>> from ZODB.blob import BlobStorage
    >>> from ZODB.DB import DB
    >>> from ZODB.serialize import referencesf
    >>> from tempfile import mkdtemp, mktemp

    >>> storagefile = mktemp()
    >>> base_storage = FileStorage(storagefile)
    >>> blob_dir = mkdtemp()
    >>> blob_storage = BlobStorage(blob_dir, base_storage)
    >>> database = DB(blob_storage)
    >>> connection = database.open()
    >>> root = connection.root()
    >>> from ZODB.blob import Blob
    >>> root['blob'] = Blob()
    >>> connection.add(root['blob'])
    >>> root['blob'].open('w').write('test')

    >>> blob_storage.pack(new_time(), referencesf)

    Clean up:

    >>> database.close()
    >>> import shutil
    >>> shutil.rmtree(blob_dir)

    >>> os.unlink(storagefile)
    >>> os.unlink(storagefile+".index")
    >>> os.unlink(storagefile+".tmp")


    """


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ZODBBlobConfigTest))
    suite.addTest(doctest.DocFileSuite(
        "blob_basic.txt",  "blob_connection.txt", "blob_transaction.txt",
        "blob_packing.txt", "blob_importexport.txt", "blob_consume.txt",
        "blob_tempdir.txt",
        setUp=ZODB.tests.util.setUp,
        tearDown=ZODB.tests.util.tearDown,
        ))
    suite.addTest(doctest.DocTestSuite(
        setUp=ZODB.tests.util.setUp,
        tearDown=ZODB.tests.util.tearDown,
        ))
    suite.addTest(unittest.makeSuite(BlobUndoTests))

    return suite

if __name__ == '__main__':
    unittest.main(defaultTest = 'test_suite')
