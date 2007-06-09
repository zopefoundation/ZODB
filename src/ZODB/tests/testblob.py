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
from zope.testing import doctest
import ZODB.tests.util

from ZODB import utils
from ZODB.FileStorage import FileStorage
from ZODB.blob import Blob, BlobStorage
from ZODB.DB import DB
import transaction

from ZODB.tests.testConfig import ConfigTestBase
from ZConfig import ConfigurationSyntaxError

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
        shutil.rmtree(self.test_dir)

    def testUndoWithoutPreviousVersion(self):
        base_storage = FileStorage(self.storagefile)
        blob_storage = BlobStorage(self.blob_dir, base_storage)
        database = DB(blob_storage)
        connection = database.open()
        root = connection.root()
        transaction.begin()
        root['blob'] = Blob()
        transaction.commit()

        serial = base64.encodestring(blob_storage._tid)

        # undo the creation of the previously added blob
        transaction.begin()
        database.undo(serial, blob_storage._transaction)
        transaction.commit()

        connection.close()
        connection = database.open()
        root = connection.root()
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

        transaction.begin()
        blob = root['blob']
        self.assertEqual(blob.open('r').read(), 'this is state 2')
        transaction.abort()

        serial = base64.encodestring(blob_storage._tid)

        transaction.begin()
        blob_storage.undo(serial, blob_storage._transaction)
        transaction.commit()

        transaction.begin()
        blob = root['blob']
        self.assertEqual(blob.open('r').read(), 'this is state 1')
        transaction.abort()
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

        transaction.begin()
        blob = root['blob']
        self.assertEqual(blob.open('r').read(), 'this is state 2')
        transaction.abort()

        serial = base64.encodestring(blob_storage._tid)

        transaction.begin()
        blob_storage.undo(serial, blob_storage._transaction)
        transaction.commit()

        transaction.begin()
        blob = root['blob']
        self.assertEqual(blob.open('r').read(), 'this is state 1')
        transaction.abort()

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

        serial = base64.encodestring(blob_storage._tid)

        transaction.begin()
        database.undo(serial)
        transaction.commit()

        transaction.begin()
        blob = root['blob']
        self.assertEqual(blob.open('r').read(), 'this is state 1')
        transaction.abort()

        serial = base64.encodestring(blob_storage._tid)

        transaction.begin()
        database.undo(serial)
        transaction.commit()

        transaction.begin()
        blob = root['blob']
        self.assertEqual(blob.open('r').read(), 'this is state 2')
        transaction.abort()

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

        serial = base64.encodestring(blob_storage._tid)

        transaction.begin()
        database.undo(serial)
        transaction.commit()

        self.assertRaises(KeyError, root.__getitem__, 'blob')

        serial = base64.encodestring(blob_storage._tid)

        transaction.begin()
        database.undo(serial)
        transaction.commit()

        transaction.begin()
        blob = root['blob']
        self.assertEqual(blob.open('r').read(), 'this is state 1')
        transaction.abort()

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
