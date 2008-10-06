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

import base64, os, re, shutil, stat, sys, tempfile, unittest
import time
from zope.testing import doctest, renormalizing
import ZODB.tests.util

from StringIO import StringIO
from pickle import Pickler
from pickle import Unpickler

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


class BlobTests(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.here = os.getcwd()
        os.chdir(self.test_dir)
        self.storagefile = 'Data.fs'
        self.blob_dir = 'blobs'

    def tearDown(self):
        os.chdir(self.here)
        ZODB.blob.remove_committed_dir(self.test_dir)

class BlobCloneTests(BlobTests):

    def testDeepCopyCanInvalidate(self):
        """
        Tests regression for invalidation problems related to missing
        readers and writers values in cloned objects (see
        http://mail.zope.org/pipermail/zodb-dev/2008-August/012054.html)
        """
        base_storage = FileStorage(self.storagefile)
        blob_storage = BlobStorage(self.blob_dir, base_storage)
        database = DB(blob_storage)
        connection = database.open()
        root = connection.root()
        transaction.begin()
        root['blob'] = Blob()
        transaction.commit()

        stream = StringIO()
        p = Pickler(stream, 1)
        p.dump(root['blob'])
        u = Unpickler(stream)
        stream.seek(0)
        clone = u.load()
        clone._p_invalidate()

        # it should also be possible to open the cloned blob
        # (even though it won't contain the original data)
        clone.open()

        # tearDown
        database.close()


class BlobUndoTests(BlobTests):

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


def secure_blob_directory():
    """
    This is a test for secure creation and verification of secure settings of
    blob directories.

    >>> from ZODB.FileStorage.FileStorage import FileStorage
    >>> from ZODB.blob import BlobStorage
    >>> from tempfile import mkdtemp
    >>> import os.path

    >>> working_directory = mkdtemp()
    >>> base_storage = FileStorage(os.path.join(working_directory, 'Data.fs'))
    >>> blob_storage = BlobStorage(os.path.join(working_directory, 'blobs'),
    ...                            base_storage)

    Two directories are created:

    >>> blob_dir = os.path.join(working_directory, 'blobs')
    >>> os.path.isdir(blob_dir)
    True
    >>> tmp_dir = os.path.join(blob_dir, 'tmp')
    >>> os.path.isdir(tmp_dir)
    True

    They are only accessible by the owner:

    >>> oct(os.stat(blob_dir).st_mode)
    '040700'
    >>> oct(os.stat(tmp_dir).st_mode)
    '040700'

    These settings are recognized as secure:

    >>> blob_storage.fshelper.isSecure(blob_dir)
    True
    >>> blob_storage.fshelper.isSecure(tmp_dir)
    True

    After making the permissions of tmp_dir more liberal, the directory is
    recognized as insecure:

    >>> os.chmod(tmp_dir, 040711)
    >>> blob_storage.fshelper.isSecure(tmp_dir)
    False

    Clean up:

    >>> blob_storage.close()
    >>> import shutil
    >>> shutil.rmtree(working_directory)

    """

# On windows, we can't create secure blob directories, at least not
# with APIs in the standard library, so there's no point in testing
# this.
if sys.platform == 'win32':
    del secure_blob_directory

def loadblob_tmpstore():
    """
    This is a test for assuring that the TmpStore's loadBlob implementation
    falls back correctly to loadBlob on the backend.

    First, let's setup a regular database and store a blob:

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
    >>> import transaction
    >>> transaction.commit()
    >>> blob_oid = root['blob']._p_oid
    >>> tid = blob_storage.lastTransaction()

    Now we open a database with a TmpStore in front:

    >>> database.close()

    >>> from ZODB.Connection import TmpStore
    >>> tmpstore = TmpStore('', blob_storage)

    We can access the blob correctly:

    >>> tmpstore.loadBlob(blob_oid, tid) # doctest: +ELLIPSIS
    '.../0x00/0x00/0x00/0x00/0x00/0x00/0x00/0x01/0x...blob'

    Clean up:

    >>> database.close()
    >>> import shutil
    >>> rmtree(blob_dir)

    >>> os.unlink(storagefile)
    >>> os.unlink(storagefile+".index")
    >>> os.unlink(storagefile+".tmp")
"""

def setUp(test):
    ZODB.tests.util.setUp(test)
    def rmtree(path):
        for path, dirs, files in os.walk(path, False):
            for fname in files:
                fname = os.path.join(path, fname)
                os.chmod(fname, stat.S_IWUSR)
                os.remove(fname)
            for dname in dirs:
                dname = os.path.join(path, dname)
                os.rmdir(dname)
        os.rmdir(path)

    test.globs['rmtree'] = rmtree

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ZODBBlobConfigTest))
    suite.addTest(doctest.DocFileSuite(
        "blob_basic.txt",  "blob_connection.txt", "blob_transaction.txt",
        "blob_packing.txt", "blob_importexport.txt", "blob_consume.txt",
        "blob_tempdir.txt",
        optionflags=doctest.ELLIPSIS,
        setUp=setUp,
        tearDown=ZODB.tests.util.tearDown,
        ))
    suite.addTest(doctest.DocFileSuite(
        "blob_layout.txt",
        optionflags=doctest.ELLIPSIS|doctest.NORMALIZE_WHITESPACE,
        setUp=setUp,
        tearDown=ZODB.tests.util.tearDown,
        checker = renormalizing.RENormalizing([
            (re.compile(r'\%(sep)s\%(sep)s' % dict(sep=os.path.sep)), '/'),
            (re.compile(r'\%(sep)s' % dict(sep=os.path.sep)), '/'),
            (re.compile(r'\S+/((old|bushy|lawn)/\S+/foo[23456]?)'), r'\1'),
            ]),
        ))
    suite.addTest(doctest.DocTestSuite(
        setUp=setUp,
        tearDown=ZODB.tests.util.tearDown,
        checker = renormalizing.RENormalizing([
            (re.compile(r'\%(sep)s\%(sep)s' % dict(sep=os.path.sep)), '/'),
            (re.compile(r'\%(sep)s' % dict(sep=os.path.sep)), '/'),
            ]),
        ))
    suite.addTest(unittest.makeSuite(BlobCloneTests))
    suite.addTest(unittest.makeSuite(BlobUndoTests))

    return suite

if __name__ == '__main__':
    unittest.main(defaultTest = 'test_suite')
