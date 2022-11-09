##############################################################################
#
# Copyright (c) 2004 Zope Foundation and Contributors.
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
import doctest
import os
import random
import re
import struct
import sys
import time
import unittest
from io import BytesIO

import transaction
import ZConfig
import zope.testing.renormalizing

import ZODB.blob
import ZODB.interfaces
import ZODB.tests.IteratorStorage
import ZODB.tests.StorageTestBase
import ZODB.tests.util
from ZODB._compat import Pickler
from ZODB._compat import Unpickler
from ZODB._compat import _protocol
from ZODB.blob import Blob
from ZODB.blob import BushyLayout
from ZODB.DB import DB
from ZODB.FileStorage import FileStorage
from ZODB.tests.testConfig import ConfigTestBase


try:
    file_type = file
except NameError:
    # Py3: Python 3 does not have a file type.
    import io
    file_type = io.BufferedReader

from . import util


def new_time():
    """Create a _new_ time stamp.

    This method also makes sure that after retrieving a timestamp that was
    *before* a transaction was committed, that at least one second passes so
    the packing time actually is before the commit time.

    """
    now = new_time = time.time()
    while new_time <= now:
        new_time = time.time()
    if time.time() - new_time < 1.0:
        # Detect if we're in a time monotonically increasing
        # layer (two back-to-back calls of time.time() advance the clock
        # by a whole second); if so, we don't need to sleep
        time.sleep(1.0)
    return new_time


class ZODBBlobConfigTest(ConfigTestBase):

    def test_map_config1(self):
        self._test(
            """
            <zodb>
              <blobstorage>
                blob-dir blobs
                <mappingstorage/>
              </blobstorage>
            </zodb>
            """)

    def test_file_config1(self):
        self._test(
            """
            <zodb>
              <blobstorage>
                blob-dir blobs
                <filestorage>
                  path Data.fs
                </filestorage>
              </blobstorage>
            </zodb>
            """)

    def test_blob_dir_needed(self):
        self.assertRaises(ZConfig.ConfigurationSyntaxError,
                          self._test,
                          """
                          <zodb>
                            <blobstorage>
                              <mappingstorage/>
                            </blobstorage>
                          </zodb>
                          """)


class BlobCloneTests(ZODB.tests.util.TestCase):

    def testDeepCopyCanInvalidate(self):
        """
        Tests regression for invalidation problems related to missing
        readers and writers values in cloned objects (see
        http://mail.zope.org/pipermail/zodb-dev/2008-August/012054.html)
        """
        import ZODB.MappingStorage
        database = DB(ZODB.blob.BlobStorage(
            'blobs', ZODB.MappingStorage.MappingStorage()))
        connection = database.open()
        root = connection.root()
        transaction.begin()
        root['blob'] = Blob()
        transaction.commit()

        stream = BytesIO()
        p = Pickler(stream, _protocol)
        p.dump(root['blob'])
        u = Unpickler(stream)
        stream.seek(0)
        clone = u.load()
        clone._p_invalidate()

        # it should also be possible to open the cloned blob
        # (even though it won't contain the original data)
        clone.open().close()

        # tearDown
        database.close()


class BushyLayoutTests(ZODB.tests.util.TestCase):

    def testBushyLayoutOIDToPathUnicode(self):
        "OID-to-path should produce valid results given non-ASCII byte strings"
        non_ascii_oid = b'>\xf1<0\xe9Q\x99\xf0'
        # The argument should already be bytes;
        # os.path.sep is native string type under both 2 and 3
        # binascii.hexlify takes bytes and produces bytes under both py2 and
        # py3 the result should be the native string type
        oid_as_path = BushyLayout().oid_to_path(non_ascii_oid)
        self.assertEqual(
            oid_as_path,
            os.path.sep.join(
                '0x3e/0xf1/0x3c/0x30/0xe9/0x51/0x99/0xf0'.split('/')))

        # the reverse holds true as well
        path_as_oid = BushyLayout().path_to_oid(oid_as_path)
        self.assertEqual(
            path_as_oid,
            non_ascii_oid)


class BlobTestBase(ZODB.tests.StorageTestBase.StorageTestBase):

    def setUp(self):
        ZODB.tests.StorageTestBase.StorageTestBase.setUp(self)
        self._storage = self.create_storage()


class BlobUndoTests(BlobTestBase):

    def testUndoWithoutPreviousVersion(self):
        database = DB(self._storage)
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
        database = DB(self._storage)
        connection = database.open()
        root = connection.root()
        transaction.begin()
        blob = Blob()
        with blob.open('w') as file:
            file.write(b'this is state 1')
        root['blob'] = blob
        transaction.commit()

        transaction.begin()
        blob = root['blob']
        with blob.open('w') as file:
            file.write(b'this is state 2')
        transaction.commit()

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()
        with blob.open('r') as file:
            self.assertEqual(file.read(), b'this is state 1')

        database.close()

    def testUndoAfterConsumption(self):
        database = DB(self._storage)
        connection = database.open()
        root = connection.root()
        transaction.begin()
        with open('consume1', 'wb') as file:
            file.write(b'this is state 1')
        blob = Blob()
        blob.consumeFile('consume1')
        root['blob'] = blob
        transaction.commit()

        transaction.begin()
        blob = root['blob']
        with open('consume2', 'wb') as file:
            file.write(b'this is state 2')
        blob.consumeFile('consume2')
        transaction.commit()

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        with blob.open('r') as file:
            self.assertEqual(file.read(), b'this is state 1')

        database.close()

    def testRedo(self):
        database = DB(self._storage)
        connection = database.open()
        root = connection.root()
        blob = Blob()

        transaction.begin()
        with blob.open('w') as file:
            file.write(b'this is state 1')
        root['blob'] = blob
        transaction.commit()

        transaction.begin()
        blob = root['blob']
        with blob.open('w') as file:
            file.write(b'this is state 2')
        transaction.commit()

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        with blob.open('r') as file:
            self.assertEqual(file.read(), b'this is state 1')

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        with blob.open('r') as file:
            self.assertEqual(file.read(), b'this is state 2')

        database.close()

    def testRedoOfCreation(self):
        database = DB(self._storage)
        connection = database.open()
        root = connection.root()
        blob = Blob()

        transaction.begin()
        with blob.open('w') as file:
            file.write(b'this is state 1')
        root['blob'] = blob
        transaction.commit()

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        self.assertRaises(KeyError, root.__getitem__, 'blob')

        database.undo(database.undoLog(0, 1)[0]['id'])
        transaction.commit()

        with blob.open('r') as file:
            self.assertEqual(file.read(), b'this is state 1')

        database.close()


class RecoveryBlobStorage(BlobTestBase,
                          ZODB.tests.IteratorStorage.IteratorDeepCompare):

    def setUp(self):
        BlobTestBase.setUp(self)
        self._dst = self.create_storage('dest')

    def tearDown(self):
        self._dst.close()
        BlobTestBase.tearDown(self)

    # Requires a setUp() that creates a self._dst destination storage
    def testSimpleBlobRecovery(self):
        self.assertTrue(
            ZODB.interfaces.IBlobStorageRestoreable.providedBy(self._storage)
        )
        db = DB(self._storage)
        conn = db.open()
        conn.root()[1] = ZODB.blob.Blob()
        transaction.commit()
        conn.root()[2] = ZODB.blob.Blob()
        with conn.root()[2].open('w') as file:
            file.write(b'some data')
        transaction.commit()
        conn.root()[3] = ZODB.blob.Blob()
        with conn.root()[3].open('w') as file:
            file.write(
                (b''.join(struct.pack(">I", random.randint(0, (1 << 32)-1))
                          for i in range(random.randint(10000, 20000)))
                 )[:-random.randint(1, 4)]
            )
        transaction.commit()
        conn.root()[2] = ZODB.blob.Blob()
        with conn.root()[2].open('w') as file:
            file.write(b'some other data')
        transaction.commit()
        self._dst.copyTransactionsFrom(self._storage)
        self.compare(self._storage, self._dst)
        db.close()


def gc_blob_removes_uncommitted_data():
    """
    >>> blob = Blob()
    >>> with blob.open('w') as file:
    ...     _ = file.write(b'x')
    >>> fname = blob._p_blob_uncommitted
    >>> os.path.exists(fname)
    True
    >>> file = blob = None

    PyPy not being reference counted actually needs GC to be
    explicitly requested. In experiments, it finds the weakref
    on the first collection, but only does the cleanup on the second
    collection:

    >>> import gc
    >>> _ = gc.collect()
    >>> _ = gc.collect()

    Now the file is gone on all platforms:

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

    >>> import logging
    >>> logger = logging.getLogger('ZODB.blob.copied')
    >>> handler = logging.StreamHandler(sys.stdout)
    >>> logger.propagate = False
    >>> logger.setLevel(logging.DEBUG)
    >>> logger.addHandler(handler)
    >>> logger.disabled = False

    >>> blob_storage = create_storage()  # noqa: F821 undefined name
    >>> database = DB(blob_storage)
    >>> connection = database.open()
    >>> root = connection.root()
    >>> from ZODB.blob import Blob
    >>> root['blob'] = Blob()
    >>> with root['blob'].open('w') as file:
    ...     _ = file.write(b'test')
    >>> transaction.commit() # doctest: +ELLIPSIS
    Copied blob file ...

    >>> with root['blob'].open() as fp: fp.read()
    'test'

Works with savepoints too:

    >>> root['blob2'] = Blob()
    >>> with root['blob2'].open('w') as file:
    ...     _ = file.write(b'test2')
    >>> _ = transaction.savepoint() # doctest: +ELLIPSIS
    Copied blob file ...

    >>> transaction.commit() # doctest: +ELLIPSIS
    Copied blob file ...

    >>> with root['blob2'].open() as fp: fp.read()
    'test2'

    >>> os.rename = os_rename
    >>> logger.propagate = True
    >>> logger.setLevel(0)
    >>> logger.removeHandler(handler)
    >>> handler.close()

    >>> database.close()
    """


def packing_with_uncommitted_data_non_undoing():
    """
    This covers regression for bug #130459.

    When uncommitted data exists it formerly was written to the root of the
    blob_directory and confused our packing strategy. We now use a separate
    temporary directory that is ignored while packing.

    >>> from ZODB.DB import DB
    >>> from ZODB.serialize import referencesf

    >>> blob_storage = create_storage()  # noqa: F821 undefined name
    >>> database = DB(blob_storage)
    >>> connection = database.open()
    >>> root = connection.root()
    >>> from ZODB.blob import Blob
    >>> root['blob'] = Blob()
    >>> connection.add(root['blob'])
    >>> with root['blob'].open('w') as file:
    ...     _ = file.write(b'test')

    >>> blob_storage.pack(new_time(), referencesf)

    Clean up:

    >>> database.close()
    """


def packing_with_uncommitted_data_undoing():
    """
    This covers regression for bug #130459.

    When uncommitted data exists it formerly was written to the root of the
    blob_directory and confused our packing strategy. We now use a separate
    temporary directory that is ignored while packing.

    >>> from ZODB.serialize import referencesf

    >>> blob_storage = create_storage()  # noqa: F821 undefined name
    >>> database = DB(blob_storage)
    >>> connection = database.open()
    >>> root = connection.root()
    >>> from ZODB.blob import Blob
    >>> root['blob'] = Blob()
    >>> connection.add(root['blob'])
    >>> with root['blob'].open('w') as file:
    ...     _ = file.write(b'test')

    >>> blob_storage.pack(new_time(), referencesf)

    Clean up:

    >>> database.close()
    """


def test_blob_file_permissions():
    """
    >>> blob_storage = create_storage()  # noqa: F821 undefined name
    >>> conn = ZODB.connection(blob_storage)
    >>> conn.root.x = ZODB.blob.Blob(b'test')
    >>> conn.transaction_manager.commit()

    Blobs have the readability of their parent directories:

    >>> import stat
    >>> READABLE = stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH
    >>> path = conn.root.x.committed()
    >>> ((os.stat(path).st_mode & READABLE) ==
    ...  (os.stat(os.path.dirname(path)).st_mode & READABLE))
    True

    The committed file isn't writable:

    >>> WRITABLE = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
    >>> os.stat(path).st_mode & WRITABLE
    0

    >>> conn.close()
    """


def loadblob_tmpstore():
    """
    This is a test for assuring that the TmpStore's loadBlob implementation
    falls back correctly to loadBlob on the backend.

    First, let's setup a regular database and store a blob:

    >>> blob_storage = create_storage()  # noqa: F821 undefined name
    >>> database = DB(blob_storage)
    >>> connection = database.open()
    >>> root = connection.root()
    >>> from ZODB.blob import Blob
    >>> root['blob'] = Blob()
    >>> connection.add(root['blob'])
    >>> with root['blob'].open('w') as file:
    ...     _ = file.write(b'test')
    >>> import transaction
    >>> transaction.commit()
    >>> blob_oid = root['blob']._p_oid
    >>> tid = connection._storage.lastTransaction()

    Now we open a database with a TmpStore in front:

    >>> database.close()

    >>> from ZODB.Connection import TmpStore
    >>> tmpstore = TmpStore(blob_storage)

    We can access the blob correctly:

    >>> tmpstore.loadBlob(blob_oid,tid) == blob_storage.loadBlob(blob_oid,tid)
    True

    Clean up:

    >>> tmpstore.close()
    >>> database.close()
    """


def is_blob_record():
    r"""
    >>> from ZODB.utils import load_current

    >>> bs = create_storage()  # noqa: F821 undefined name
    >>> db = DB(bs)
    >>> conn = db.open()
    >>> conn.root()['blob'] = ZODB.blob.Blob()
    >>> transaction.commit()
    >>> ZODB.blob.is_blob_record(load_current(bs, ZODB.utils.p64(0))[0])
    False
    >>> ZODB.blob.is_blob_record(load_current(bs, ZODB.utils.p64(1))[0])
    True

    An invalid pickle yields a false value:

    >>> ZODB.blob.is_blob_record(b"Hello world!")
    False
    >>> ZODB.blob.is_blob_record(b'c__main__\nC\nq\x01.')
    False
    >>> ZODB.blob.is_blob_record(b'cWaaaa\nC\nq\x01.')
    False

    As does None, which may occur in delete records:

    >>> ZODB.blob.is_blob_record(None)
    False

    >>> db.close()
    """


def do_not_depend_on_cwd():
    """
    >>> bs = create_storage()  # noqa: F821 undefined name
    >>> here = os.getcwd()
    >>> os.mkdir('evil')
    >>> os.chdir('evil')
    >>> db = DB(bs)
    >>> conn = db.open()
    >>> conn.root()['blob'] = ZODB.blob.Blob()
    >>> with conn.root()['blob'].open('w') as file:
    ...     _ = file.write(b'data')
    >>> transaction.commit()
    >>> os.chdir(here)
    >>> with conn.root()['blob'].open() as fp: fp.read()
    'data'

    >>> db.close()
    """


def savepoint_isolation():
    """Make sure savepoint data is distinct accross transactions

    >>> bs = create_storage()  # noqa: F821 undefined name
    >>> db = DB(bs)
    >>> conn = db.open()
    >>> conn.root.b = ZODB.blob.Blob(b'initial')
    >>> transaction.commit()
    >>> with conn.root.b.open('w') as file:
    ...     _ = file.write(b'1')
    >>> _ = transaction.savepoint()
    >>> tm = transaction.TransactionManager()
    >>> conn2 = db.open(transaction_manager=tm)
    >>> with conn2.root.b.open('w') as file:
    ...     _ = file.write(b'2')
    >>> _ = tm.savepoint()
    >>> with conn.root.b.open() as fp: fp.read()
    '1'
    >>> with conn2.root.b.open() as fp: fp.read()
    '2'
    >>> transaction.abort()
    >>> tm.commit()
    >>> conn.sync()
    >>> with conn.root.b.open() as fp: fp.read()
    '2'
    >>> db.close()
    """


def savepoint_commits_without_invalidations_out_of_order():
    """Make sure transactions with blobs can be committed without the
    invalidations out of order error (LP #509801)

    >>> bs = create_storage()  # noqa: F821 undefined name
    >>> db = DB(bs)
    >>> tm1 = transaction.TransactionManager()
    >>> conn1 = db.open(transaction_manager=tm1)
    >>> conn1.root.b = ZODB.blob.Blob(b'initial')
    >>> tm1.commit()
    >>> with conn1.root.b.open('w') as file:
    ...     _ = file.write(b'1')
    >>> _ = tm1.savepoint()

    >>> tm2 = transaction.TransactionManager()
    >>> conn2 = db.open(transaction_manager=tm2)
    >>> with conn2.root.b.open('w') as file:
    ...     _ = file.write(b'2')
    >>> _ = tm1.savepoint()
    >>> with conn1.root.b.open() as fp: fp.read()
    '1'
    >>> with conn2.root.b.open() as fp: fp.read()
    '2'
    >>> tm2.commit()
    >>> tm1.commit()  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ConflictError: database conflict error...
    >>> tm1.abort()
    >>> db.close()
    """


def savepoint_cleanup():
    """Make sure savepoint data gets cleaned up.

    >>> bs = create_storage()  # noqa: F821 undefined name
    >>> tdir = bs.temporaryDirectory()
    >>> os.listdir(tdir)
    []

    >>> db = DB(bs)
    >>> conn = db.open()
    >>> conn.root.b = ZODB.blob.Blob(b'initial')
    >>> _ = transaction.savepoint()
    >>> len(os.listdir(tdir))
    1
    >>> transaction.abort()
    >>> os.listdir(tdir)
    []
    >>> conn.root.b = ZODB.blob.Blob(b'initial')
    >>> transaction.commit()
    >>> with conn.root.b.open('w') as file:
    ...     _ = file.write(b'1')
    >>> _ = transaction.savepoint()
    >>> transaction.abort()
    >>> os.listdir(tdir)
    []

    >>> db.close()
    """


def lp440234_Setting__p_changed_of_a_Blob_w_no_uncomitted_changes_is_noop():
    r"""
    >>> db = ZODB.DB('data.fs', blob_dir='blobs')
    >>> conn = db.open()
    >>> blob = ZODB.blob.Blob(b'blah')
    >>> conn.add(blob)
    >>> transaction.commit()
    >>> blob._p_changed = True
    >>> old_serial = blob._p_serial
    >>> transaction.commit()
    >>> with blob.open() as fp: fp.read()
    'blah'
    >>> old_serial == blob._p_serial
    True

    >>> db.close()
    """


def setUp(test):
    ZODB.tests.util.setUp(test)
    test.globs['rmtree'] = zope.testing.setupstack.rmtree


def timeIncreasesSetUp(test):
    setUp(test)
    layer = test.globs['time_layer'] = (
        ZODB.tests.util.MonotonicallyIncreasingTimeMinimalTestLayer(''))
    layer.testSetUp()


def timeIncreasesTearDown(test):
    test.globs['time_layer'].testTearDown()
    util.tearDown(test)


def setUpBlobAdaptedFileStorage(test):
    setUp(test)

    def create_storage(name='data', blob_dir=None):
        if blob_dir is None:
            blob_dir = '%s.bobs' % name
        return ZODB.blob.BlobStorage(blob_dir, FileStorage('%s.fs' % name))

    test.globs['create_storage'] = create_storage


def storage_reusable_suite(prefix, factory,
                           test_blob_storage_recovery=False,
                           test_packing=False,
                           test_undo=True,
                           ):
    """Return a test suite for a generic IBlobStorage.

    Pass a factory taking a name and a blob directory name.
    """

    def setup(test):
        setUp(test)

        def create_storage(name='data', blob_dir=None):
            if blob_dir is None:
                blob_dir = '%s.bobs' % name
            return factory(name, blob_dir)

        test.globs['create_storage'] = create_storage
        test.globs['file_type'] = file_type

    suite = unittest.TestSuite()
    suite.addTest(doctest.DocFileSuite(
        "blob_connection.txt",
        "blob_importexport.txt",
        "blob_transaction.txt",
        setUp=setup, tearDown=util.tearDown,
        checker=zope.testing.renormalizing.RENormalizing([
            # Py3k renders bytes where Python2 used native strings...
            (re.compile(r"^b'"), "'"),
            (re.compile(r'^b"'), '"'),
            # ...and native strings where Python2 used unicode.
            (re.compile("^POSKeyError: u'No blob file"),
             "POSKeyError: 'No blob file"),
            # Py3k repr's exceptions with dotted names
            (re.compile("^ZODB.interfaces.BlobError:"), "BlobError:"),
            (re.compile("^ZODB.POSException.ConflictError:"),
             "ConflictError:"),
            (re.compile("^ZODB.POSException.POSKeyError:"), "POSKeyError:"),
            (re.compile("^ZODB.POSException.Unsupported:"), "Unsupported:"),
            # Normalize out blobfile paths for sake of Windows
            (re.compile(
                r'([a-zA-Z]:)?\%(sep)s.*\%(sep)s(server-)'
                r'?blobs\%(sep)s.*\.blob' % dict(sep=os.path.sep)),
             '<BLOB STORAGE PATH>')
        ]),
        optionflags=doctest.ELLIPSIS,
    ))
    if test_packing:
        suite.addTest(doctest.DocFileSuite(
            "blob_packing.txt",
            setUp=setup, tearDown=util.tearDown,
        ))
    suite.addTest(doctest.DocTestSuite(
        setUp=setup, tearDown=util.tearDown,
        checker=(
            ZODB.tests.util.checker +
            zope.testing.renormalizing.RENormalizing([
                (re.compile(r'\%(sep)s\%(sep)s' % dict(sep=os.path.sep)), '/'),
                (re.compile(r'\%(sep)s' % dict(sep=os.path.sep)), '/'),
            ])),
    ))

    def create_storage(self, name='data', blob_dir=None):
        if blob_dir is None:
            blob_dir = '%s.bobs' % name
        return factory(name, blob_dir)

    def add_test_based_on_test_class(class_):
        new_class = class_.__class__(
            prefix+class_.__name__, (class_, ),
            dict(create_storage=create_storage),
        )
        suite.addTest(unittest.makeSuite(new_class))

    if test_blob_storage_recovery:
        add_test_based_on_test_class(RecoveryBlobStorage)
    if test_undo:
        add_test_based_on_test_class(BlobUndoTests)

    suite.layer = ZODB.tests.util.MonotonicallyIncreasingTimeMinimalTestLayer(
        prefix+'BlobTests')

    return suite


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ZODBBlobConfigTest))
    suite.addTest(unittest.makeSuite(BlobCloneTests))
    suite.addTest(unittest.makeSuite(BushyLayoutTests))
    suite.addTest(doctest.DocFileSuite(
        "blob_basic.txt",
        "blob_consume.txt",
        "blob_tempdir.txt",
        setUp=setUp,
        tearDown=util.tearDown,
        optionflags=doctest.ELLIPSIS,
        checker=ZODB.tests.util.checker,
    ))
    suite.addTest(doctest.DocFileSuite(
        "blobstorage_packing.txt",
        setUp=timeIncreasesSetUp,
        tearDown=timeIncreasesTearDown,
        optionflags=doctest.ELLIPSIS,
        checker=ZODB.tests.util.checker,
    ))
    suite.addTest(doctest.DocFileSuite(
        "blob_layout.txt",
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
        setUp=setUp,
        tearDown=util.tearDown,
        checker=ZODB.tests.util.checker +
        zope.testing.renormalizing.RENormalizing([
            (re.compile(r'\%(sep)s\%(sep)s' % dict(sep=os.path.sep)), '/'),
            (re.compile(r'\%(sep)s' % dict(sep=os.path.sep)), '/'),
            (re.compile(r'\S+/((old|bushy|lawn)/\S+/foo[23456]?)'), r'\1'),
            (re.compile(r"u('[^']*')"), r"\1"),
        ]),
    ))
    suite.addTest(storage_reusable_suite(
        'BlobAdaptedFileStorage',
        lambda name, blob_dir:
        ZODB.blob.BlobStorage(blob_dir, FileStorage('%s.fs' % name)),
        test_blob_storage_recovery=True,
        test_packing=True,
    ))

    return suite
