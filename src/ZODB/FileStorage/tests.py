##############################################################################
#
# Copyright (c) Zope Corporation and Contributors.
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
import re
import time
import unittest

import transaction
from zope.testing import renormalizing

import ZODB.blob
import ZODB.FileStorage
import ZODB.tests.util
from ZODB.Connection import TransactionMetaData


checker = renormalizing.RENormalizing([
    # Python 3 bytes add a "b".
    (re.compile("b('.*?')"), r"\1"),
    # Python 3 adds module name to exceptions.
    (re.compile("ZODB.POSException.POSKeyError"), r"POSKeyError"),
    (re.compile("ZODB.FileStorage.FileStorage.FileStorageQuotaError"),
     "FileStorageQuotaError"),
    (re.compile('data.fs:[0-9]+'), 'data.fs:<OFFSET>'),
])


def pack_keep_old():
    """Should a copy of the database be kept?

The pack_keep_old constructor argument controls whether a .old file (and .old
directory for blobs is kept.)

    >>> fs = ZODB.FileStorage.FileStorage('data.fs', blob_dir='blobs')
    >>> db = ZODB.DB(fs)
    >>> conn = db.open()
    >>> import ZODB.blob
    >>> conn.root()[1] = ZODB.blob.Blob()
    >>> with conn.root()[1].open('w') as file:
    ...     _ = file.write(b'some data')
    >>> conn.root()[2] = ZODB.blob.Blob()
    >>> with conn.root()[2].open('w') as file:
    ...     _ = file.write(b'some data')
    >>> transaction.commit()
    >>> with conn.root()[1].open('w') as file:
    ...     _ = file.write(b'some other data')
    >>> del conn.root()[2]
    >>> transaction.commit()
    >>> old_size = os.stat('data.fs').st_size
    >>> def get_blob_size(d):
    ...     result = 0
    ...     for path, dirs, file_names in os.walk(d):
    ...         for file_name in file_names:
    ...             result += os.stat(os.path.join(path, file_name)).st_size
    ...     return result
    >>> blob_size = get_blob_size('blobs')

    >>> db.pack(time.time()+1)
    >>> packed_size = os.stat('data.fs').st_size
    >>> packed_size < old_size
    True
    >>> os.stat('data.fs.old').st_size == old_size
    True

    >>> packed_blob_size = get_blob_size('blobs')
    >>> packed_blob_size < blob_size
    True
    >>> get_blob_size('blobs.old') == blob_size
    True
    >>> db.close()


    >>> fs = ZODB.FileStorage.FileStorage('data.fs', blob_dir='blobs',
    ...                                   create=True, pack_keep_old=False)
    >>> db = ZODB.DB(fs)
    >>> conn = db.open()
    >>> conn.root()[1] = ZODB.blob.Blob()
    >>> with conn.root()[1].open('w') as file:
    ...     _ = file.write(b'some data')
    >>> conn.root()[2] = ZODB.blob.Blob()
    >>> with conn.root()[2].open('w') as file:
    ...     _ = file.write(b'some data')
    >>> transaction.commit()
    >>> with conn.root()[1].open('w') as file:
    ...     _ = file.write(b'some other data')
    >>> del conn.root()[2]
    >>> transaction.commit()

    >>> db.pack(time.time()+1)
    >>> os.stat('data.fs').st_size == packed_size
    True
    >>> os.path.exists('data.fs.old')
    False
    >>> get_blob_size('blobs') == packed_blob_size
    True
    >>> os.path.exists('blobs.old')
    False
    >>> db.close()
    """


def pack_with_repeated_blob_records():
    """
    There is a bug in ZEO that causes duplicate bloc database records
    to be written in a blob store operation. (Maybe this has been
    fixed by the time you read this, but there might still be
    transactions in the wild that have duplicate records.

    >>> fs = ZODB.FileStorage.FileStorage('t', blob_dir='bobs')
    >>> db = ZODB.DB(fs)
    >>> conn = db.open()
    >>> conn.root()[1] = ZODB.blob.Blob()
    >>> transaction.commit()
    >>> tm = transaction.TransactionManager()
    >>> oid = conn.root()[1]._p_oid
    >>> from ZODB.utils import load_current
    >>> blob_record, oldserial = load_current(fs, oid)

    Now, create a transaction with multiple saves:

    >>> trans = TransactionMetaData()
    >>> fs.tpc_begin(trans)
    >>> with open('ablob', 'w') as file:
    ...     _ = file.write('some data')
    >>> fs.store(oid, oldserial, blob_record, '', trans)
    >>> fs.storeBlob(oid, oldserial, blob_record, 'ablob', '', trans)
    >>> _ = fs.tpc_vote(trans)
    >>> _ = fs.tpc_finish(trans)

    >>> time.sleep(.01)
    >>> db.pack()

    >>> conn.sync()
    >>> with conn.root()[1].open() as fp: fp.read()
    'some data'

    >>> db.close()
    """


def _save_index():
    """

_save_index can fail for large indexes.

    >>> import ZODB.utils
    >>> fs = ZODB.FileStorage.FileStorage('data.fs')

    >>> t = TransactionMetaData()
    >>> fs.tpc_begin(t)
    >>> oid = 0
    >>> for i in range(5000):
    ...     oid += (1<<16)
    ...     fs.store(ZODB.utils.p64(oid), ZODB.utils.z64, b'x', '', t)
    >>> _ = fs.tpc_vote(t)
    >>> _ = fs.tpc_finish(t)

    >>> import sys
    >>> old_limit = sys.getrecursionlimit()
    >>> sys.setrecursionlimit(50)
    >>> fs._save_index()

Make sure we can restore:

    >>> import logging
    >>> handler = logging.StreamHandler(sys.stdout)
    >>> logger = logging.getLogger('ZODB.FileStorage')
    >>> logger.setLevel(logging.DEBUG)
    >>> logger.addHandler(handler)
    >>> log_disabled = logger.disabled
    >>> logger.disabled = False
    >>> index, pos, tid = fs._restore_index()
    >>> index.items() == fs._index.items()
    True
    >>> pos, tid = fs._pos, fs._tid

cleanup

    >>> fs.close()
    >>> logger.setLevel(logging.NOTSET)
    >>> logger.removeHandler(handler)
    >>> logger.disabled = log_disabled
    >>> sys.setrecursionlimit(old_limit)

    """


def pack_disk_full_copyToPacktime():
    """Recover from a disk full situation by removing the `.pack` file

`copyToPacktime` fails

Add some data

    >>> fs = ZODB.FileStorage.FileStorage('data.fs')
    >>> db = ZODB.DB(fs)
    >>> conn = db.open()
    >>> conn.root()[1] = 'foobar'
    >>> transaction.commit()

patch `copyToPacktime` to fail

    >>> from ZODB.FileStorage import fspack
    >>> save_copyToPacktime = fspack.FileStoragePacker.copyToPacktime

    >>> def failing_copyToPacktime(self):
    ...     self._tfile.write(b'somejunkdata')
    ...     raise OSError("No space left on device")

    >>> fspack.FileStoragePacker.copyToPacktime = failing_copyToPacktime

pack -- it still raises `OSError`

    >>> db.pack(time.time()+1)
    Traceback (most recent call last):
    ...
    OSError: No space left on device

`data.fs.pack` must not exist

    >>> os.path.exists('data.fs.pack')
    False

undo patching

    >>> fspack.FileStoragePacker.copyToPacktime = save_copyToPacktime

    >>> db.close()

check the data we added

    >>> fs = ZODB.FileStorage.FileStorage('data.fs')
    >>> db = ZODB.DB(fs)
    >>> conn = db.open()
    >>> conn.root()[1]
    'foobar'
    >>> db.close()
    """


def pack_disk_full_copyRest():
    """Recover from a disk full situation by removing the `.pack` file

`copyRest` fails

Add some data

    >>> fs = ZODB.FileStorage.FileStorage('data.fs')
    >>> db = ZODB.DB(fs)
    >>> conn = db.open()
    >>> conn.root()[1] = 'foobar'
    >>> transaction.commit()

patch `copyToPacktime` to add one more transaction

    >>> from ZODB.FileStorage import fspack
    >>> save_copyToPacktime = fspack.FileStoragePacker.copyToPacktime

    >>> def patched_copyToPacktime(self):
    ...     res = save_copyToPacktime(self)
    ...     conn2 = db.open()
    ...     conn2.root()[2] = 'another bar'
    ...     transaction.commit()
    ...     return res

    >>> fspack.FileStoragePacker.copyToPacktime = patched_copyToPacktime

patch `copyRest` to fail

    >>> save_copyRest = fspack.FileStoragePacker.copyRest

    >>> def failing_copyRest(self, ipos):
    ...     self._tfile.write(b'somejunkdata')
    ...     raise OSError("No space left on device")

    >>> fspack.FileStoragePacker.copyRest = failing_copyRest

pack -- it still raises `OSError`

    >>> db.pack(time.time()+1)
    Traceback (most recent call last):
    ...
    OSError: No space left on device

`data.fs.pack` must not exist

    >>> os.path.exists('data.fs.pack')
    False

undo patching

    >>> fspack.FileStoragePacker.copyToPacktime = save_copyToPacktime
    >>> fspack.FileStoragePacker.copyRest = save_copyRest

    >>> db.close()

check the data we added

    >>> fs = ZODB.FileStorage.FileStorage('data.fs')
    >>> db = ZODB.DB(fs)
    >>> conn = db.open()
    >>> conn.root()[1]
    'foobar'
    >>> conn.root()[2]
    'another bar'
    >>> db.close()
    """


def test_suite():
    return unittest.TestSuite((
        doctest.DocFileSuite(
            'zconfig.txt',
            'iterator.test',
            setUp=ZODB.tests.util.setUp,
            tearDown=ZODB.tests.util.tearDown,
            checker=checker),
        doctest.DocTestSuite(
            setUp=ZODB.tests.util.setUp,
            tearDown=ZODB.tests.util.tearDown,
            checker=checker),
    ))
