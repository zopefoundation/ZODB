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
import unittest

def pack_keep_old():
    """Should a copy of the database be kept?

The pack_keep_old constructor argument controls whether a .old file (and .old directory for blobs is kept.)

    >>> import os
    >>> import time
    >>> import transaction
    >>> from ZODB.db import DB
    >>> from ZODB.filestorage import FileStorage
    >>> from ZODB.blob import Blob
    >>> fs = FileStorage('data.fs', blob_dir='blobs')
    >>> db = DB(fs)
    >>> conn = db.open()
    >>> conn.root()[1] = Blob()
    >>> conn.root()[1].open('w').write('some data')
    >>> conn.root()[2] = Blob()
    >>> conn.root()[2].open('w').write('some data')
    >>> transaction.commit()
    >>> conn.root()[1].open('w').write('some other data')
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


    >>> fs = FileStorage('data.fs', blob_dir='blobs',
    ...                  create=True, pack_keep_old=False)
    >>> db = DB(fs)
    >>> conn = db.open()
    >>> conn.root()[1] = Blob()
    >>> conn.root()[1].open('w').write('some data')
    >>> conn.root()[2] = Blob()
    >>> conn.root()[2].open('w').write('some data')
    >>> transaction.commit()
    >>> conn.root()[1].open('w').write('some other data')
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

    >>> import time
    >>> import transaction
    >>> from ZODB.db import DB
    >>> from ZODB.filestorage import FileStorage
    >>> from ZODB.blob import Blob
    >>> fs = FileStorage('t', blob_dir='bobs')
    >>> db = DB(fs)
    >>> conn = db.open()
    >>> conn.root()[1] = Blob()
    >>> transaction.commit()
    >>> tm = transaction.TransactionManager()
    >>> oid = conn.root()[1]._p_oid
    >>> blob_record, oldserial = fs.load(oid)

    Now, create a transaction with multiple saves:

    >>> trans = tm.begin()
    >>> fs.tpc_begin(trans)
    >>> open('ablob', 'w').write('some data')
    >>> _ = fs.store(oid, oldserial, blob_record, '', trans)
    >>> _ = fs.storeBlob(oid, oldserial, blob_record, 'ablob', '', trans)
    >>> fs.tpc_vote(trans)
    >>> fs.tpc_finish(trans)

    >>> time.sleep(.01)
    >>> db.pack()

    >>> conn.sync()
    >>> conn.root()[1].open().read()
    'some data'

    >>> db.close()
    """

def _save_index():
    """

_save_index can fail for large indexes.

    >>> import transaction
    >>> import ZODB.utils
    >>> from ZODB.filestorage import FileStorage
    >>> fs = FileStorage('data.fs')

    >>> t = transaction.begin()
    >>> fs.tpc_begin(t)
    >>> oid = 0
    >>> for i in range(5000):
    ...     oid += (1<<16)
    ...     _ = fs.store(ZODB.utils.p64(oid), ZODB.utils.z64, 'x', '', t)
    >>> fs.tpc_vote(t)
    >>> fs.tpc_finish(t)

    >>> import sys
    >>> old_limit = sys.getrecursionlimit()
    >>> sys.setrecursionlimit(50)
    >>> fs._save_index()

Make sure we can restore:

    >>> import logging
    >>> handler = logging.StreamHandler(sys.stdout)
    >>> logger = logging.getLogger('ZODB.filestorage')
    >>> logger.setLevel(logging.DEBUG)
    >>> logger.addHandler(handler)
    >>> index, pos, tid = fs._restore_index()
    >>> index.items() == fs._index.items()
    True
    >>> pos, tid = fs._pos, fs._tid

cleanup

    >>> fs.close()
    >>> logger.setLevel(logging.NOTSET)
    >>> logger.removeHandler(handler)
    >>> sys.setrecursionlimit(old_limit)

    """


def test_suite():
    import doctest
    from ZODB.tests.util import setUp
    from ZODB.tests.util import tearDown
    return unittest.TestSuite((
        doctest.DocFileSuite(
            'zconfig.txt', 'iterator.test', setUp=setUp, tearDown=tearDown),
        doctest.DocTestSuite(setUp=setUp, tearDown=tearDown),
        ))
