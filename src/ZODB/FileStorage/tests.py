##############################################################################
#
# Copyright (c) Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

from zope.testing import doctest

import os
import time
import transaction
import unittest
import ZODB.FileStorage
import ZODB.tests.util

def pack_keep_old():
    """Should a copy of the database be kept?
    
The pack_keep_old constructor argument controls whether a .old file (and .old directory for blobs is kept.)

    >>> fs = ZODB.FileStorage.FileStorage('data.fs', blob_dir='blobs')
    >>> db = ZODB.DB(fs)
    >>> conn = db.open()
    >>> import ZODB.blob
    >>> conn.root()[1] = ZODB.blob.Blob()
    >>> conn.root()[1].open('w').write('some data')
    >>> conn.root()[2] = ZODB.blob.Blob()
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

    
    >>> fs = ZODB.FileStorage.FileStorage('data.fs', blob_dir='blobs',
    ...                                   create=True, pack_keep_old=False)
    >>> db = ZODB.DB(fs)
    >>> conn = db.open()
    >>> conn.root()[1] = ZODB.blob.Blob()
    >>> conn.root()[1].open('w').write('some data')
    >>> conn.root()[2] = ZODB.blob.Blob()
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


def test_suite():
    return unittest.TestSuite((
        doctest.DocFileSuite(
            'zconfig.txt', 'iterator.test',
            setUp=ZODB.tests.util.setUp, tearDown=ZODB.tests.util.tearDown,
            ),
        doctest.DocTestSuite(
            setUp=ZODB.tests.util.setUp, tearDown=ZODB.tests.util.tearDown,
            ),
        ))

