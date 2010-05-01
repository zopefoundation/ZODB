##############################################################################
#
# Copyright Zope Foundation and Contributors.
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
from zope.testing import doctest, setupstack, renormalizing
import logging
import re
import sys
import transaction
import unittest
import ZEO.StorageServer
import ZEO.tests.servertesting
import ZODB.blob
import ZODB.FileStorage
import ZODB.tests.util
import ZODB.utils

def proper_handling_of_blob_conflicts():
    r"""

Conflict errors weren't properly handled when storing blobs, the
result being that the storage was left in a transaction.

We originally saw this when restarting a block transaction, although
it doesn't really matter.

Set up the storage with some initial blob data.

    >>> fs = ZODB.FileStorage.FileStorage('t.fs', blob_dir='t.blobs')
    >>> db = ZODB.DB(fs)
    >>> conn = db.open()
    >>> conn.root.b = ZODB.blob.Blob('x')
    >>> transaction.commit()

Get the iod and first serial. We'll use the serial later to provide
out-of-date data.

    >>> oid = conn.root.b._p_oid
    >>> serial = conn.root.b._p_serial
    >>> conn.root.b.open('w').write('y')
    >>> transaction.commit()
    >>> data = fs.load(oid)[0]

Create the server:

    >>> server = ZEO.tests.servertesting.StorageServer('x', {'1': fs})

And an initial client.

    >>> zs1 = ZEO.StorageServer.ZEOStorage(server)
    >>> conn1 = ZEO.tests.servertesting.Connection(1)
    >>> zs1.notifyConnected(conn1)
    >>> zs1.register('1', 0)
    >>> zs1.tpc_begin('0', '', '', {})
    >>> zs1.storea(ZODB.utils.p64(99), ZODB.utils.z64, 'x', '0')
    >>> _ = zs1.vote('0') # doctest: +ELLIPSIS
    1 callAsync serialnos ...

In a second client, we'll try to commit using the old serial. This
will conflict. It will be blocked at the vote call.

    >>> zs2 = ZEO.StorageServer.ZEOStorage(server)
    >>> conn2 = ZEO.tests.servertesting.Connection(2)
    >>> zs2.notifyConnected(conn2)
    >>> zs2.register('1', 0)
    >>> zs2.tpc_begin('1', '', '', {})
    >>> zs2.storeBlobStart()
    >>> zs2.storeBlobChunk('z')
    >>> zs2.storeBlobEnd(oid, serial, data, '1')
    >>> delay = zs2.vote('1')

    >>> class Sender:
    ...     def send_reply(self, id, reply):
    ...         print 'reply', id, reply
    >>> delay.set_sender(1, Sender())

    >>> logger = logging.getLogger('ZEO')
    >>> handler = logging.StreamHandler(sys.stdout)
    >>> logger.setLevel(logging.INFO)
    >>> logger.addHandler(handler)

Now, when we abort the transaction for the first client. the second
client will be restarted.  It will get a conflict error, that is
handled correctly:

    >>> zs1.tpc_abort('0') # doctest: +ELLIPSIS
    (511/test-addr) ('1') unlock: transactions waiting: 0
    2 callAsync serialnos ...
    reply 1 None

    >>> fs.tpc_transaction() is not None
    True
    >>> conn2.connected
    True

    >>> logger.setLevel(logging.NOTSET)
    >>> logger.removeHandler(handler)
    >>> zs2.tpc_abort('1')
    >>> fs.close()
    """

def proper_handling_of_errors_in_restart():
    r"""

It's critical that if there is an error in vote that the
storage isn't left in tpc.

    >>> fs = ZODB.FileStorage.FileStorage('t.fs', blob_dir='t.blobs')
    >>> server = ZEO.tests.servertesting.StorageServer('x', {'1': fs})

And an initial client.

    >>> zs1 = ZEO.StorageServer.ZEOStorage(server)
    >>> conn1 = ZEO.tests.servertesting.Connection(1)
    >>> zs1.notifyConnected(conn1)
    >>> zs1.register('1', 0)
    >>> zs1.tpc_begin('0', '', '', {})
    >>> zs1.storea(ZODB.utils.p64(99), ZODB.utils.z64, 'x', '0')

Intentionally break zs1:

    >>> zs1._store = lambda : None
    >>> _ = zs1.vote('0') # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    TypeError: <lambda>() takes no arguments (3 given)

We're not in a transaction:

    >>> fs.tpc_transaction() is None
    True

We can start another client and get the storage lock.

    >>> zs1 = ZEO.StorageServer.ZEOStorage(server)
    >>> conn1 = ZEO.tests.servertesting.Connection(1)
    >>> zs1.notifyConnected(conn1)
    >>> zs1.register('1', 0)
    >>> zs1.tpc_begin('1', '', '', {})
    >>> zs1.storea(ZODB.utils.p64(99), ZODB.utils.z64, 'x', '1')
    >>> _ = zs1.vote('1') # doctest: +ELLIPSIS
    1 callAsync serialnos ...

    >>> zs1.tpc_finish('1').set_sender(0, conn1)

    >>> fs.close()
    """

def errors_in_vote_should_clear_lock():
    """

So, we arrange to get an error in vote:

    >>> import ZODB.MappingStorage
    >>> vote_should_fail = True
    >>> class MappingStorage(ZODB.MappingStorage.MappingStorage):
    ...     def tpc_vote(*args):
    ...         if vote_should_fail:
    ...             raise ValueError
    ...         return ZODB.MappingStorage.MappingStorage.tpc_vote(*args)

    >>> server = ZEO.tests.servertesting.StorageServer(
    ...      'x', {'1': MappingStorage()})
    >>> zs = ZEO.StorageServer.ZEOStorage(server)
    >>> conn = ZEO.tests.servertesting.Connection(1)
    >>> zs.notifyConnected(conn)
    >>> zs.register('1', 0)
    >>> zs.tpc_begin('0', '', '', {})
    >>> zs.storea(ZODB.utils.p64(99), ZODB.utils.z64, 'x', '0')
    >>> zs.vote('0')
    Traceback (most recent call last):
    ...
    ValueError

When we do, the storage server's transaction lock shouldn't be held:

    >>> '1' in server._commit_locks
    False

Of course, if vote suceeds, the lock will be held:

    >>> vote_should_fail = False
    >>> zs.tpc_begin('1', '', '', {})
    >>> zs.storea(ZODB.utils.p64(99), ZODB.utils.z64, 'x', '1')
    >>> _ = zs.vote('1') # doctest: +ELLIPSIS
    1 callAsync serialnos ...

    >>> '1' in server._commit_locks
    True

    """


def test_suite():
    return unittest.TestSuite((
        doctest.DocTestSuite(
            setUp=ZODB.tests.util.setUp, tearDown=setupstack.tearDown,
            checker=renormalizing.RENormalizing([
                (re.compile('\d+/test-addr'), ''),
                ]),
            ),
        ))

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')

