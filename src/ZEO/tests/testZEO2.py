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
from zope.testing import setupstack, renormalizing
import doctest
import logging
import pprint
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

    >>> zs.tpc_abort('1')
    """


def some_basic_locking_tests():
    r"""

    >>> itid = 0
    >>> def start_trans(zs):
    ...     global itid
    ...     itid += 1
    ...     tid = str(itid)
    ...     zs.tpc_begin(tid, '', '', {})
    ...     zs.storea(ZODB.utils.p64(99), ZODB.utils.z64, 'x', tid)
    ...     return tid

    >>> server = ZEO.tests.servertesting.StorageServer()

    >>> handler = logging.StreamHandler(sys.stdout)
    >>> handler.setFormatter(logging.Formatter(
    ...     '%(name)s %(levelname)s\n%(message)s'))
    >>> logging.getLogger('ZEO').addHandler(handler)
    >>> logging.getLogger('ZEO').setLevel(logging.DEBUG)

We start a transaction and vote, this leads to getting the lock.

    >>> zs1 = ZEO.tests.servertesting.client(server, '1')
    >>> tid1 = start_trans(zs1)
    >>> zs1.vote(tid1) # doctest: +ELLIPSIS
    ZEO.StorageServer DEBUG
    (test-addr-1) ('1') lock: transactions waiting: 0
    ZEO.StorageServer BLATHER
    (test-addr-1) Preparing to commit transaction: 1 objects, 36 bytes
    1 callAsync serialnos ...

If another client tried to vote, it's lock request will be queued and
a delay will be returned:

    >>> zs2 = ZEO.tests.servertesting.client(server, '2')
    >>> tid2 = start_trans(zs2)
    >>> delay = zs2.vote(tid2)
    ZEO.StorageServer DEBUG
    (test-addr-2) ('1') queue lock: transactions waiting: 1

    >>> delay.set_sender(0, zs2.connection)

When we end the first transaction, the queued vote gets the lock.

    >>> zs1.tpc_abort(tid1) # doctest: +ELLIPSIS
    ZEO.StorageServer DEBUG
    (test-addr-1) ('1') unlock: transactions waiting: 1
    ZEO.StorageServer DEBUG
    (test-addr-2) ('1') lock: transactions waiting: 0
    ZEO.StorageServer BLATHER
    (test-addr-2) Preparing to commit transaction: 1 objects, 36 bytes
    2 callAsync serialnos ...

Let's try again with the first client. The vote will be queued:

    >>> tid1 = start_trans(zs1)
    >>> delay = zs1.vote(tid1)
    ZEO.StorageServer DEBUG
    (test-addr-1) ('1') queue lock: transactions waiting: 1

If the queued transaction is aborted, it will be dequeued:

    >>> zs1.tpc_abort(tid1) # doctest: +ELLIPSIS
    ZEO.StorageServer DEBUG
    (test-addr-1) ('1') dequeue lock: transactions waiting: 0

BTW, voting multiple times will error:

    >>> zs2.vote(tid2)
    Traceback (most recent call last):
    ...
    StorageTransactionError: Already voting (locked)

    >>> tid1 = start_trans(zs1)
    >>> delay = zs1.vote(tid1)
    ZEO.StorageServer DEBUG
    (test-addr-1) ('1') queue lock: transactions waiting: 1

    >>> delay.set_sender(0, zs1.connection)

    >>> zs1.vote(tid1)
    Traceback (most recent call last):
    ...
    StorageTransactionError: Already voting (waiting)

Note that the locking activity is logged at debug level to avoid
cluttering log files, however, as the number of waiting votes
increased, so does the logging level:

    >>> clients = []
    >>> for i in range(9):
    ...     client = ZEO.tests.servertesting.client(server, str(i+10))
    ...     tid = start_trans(client)
    ...     delay = client.vote(tid)
    ...     clients.append(client)
    ZEO.StorageServer DEBUG
    (test-addr-10) ('1') queue lock: transactions waiting: 2
    ZEO.StorageServer DEBUG
    (test-addr-11) ('1') queue lock: transactions waiting: 3
    ZEO.StorageServer WARNING
    (test-addr-12) ('1') queue lock: transactions waiting: 4
    ZEO.StorageServer WARNING
    (test-addr-13) ('1') queue lock: transactions waiting: 5
    ZEO.StorageServer WARNING
    (test-addr-14) ('1') queue lock: transactions waiting: 6
    ZEO.StorageServer WARNING
    (test-addr-15) ('1') queue lock: transactions waiting: 7
    ZEO.StorageServer WARNING
    (test-addr-16) ('1') queue lock: transactions waiting: 8
    ZEO.StorageServer WARNING
    (test-addr-17) ('1') queue lock: transactions waiting: 9
    ZEO.StorageServer CRITICAL
    (test-addr-18) ('1') queue lock: transactions waiting: 10

If a client with the transaction lock disconnects, it will abort and
release the lock and one of the waiting clients will get the lock.

    >>> zs2.notifyDisconnected() # doctest: +ELLIPSIS
    ZEO.StorageServer INFO
    (test-addr-2) disconnected during locked transaction
    ZEO.StorageServer CRITICAL
    (test-addr-2) ('1') unlock: transactions waiting: 10
    ZEO.StorageServer WARNING
    (test-addr-1) ('1') lock: transactions waiting: 9
    ZEO.StorageServer BLATHER
    (test-addr-1) Preparing to commit transaction: 1 objects, 36 bytes
    1 callAsync serialnos ...

(In practice, waiting clients won't necessarily get the lock in order.)

We can find out about the current lock state, and get other server
statistics using the server_status method:

    >>> pprint.pprint(zs1.server_status(), width=1)
    {'aborts': 3,
     'active_txns': 10,
     'commits': 0,
     'conflicts': 0,
     'conflicts_resolved': 0,
     'connections': 11,
     'loads': 0,
     'lock_time': 1272653598.693882,
     'start': 'Fri Apr 30 14:53:18 2010',
     'stores': 13,
     'timeout-thread-is-alive': 'stub',
     'verifying_clients': 0,
     'waiting': 9}

(Note that the connections count above is off by 1 due to the way the
test infrastructure works.)

If clients disconnect while waiting, they will be dequeued:

    >>> for client in clients:
    ...     client.notifyDisconnected()
    ZEO.StorageServer INFO
    (test-addr-10) disconnected during unlocked transaction
    ZEO.StorageServer WARNING
    (test-addr-10) ('1') dequeue lock: transactions waiting: 8
    ZEO.StorageServer INFO
    (test-addr-11) disconnected during unlocked transaction
    ZEO.StorageServer WARNING
    (test-addr-11) ('1') dequeue lock: transactions waiting: 7
    ZEO.StorageServer INFO
    (test-addr-12) disconnected during unlocked transaction
    ZEO.StorageServer WARNING
    (test-addr-12) ('1') dequeue lock: transactions waiting: 6
    ZEO.StorageServer INFO
    (test-addr-13) disconnected during unlocked transaction
    ZEO.StorageServer WARNING
    (test-addr-13) ('1') dequeue lock: transactions waiting: 5
    ZEO.StorageServer INFO
    (test-addr-14) disconnected during unlocked transaction
    ZEO.StorageServer WARNING
    (test-addr-14) ('1') dequeue lock: transactions waiting: 4
    ZEO.StorageServer INFO
    (test-addr-15) disconnected during unlocked transaction
    ZEO.StorageServer DEBUG
    (test-addr-15) ('1') dequeue lock: transactions waiting: 3
    ZEO.StorageServer INFO
    (test-addr-16) disconnected during unlocked transaction
    ZEO.StorageServer DEBUG
    (test-addr-16) ('1') dequeue lock: transactions waiting: 2
    ZEO.StorageServer INFO
    (test-addr-17) disconnected during unlocked transaction
    ZEO.StorageServer DEBUG
    (test-addr-17) ('1') dequeue lock: transactions waiting: 1
    ZEO.StorageServer INFO
    (test-addr-18) disconnected during unlocked transaction
    ZEO.StorageServer DEBUG
    (test-addr-18) ('1') dequeue lock: transactions waiting: 0

    >>> zs1.tpc_abort(tid1)

    >>> logging.getLogger('ZEO').setLevel(logging.NOTSET)
    >>> logging.getLogger('ZEO').removeHandler(handler)
    """

def lock_sanity_check():
    r"""
On one occasion with 3.10.0a1 in production, we had a case where a
transaction lock wasn't released properly.  One possibility, fron
scant log information, is that the server and ZEOStorage had different
ideas about whether the ZEOStorage was locked. The timeout thread
properly closed the ZEOStorage's connection, but the ZEOStorage didn't
release it's lock, presumably because it thought it wasn't locked. I'm
not sure why this happened.  I've refactored the logic quite a bit to
try to deal with this, but the consequences of this failure are so
severe, I'm adding some sanity checking when queueing lock requests.

Helper to manage transactions:

    >>> itid = 0
    >>> def start_trans(zs):
    ...     global itid
    ...     itid += 1
    ...     tid = str(itid)
    ...     zs.tpc_begin(tid, '', '', {})
    ...     zs.storea(ZODB.utils.p64(99), ZODB.utils.z64, 'x', tid)
    ...     return tid

Set up server and logging:

    >>> server = ZEO.tests.servertesting.StorageServer()

    >>> handler = logging.StreamHandler(sys.stdout)
    >>> handler.setFormatter(logging.Formatter(
    ...     '%(name)s %(levelname)s\n%(message)s'))
    >>> logging.getLogger('ZEO').addHandler(handler)
    >>> logging.getLogger('ZEO').setLevel(logging.DEBUG)

Now, we'll start a transaction, get the lock and then mark the
ZEOStorage as closed and see if trying to get a lock cleans it up:

    >>> zs1 = ZEO.tests.servertesting.client(server, '1')
    >>> tid1 = start_trans(zs1)
    >>> zs1.vote(tid1) # doctest: +ELLIPSIS
    ZEO.StorageServer DEBUG
    (test-addr-1) ('1') lock: transactions waiting: 0
    ZEO.StorageServer BLATHER
    (test-addr-1) Preparing to commit transaction: 1 objects, 36 bytes
    1 callAsync serialnos ...

    >>> zs1.connection = None

    >>> zs2 = ZEO.tests.servertesting.client(server, '2')
    >>> tid2 = start_trans(zs2)
    >>> zs2.vote(tid2) # doctest: +ELLIPSIS
    ZEO.StorageServer CRITICAL
    (test-addr-1) Still locked after disconnected. Unlocking.
    ZEO.StorageServer DEBUG
    (test-addr-2) ('1') lock: transactions waiting: 0
    ZEO.StorageServer BLATHER
    (test-addr-2) Preparing to commit transaction: 1 objects, 36 bytes
    2 callAsync serialnos ...

    >>> zs1.txnlog.close()
    >>> zs2.tpc_abort(tid2)

    >>> logging.getLogger('ZEO').setLevel(logging.NOTSET)
    >>> logging.getLogger('ZEO').removeHandler(handler)
    """


def test_suite():
    return unittest.TestSuite((
        doctest.DocTestSuite(
            setUp=ZODB.tests.util.setUp, tearDown=setupstack.tearDown,
            checker=renormalizing.RENormalizing([
                (re.compile('\d+/test-addr'), ''),
                (re.compile("'lock_time': \d+.\d+"), 'lock_time'),
                (re.compile(r"'start': '[^\n]+'"), 'start'),
                ]),
            ),
        ))

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')

