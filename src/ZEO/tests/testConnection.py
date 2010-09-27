##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Test setup for ZEO connection logic.

The actual tests are in ConnectionTests.py; this file provides the
platform-dependent scaffolding.
"""

from __future__ import with_statement

from ZEO.tests import ConnectionTests, InvalidationTests
from zope.testing import setupstack
import os
if os.environ.get('USE_ZOPE_TESTING_DOCTEST'):
    from zope.testing import doctest
else:
    import doctest
import unittest
import ZEO.tests.forker
import ZEO.tests.testMonitor
import ZEO.zrpc.connection
import ZODB.tests.util

class FileStorageConfig:
    def getConfig(self, path, create, read_only):
        return """\
        <filestorage 1>
        path %s
        create %s
        read-only %s
        </filestorage>""" % (path,
                             create and 'yes' or 'no',
                             read_only and 'yes' or 'no')

class MappingStorageConfig:
    def getConfig(self, path, create, read_only):
        return """<mappingstorage 1/>"""


class FileStorageConnectionTests(
    FileStorageConfig,
    ConnectionTests.ConnectionTests,
    InvalidationTests.InvalidationTests
    ):
    """FileStorage-specific connection tests."""

class FileStorageReconnectionTests(
    FileStorageConfig,
    ConnectionTests.ReconnectionTests,
    ):
    """FileStorage-specific re-connection tests."""
    # Run this at level 1 because MappingStorage can't do reconnection tests

class FileStorageInvqTests(
    FileStorageConfig,
    ConnectionTests.InvqTests
    ):
    """FileStorage-specific invalidation queue tests."""

class FileStorageTimeoutTests(
    FileStorageConfig,
    ConnectionTests.TimeoutTests
    ):
    pass


class MappingStorageConnectionTests(
    MappingStorageConfig,
    ConnectionTests.ConnectionTests
    ):
    """Mapping storage connection tests."""

# The ReconnectionTests can't work with MappingStorage because it's only an
# in-memory storage and has no persistent state.

class MappingStorageTimeoutTests(
    MappingStorageConfig,
    ConnectionTests.TimeoutTests
    ):
    pass

class MonitorTests(ZEO.tests.testMonitor.MonitorTests):

    def check_connection_management(self):
        # Open and close a few connections, making sure that
        # the resulting number of clients is 0.

        s1 = self.openClientStorage()
        s2 = self.openClientStorage()
        s3 = self.openClientStorage()
        stats = self.parse(self.get_monitor_output())[1]
        self.assertEqual(stats.clients, 3)
        s1.close()
        s3.close()
        s2.close()

        ZEO.tests.forker.wait_until(
            "Number of clients shown in monitor drops to 0",
            lambda :
            self.parse(self.get_monitor_output())[1].clients == 0
            )

    def check_connection_management_with_old_client(self):
        # Check that connection management works even when using an
        # older protcool that requires a connection adapter.
        test_protocol = "Z303"
        current_protocol = ZEO.zrpc.connection.Connection.current_protocol
        ZEO.zrpc.connection.Connection.current_protocol = test_protocol
        ZEO.zrpc.connection.Connection.servers_we_can_talk_to.append(
            test_protocol)
        try:
            self.check_connection_management()
        finally:
            ZEO.zrpc.connection.Connection.current_protocol = current_protocol
            ZEO.zrpc.connection.Connection.servers_we_can_talk_to.pop()


test_classes = [FileStorageConnectionTests,
                FileStorageReconnectionTests,
                FileStorageInvqTests,
                FileStorageTimeoutTests,
                MappingStorageConnectionTests,
                MappingStorageTimeoutTests,
                MonitorTests,
                ]

def invalidations_while_connecting():
    r"""
As soon as a client registers with a server, it will recieve
invalidations from the server.  The client must be careful to queue
these invalidations until it is ready to deal with them.  At the time
of the writing of this test, clients weren't careful enough about
queing invalidations.  This led to cache corruption in the form of
both low-level file corruption as well as out-of-date records marked
as current.

This tests tries to provoke this bug by:

- starting a server

    >>> addr, _ = start_server()

- opening a client to the server that writes some objects, filling
  it's cache at the same time,

    >>> import ZODB.tests.MinPO, transaction
    >>> db = ZEO.DB(addr, client='x')
    >>> conn = db.open()
    >>> nobs = 1000
    >>> for i in range(nobs):
    ...     conn.root()[i] = ZODB.tests.MinPO.MinPO(0)
    >>> transaction.commit()

    >>> import zope.testing.loggingsupport, logging
    >>> handler = zope.testing.loggingsupport.InstalledHandler(
    ...    'ZEO', level=logging.INFO)

    # >>> logging.getLogger('ZEO').debug(
    # ...     'Initial tid %r' % conn.root()._p_serial)

- disconnecting the first client (closing it with a persistent cache),

    >>> db.close()

- starting a second client that writes objects more or less
  constantly,

    >>> import random, threading, time
    >>> stop = False
    >>> db2 = ZEO.DB(addr)
    >>> tm = transaction.TransactionManager()
    >>> conn2 = db2.open(transaction_manager=tm)
    >>> random = random.Random(0)
    >>> lock = threading.Lock()
    >>> def run():
    ...     while 1:
    ...         i = random.randint(0, nobs-1)
    ...         if stop:
    ...             return
    ...         with lock:
    ...             conn2.root()[i].value += 1
    ...             tm.commit()
    ...             #logging.getLogger('ZEO').debug(
    ...             #   'COMMIT %s %s %r' % (
    ...             #   i, conn2.root()[i].value, conn2.root()[i]._p_serial))
    ...         time.sleep(0)
    >>> thread = threading.Thread(target=run)
    >>> thread.setDaemon(True)
    >>> thread.start()

- restarting the first client, and
- testing for cache validity.

    >>> bad = False
    >>> try:
    ...     for c in range(10):
    ...        time.sleep(.1)
    ...        db = ZODB.DB(ZEO.ClientStorage.ClientStorage(addr, client='x'))
    ...        with lock:
    ...            #logging.getLogger('ZEO').debug('Locked %s' % c)
    ...            @wait_until("connected and we have caught up", timeout=199)
    ...            def _():
    ...                if (db.storage.is_connected()
    ...                        and db.storage.lastTransaction()
    ...                            == db.storage._server.lastTransaction()
    ...                        ):
    ...                    #logging.getLogger('ZEO').debug(
    ...                    #   'Connected %r' % db.storage.lastTransaction())
    ...                    return True
    ...
    ...            conn = db.open()
    ...            for i in range(1000):
    ...                if conn.root()[i].value != conn2.root()[i].value:
    ...                    print 'bad', c, i, conn.root()[i].value,
    ...                    print  conn2.root()[i].value
    ...                    bad = True
    ...                    print 'client debug log with lock held'
    ...                    while handler.records:
    ...                          record = handler.records.pop(0)
    ...                          print record.name, record.levelname,
    ...                          print handler.format(record)
    ...        if bad:
    ...           print open('server-%s.log' % addr[1]).read()
    ...        #else:
    ...        #   logging.getLogger('ZEO').debug('GOOD %s' % c)
    ...        db.close()
    ... finally:
    ...     stop = True
    ...     thread.join(10)

    >>> thread.isAlive()
    False

    >>> for record in handler.records:
    ...     if record.levelno < logging.ERROR:
    ...         continue
    ...     print record.name, record.levelname
    ...     print handler.format(record)

    >>> handler.uninstall()

    >>> db.close()
    >>> db2.close()
    """

def test_suite():
    suite = unittest.TestSuite()
    for klass in test_classes:
        sub = unittest.makeSuite(klass, 'check')
        suite.addTest(sub)
    suite.addTest(doctest.DocTestSuite(
        setUp=ZEO.tests.forker.setUp, tearDown=setupstack.tearDown,
        ))
    suite.layer = ZODB.tests.util.MininalTestLayer('ZEO Connection Tests')
    return suite
