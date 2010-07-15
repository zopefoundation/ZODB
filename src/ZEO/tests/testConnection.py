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
from ZEO.tests import ConnectionTests, InvalidationTests
from zope.testing import setupstack
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

def test_suite():
    suite = unittest.TestSuite()
    for klass in test_classes:
        sub = unittest.makeSuite(klass, 'check')
        suite.addTest(sub)
    suite.addTest(doctest.DocFileSuite(
        'invalidations_while_connecting.test',
        setUp=ZEO.tests.forker.setUp, tearDown=setupstack.tearDown,
        ))
    suite.layer = ZODB.tests.util.MininalTestLayer('ZEO Connection Tests')
    return suite


if __name__ == "__main__":
    unittest.main(defaultTest='test_suite')
