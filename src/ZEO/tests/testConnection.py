##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
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

# System imports
import unittest
# Import the actual test class
from ZEO.tests import ConnectionTests, InvalidationTests
from zope.testing import doctest, setupstack
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

test_classes = [FileStorageConnectionTests,
                FileStorageReconnectionTests,
                FileStorageInvqTests,
                FileStorageTimeoutTests,
                MappingStorageConnectionTests,
                MappingStorageTimeoutTests]

def test_suite():
    suite = unittest.TestSuite()
    for klass in test_classes:
        sub = unittest.makeSuite(klass, 'check')
        suite.addTest(sub)
    suite.addTest(doctest.DocFileSuite(
        'invalidations_while_connecting.test',
        setUp=setupstack.setUpDirectory, tearDown=setupstack.tearDown,
        ))
    suite.layer = ZODB.tests.util.MininalTestLayer('ZEO Connection Tests')
    return suite


if __name__ == "__main__":
    unittest.main(defaultTest='test_suite')
