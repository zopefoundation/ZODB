##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
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

class BerkeleyStorageConfig:
    def getConfig(self, path, create, read_only):
        return """\
        <fullstorage 1>
        envdir %s
        read-only %s
        </fullstorage>""" % (path, read_only and "yes" or "no")

class MappingStorageConfig:
    def getConfig(self, path, create, read_only):
        return """<mappingstorage 1/>"""


class FileStorageConnectionTests(
    FileStorageConfig,
    ConnectionTests.ConnectionTests,
    InvalidationTests.InvalidationTests
    ):
    """FileStorage-specific connection tests."""
    level = 2

class FileStorageReconnectionTests(
    FileStorageConfig,
    ConnectionTests.ReconnectionTests,
    ):
    """FileStorage-specific re-connection tests."""
    # Run this at level 1 because MappingStorage can't do reconnection tests
    level = 1

class FileStorageInvqTests(
    FileStorageConfig,
    ConnectionTests.InvqTests
    ):
    """FileStorage-specific invalidation queue tests."""
    level = 1

class FileStorageTimeoutTests(
    FileStorageConfig,
    ConnectionTests.TimeoutTests
    ):
    level = 2

class BDBConnectionTests(
    BerkeleyStorageConfig,
    ConnectionTests.ConnectionTests,
    InvalidationTests.InvalidationTests
    ):
    """Berkeley storage connection tests."""
    level = 2

class BDBReconnectionTests(
    BerkeleyStorageConfig,
    ConnectionTests.ReconnectionTests
    ):
    """Berkeley storage re-connection tests."""
    level = 2

class BDBInvqTests(
    BerkeleyStorageConfig,
    ConnectionTests.InvqTests
    ):
    """Berkeley storage invalidation queue tests."""
    level = 2

class BDBTimeoutTests(
    BerkeleyStorageConfig,
    ConnectionTests.TimeoutTests
    ):
    level = 2


class MappingStorageConnectionTests(
    MappingStorageConfig,
    ConnectionTests.ConnectionTests
    ):
    """Mapping storage connection tests."""
    level = 1

# The ReconnectionTests can't work with MappingStorage because it's only an
# in-memory storage and has no persistent state.

class MappingStorageTimeoutTests(
    MappingStorageConfig,
    ConnectionTests.TimeoutTests
    ):
    level = 1



test_classes = [FileStorageConnectionTests,
                FileStorageReconnectionTests,
                FileStorageInvqTests,
                FileStorageTimeoutTests,
                MappingStorageConnectionTests,
                MappingStorageTimeoutTests]

import BDBStorage
if BDBStorage.is_available:
    test_classes += [BDBConnectionTests,
                     BDBReconnectionTests,
                     BDBInvqTests,
                     BDBTimeoutTests]

def test_suite():
    suite = unittest.TestSuite()
    for klass in test_classes:
        sub = unittest.makeSuite(klass, 'check')
        suite.addTest(sub)
    return suite


if __name__ == "__main__":
    unittest.main(defaultTest='test_suite')
