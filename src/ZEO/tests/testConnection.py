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
from ZEO.tests import ConnectionTests


class FileStorageConfig:
    def getConfig(self, path, create, read_only):
        return """\
        <Storage>
            type FileStorage
            file_name %s
            create %s
            read_only %s
        </Storage>""" % (path,
                         create and 'yes' or 'no',
                         read_only and 'yes' or 'no')


class BerkeleyStorageConfig:
    def getConfig(self, path, create, read_only):
        return """\
        <Storage>
            type BDBFullStorage
            name %s
            read_only %s
        </Storage>""" % (path, read_only)


class FileStorageConnectionTests(
    FileStorageConfig,
    ConnectionTests.ConnectionTests
    ):
    """FileStorage-specific connection tests."""


class FileStorageReconnectionTests(
    FileStorageConfig,
    ConnectionTests.ReconnectionTests
    ):
    """FileStorage-specific re-connection tests."""


class BDBConnectionTests(
    BerkeleyStorageConfig,
    ConnectionTests.ConnectionTests
    ):
    """Berkeley storage connection tests."""


class BDBReconnectionTests(
    BerkeleyStorageConfig,
    ConnectionTests.ReconnectionTests
    ):
    """Berkeley storage re-connection tests."""


test_classes = [FileStorageConnectionTests, FileStorageReconnectionTests]
try:
    from BDBStorage.BDBFullStorage import BDBFullStorage
except ImportError:
    pass
else:
    test_classes.append(BDBConnectionTests)
    test_classes.append(BDBReconnectionTests)


def test_suite():
    # shutup warnings about mktemp
    import warnings
    warnings.filterwarnings("ignore", "mktemp")

    suite = unittest.TestSuite()
    for klass in test_classes:
        sub = unittest.makeSuite(klass, 'check')
        suite.addTest(sub)
    return suite


if __name__ == "__main__":
    unittest.main(defaultTest='test_suite')
