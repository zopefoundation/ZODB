##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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
"""Test suite for AuthZEO."""

import os
import tempfile
import time
import unittest

import zLOG

from ThreadedAsync import LoopCallback
from ZEO.ClientStorage import ClientStorage
from ZEO.Exceptions import ClientDisconnected
from ZEO.StorageServer import StorageServer
from ZEO.tests.ConnectionTests import CommonSetupTearDown

from ZODB.FileStorage import FileStorage
from ZODB.tests.StorageTestBase import removefs

class AuthTest(CommonSetupTearDown):
    __super_getServerConfig = CommonSetupTearDown.getServerConfig
    __super_setUp = CommonSetupTearDown.setUp
    __super_tearDown = CommonSetupTearDown.tearDown

    realm = None

    def setUp(self):
        self.pwfile = tempfile.mktemp()
        if self.realm:
            self.pwdb = self.dbclass(self.pwfile, self.realm)
        else:
            self.pwdb = self.dbclass(self.pwfile)
        self.pwdb.add_user("foo", "bar")
        self.pwdb.save()
        self.__super_setUp()

    def tearDown(self):
        self.__super_tearDown()
        os.remove(self.pwfile)

    def getConfig(self, path, create, read_only):
        return "<mappingstorage 1/>"

    def getServerConfig(self, addr, ro_svr):
        zconf = self.__super_getServerConfig(addr, ro_svr)
        zconf.authentication_protocol = self.protocol
        zconf.authentication_database = self.pwfile
        zconf.authentication_realm = self.realm
        return zconf

    def wait(self):
        for i in range(25):
            if self._storage.test_connection:
                return
            time.sleep(0.1)
        self.fail("Timed out waiting for client to authenticate")

    def testOK(self):
        # Sleep for 0.2 seconds to give the server some time to start up
        # seems to be needed before and after creating the storage
        self._storage = self.openClientStorage(wait=0, username="foo",
                                              password="bar", realm=self.realm)
        self.wait()

        self.assert_(self._storage._connection)
        self._storage._connection.poll()
        self.assert_(self._storage.is_connected())
        # Make a call to make sure the mechanism is working
        self._storage.versions()

    def testNOK(self):
        self._storage = self.openClientStorage(wait=0, username="foo",
                                              password="noogie",
                                              realm=self.realm)
        self.wait()
        # If the test established a connection, then it failed.
        self.failIf(self._storage._connection)

    def testUnauthenticatedMessage(self):
        # Test that an unauthenticated message is rejected by the server
        # if it was sent after the connection was authenticated.
        # Sleep for 0.2 seconds to give the server some time to start up
        # seems to be needed before and after creating the storage
        self._storage = self.openClientStorage(wait=0, username="foo",
                                              password="bar", realm=self.realm)
        self.wait()
        self._storage.versions()
        # Manually clear the state of the hmac connection
        self._storage._connection._SizedMessageAsyncConnection__hmac_send = None
        # Once the client stops using the hmac, it should be disconnected.
        self.assertRaises(ClientDisconnected, self._storage.versions)

class PlainTextAuth(AuthTest):
    import ZEO.tests.auth_plaintext
    protocol = "plaintext"
    database = "authdb.sha"
    dbclass = ZEO.tests.auth_plaintext.Database
    realm = "Plaintext Realm"

class DigestAuth(AuthTest):
    import ZEO.auth.auth_digest
    protocol = "digest"
    database = "authdb.digest"
    dbclass = ZEO.auth.auth_digest.DigestDatabase
    realm = "Digest Realm"

test_classes = [PlainTextAuth, DigestAuth]

def test_suite():
    suite = unittest.TestSuite()
    for klass in test_classes:
        sub = unittest.makeSuite(klass)
        suite.addTest(sub)
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest='test_suite')
