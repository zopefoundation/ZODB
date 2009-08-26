##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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
"""Test suite for AuthZEO."""

import os
import tempfile
import time
import unittest

from ZEO import zeopasswd
from ZEO.Exceptions import ClientDisconnected
from ZEO.tests.ConnectionTests import CommonSetupTearDown

class AuthTest(CommonSetupTearDown):
    __super_getServerConfig = CommonSetupTearDown.getServerConfig
    __super_setUp = CommonSetupTearDown.setUp
    __super_tearDown = CommonSetupTearDown.tearDown

    realm = None

    def setUp(self):
        fd, self.pwfile = tempfile.mkstemp('pwfile')
        os.close(fd)
        
        if self.realm:
            self.pwdb = self.dbclass(self.pwfile, self.realm)
        else:
            self.pwdb = self.dbclass(self.pwfile)
        self.pwdb.add_user("foo", "bar")
        self.pwdb.save()
        self._checkZEOpasswd()
        
        self.__super_setUp()

    def _checkZEOpasswd(self):
        args = ["-f", self.pwfile, "-p", self.protocol]
        if self.protocol == "plaintext":
            from ZEO.auth.base import Database
            zeopasswd.main(args + ["-d", "foo"], Database)
            zeopasswd.main(args + ["foo", "bar"], Database)
        else:
            zeopasswd.main(args + ["-d", "foo"])
            zeopasswd.main(args + ["foo", "bar"])

    def tearDown(self):
        os.remove(self.pwfile)
        self.__super_tearDown()

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
            time.sleep(0.1)
            if self._storage.test_connection:
                return
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
        self._storage.undoInfo()

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

        self._storage = self.openClientStorage(wait=0, username="foo",
                                              password="bar", realm=self.realm)
        # Sleep for 0.2 seconds to give the server some time to start up
        # seems to be needed before and after creating the storage
        self.wait()
        self._storage.undoInfo()
        # Manually clear the state of the hmac connection
        self._storage._connection._SizedMessageAsyncConnection__hmac_send = None
        # Once the client stops using the hmac, it should be disconnected.
        self.assertRaises(ClientDisconnected, self._storage.undoInfo)


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
