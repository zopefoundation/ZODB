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
"""Implements plaintext password authentication. The password is stored in
an SHA hash in the Database. The client sends over the plaintext
password, and the SHA hashing is done on the server side.

This mechanism offers *no network security at all*; the only security
is provided by not storing plaintext passwords on disk.
"""

import sha

from ZEO.StorageServer import ZEOStorage
from ZEO.auth import register_module
from ZEO.auth.base import Client, Database

def session_key(username, realm, password):
    return sha.new("%s:%s:%s" % (username, realm, password)).hexdigest()

class StorageClass(ZEOStorage):
    def auth(self, username, password):
        try:
            dbpw = self.database.get_password(username)
        except LookupError:
            return 0

        password_dig = sha.new(password).hexdigest()
        if dbpw == password_dig:
            self.connection.setSessionKey(session_key(username,
                                                      self.database.realm,
                                                      password))
        return self.finish_auth(dbpw == password_dig)
            

class PlaintextClient(Client):
    extensions = ["auth"]

    def start(self, username, realm, password):
        if self.stub.auth(username, password):
            return session_key(username, realm, password)
        else:
            return None

register_module("plaintext", StorageClass, PlaintextClient, Database)
