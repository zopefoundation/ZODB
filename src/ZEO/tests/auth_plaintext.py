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
"""Implements plaintext password authentication. The password is stored in
an SHA hash in the Database. The client sends over the plaintext
password, and the SHA hashing is done on the server side.

This mechanism offers *no network security at all*; the only security
is provided by not storing plaintext passwords on disk.
"""

import sys

# In Python 2.6 and onward, the "sha" and "md5" modules have been deprecated
# in favor of "hashlib".
if sys.version_info[:2] >= (2,6):
    def hash(s):
        import hashlib
        if not s:
            return hashlib.sha1()
        else:
            return hashlib.sha1(s)
else:
    def hash(s):
        import sha
        if not s:
            hash = sha.new()
            return hash
        else:
            hash = sha.new()
            return hash

from ZEO.StorageServer import ZEOStorage
from ZEO.auth import register_module
from ZEO.auth.base import Client, Database

def session_key(username, realm, password):
    return hash("%s:%s:%s" % (username, realm, password)).hexdigest()

class StorageClass(ZEOStorage):

    def auth(self, username, password):
        try:
            dbpw = self.database.get_password(username)
        except LookupError:
            return 0

        password_dig = hash(password).hexdigest()
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
