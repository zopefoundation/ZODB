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
is provided by not storing plaintext passwords on disk.  (See the
auth_srp module for a secure mechanism)"""

import sha

from ZEO.StorageServer import ZEOStorage
from ZEO.auth import register_module
from ZEO.auth.base import Client, Database

class StorageClass(ZEOStorage):
    def auth(self, username, password):
        try:
            dbpw = self.database.get_password(username)
        except LookupError:
            return 0
        
        password = sha.new(password).hexdigest()
        return self.finish_auth(dbpw == password)
    
class PlaintextClient(Client):
    extensions = ["auth"]

    def start(self, username, realm, password):
        return self.stub.auth(username, password)

register_module("plaintext", StorageClass, PlaintextClient, Database)
