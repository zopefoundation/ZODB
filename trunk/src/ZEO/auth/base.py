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
"""Base classes for defining an authentication protocol.

Database -- abstract base class for password database
Client -- abstract base class for authentication client
"""

import os
import sha

class Client:
    # Subclass should override to list the names of methods that
    # will be called on the server.
    extensions = []

    def __init__(self, stub):
        self.stub = stub
        for m in self.extensions:
            setattr(self.stub, m, self.stub.extensionMethod(m))

def sort(L):
    """Sort a list in-place and return it."""
    L.sort()
    return L

class Database:
    """Abstracts a password database.

    This class is used both in the authentication process (via
    get_password()) and by client scripts that manage the password
    database file.

    The password file is a simple, colon-separated text file mapping
    usernames to password hashes. The hashes are SHA hex digests
    produced from the password string.
    """

    def __init__(self, filename, realm=None):
        """Creates a new Database

        filename: a string containing the full pathname of
            the password database file. Must be readable by the user
            running ZEO. Must be writeable by any client script that
            accesses the database.

        realm: the realm name (a string)
        """
        self._users = {}
        self.filename = filename
        self.realm = realm
        self.load()

    def save(self, fd=None):
        filename = self.filename

        if not fd:
            fd = open(filename, 'w')
        if self.realm:
            print >> fd, "realm", self.realm

        for username in sort(self._users.keys()):
            print >> fd, "%s: %s" % (username, self._users[username])

    def load(self):
        filename = self.filename
        if not filename:
            return

        if not os.path.exists(filename):
            return

        fd = open(filename)
        L = fd.readlines()

        if not L:
            return

        if L[0].startswith("realm "):
            line = L.pop(0).strip()
            self.realm = line[len("realm "):]

        for line in L:
            username, hash = line.strip().split(":", 1)
            self._users[username] = hash.strip()

    def _store_password(self, username, password):
        self._users[username] = self.hash(password)

    def get_password(self, username):
        """Returns password hash for specified username.

        Callers must check for LookupError, which is raised in
        the case of a non-existent user specified."""
        if not self._users.has_key(username):
            raise LookupError, "No such user: %s" % username
        return self._users[username]

    def hash(self, s):
        return sha.new(s).hexdigest()

    def add_user(self, username, password):
        if self._users.has_key(username):
            raise LookupError, "User %s already exists" % username
        self._store_password(username, password)

    def del_user(self, username):
        if not self._users.has_key(username):
            raise LookupError, "No such user: %s" % username
        del self._users[username]

    def change_password(self, username, password):
        if not self._users.has_key(username):
            raise LookupError, "No such user: %s" % username
        self._store_password(username, password)
