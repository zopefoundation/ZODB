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
"""Digest authentication for ZEO

This authentication mechanism follows the design of HTTP digest
authentication (RFC 2069).  It is a simple challenge-response protocol
that does not send passwords in the clear, but does not offer strong
security.  The RFC discusses many of the limitations of this kind of
protocol.

Guard the password database as if it contained plaintext passwords.
It stores the hash of a username and password.  This does not expose
the plaintext password, but it is sensitive nonetheless.  An attacker
with the hash can impersonate the real user.  This is a limitation of
the simple digest scheme.

HTTP is a stateless protocol, and ZEO is a stateful protocol.  The
security requirements are quite different as a result.  The HTTP
protocol uses a nonce as a challenge.  The ZEO protocol requires a
separate session key that is used for message authentication.  We
generate a second nonce for this purpose; the hash of nonce and
user/realm/password is used as the session key.  XXX I'm not sure if
this is a sound approach; SRP would be preferred.
"""

import base64
import os
import random
import sha
import struct
import time

from ZEO.auth.base import Database, Client
from ZEO.StorageServer import ZEOStorage
from ZEO.Exceptions import AuthError

def get_random_bytes(n=8):
    if os.path.exists("/dev/urandom"):
        f = open("/dev/urandom")
        s = f.read(n)
        f.close()
    else:
        L = [chr(random.randint(0, 255)) for i in range(n)]
        s = "".join(L)
    return s

def hexdigest(s):
    return sha.new(s).hexdigest()

class DigestDatabase(Database):
    def __init__(self, filename, realm=None):
        Database.__init__(self, filename, realm)
        
        # Initialize a key used to build the nonce for a challenge.
        # We need one key for the lifetime of the server, so it
        # is convenient to store in on the database.
        self.noncekey = get_random_bytes(8)

    def _store_password(self, username, password):
        dig = hexdigest("%s:%s:%s" % (username, self.realm, password))
        self._users[username] = dig

def session_key(h_up, nonce):
    # The hash itself is a bit too short to be a session key.
    # HMAC wants a 64-byte key.  We don't want to use h_up
    # directly because it would never change over time.  Instead
    # use the hash plus part of h_up.
    return sha.new("%s:%s" % (h_up, nonce)).digest() + h_up[:44]

class StorageClass(ZEOStorage):
    def set_database(self, database):
        assert isinstance(database, DigestDatabase)
        self.database = database
        self.noncekey = database.noncekey

    def _get_time(self):
        # Return a string representing the current time.
        t = int(time.time())
        return struct.pack("i", t)

    def _get_nonce(self):
        # RFC 2069 recommends a nonce of the form
        # H(client-IP ":" time-stamp ":" private-key)
        dig = sha.sha()
        dig.update(str(self.connection.addr))
        dig.update(self._get_time())
        dig.update(self.noncekey)
        return dig.hexdigest()

    def auth_get_challenge(self):
        """Return realm, challenge, and nonce."""
        self._challenge = self._get_nonce()
        self._key_nonce = self._get_nonce()
        return self.auth_realm, self._challenge, self._key_nonce

    def auth_response(self, resp):
        # verify client response
        user, challenge, response = resp

        # Since zrpc is a stateful protocol, we just store the nonce
        # we sent to the client.  It will need to generate a new
        # nonce for a new connection anyway.
        if self._challenge != challenge:
            raise ValueError, "invalid challenge"

        # lookup user in database
        h_up = self.database.get_password(user)

        # regeneration resp from user, password, and nonce
        check = hexdigest("%s:%s" % (h_up, challenge))
        if check == response:
            self.connection.setSessionKey(session_key(h_up, self._key_nonce))
        return self.finish_auth(check == response)

    extensions = [auth_get_challenge, auth_response]

class DigestClient(Client):
    extensions = ["auth_get_challenge", "auth_response"]

    def start(self, username, realm, password):
        _realm, challenge, nonce = self.stub.auth_get_challenge()
        if _realm != realm:
            raise AuthError("expected realm %r, got realm %r"
                            % (_realm, realm))
        h_up = hexdigest("%s:%s:%s" % (username, realm, password))
        
        resp_dig = hexdigest("%s:%s" % (h_up, challenge))
        result = self.stub.auth_response((username, challenge, resp_dig))
        if result:
            return session_key(h_up, nonce)
        else:
            return None
