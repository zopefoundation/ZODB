#!python
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
"""Update a user's authentication tokens for a ZEO server.

usage: python zeopasswd.py [options] username [password]

Specify either a configuration file:

    -C/--configuration -- ZConfig configuration file

or the individual options:

    -f/--filename -- authentication database filename
    -p/--protocol -- authentication protocol name
    -r/--realm -- authentication database realm

Additional options:

    -d/--delete -- delete user instead of updating password
"""

import getopt
import getpass
import sys
import os

import ZConfig
import ZEO

def usage(msg):
    print __doc__
    print msg
    sys.exit(2)

def options(args):
    """Password-specific options loaded from regular ZEO config file."""

    try:
        options, args = getopt.getopt(args, "dr:p:f:C:", ["configure=", 
                                                          "protocol=", 
                                                          "filename=",
                                                          "realm"])
    except getopt.error, msg:
        usage(msg)
    config = None
    delete = 0
    auth_protocol = None
    auth_db = "" 
    auth_realm = None
    for k, v in options:
        if k == '-C' or k == '--configure':
            schemafile = os.path.join(os.path.dirname(ZEO.__file__),
                                                     "schema.xml")
            schema = ZConfig.loadSchema(schemafile)
            config, nil = ZConfig.loadConfig(schema, v)
        if k == '-d' or k == '--delete':
            delete = 1
        if k == '-p' or k == '--protocol':
            auth_protocol = v
        if k == '-f' or k == '--filename':
            auth_db = v
        if k == '-r' or k == '--realm':
            auth_realm = v

    if config is not None:
        if auth_protocol or auth_db:
            usage("Error: Conflicting options; use either -C *or* -p and -f")
        auth_protocol = config.zeo.authentication_protocol
        auth_db = config.zeo.authentication_database
        auth_realm = config.zeo.authentication_realm
    elif not (auth_protocol and auth_db):
        usage("Error: Must specifiy configuration file or protocol and database")

    password = None
    if delete:
        if not args:
            usage("Error: Must specify a username to delete")
        elif len(args) > 1:
            usage("Error: Too many arguments")
        username = args[0]
    else:
        if not args:
            usage("Error: Must specify a username")
        elif len(args) > 2:
            usage("Error: Too many arguments")
        elif len(args) == 1:
            username = args[0]
        else:
            username, password = args

    return auth_protocol, auth_db, auth_realm, delete, username, password

def main(args=None):
    p, auth_db, auth_realm, delete, username, password = options(args)
    if p is None:
        usage("Error: configuration does not specify auth protocol")
    if p == "digest":
        from ZEO.auth.auth_digest import DigestDatabase as Database
    elif p == "srp":
        from ZEO.auth.auth_srp import SRPDatabase as Database
    else:
        raise ValueError, "Unknown database type %r" % p
    if auth_db is None:
        usage("Error: configuration does not specify auth database")
    db = Database(auth_db, auth_realm)
    if delete:
        db.del_user(username)
    else:
        if password is None:
            password = getpass.getpass("Enter password: ")
        db.add_user(username, password)
    db.save()

if __name__ == "__main__":
    main(sys.argv[1:])

