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

-C/--configuration URL -- configuration file or URL
-d/--delete -- delete user instead of updating password
"""

import getopt
import getpass
import sys

import ZConfig
import ZEO

def usage(msg):
    print msg
    print __doc__
    sys.exit(2)

def options(args):
    """Password-specific options loaded from regular ZEO config file."""

    schema = ZConfig.loadSchema(os.path.join(os.path.dirname(ZEO.__file__),
                                             "schema.xml"))

    try:
        options, args = getopt.getopt(args, "C:", ["configure="])
    except getopt.error, msg:
        usage(msg)
    config = None
    delete = False
    for k, v in options:
        if k == '-C' or k == '--configure':
            config, nil = ZConfig.loadConfig(schema, v)
        if k == '-d' or k == '--delete':
            delete = True
    if config is None:
        usage("Must specifiy configuration file")

    password = None
    if delete:
        if not args:
            usage("Must specify username to delete")
        elif len(args) > 1:
            usage("Too many arguments")
        username = args[0]
    else:
        if not args:
            usage("Must specify username")
        elif len(args) > 2:
            usage("Too many arguments")
        elif len(args) == 1:
            username = args[0]
        else:
            username, password = args

    return config.zeo, delete, username, password

def main(args=None):
    options, delete, username, password = options(args)
    p = options.authentication_protocol
    if p is None:
        usage("ZEO configuration does not specify authentication-protocol")
    if p == "digest":
        from ZEO.auth.auth_digest import DigestDatabase as Database
    elif p == "srp":
        from ZEO.auth.auth_srp import SRPDatabase as Database
    if options.authentication_database is None:
        usage("ZEO configuration does not specify authentication-database")
    db = Database(options.authentication_database)
    if delete:
        db.del_user(username)
    else:
        if password is None:
            password = getpass.getpass("Enter password: ")
        db.add_user(username, password)
    db.save()

if __name__ == "__main__":
    main(sys.argv)
