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
"""Usage:
        zpasswd [-cd] passwordfile username
        zpasswd -b[cd] passwordfile username password

        zpasswd -n[d] username
        zpasswd -nb[d] username password
 -c  Create a new file.
 -d  Delete user
 -n  Don't update file; display results on stdout.
 -b  Use the password from the command line rather than prompting for it."""

import sys
import getopt
import getpass

from ZEO.auth.database import Database
#from ZEO.auth.srp import SRPDatabase
   
try:
    opts, args = getopt.getopt(sys.argv[1:], 'cdnbs')
except getopt.GetoptError:
    # print help information and exit:
    print __doc__
    sys.exit(2)

stdout = 0
create = 0
delete = 0
prompt = 1
#srp = 0

for opt, arg in opts:
    if opt in ("-h", "--help"):
        print __doc__
        sys.exit()
    if opt == "-n":
        stdout = 1
    if opt == "-c":
        create = 1
    if opt == "-d":
        delete = 1
    if opt == "b":
        prompt = 0
#    if opt == "-s":
#        srp = 1

if create and delete:
    print "Can't create and delete at the same time"
    sys.exit(3)

if len(args) < 2:
    print __doc__
    sys.exit()

output = args[0]
username = args[1]

if not delete:
    if len(args) > 3:
        print __doc__
        sys.exit()
        
    if prompt:
        password = getpass.getpass('Enter passphrase: ')
    else:
        password = args[2]

#if srp:
#    db = SRPDatabase(output)
#else:
db = Database(output)

if create:
    try:
        db.add_user(username, password)
    except LookupError:
        print 'The username already exists'
        sys.exit(4)
    if stdout:
        db.save(fd=sys.stdout)
    else:
        db.save()
    
if delete:
    try:
        db.del_user(username)
    except LockupError:
        print 'The username doesn\'t exist'
        sys.exit(5)
    if stdout:
        db.save(fd=sys.stdout)
    else:
        db.save()

