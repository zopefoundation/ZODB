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

_auth_modules = {}

def get_module(name):
    if name == 'sha':
        from auth_sha import StorageClass, SHAClient, Database
        return StorageClass, SHAClient, Database
    elif name == 'digest':
        from auth_digest import StorageClass, DigestClient, DigestDatabase
        return StorageClass, DigestClient, DigestDatabase
    else:
        return _auth_modules.get(name)

def register_module(name, storage_class, client, db):
    if _auth_modules.has_key(name):
        raise TypeError, "%s is already registred" % name
    _auth_modules[name] = storage_class, client, db

