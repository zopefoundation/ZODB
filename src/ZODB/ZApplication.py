##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
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
"""Implement an bobo_application object that is BoboPOS3 aware

This module provides a wrapper that causes a database connection to be created
and used when bobo publishes a bobo_application object.
"""
__version__='$Revision: 1.12 $'[11:-2]

StringType=type('')
connection_open_hooks = []

class ZApplicationWrapper:

    def __init__(self, db, name, klass= None, klass_args= (),
                 version_cookie_name=None):
        self._stuff = db, name, version_cookie_name
        if klass is not None:
            conn=db.open()
            root=conn.root()
            if not root.has_key(name):
                root[name]=klass()
                conn.getTransaction().commit()
            conn.close()
            self._klass=klass


    # This hack is to overcome a bug in Bobo!
    def __getattr__(self, name):
        return getattr(self._klass, name)

    def __bobo_traverse__(self, REQUEST=None, name=None):
        db, aname, version_support = self._stuff
        if version_support is not None and REQUEST is not None:
            version=REQUEST.get(version_support,'')
        else: version=''
        conn=db.open(version)

        if connection_open_hooks:
            for hook in connection_open_hooks:
                hook(conn)

        # arrange for the connection to be closed when the request goes away
        cleanup=Cleanup()
        cleanup.__del__=conn.close
        REQUEST._hold(cleanup)

        conn.setDebugInfo(REQUEST.environ, REQUEST.other)

        v=conn.root()[aname]

        if name is not None:
            if hasattr(v, '__bobo_traverse__'):
                return v.__bobo_traverse__(REQUEST, name)

            if hasattr(v,name): return getattr(v,name)
            return v[name]

        return v


    def __call__(self, connection=None):
        db, aname, version_support = self._stuff

        if connection is None:
            connection=db.open()
        elif type(connection) is StringType:
            connection=db.open(connection)

        return connection.root()[aname]


class Cleanup: pass
