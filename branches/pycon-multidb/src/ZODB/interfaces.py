##############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Interfaces for ZODB.

$Id$
"""

from zope.interface import Interface, Attribute

class IConnection(Interface):
    """ZODB connection.

    TODO: This interface is incomplete.
    """


    def add(ob):
        """Add a new object 'obj' to the database and assign it an oid.

        A persistent object is normally added to the database and
        assigned an oid when it becomes reachable to an object already in
        the database.  In some cases, it is useful to create a new
        object and use its oid (_p_oid) in a single transaction.

        This method assigns a new oid regardless of whether the object
        is reachable.

        The object is added when the transaction commits.  The object
        must implement the IPersistent interface and must not
        already be associated with a Connection.
        """

    # Multi-database support.

    connections = Attribute("""\
        A mapping from database name to a Connection to that database.

        In multi-database use, the Connections of all members of a database
        collection share the same .connections object.

        In single-database use, of course this mapping contains a single
        entry.
        """)

    # TODO:  should this accept all the arguments one may pass to DB.open()?
    def get_connection(database_name):
        """Return a Connection for the named database.

        This is intended to be called from an open Connection associated with
        a multi-database.  In that case, database_name must be the name of a
        database within the database collection (probably the name of a
        different database than is associated with the calling Connection
        instance, but it's fine to use the name of the calling Connection
        object's database).  A Connection for the named database is
        returned.  If no connection to that database is already open, a new
        Connection is opened.  So long as the multi-database remains open,
        passing the same name to get_connection() multiple times returns the
        same Connection object each time.
        """

class IDatabase(Interface):
    """ZODB DB.

    TODO: This interface is incomplete.
    """

    def __init__(storage,
                 pool_size=7,
                 cache_size=400,
                 version_pool_size=3,
                 version_cache_size=100,
                 database_name='unnamed',
                 databases=None,
                 ):
        """Create an object database.

        storage: the storage used by the database, e.g. FileStorage
        pool_size: expected maximum number of open connections
        cache_size: target size of Connection object cache, in number of
            objects
        version_pool_size: expected maximum number of connections (per
            version)
        version_cache_size: target size of Connection object cache for
             version connections, in number of objects
        database_name: when using a multi-database, the name of this DB
            within the database group.  It's a (detected) error if databases
            is specified too and database_name is already a key in it.
            This becomes the value of the DB's database_name attribute.
        databases: when using a multi-database, a mapping to use as the
            binding of this DB's .databases attribute.  It's intended
            that the second and following DB's added to a multi-database
            pass the .databases attribute set on the first DB added to the
            collection.
        """

    databases = Attribute("""\
        A mapping from database name to DB (database) object.

        In multi-database use, all DB members of a database collection share
        the same .databases object.

        In single-database use, of course this mapping contains a single
        entry.
        """)

