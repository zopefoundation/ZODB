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

import zope.interface

class IConnection(zope.interface.Interface):
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

class IBlobStorage(zope.interface.Interface):
    """A storage supporting BLOBs."""

    def storeBlob(oid, serial, data, blob, version, transaction):
        """Stores data that has a BLOB attached."""

    def loadBlob(oid, serial, version, blob):
        """Loads the BLOB data for 'oid' into the given blob object.
        """

