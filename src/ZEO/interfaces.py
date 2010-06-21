##############################################################################
#
# Copyright (c) 2006 Zope Foundation and Contributors.
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

import zope.interface

class StaleCache(object):
    """A ZEO cache is stale and requires verification.
    """

    def __init__(self, storage):
        self.storage = storage

class IServeable(zope.interface.Interface):
    """Interface provided by storages that can be served by ZEO
    """

    def getTid(oid):
        """The last transaction to change an object

        Return the transaction id of the last transaction that committed a
        change to an object with the given object id.

        """

    def tpc_transaction():
        """The current transaction being committed.

        If a storage is participating in a two-phase commit, then
        return the transaction (object) being committed.  Otherwise
        return None.
        """

    def lastInvalidations(size):
        """Get recent transaction invalidations

        This method is optional and is used to get invalidations
        performed by the most recent transactions.

        An iterable of up to size entries must be returned, where each
        entry is a transaction id and a sequence of object-id/empty-string
        pairs describing the objects written by the
        transaction, in chronological order.
        """
