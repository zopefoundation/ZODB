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
from ZODB import POSException
from ZEO.Exceptions import ClientDisconnected

class ZRPCError(POSException.StorageError):
    pass

class DisconnectedError(ZRPCError, ClientDisconnected):
    """The database storage is disconnected from the storage server.

    The error occurred because a problem in the low-level RPC connection,
    or because the connection was closed.
    """

    # This subclass is raised when zrpc catches the error.
