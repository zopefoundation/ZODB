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
"""Exceptions for ZEO."""

from ZODB.POSException import StorageError

class ClientStorageError(StorageError):
    """An error occured in the ZEO Client Storage."""

class UnrecognizedResult(ClientStorageError):
    """A server call returned an unrecognized result."""

class ClientDisconnected(ClientStorageError):
    """The database storage is disconnected from the storage."""

class AuthError(StorageError):
    """The client provided invalid authentication credentials."""
