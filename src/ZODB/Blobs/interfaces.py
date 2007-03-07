##############################################################################
#
# Copyright (c) 2005-2007 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Blob-related interfaces

"""

from zope.interface import Interface


class IBlob(Interface):
    """A BLOB supports efficient handling of large data within ZODB."""

    def open(mode):
        """Returns a file(-like) object for handling the blob data.

        mode: Mode to open the file with. Possible values: r,w,r+,a
        """

    def openDetached():
        """Returns a file(-like) object in read mode that can be used
        outside of transaction boundaries.

        The file handle returned by this method is read-only and at the
        beginning of the file. 

        The handle is not attached to the blob and can be used outside of a
        transaction.
        """

    def consumeFile(filename):
        """Will replace the current data of the blob with the file given under
        filename.

        This method uses link() internally and has the same requirements (UNIX
        only and must live on the same partition as the original file).

        The blob must not be opened for reading or writing when consuming a 
        file.
        """


class IBlobStorage(Interface):
    """A storage supporting BLOBs."""

    def storeBlob(oid, oldserial, data, blob, version, transaction):
        """Stores data that has a BLOB attached."""

    def loadBlob(oid, serial, version):
        """Return the filename of the Blob data responding to this OID and
        serial.

        Returns a filename or None if no Blob data is connected with this OID. 

        Raises POSKeyError if the blobfile cannot be found.
        """
