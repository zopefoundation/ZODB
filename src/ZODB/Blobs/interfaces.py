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

    def openDetached(class_=file):
        """Returns a file(-like) object in read mode that can be used
        outside of transaction boundaries.

        The file handle returned by this method is read-only and at the
        beginning of the file. 

        The handle is not attached to the blob and can be used outside of a
        transaction.

        Optionally the class that should be used to open the file can be
        specified. This can be used to e.g. use Zope's FileStreamIterator.
        """

    def consumeFile(filename):
        """Will replace the current data of the blob with the file given under
        filename.

        This method uses link-like semantics internally and has the requirement
        that the file that is to be consumed lives on the same volume (or
        mount/share) as the blob directory.

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
