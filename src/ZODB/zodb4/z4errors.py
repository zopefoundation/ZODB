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
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""FileStorage-specific exceptions."""

from ZODB.zodb4.z4interfaces import _fmt_oid, POSError

# originally from zodb.storage.interfaces

class StorageError(POSError):
    """Base class for storage based exceptions."""

class StorageSystemError(StorageError):
    """Panic! Internal storage error!"""


# originally zodb.storage.file.errors

class FileStorageError(StorageError):
    pass

class PackError(FileStorageError):
    pass

class FileStorageFormatError(FileStorageError):
    """Invalid file format

    The format of the given file is not valid.
    """

class CorruptedError(FileStorageError, StorageSystemError):
    """Corrupted file storage."""

class CorruptedDataError(CorruptedError):

    def __init__(self, oid=None, buf=None, pos=None):
        self.oid = oid
        self.buf = buf
        self.pos = pos

    def __str__(self):
        if self.oid:
            msg = "Error reading oid %s.  Found %r" % (_fmt_oid(self.oid),
                                                       self.buf)
        else:
            msg = "Error reading unknown oid.  Found %r" % self.buf
        if self.pos:
            msg += " at %d" % self.pos
        return msg

class FileStorageQuotaError(FileStorageError, StorageSystemError):
    """File storage quota exceeded."""

