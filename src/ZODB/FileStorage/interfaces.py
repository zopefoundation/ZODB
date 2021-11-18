##############################################################################
#
# Copyright (c) Zope Corporation and Contributors.
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


class IFileStoragePacker(zope.interface.Interface):

    def __call__(storage, referencesf, stop, gc):
        r"""Pack the file storage into a new file

        :param FileStorage storage: The storage object to be packed
        :param callable referencesf: A function that extracts object
            references from a pickle bytes string.  This is usually
            ``ZODB.serialize.referencesf``.
        :param bytes stop: A transaction id representing the time at
            which to stop packing.
        :param bool gc: A flag indicating whether garbage collection
            should be performed.

        The new file will have the same name as the old file with
        ``.pack`` appended. (The packer can get the old file name via
        storage._file.name.) If blobs are supported, if the storages
        blob_dir attribute is not None or empty, then a .removed file
        must be created in the blob directory. This file contains records of
        the form::

           (oid+serial).encode('hex')+'\n'

        or, of the form::

           oid.encode('hex')+'\n'

        If packing is unnecessary, or would not change the file, then
        no pack or removed files are created None is returned,
        otherwise a tuple is returned with:

        - the size of the packed file, and

        - the packed index

        If and only if packing was necessary (non-None) and there was
        no error, then the commit lock must be acquired.  In addition,
        it is up to FileStorage to:

        - Rename the .pack file, and

        - process the blob_dir/.removed file by removing the blobs
          corresponding to the file records.
        """


class IFileStorage(zope.interface.Interface):

    packer = zope.interface.Attribute(
        "The IFileStoragePacker to be used for packing."
    )

    _file = zope.interface.Attribute(
        "The file object used to access the underlying data."
    )

    _lock = zope.interface.Attribute(
        "The storage lock."
    )

    _commit_lock = zope.interface.Attribute(
        "The storage commit lock."
    )
