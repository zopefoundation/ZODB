##############################################################################
#
# Copyright (c) 2001 Zope Corporation and Contributors. All Rights Reserved.
# 
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
# 
##############################################################################
'''BoboPOS-defined exceptions

$Id: POSException.py,v 1.8 2001/11/28 15:51:20 matt Exp $'''
__version__='$Revision: 1.8 $'[11:-2]

from string import join
StringType=type('')
DictType=type({})

class POSError(Exception):
    """Persistent object system error
    """

class TransactionError(POSError):
    """An error occured due to normal transaction processing
    """

class ConflictError(TransactionError):
    """Two transactions tried to modify the same object at once

    This transaction should be resubmitted.
    """

class VersionError(POSError):
    """An error in handling versions occurred
    """

class VersionCommitError(VersionError):
    """An invalid combination of versions was used in a version commit
    """

class VersionLockError(VersionError, TransactionError):
    """An attempt was made to modify an object that has
    been modified in an unsaved version"""

class UndoError(POSError):
    """An attempt was made to undo a non-undoable transaction.
    """
    def __init__(self, *reason):
        if len(reason) == 1: reason=reason[0]
        self.__reason=reason

    def __repr__(self):
        reason=self.__reason
        if type(reason) is not DictType:
            if reason: return str(reason)
            return "non-undoable transaction"
        r=[]
        for oid, reason in reason.items():
            if reason:
                r.append("Couldn't undo change to %s because %s"
                         % (`oid`, reason))
            else:
                r.append("Couldn't undo change to %s" % (`oid`))

        return join(r,'\n')

    __str__=__repr__

class StorageError(POSError):
    pass

class StorageTransactionError(StorageError):
    """An operation was invoked for an invalid transaction or state
    """

class StorageSystemError(StorageError):
    """Panic! Internal storage error!
    """

class MountedStorageError(StorageError):
    """Unable to access mounted storage.
    """

class ExportError(POSError):
    """An export file doesn't have the right format.
    """
    pass

class Unimplemented(POSError):
    """An unimplemented feature was used
    """
    pass

class Unsupported(POSError):
    """An feature that is unsupported bt the storage was used.
    """
    
class InvalidObjectReference(POSError):
    """An object contains an invalid reference to another object.

    An invalid reference may be one of:

    o A reference to a wrapped persistent object.

    o A reference to an object in a different database connection.
    """
