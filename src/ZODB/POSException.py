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
"""BoboPOS-defined exceptions

$Id: POSException.py,v 1.10 2002/01/25 02:15:07 gvanrossum Exp $"""
__version__ = '$Revision: 1.10 $'.split()[-2:][0]

from string import join
from types import StringType, DictType
from ZODB import utils

class POSError(Exception):
    """Persistent object system error
    """

class POSKeyError(KeyError, POSError):
    """Key not found in database
    """

    def __str__(self):
        return "%016x" % utils.U64(self.args[0])

class TransactionError(POSError):
    """An error occured due to normal transaction processing
    """

class ConflictError(TransactionError):
    """Two transactions tried to modify the same object at once.  This
    transaction should be resubmitted.

    Instance attributes:
      oid : string
        the OID (8-byte packed string) of the object in conflict
      class_name : string
        the fully-qualified name of that object's class
      message : string
        a human-readable explanation of the error
      serials : (string, string)
        a pair of 8-byte packed strings; these are the serial numbers
        (old and new) of the object in conflict.  (Serial numbers are
        closely related [equal?] to transaction IDs; a ConflictError may
        be triggered by a serial number mismatch.)

    The caller should pass either object or oid as a keyword argument,
    but not both of them.  If object is passed, it should be a
    persistent object with an _p_oid attribute.
    """

    def __init__(self, message=None, object=None, oid=None, serials=None):
        if message is None:
            self.message = "database conflict error"
        else:
            self.message = message

        if object is None:
            self.oid = None
            self.class_name = None
        else:
            self.oid = object._p_oid
            klass = object.__class__
            self.class_name = klass.__module__ + "." + klass.__name__

        if oid is not None:
            assert self.oid is None
            self.oid = oid

        self.serials = serials

    def __str__(self):
        extras = []
        if self.oid:
            extras.append("oid %016x" % utils.U64(self.oid))
        if self.class_name:
            extras.append("class %s" % self.class_name)
        if self.serials:
            extras.append("serial was %016x, now %016x" %
                          tuple(map(utils.U64, self.serials)))
        if extras:
            return "%s (%s)" % (self.message, ", ".join(extras))
        else:
            return self.message

    def get_oid(self):
        return self.oid

    def get_class_name(self):
        return self.class_name

    def get_old_serial(self):
        return self.serials[0]

    def get_new_serial(self):
        return self.serials[1]

    def get_serials(self):
        return self.serials


class ReadConflictError(ConflictError):
    """A conflict detected at read time -- attempt to read an object
    that has changed in another transaction (eg. another thread
    or process).
    """
    def __init__(self, message=None, object=None, serials=None):
        if message is None:
            message = "database read conflict error"
        ConflictError.__init__(self, message=message, object=object,
                               serials=serials)

class BTreesConflictError(ConflictError):
    """A special subclass for BTrees conflict errors, which return
    an undocumented four-tuple."""
    def __init__(self, *btree_args):
        ConflictError.__init__(self, message="BTrees conflict error")
        self.btree = btree_args

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

class ReadOnlyError(StorageError):
    """Unable to modify objects in a read-only storage.
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
