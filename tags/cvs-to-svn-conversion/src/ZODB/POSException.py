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
"""ZODB-defined exceptions

$Id: POSException.py,v 1.24 2004/04/16 19:07:00 tim_one Exp $"""

from ZODB.utils import oid_repr, serial_repr

def _fmt_undo(oid, reason):
    s = reason and (": %s" % reason) or ""
    return "Undo error %s%s" % (oid_repr(oid), s)

class POSError(StandardError):
    """Persistent object system error."""

class POSKeyError(KeyError, POSError):
    """Key not found in database."""

    def __str__(self):
        return oid_repr(self.args[0])

class TransactionError(POSError):
    """An error occured due to normal transaction processing."""

class ConflictError(TransactionError):
    """Two transactions tried to modify the same object at once.

    This transaction should be resubmitted.

    Instance attributes:
      oid : string
        the OID (8-byte packed string) of the object in conflict
      class_name : string
        the fully-qualified name of that object's class
      message : string
        a human-readable explanation of the error
      serials : (string, string)
        a pair of 8-byte packed strings; these are the serial numbers
        related to conflict.  The first is the revision of object that
        is in conflict, the second is the revision of that the current
        transaction read when it started.
      data : string
        The database record that failed to commit, used to put the
        class name in the error message.

    The caller should pass either object or oid as a keyword argument,
    but not both of them.  If object is passed, it should be a
    persistent object with an _p_oid attribute.
    """

    def __init__(self, message=None, object=None, oid=None, serials=None,
                 data=None):
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

        if data is not None:
            # avoid circular import chain
            from ZODB.serialize import SimpleObjectReader
            self.class_name = SimpleObjectReader().getClassName(data)
##        else:
##            if message != "data read conflict error":
##                raise RuntimeError

        self.serials = serials

    def __str__(self):
        extras = []
        if self.oid:
            extras.append("oid %s" % oid_repr(self.oid))
        if self.class_name:
            extras.append("class %s" % self.class_name)
        if self.serials:
            extras.append("serial was %s, now %s" %
                          tuple(map(serial_repr, self.serials)))
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
    """Conflict detected when object was loaded.

    An attempt was made to read an object that has changed in another
    transaction (eg. another thread or process).
    """
    def __init__(self, message=None, object=None, serials=None):
        if message is None:
            message = "database read conflict error"
        ConflictError.__init__(self, message=message, object=object,
                               serials=serials)

class BTreesConflictError(ConflictError):
    """A special subclass for BTrees conflict errors."""

    msgs = [# 0; i2 or i3 bucket split; positions are all -1
            'Conflicting bucket split',

            # 1; keys the same, but i2 and i3 values differ, and both values
            # differ from i1's value
            'Conflicting changes',

            # 2; i1's value changed in i2, but key+value deleted in i3
            'Conflicting delete and change',

            # 3; i1's value changed in i3, but key+value deleted in i2
            'Conflicting delete and change',

            # 4; i1 and i2 both added the same key, or both deleted the
            # same key
            'Conflicting inserts or deletes',

            # 5;  i2 and i3 both deleted the same key
            'Conflicting deletes',

            # 6; i2 and i3 both added the same key
            'Conflicting inserts',

            # 7; i2 and i3 both deleted the same key, or i2 changed the value
            # associated with a key and i3 deleted that key
            'Conflicting deletes, or delete and change',

            # 8; i2 and i3 both deleted the same key, or i3 changed the value
            # associated with a key and i2 deleted that key
            'Conflicting deletes, or delete and change',

            # 9; i2 and i3 both deleted the same key
            'Conflicting deletes',

            # 10; i2 and i3 deleted all the keys, and didn't insert any,
            # leaving an empty bucket; conflict resolution doesn't have
            # enough info to unlink an empty bucket from its containing
            # BTree correctly
            'Empty bucket from deleting all keys',

            # 11; conflicting changes in an internal BTree node
            'Conflicting changes in an internal BTree node',
            ]

    def __init__(self, p1, p2, p3, reason):
        self.p1 = p1
        self.p2 = p2
        self.p3 = p3
        self.reason = reason

    def __repr__(self):
        return "BTreesConflictError(%d, %d, %d, %d)" % (self.p1,
                                                        self.p2,
                                                        self.p3,
                                                        self.reason)
    def __str__(self):
        return "BTrees conflict error at %d/%d/%d: %s" % (
            self.p1, self.p2, self.p3, self.msgs[self.reason])

class DanglingReferenceError(TransactionError):
    """An object has a persistent reference to a missing object.

    If an object is stored and it has a reference to another object
    that does not exist (for example, it was deleted by pack), this
    exception may be raised.  Whether a storage supports this feature,
    it a quality of implementation issue.

    Instance attributes:
    referer: oid of the object being written
    missing: referenced oid that does not have a corresponding object
    """

    def __init__(self, Aoid, Boid):
        self.referer = Aoid
        self.missing = Boid

    def __str__(self):
        return "from %s to %s" % (oid_repr(self.referer),
                                  oid_repr(self.missing))

class VersionError(POSError):
    """An error in handling versions occurred."""

class VersionCommitError(VersionError):
    """An invalid combination of versions was used in a version commit."""

class VersionLockError(VersionError, TransactionError):
    """Modification to an object modified in an unsaved version.

    An attempt was made to modify an object that has been modified in an
    unsaved version.
    """

class UndoError(POSError):
    """An attempt was made to undo a non-undoable transaction."""

    def __init__(self, reason, oid=None):
        self._reason = reason
        self._oid = oid

    def __str__(self):
        return _fmt_undo(self._oid, self._reason)

class MultipleUndoErrors(UndoError):
    """Several undo errors occured during a single transaction."""

    def __init__(self, errs):
        # provide a reason and oid for clients that only look at that
        UndoError.__init__(self, *errs[0])
        self._errs = errs

    def __str__(self):
        return "\n".join([_fmt_undo(*pair) for pair in self._errs])

class StorageError(POSError):
    """Base class for storage based exceptions."""

class StorageTransactionError(StorageError):
    """An operation was invoked for an invalid transaction or state."""

class StorageSystemError(StorageError):
    """Panic! Internal storage error!"""

class MountedStorageError(StorageError):
    """Unable to access mounted storage."""

class ReadOnlyError(StorageError):
    """Unable to modify objects in a read-only storage."""

class TransactionTooLargeError(StorageTransactionError):
    """The transaction exhausted some finite storage resource."""

class ExportError(POSError):
    """An export file doesn't have the right format."""

class Unsupported(POSError):
    """A feature was used that is not supported by the storage."""

class InvalidObjectReference(POSError):
    """An object contains an invalid reference to another object.

    An invalid reference may be one of:

    o A reference to a wrapped persistent object.

    o A reference to an object in a different database connection.

    XXX The exception ought to have a member that is the invalid object.
    """

class ConnectionStateError(POSError):
    """A Connection isn't in the required state for an operation.

    o An operation such as a load is attempted on a closed connection.

    o An attempt to close a connection is made while the connection is
      still joined to a transaction (for example, a transaction is in
      progress, with uncommitted modifications in the connection).
    """
