##############################################################################
#
# Copyright (c) 2001 Zope Corporation and Contributors.
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

# originally zodb.interfaces

"""ZODB database interfaces and exceptions

The Zope Object Database (ZODB) manages persistent objects using
pickle-based object serialization.  The database has a pluggable
storage backend.

The IAppDatabase, IAppConnection, and ITransaction interfaces describe
the public APIs of the database.

The IDatabase, IConnection, and ITransactionAttrs interfaces describe
private APIs used by the implementation.

$Id: z4interfaces.py,v 1.3 2004/04/19 21:19:09 tim_one Exp $
"""

from ZODB.zodb4 import z4utils
from zope.interface import Interface, Attribute

##from transaction.interfaces import ITransaction as _ITransaction
##from transaction.interfaces \
##     import TransactionError, RollbackError, ConflictError as _ConflictError

__all__ = [
    # Constants
    'ZERO',
    'MAXTID',
    # Exceptions
    'POSError',
    'POSKeyError',
##    'ConflictError',
##    'ReadConflictError',
##    'DanglingReferenceError',
##    'VersionError',
##    'VersionCommitError',
##    'VersionLockError',
##    'UndoError',
##    'MultipleUndoErrors',
##    'ExportError',
##    'Unsupported',
##    'InvalidObjectReference',
##    # Interfaces
##    'IAppConnection',
##    'IConnection',
##    'ITransaction',
##    'ITransactionAttrs',
    ]

ZERO = '\0'*8
MAXTID = '\377'*8

def _fmt_oid(oid):
    return "%016x" % z4utils.u64(oid)

def _fmt_undo(oid, reason):
    s = reason and (": %s" % reason) or ""
    return "Undo error %s%s" % (_fmt_oid(oid), s)

class POSError(StandardError):
    """Persistent object system error."""

class POSKeyError(KeyError, POSError):
    """Key not found in database."""

    def __str__(self):
        return _fmt_oid(self.args[0])

##class ConflictError(_ConflictError):
##    """Two transactions tried to modify the same object at once.

##    This transaction should be resubmitted.

##    Instance attributes:
##      oid : string
##        the OID (8-byte packed string) of the object in conflict
##      class_name : string
##        the fully-qualified name of that object's class
##      message : string
##        a human-readable explanation of the error
##      serials : (string, string)
##        a pair of 8-byte packed strings; these are the serial numbers
##        related to conflict.  The first is the revision of object that
##        is in conflict, the second is the revision of that the current
##        transaction read when it started.

##    The caller should pass either object or oid as a keyword argument,
##    but not both of them.  If object is passed, it should be a
##    persistent object with an _p_oid attribute.
##    """

##    def __init__(self, message=None, object=None, oid=None, serials=None):
##        if message is None:
##            self.message = "database conflict error"
##        else:
##            self.message = message

##        if object is None:
##            self.oid = None
##            self.class_name = None
##        else:
##            self.oid = object._p_oid
##            klass = object.__class__
##            self.class_name = klass.__module__ + "." + klass.__name__

##        if oid is not None:
##            assert self.oid is None
##            self.oid = oid

##        self.serials = serials

##    def __str__(self):
##        extras = []
##        if self.oid:
##            extras.append("oid %s" % _fmt_oid(self.oid))
##        if self.class_name:
##            extras.append("class %s" % self.class_name)
##        if self.serials:
##            extras.append("serial was %s, now %s" %
##                          tuple(map(_fmt_oid, self.serials)))
##        if extras:
##            return "%s (%s)" % (self.message, ", ".join(extras))
##        else:
##            return self.message

##    def get_oid(self):
##        return self.oid

##    def get_class_name(self):
##        return self.class_name

##    def get_old_serial(self):
##        return self.serials[0]

##    def get_new_serial(self):
##        return self.serials[1]

##    def get_serials(self):
##        return self.serials

##class ReadConflictError(ConflictError):
##    """Conflict detected when object was loaded.

##    An attempt was made to read an object that has changed in another
##    transaction (eg. another thread or process).
##    """
##    def __init__(self, message=None, object=None, serials=None):
##        if message is None:
##            message = "database read conflict error"
##        ConflictError.__init__(self, message=message, object=object,
##                               serials=serials)

##class DanglingReferenceError(TransactionError):
##    """An object has a persistent reference to a missing object.

##    If an object is stored and it has a reference to another object
##    that does not exist (for example, it was deleted by pack), this
##    exception may be raised.  Whether a storage supports this feature,
##    it a quality of implementation issue.

##    Instance attributes:
##    referer: oid of the object being written
##    missing: referenced oid that does not have a corresponding object
##    """

##    def __init__(self, Aoid, Boid):
##        self.referer = Aoid
##        self.missing = Boid

##    def __str__(self):
##        return "from %s to %s" % (_fmt_oid(self.referer),
##                                  _fmt_oid(self.missing))

##class VersionError(POSError):
##    """An error in handling versions occurred."""

##class VersionCommitError(VersionError):
##    """An invalid combination of versions was used in a version commit."""

##class VersionLockError(VersionError, TransactionError):
##    """Can't modify an object that is modified in unsaved version."""

##    def __init__(self, oid, version):
##        self.oid = oid
##        self.version = version

##    def __str__(self):
##        return "%s locked in version %r" % (_fmt_oid(self.oid),
##                                            self.version)

##class UndoError(POSError):
##    """An attempt was made to undo a non-undoable transaction."""

##    def __init__(self, oid, reason=None):
##        self._oid = oid
##        self._reason = reason

##    def __str__(self):
##        return _fmt_undo(self._oid, self._reason)

##class MultipleUndoErrors(UndoError):
##    """Several undo errors occured during a single transaction."""

##    def __init__(self, errs):
##        # provide an oid and reason for clients that only look at that
##        UndoError.__init__(self, *errs[0])
##        self._errs = errs

##    def __str__(self):
##        return "\n".join([_fmt_undo(*pair) for pair in self._errs])

##class ExportError(POSError):
##    """An export file doesn't have the right format."""

##class Unsupported(POSError):
##    """An feature that is unsupported bt the storage was used."""

##class InvalidObjectReference(POSError):
##    """An object contains an invalid reference to another object.

##    A reference is invalid if it refers to an object managed
##    by a different database connection.

##    Attributes:
##    obj is the invalid object
##    jar is the manager that attempted to store it.

##    obj._p_jar != jar
##    """

##    def __init__(self, obj, jar):
##        self.obj = obj
##        self.jar = jar

##    def __str__(self):
##        return "Invalid reference to object %s." % _fmt_oid(self.obj._p_jar)

##class IAppDatabase(Interface):
##    """Interface exported by database to applications.

##    The database contains a graph of objects reachable from the
##    distinguished root object.  The root object is a mapping object
##    that can contain arbitrary application data.

##    There is only rudimentary support for using more than one database
##    in a single application.  The persistent state of an object in one
##    database can not contain a direct reference to an object in
##    another database.
##    """

##    def open(version="", transaction=None, temporary=False, force=False,
##             waitflag=True):
##        # XXX Most of these arguments should eventually go away
##        """Open a new database connection."""

##    def abortVersion(version):
##        """Abort the locked database version named version."""

##    def commitVersion(source, dest=""):
##        """Commit changes from locked database version source to dest.

##        The default value of dest means commit the changes to the
##        default version.
##        """

##    def pack(time):
##        """Pack database to time."""

##    def undo(txnid):
##        """Undo changes caused by transaction txnid."""

##class IAppConnection(Interface):
##    """Interface exported by database connection to applications.

##    Each database connection provides an independent copy of the
##    persistent object space.  ZODB supports multiple threads by
##    providing each thread with a separate connection.

##    Connections are synchronized through database commits and explicit
##    sync() calls.  Changes to the object space are only made visible
##    when a transaction commits.  When a connection commits its
##    changes, they become visible to other connections.  Changes made
##    by other connections are also become visible at this time.
##    """

##    def root():
##        """Return the root of the database."""

##    def sync():
##        """Process pending invalidations.

##        If there is a current transaction, it will be aborted.
##        """

##    def get(oid):
##        """Return object for `oid`.

##        The object may be a ghost.
##        """

##class IDatabase(Interface):
##    """Interface between the database and its connections."""

##    def invalidate(oids, conn=None, version=""):
##        pass

##    def _closeConnection(conn):
##        pass


##class IConnection(Interface):
##    """Interface required of Connection by ZODB DB.

##    The Connection also implements IPersistentDataManager.
##    """

##    def invalidate(oids):
##        """Invalidate a set of oids modified by a single transaction.

##        This marks the oids as invalid, but doesn't actually
##        invalidate them.  The object data will be actually invalidated
##        at certain transaction boundaries.
##        """

##    def reset(version=""):
##        """Reset connection to use specified version."""

##    def getVersion():
##        """Return the version that connection is using."""

##    def close():
##        pass

##    def cacheGC():
##        pass

##    def add(obj):
##        """Add a persistent object to this connection.

##        Essentially, set _p_jar and assign _p_oid on the object.

##        Raises a TypeError if obj is not persistent. Does nothing if
##        obj is already added to this connection.
##        """

##class ITransaction(_ITransaction):
##    """Extends base ITransaction with with metadata.

##    Client code should use this interface to set attributes.
##    """

##    def note(text):
##        """Add the text to the transaction description

##        If there previous description isn't empty, a blank line is
##        added before the new text.
##        """

##    def setUser(user_name):
##        """Set the transaction user name."""

##    def setExtendedInfo(name, value):
##        """Set extended information."""

##class ITransactionAttrs(_ITransaction):
##    # XXX The following attributes used by storages, so they are part
##    # of the interface.  But I'd rather not have user code explicitly
##    # use the attributes.

##    user = Attribute("The user as set by setUser()")

##    description = Attribute("A description as set by note()")

##    _extension = Attribute(
##        """Extended info as set by setExtendedInfo()

##        Should be None or a dictionary.""")
