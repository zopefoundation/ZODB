Storage API Reference
=====================

A ZODB storage provides the low-level storage for ZODB transactions.
Examples include FileStorage, OracleStor- age, and bsddb3Storage. The
storage API handles storing and retrieving individual objects in a
transaction-specifc way. It also handles operations like pack and undo.
This document describes the interface implemented by storages.

Concepts
########

Versions
--------

Versions provide support for long-running transactions. They extend
transaction semantics, such as atomicity and serializability, to
computation that involves many basic transactions, spread over long
periods of time, which may be minutes, or years.

Versions were motivated by a common problem in website management, but
may be useful in other domains as well.  Often, a website must be
changed in such a way that changes, which may require many operations
over a period of time, must not be visible until completed and approved.
Typically this problem is solved through the use of staging servers.
Essentially, two copies of a website are maintained. Work is performed
on a staging server. When work is completed, the entire site is copied
from the staging server to the production server. This process is too
resource-intensive and too monolithic. It is not uncommon for separate
unrelated changes to be made to a website and these changes will need to
be copied to the production server independently. This requires an
unreasonable amount of coordination, or multiple staging servers.

ZODB addresses this problem through long-running transactions, called
versions. Changes made to a website can be made to a version (of the
website). The author sees the version of the site that reﬂects the
changes, but people working outside of the version cannot see the
changes. When the changes are completed and approved, they can be saved,
making them visible to others, almost instantaneously.

Versions require support from storage managers. Version support is an
optional feature of storage managers and support in a particular
database will depend on support in the underlying storage manager.

Storage Interface
#################

General issues:

The objects are stored as Python pickles. The pickle format is
important, because various parts of ZODB depend on it, e.g. pack.

Conﬂict resolution

Various versions of the interface.

Concurrency and transactions.

The various exceptions that can be raised.

An object that implements the Storage interface must support the
following methods:

:meth:`tpc begin(transaction[, tid[, status ] ])`
-------------------------------------------------

Begin the two-phase commit for ``transaction``.

This method blocks until the storage is in the not committing state, and
then places the storage in the committing state. If the storage is in
the committing state and the given transaction is the transaction that
is already being committed, then the call does not block and returns
immediately without any effect.

The optional ``tid`` argument speciﬁes the timestamp to be used for the
transaction ID and the new object serial numbers. If it is not speciﬁed,
the implementation chooses the timestamp.

The optional ``status`` argument, which has a default value of ’ ’, has
something to do with copying transactions.


:meth:`store(oid, serial, data, version, transaction)`
------------------------------------------------------

Store data, a Python pickle, for the object ID identiﬁed by oid . A
Storage need not and often will not write data immediately. If data are
written, then the storage should be prepared to undo the write if a
transaction is aborted.  it should be the value returned by the load()
call that read the ob-

The value of ``serial`` is opaque; it should be the value returned by
the load() call that read the object.

``version`` is a string that identiﬁes the version or the empty string.

``transaction`` an instance of :class:`ZODB.Transaction.Transaction`, is
the current transaction. The current transaction is the transaction
passed to the most recent tpc begin() call.

There are several possible return values, depending in part on whether
the storage writes the data immediately.  The return value will be one
of:

- ``None``, indicating the data has not been stored yet

- a string, containing the new serial number for the object

- a sequence of ``(object_id, serial_number)`` pairs, containing the new
  serial numbers for objects updated by earlier ``store()`` calls that are
  part of this transaction.
  
  If the serial number is not a string, it is an exception object that
  should be raised by the caller.
  
.. note::
     
   This explanation is confusing; how to tell the sequence of
   pairs from the exception? Barry, Jeremy, please clarify here.

Several different exceptions can be raised when an error occurs.

- :class:`ZODB.POSException.ConflictError` is raised when serial does
  not match the most recent serial number for object oid .

- :class:`ZODB.POSException.VersionLockError` is raised when a given
  object oid is locked in a version and the version argument contains
  a different version name or is empty.

- :class:`ZODB.POSException.StorageTransactionError` is raised when
  transaction does not match the current transaction.

- :class:`ZODB.POSException.StorageError` or, more often, a subclass of
  it, is raised when an internal error occurs while the storage is
  handling the ``store()`` call.

:meth:`restore(oid, serial, data, version, transaction)`
--------------------------------------------------------

A lot like :meth:`store()`, but without all the consistency checks. This
should only be used when we know the data is good, hence the method
name.

While the signature looks like :meth:`store()`, there are some differences:

- ``serial`` is the serial number of this revision, not of the previous
  revision.  It is used instead of ``self.serial``, which is ignored.

- ``data`` can be ``None``, which indicates a "George Bailey" object
  (one who’s creation has been transactionally undone).

Nothing is returned.


:meth:`new oid()`
-----------------

XXX

:meth:`tpc_vote(transaction)`
----------------------------

XXX

:meth:`tpc finish(transaction, func)`
-------------------------------------

Finish the transaction, making any transaction changes permanent.
Changes must be made permanent at this point.

If ``transaction`` is not the current transaction, nothing happens.

``func`` is called with no arguments while the storage lock is held, but
possibly before the updated date is made durable. This argument exists
to support the ``Connection`` object’s invalidation protocol.

:meth:`abortVersion(version, transaction)`
------------------------------------------

Clear any changes made by the given version.

``version`` is the version to be aborted; it may not be the empty
string.

``transaction`` is the current transaction.

This method is state dependent:

- It is an error to call this method if the storage is not committing,
  or if the given transaction is not the transaction given in the most
  recent :meth:`tpc begin()`.

- If undo is not supported, then version data may be simply discarded.

- If undo is supported, however, then the :meth:`abortVersion()`
  operation must be undoable, which implies that version data must be
  retained.

Use the :meth:`supportsUndo()` method to determine if the storage
supports the undo operation.

:meth:`commitVersion(source, destination, transaction)`
-------------------------------------------------------

Store changes made in the source version into the destination version. A
:class:`ZODB.POSException.VersionCommitError` is raised if the source
and destination are equal or if source is an empty string. The
destination may be an empty string, in which case the data are saved to
non-version storage.

This method is state dependent:

- It is an error to call this method if the storage is not committing,
  or if the given transaction is not the transaction given in the most
  recent :meth:`tpc begin()`.

- If the storage doesn’t support undo, then the old version data may be
  discarded.

- If undo is supported, then this operation must be undoable and old
  transaction data may not be discarded.

Use the :meth:`supportsUndo()` method to determine if the storage
supports the :meth:`undo` operation.

:meth:`close()`
---------------

Finalize the storage, releasing any external resources. The storage
should not be used after this method is called.

:meth:`lastSerial(oid)`
-----------------------

Returns the serial number for the last committed transaction for the
object identiﬁed by ``oid``.

If there is no serial number for ``oid`` — which can only occur if it
represents a new object — returns ``None``.

Note: This is not deﬁned for :class:`ZODB.BaseStorage`.

:meth:`lastTransaction()`
-------------------------

Returns the transaction ID for last committed transaction. 

Note: This is not deﬁned for :class:`ZODB.BaseStorage`.

:meth:`getName()`
-----------------

Returns the name of the store. The format and interpretation of this
name is storage dependent. It could be a ﬁle name, a database name, etc.

:meth:`getSize()`
-----------------

An approximate size of the database, in bytes.

:meth:`getSerial(oid)`
----------------------

Returns the serial number of the most recent version of the object
identiﬁed by ``oid``.

:meth:`load(oid, version)`
--------------------------

Returns the pickle data and serial number for the object identiﬁed by
``oid`` in the version ``version``.

:meth:`loadSerial(oid, serial)`
-------------------------------

Load a historical version of the object identiﬁed by ``oid`` having
serial number ``serial``.

:meth:`modifiedInVersion(oid)`
------------------------------

Returns the version that the object with identiﬁer ``oid`` was modiﬁed
in, or an empty string if the object was not modiﬁed in a version.

:meth:`isReadOnly()`
--------------------

Returns ``True`` if the storage is read-only, otherwise returns
``False``.

:meth:`supportsTransactionalUndo()`
-----------------------------------

Returns ``True`` if the storage implementation supports transactional
undo, or ``False`` if it does not.

Note: This is not deﬁned for :class:`ZODB.BaseStorage`.

:meth:`supportsUndo()`
----------------------

Returns ``True`` if the storage implementation supports undo, or
``False`` if it does not.

:meth:`supportsVersions()`
--------------------------

Returns ``True`` if the storage implementation supports versions, or
``False`` if it does not.

:meth:`transactionalUndo(transaction_id, transaction)`
------------------------------------------------------

Undo a transaction speciﬁed by ``transaction_id`` . This may need to do
con ﬂict resolution. 

Note: This is not deﬁned for :class:`ZODB.BaseStorage`.

:meth:`undo(transaction_id)`
----------------------------

Undo the transaction corresponding to ``transaction_id``.

If the transaction cannot be undone, then
:class:`ZODB.POSException.UndoError` is raised.

On success, returns a sequence of object IDs that were affected.

:meth:`undoInfo(XXX)`
---------------------

XXX

:meth:`undoLog([ﬁrst [, last[, ﬁlter ] ] ])`
--------------------------------------------

Returns a sequence of mappings describing undoable transactions.

``ﬁrst`` gives the index of the ﬁrst transaction to be retured, with
``0`` (the default) being the most recent.

Note: last is confusing; can Barry or Jeremy try to explain this?

If ``ﬁlter`` is provided and not ``None``, it must be a function which
accepts a mapping as a parameter and returns ``True`` if the entry
should be reported. If ``filter`` is omitted or ``None``, all entries
are reported.

:meth:`versionEmpty(version)`
-----------------------------

Return ``True`` if there are no transactions for the speciﬁed version.

:meth:`versions([max])`
------------------------

Return a sequence of names of versions stored in the storage.

If ``max`` is given, the implementation may choose not to
return more than ``max`` version names.

:meth:`history(oid[, version[, size[, ﬁlter ] ] ])`
---------------------------------------------------

Return a sequence of mappings, providing a log of the changes made to the
object ideintified by ``oic``.

Data are reported in reverse chronological order.

If ``version`` is given, history information is given with respect to
the speciﬁed version, or only the non-versioned changes if the empty
string is given.  By default, all changes are reported.

The number of history entries reported is constrained by ``size``, which
defaults to 1.

If ``ﬁlter`` is provided and not None, it must be a function which
accepts a mapping as a parameter and returns ``True`` if the entry should be
reported.  If ``filter`` is omitted or None, all entries are reported.

:meth:`pack(t, referencesf)`
----------------------------

Remove transactions from the database that are no longer needed to
maintain the current state of the database contents.

XXX ``t``

XXX ``referencesf``

:meth:`undo()` will not be restore objects to states from before the
most recent call to :meth:`pack()`.


:meth:`copyTransactionsFrom(other[, verbose ])`
-----------------------------------------------

Copy transactions from another storage, given by ``other``.

This method is typically used when converting a database from one
storage implementation to another.

This method will use :meth:`restore()` if available, but falls back to
:meth:`store()` if restore() is not available. In such cases, this
method may fail with :class:`ZODB.POSException.ConflictError` or
:class:`ZODB.POSException.VersionLockError`.

:meth:`iterator([start[, stop ] ])`
-----------------------------------

Return an iterable object which produces all the transactions from a range.

If ``start`` is given and not ``None``, transactions which occurred
before the transaction identiﬁed by ``start`` are ignored.

If ``stop`` is given and not ``None``, transactions which occurred after
the transaction identiﬁed by ``stop`` are ignored.  The transaction
identiﬁed by ``stop`` **will** be included in the series of transactions
produced by the iterator.

Note: This is not deﬁned for :class:`ZODB.BaseStorage`.

:meth:`registerDB(db, limit)`
-----------------------------

Register a database ``db`` for distributed storage invalidation messages.

The maximum number of objects to invalidate is given by ``limit``. If
more objects need to be invalidated than this limit, then all objects
are invalidated.  The ``limit`` argument may be None, in which case no limit is
set.

Non-distributed storages should treat this is a null operation.

Storages should work correctly even if this method is not called.

:class:`ZODB.BaseStorage` Implementation
========================================

Notes for Storage Implementors
==============================

Distributed Storage Interface
=============================

Distributed storages support use with multiple application processes.

Distributed storages have a storage instance per application and some
sort of central storage server that manages data on behalf of the
individual storage instances.

When a process changes an object, the object must be invaidated in all
other processes using the storage. The central storage sends a
notiﬁcation message to the other storage instances, which, in turn, send
invalidation messages to their respective databases.
