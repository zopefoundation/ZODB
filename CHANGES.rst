================
 Change History
================

5.6.0 (2020-06-11)
==================

- Fix race with invalidations when starting a new transaction. The bug
  affected Storage implementations that rely on mvccadapter, and could result
  in data corruption (oid loaded at wrong serial after a concurrent commit).
  See `issue 290 <https://github.com/zopefoundation/ZODB/issues/290>`_.
  As mentionned in pull request #307, interfaces are clarified about the fact
  that storage implementations must update at a precise moment the value that
  is returned by lastTransaction(): just after invalidate() or
  tpc_finish callback.

- Improve volatile attribute ``_v_`` documentation.

- Make repozo's recover mode atomic by recovering the backup in a
  temporary file which is then moved to the expected output file.

- Add a new option to repozo in recover mode which allows to verify
  backups integrity on the fly.

- Drop support for Python 3.4.

- Add support for Python 3.8.

- Fix ``DB.undo()`` and ``DB.undoMultiple()`` to close the storage
  they open behind the scenes when the transaction is committed or
  rolled back. See `issue 268
  <https://github.com/zopefoundation/ZODB/issues/268>`_.

- Make TransactionMetaData in charge of (de)serializing extension data.
  A new ``extension_bytes`` attribute converts automatically from
  ``extension``, or vice-versa. During storage iteration, ``extension_bytes``
  holds bytes as they are stored (i.e. no deserialization happens).
  See `issue 207 <https://github.com/zopefoundation/ZODB/pull/207>`_.

- Make a connection's savepoint storage implement its own
  (approximate) ``getSize`` method instead of relying on the original
  storage. Previously, this produced confusing DEBUG logging. See
  `issue 282 <https://github.com/zopefoundation/ZODB/issues/282>`_.

- Fix tests with transaction 3.0.

- Fix inconsistent resolution order with zope.interface v5.

5.5.1 (2018-10-25)
==================

- Fix KeyError on releasing resources of a Connection when closing the DB.
  This requires at least version 2.4 of the ``transaction`` package.
  See `issue 208 <https://github.com/zopefoundation/ZODB/issues/208>`_.

5.5.0 (2018-10-13)
==================

- Add support for Python 3.7.

- Bump the dependency on zodbpickle to at least 1.0.1. This is
  required to avoid a memory leak on Python 2.7. See `issue 203
  <https://github.com/zopefoundation/ZODB/issues/203>`_.

- Bump the dependency on persistent to at least 4.4.0.

- Make the internal support functions for dealing with OIDs (``p64``
  and ``u64``) somewhat faster and raise more informative
  exceptions on certain types of bad input. See `issue 216
  <https://github.com/zopefoundation/ZODB/issues/216>`_.

- Remove support for ``python setup.py test``. It hadn't been working
  for some time. See `issue #218
  <https://github.com/zopefoundation/ZODB/issues/218>`_.

- Make the tests run faster by avoiding calls to ``time.sleep()``.

5.4.0 (2018-03-26)
==================

- ZODB now uses pickle protocol 3 for both Python 2 and Python 3.

  (Previously, protocol 2 was used for Python 2.)

  The zodbpickle package provides a `zodbpickle.binary` string type
  that should be used in Python 2 to cause binary strings to be saved
  in a pickle binary format, so they can be loaded correctly in
  Python 3.  Pickle protocol 3 is needed for this to work correctly.

- Object identifiers in persistent references are saved as
  `zodbpickle.binary` strings in Python 2, so that they are loaded
  correctly in Python 3.

- If an object is missing from the index while packing a ``FileStorage``,
  report its full ``oid``.

- Storage imports are a bit faster.

- Storages can be important from non-seekable sources, like
  file-wrapped pipes.

5.3.0 (2017-08-30)
==================

- Add support for Python 3.6.

- Drop support for Python 3.3.

- Ensure that the ``HistoricalStorageAdapter`` forwards the ``release`` method to
  its base instance. See `issue 78 <https://github.com/zopefoundation/ZODB/issues/788>`_.

- Use a higher pickle protocol (2) for serializing objects on Python
  2; previously protocol 1 was used. This is *much* more efficient for
  new-style classes (all persistent objects are new-style), at the
  cost of being very slightly less efficient for old-style classes.

  .. note:: On Python 2, this will now allow open ``file`` objects
            (but **not** open blobs or sockets) to be pickled (loading
            the object will result in a closed file); previously this
            would result in a ``TypeError``. Doing so is not
            recommended as they cannot be loaded in Python 3.

  See `issue 179 <https://github.com/zopefoundation/ZODB/pull/179>`_.

5.2.4 (2017-05-17)
==================

- ``DB.close`` now explicitly frees internal resources.  This is
  helpful to avoid false positives in tests that check for leaks.

- Optimize getting the path to a blob file. See
  `issue 161 <https://github.com/zopefoundation/ZODB/pull/161>`_.

- All classes are new-style classes on Python 2 (they were already
  new-style on Python 3). This improves performance on PyPy. See
  `issue 160 <https://github.com/zopefoundation/ZODB/pull/160>`_.

5.2.3 (2017-04-11)
==================

- Fix an import error. See `issue 158 <https://github.com/zopefoundation/ZODB/issues/158>`_.

5.2.2 (2017-04-11)
==================

- Fixed: A blob misfeature set blob permissions so that blobs and blob
  directories were only readable by the database process owner, rather
  than honoring user-controlled permissions (e.g. ``umask``).
  See `issue 155 <https://github.com/zopefoundation/ZODB/issues/155>`_.

5.2.1 (2017-04-08)
==================

- Fixed: When opening FileStorages in read-only mode, non-existent
  files were silently created.  Creating a read-only file-storage
  against a non-existent file errors.

5.2.0 (2017-02-09)
==================

- Call new afterCompletion API on storages to allow them to free
  resources after transaction complete.
  See `issue 147 <https://github.com/zodb/relstorage/issues/147>`__.
- Take advantage of the new transaction-manager explicit mode to avoid
  starting transactions unnecessarily when transactions end.

- ``Connection.new_oid`` delegates to its storage, not the DB. This is
  helpful for improving concurrency in MVCC storages like RelStorage.
  See `issue 139 <https://github.com/zopefoundation/ZODB/issues/139>`_.

- ``persistent`` is no longer required at setup time.
  See `issue 119 <https://github.com/zopefoundation/ZODB/issues/119>`_.

- ``Connection.close`` and ``Connection.open`` no longer race on
  ``self.transaction_manager``, which could lead to
  ``AttributeError``. This was a bug introduced in 5.0.1. See `issue
  142 <https://github.com/zopefoundation/ZODB/pull/143>`_.


5.1.1 (2016-11-18)
==================

- Fixed: ``ZODB.Connection.TransactionMetaData`` didn't support custom data
  storage that some storages rely on.

5.1.0 (2016-11-17)
==================

- ZODB now translates transaction meta data, ``user`` and
  ``description`` from text to bytes before passing them to storages,
  and converts them back to text when retrieving them from storages in
  the ``history``, ``undoLog`` and ``undoInfo`` methods.

  The ``IDatabase`` interface was updated to reflect that ``history``,
  ``undoLog`` and ``undoInfo`` are available on database objects.
  (They were always available, but not documented in the interface.)

5.0.1 (2016-11-17)
==================

- Fix an AttributeError that DemoStorage could raise if it was asked
  to store a blob into a temporary changes before reading a blob. See
  `issue 103 <https://github.com/zopefoundation/ZODB/issues/103>`_.

- Call _p_resolveConflict() even if a conflicting change doesn't change the
  state. This reverts to the behaviour of 3.10.3 and older.

- Closing a Connection now reverts its ``transaction_manager`` to
  None. This helps prevent errors and release resources when the
  ``transaction_manager`` was the (default) thread-local manager. See
  `issue 114 <https://github.com/zopefoundation/ZODB/issues/114>`_.

- Many docstrings have been improved.

5.0.0 (2016-09-06)
==================

Major internal improvements and cleanups plus:

- Added a connection ``prefetch`` method that can be used to request
  that a storage prefetch data an application will need::

    conn.prefetch(obj, ...)

  Where arguments can be objects, object ids, or iterables of objects
  or object ids.

  Added optional ``prefetch`` methods to the storage APIs. If a
  storage doesn't support prefetch, then the connection prefetch
  method is a noop.

- fstail: print the txn offset and header size, instead of only the data offset.
  fstail can now be used to truncate a DB at the right offset.

- Drop support for old commit protocol.  All of the build-in storages
  implement the new protocol.  This new protocol allows storages to
  provide better write performance by allowing multiple commits to
  execute in parallel.

5.0.0b1 (2016-08-04)
====================

- fstail: print the txn offset and header size, instead of only the data offset.
  fstail can now be used to truncate a DB at the right offset.

Numerous internal cleanups, including:

- Changed the way the root object was created.  Now the root object is
  created using a database connection, rather than by making low-level
  storage calls.

- Drop support for the old commit protocol.

- Internal FileStorage-undo fixes that should allow undo in some cases
  where it didn't work before.

- Drop the ``version`` argument to some methods where it was the last
  argument and optional.

5.0.0a6 (2016-07-21)
====================

- Added a connection ``prefetch`` method that can be used to request
  that a storage prefect data an application will need::

    conn.prefetch(obj, ...)

  Where arguments can be objects, object ids, or iterables of objects
  or object ids.

  Added optional ``prefetch`` methods to the storage APIs. If a
  storage doesn't support prefetch, then the connection prefetch
  method is a noop.

5.0.0a5 (2016-07-06)
====================

Drop support for old commit protocol.  All of the build-in storages
implement the new protocol.  This new protocol allows storages to
provide better write performance by allowing multiple commits to
execute in parallel.

5.0.0a4 (2016-07-05)
====================

See 4.4.2.

5.0.0a3 (2016-07-01)
====================

See 4.4.1.

5.0.0a2 (2016-07-01)
====================

See 4.4.0.

5.0.0a1 (2016-06-20)
====================

Major **internal** implementation changes to the Multi Version
Concurrency Control (MVCC) implementation:

- For storages that implement IMVCCStorage (RelStorage), no longer
  implement MVCC in ZODB.

- For other storages, MVCC is implemented using an additional storage
  layer. This underlying layer works by calling ``loadBefore``. The
  low-level storage ``load`` method isn't used any more.

  This change allows server-based storages like ZEO and NEO to be
  implemented more simply and cleanly.

4.4.3 (2016-08-04)
==================

- Internal FileStorage-undo fixes that should allow undo in some cases
  where it didn't work before.

- fstail: print the txn offset and header size, instead of only the data offset.
  fstail can now be used to truncate a DB at the right offset.

4.4.2 (2016-07-08)
==================

Better support of the new commit protocol. This fixes issues with blobs and
undo. See pull requests #77, #80, #83

4.4.1 (2016-07-01)
==================

Added IMultiCommitStorage to directly represent the changes in the 4.4.0
release and to make complient storages introspectable.

4.4.0 (2016-06-30)
==================

This release begins evolution to a more effcient commit protocol that
allows storage implementations, like `NEO <http://www.neoppod.org/>`_,
to support multiple transactions committing at the same time, for
greater write parallelism.

This release updates IStorage:

- The committed transaction's ID is returned by ``tpc_finish``, rather
  than being returned in response store and tpc_vote results.

- ``tpc_vote`` is now expected to return ``None`` or a list of object
  ids for objects for which conflicts were resolved.

This release works with storages that implemented the older version of
the storage interface, but also supports storages that implement the
updated interface.

4.3.1 (2016-06-06)
==================

- Fixed: FileStorage loadBefore didn't handle deleted/undone data correctly.

4.3.0 (2016-05-31)
==================

- Drop support for Python 2.6 and 3.2.

- Make the ``zodbpickle`` dependency required and not conditional.
  This fixes various packaging issues involving pip and its wheel
  cache. zodbpickle was only optional under Python 2.6 so this change
  only impacts users of that version.  See
  https://github.com/zopefoundation/ZODB/pull/42.

- Add support for Python 3.5.

- Avoid failure during cleanup of nested databases that provide MVCC
  on storage level (Relstorage).
  https://github.com/zopefoundation/ZODB/issues/45

- Remove useless dependency to `zdaemon` in setup.py. Remove ZEO documentation.
  Both were leftovers from the time where ZEO was part of this repository.

- Fix possible data corruption after FileStorage is truncated to roll back a
  transaction.
  https://github.com/zopefoundation/ZODB/pull/52

- DemoStorage: add support for conflict resolution and fix history()
  https://github.com/zopefoundation/ZODB/pull/58

- Fixed a test that depended on implementation-specific behavior in tpc_finish

4.2.0 (2015-06-02)
==================

- Declare conditional dependencies using PEP-426 environment markers
  (fixing interation between pip 7's wheel cache and tox).  See
  https://github.com/zopefoundation/ZODB/issues/36.

4.2.0b1 (2015-05-22)
====================

- Log failed conflict resolution attempts at ``DEBUG`` level.  See:
  https://github.com/zopefoundation/ZODB/pull/29.

- Fix command-line parsing of ``--verbose`` and ``--verify`` arguments.
  (The short versions, ``-v`` and ``-V``, were parsed correctly.)

- Add support for PyPy.

- Fix the methods in ``ZODB.serialize`` that find object references
  under Python 2.7 (used in scripts like ``referrers``, ``netspace``,
  and ``fsrecover`` among others). This requires the addition of the
  ``zodbpickle`` dependency.

- FileStorage: fix an edge case when disk space runs out while packing,
  do not leave the ``.pack`` file around. That would block any write to the
  to-be-packed ``Data.fs``, because the disk would stay at 0 bytes free.
  See https://github.com/zopefoundation/ZODB/pull/21.

4.1.0 (2015-01-11)
==================

- Fix registration of custom logging level names ("BLATHER", "TRACE").

  We have been registering them in the wrong order since 2004.  Before
  Python 3.4, the stdlib ``logging`` module masked the error by registering
  them in *both* directions.

- Add support for Python 3.4.

4.0.1 (2014-07-13)
==================

- Fix ``POSKeyError`` during ``transaction.commit`` when after
  ``savepoint.rollback``.  See
  https://github.com/zopefoundation/ZODB/issues/16

- Ensure that the pickler used in PyPy always has a ``persistent_id``
  attribute (``inst_persistent_id`` is not present on the pure-Python
  pickler). (PR #17)

- Provide better error reporting when trying to load an object on a
  closed connection.

4.0.0 (2013-08-18)
==================

Finally released.

4.0.0b3 (2013-06-11)
====================

- Switch to using non-backward-compatible pickles (protocol 3, without
  storing bytes as strings) under Python 3.  Updated the magic number
  for file-storage files under Python3 to indicate the incompatibility.

- Fixed: A ``UnicodeDecodeError`` could happen for non-ASCII OIDs
  when using bushy blob layout.

4.0.0b2 (2013-05-14)
====================

- Extended the filename renormalizer used for blob doctests to support
  the filenames used by ZEO in non-shared mode.

- Added ``url`` parameter to ``setup()`` (PyPI says it is required).

4.0.0b1 (2013-05-10)
=====================

- Skipped non-unit tests in ``setup.py test``.  Use the buildout to run tests
  requiring "layer" support.

- Included the filename in the exception message to support debugging in case
  ``loadBlob`` does not find the file.

- Added support for Python 3.2 / 3.3.

.. note::

   ZODB 4.0.x is supported on Python 3.x for *new* applications only.
   Due to changes in the standard library's pickle support, the Python3
   support does **not** provide forward- or backward-compatibility
   at the data level with Python2.  A future version of ZODB may add
   such support.

   Applications which need migrate data from Python2 to Python3 should
   plan to script this migration using separte databases, e.g. via a
   "dump-and-reload" approach, or by providing explicit fix-ups of the
   pickled values as transactions are copied between storages.


4.0.0a4 (2012-12-17)
=====================

- Enforced usage of bytes for ``_p_serial`` of persistent objects (fixes
  compatibility with recent persistent releases).

4.0.0a3 (2012-12-01)
=====================

- Fixed: An elaborate test for trvial logic corrupted module state in a
        way that made other tests fail spuriously.

4.0.0a2 (2012-11-13)
=====================

Bugs Fixed
----------

- An unneeded left-over setting in setup.py caused installation with
  pip to fail.

4.0.0a1 (2012-11-07)
=====================

New Features
------------

- The ``persistent`` and ``BTrees`` packages are now released as separate
  distributions, on which ZODB now depends.

- ZODB no longer depends on zope.event.  It now uses ZODB.event, which
  uses zope.event if it is installed.  You can override
  ZODB.event.notify to provide your own event handling, although
  zope.event is recommended.

- BTrees allowed object keys with insane comparison. (Comparison
  inherited from object, which compares based on in-process address.)
  Now BTrees raise TypeError if an attempt is made to save a key with
  comparison inherited from object. (This doesn't apply to old-style
  class instances.)

Bugs Fixed
----------

- Ensured that the export file and index file created by ``repozo`` share
  the same timestamp.

  https://bugs.launchpad.net/zodb/+bug/993350

- Pinned the ``transaction`` and ``manuel`` dependencies to Python 2.5-
  compatible versions when installing under Python 2.5.


.. note::
   Please see https://github.com/zopefoundation/ZODB/blob/master/HISTORY.rst
   for older versions of ZODB.
