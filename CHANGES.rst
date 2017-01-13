================
 Change History
================

4.4.5 (unreleased)
==================

- ``persistent`` is no longer required at setup time.
  See `issue 119 <https://github.com/zopefoundation/ZODB/issues/119>`_.

- ``Connection.close`` and ``Connection.open`` no longer race on
  ``self.transaction_manager``, which could lead to
  ``AttributeError``. This was a bug introduced in 5.0.1. See `issue
  142 <https://github.com/zopefoundation/ZODB/pull/143>`_.

- Fix deadlock with storages that "sync" on a new transaction.
  https://github.com/zopefoundation/ZODB/pull/136

4.4.4 (2016-11-27)
==================

- Fixed to work with transaction 2.0.3.

- Call _p_resolveConflict() even if a conflicting change doesn't change the
  state. This reverts to the behaviour of 3.10.3 and older.

- Closing a Connection now reverts its ``transaction_manager`` to
  None. This helps prevent errors and release resources when the
  ``transaction_manager`` was the (default) thread-local manager. See
  `issue 114 <https://github.com/zopefoundation/ZODB/issues/114>`_.

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
   Please see ``doc/HISTORY.txt`` for changelog entries for older versions
   of ZODB.
