================
 Change History
================

4.1.1 (unreleased)
==================

- Fix #21, FileStorage: an edge case when disk space runs out while packing,
  do not leave the ``.pack`` file around. That would block any write to the
  to-be-packed ``Data.fs``, because the disk would stay at 0 bytes free.


4.1.0 (2015-01-11)
==================

- Fix registration of custom logging level names ("BLATHER", "TRACE).

  We have been registering them in the wrong order since 2004.  Before
  Python 3.4, the stdlib ``logging`` module masked the error by registering
  them in *both* directions.

- Add support for Python 3.4.

4.0.1 (2014-07-13)
==================

- Fix POSKeyError during transaction.commit when after savepoint.rollback.
  see https://github.com/zopefoundation/ZODB/issues/16

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
