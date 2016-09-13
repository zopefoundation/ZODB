=============
Storage APIs
=============

.. contents::

Storage interfaces
==================

There are various storage implementations that implement standard
storage interfaces. They differ primarily in their constructors.

Application code rarely calls storage methods, and those it calls are
generally called indirectly through databases.  There are
interface-defined methods that are called internally by ZODB. These
aren't shown below.


IStorage
--------

.. autointerface:: ZODB.interfaces.IStorage
   :members: close, getName, getSize, history, isReadOnly, lastTransaction,
             __len__, pack, sortKey

IStorageIteration
-----------------

.. autointerface:: ZODB.interfaces.IStorageIteration

IStorageUndoable
----------------

.. autointerface:: ZODB.interfaces.IStorageUndoable
   :members: undoLog, undoInfo

IStorageCurrentRecordIteration
------------------------------

.. autointerface:: ZODB.interfaces.IStorageCurrentRecordIteration

IBlobStorage
------------

.. autointerface:: ZODB.interfaces.IBlobStorage
   :members: temporaryDirectory

IStorageRecordInformation
-------------------------

.. autointerface:: ZODB.interfaces.IStorageRecordInformation

IStorageTransactionInformation
------------------------------

.. autointerface:: ZODB.interfaces.IStorageTransactionInformation

.. _included-storages-label:

Included storages
=================

FileStorage
-----------


.. autoclass:: ZODB.FileStorage.FileStorage.FileStorage
   :members: __init__


.. autointerface:: ZODB.FileStorage.interfaces.IFileStoragePacker

FileStorage text configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

File storages are configured using the ``filestorage`` section::

  <filestorage>
    path Data.fs
  </filestorage>

which accepts the following options:

.. zconfigsectionkeys:: ZODB component.xml filestorage

MappingStorage
--------------

.. autoclass:: ZODB.MappingStorage.MappingStorage
   :members: __init__

.. _mappingstorage-text-configuration:

MappingStorage text configuration
---------------------------------

File storages are configured using the ``mappingstorage`` section::

  <mappingstorage>
  </mappingstorage>

Options:

.. zconfigsectionkeys:: ZODB component.xml mappingstorage

DemoStorage
-----------

.. autoclass:: ZODB.DemoStorage.DemoStorage
   :members: __init__, push, pop

DemoStorage text configuration
------------------------------

Demo storages are configured using the ``demostorage`` section::

  <demostorage>
    <filestorage base>
      path base.fs
    </filestorage>
    <mappingstorage changes>
      name Changes
    </mappingstorage>
  </demostorage>

.. -> src

   >>> import ZODB.config
   >>> storage = ZODB.config.storageFromString(src)
   >>> storage.base.getName()
   'base.fs'
   >>> storage.changes.getName()
   'Changes'

``demostorage`` sections can contain up to 2 storage subsections,
named ``base`` and ``changes``, specifying the demo storage's base and
changes storages.  See :meth:`ZODB.DemoStorage.DemoStorage.__init__`
for more on the base and changes storages.

Options:

.. zconfigsectionkeys:: ZODB component.xml demostorage

Noteworthy non-included storages
================================

A number of important ZODB storages are distributed separately.

Base storages
-------------

Unlike the included storages, all the implementations listed in this section
allow multiple processes to share the same database.

NEO
  `NEO <https://lab.nexedi.com/nexedi/neoppod>`_ can spread data among several
  computers for load-balancing and multi-master replication. It also supports
  asynchronous replication to off-site NEO databases for further disaster
  resistance without affecting local operation latency.

  For more information, see https://lab.nexedi.com/nexedi/neoppod.

RelStorage
  `RelStorage <http://relstorage.readthedocs.io/en/latest/>`_
  stores data in relational databases.  This is especially
  useful when you have requirements or existing infrastructure for
  storing data in relational databases.

  For more information, see http://relstorage.readthedocs.io/en/latest/.

ZEO
  `ZEO <https://github.com/zopefoundation/ZEO>`_ is a client-server
  database implementation for ZODB.  To use ZEO, you run a ZEO server,
  and use ZEO clients in your application.

  For more information, see https://github.com/zopefoundation/ZEO.

Optional layers
---------------

ZRS
  `ZRS <https://github.com/zc/zrs>`_
  provides replication from one database to another.  It's most
  commonly used with ZEO.  With ZRS, you create a ZRS primary database
  around a :class:`~ZODB.FileStorage.FileStorage.FileStorage` and in a
  separate process, you create a ZRS secondary storage around any
  :interface:`storage <ZODB.interfaces.IStorage>`. As transactions are
  committed on the primary, they're copied asynchronously to
  secondaries.

  For more information, see https://github.com/zc/zrs.

zlibstorage
  `zlibstorage <https://pypi.python.org/pypi/zc.zlibstorage>`_
  compresses database records using the compression
  algorithm used by `gzip <http://www.gzip.org/>`_.

  For more information, see https://pypi.python.org/pypi/zc.zlibstorage.

beforestorage
  `beforestorage <https://pypi.python.org/pypi/zc.beforestorage>`_
  provides a point-in-time view of a database that might
  be changing.  This can be useful to provide a non-changing view of a
  production database for use with a :class:`~ZODB.DemoStorage.DemoStorage`.

  For more information, see https://pypi.python.org/pypi/zc.beforestorage.

cipher.encryptingstorage
  `cipher.encryptingstorage
  <https://pypi.python.org/pypi/cipher.encryptingstorage/>`_ provided
  compression and encryption of database records.

  For more information, see
  https://pypi.python.org/pypi/cipher.encryptingstorage/.
