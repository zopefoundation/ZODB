=========
ZODB APIs
=========

.. contents::

ZODB module functions
=====================

.. method:: DB(storage, *args, **kw)

      Create a database. See :py:class:`ZODB.DB`.

.. autofunction:: ZODB.connection

Databases
=========

.. autoclass:: ZODB.DB
   :members: __init__, open, close, pack,
             cacheDetail, cacheExtremeDetail, cacheMinimize,
             cacheSize, cacheDetailSize, getCacheSize, getCacheSizeBytes,
             lastTransaction, getName, getPoolSize, getSize,
             getHistoricalCacheSize, getHistoricalCacheSizeBytes,
             getHistoricalPoolSize, getHistoricalTimeout,
             objectCount, connectionDebugInfo,
             setCacheSize, setCacheSizeBytes,
             setHistoricalCacheSize, setHistoricalCacheSizeBytes,
             setPoolSize, setHistoricalPoolSize, setHistoricalTimeout,
             history,
             supportsUndo, undoLog, undoInfo, undoMultiple, undo,
             transaction, storage

.. _database-text-configuration:

Database text configuration
---------------------------

Databases are configured with ``zodb`` sections::

  <zodb>
    cache-size-bytes 100MB
    <mappingstorage>
    </mappingstorage>
  </zodb>

A ``zodb`` section must have a storage sub-section specifying a
storage and any of the following options:

.. zconfigsectionkeys:: ZODB component.xml zodb

.. _multidatabase-text-configuration:

For a multi-database configuration, use multiple ``zodb`` sections and
give the sections names::

  <zodb first>
    cache-size-bytes 100MB
    <mappingstorage>
    </mappingstorage>
  </zodb>

  <zodb second>
    <mappingstorage>
    </mappingstorage>
  </zodb>

.. -> src

   >>> import ZODB.config
   >>> db = ZODB.config.databaseFromString(src)
   >>> sorted(db.databases)
   ['first', 'second']
   >>> db._cache_size_bytes
   104857600

When the configuration is loaded, a single database will be returned,
but all of the databases will be available through the returned
database's ``databases`` attribute.

Connections
===========

.. autoclass:: ZODB.Connection.Connection
   :members: add, cacheGC, cacheMinimize, close, db, get,
             getDebugInfo, get_connection, isReadOnly, oldstate,
             onCloseCallback, root, setDebugInfo, sync,
             transaction_manager

TimeStamp (transaction ids)
===========================

.. class:: ZODB.TimeStamp.TimeStamp(year, month, day, hour, minute, seconds)

   Create a time-stamp object. Time stamps facilitate the computation
   of transaction ids, which are based on times. The arguments are
   integers, except for seconds, which may be a floating-point
   number. Time stamps have microsecond precision. Time stamps are
   implicitly UTC based.

   Time stamps are orderable and hashable.

   .. method:: day()

      Return the time stamp's day.

   .. method:: hour()

      Return the time stamp's hour.

   .. method:: laterThan(other)

      Return a timestamp instance which is later than 'other'.

      If self already qualifies, return self.

      Otherwise, return a new instance one moment later than 'other'.

   .. method:: minute()

      Return the time stamp's minute.

   .. method:: month()

      Return the time stamp's month.

   .. method:: raw()

      Get an 8-byte representation of the time stamp for use in APIs
      that require a time stamp.

   .. method:: second()

      Return the time stamp's second.

   .. method:: timeTime()

      Return the time stamp as seconds since the epoc, as used by the
      ``time`` module.

   .. method:: year()

      Return the time stamp's year.

Loading configuration
=====================

.. automodule:: ZODB.config
   :members: databaseFromString, databaseFromFile, databaseFromURL,
             storageFromString, storageFromFile, storageFromURL
