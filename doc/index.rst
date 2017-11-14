==========================================
ZODB - a native object database for Python
==========================================

Because ZODB is an object database:

- no separate language for database operations

- very little impact on your code to make objects persistent

- no database mapper that partially hides the database.

  Using an object-relational mapping **is not** like using an object database.

- almost no seam between code and database.

- Relationships between objects are handled very naturally, supporting
  complex object graphs without joins.

Check out the :doc:`tutorial`!

ZODB runs on Python 2.7 or Python 3.4 and above. It also runs on PyPy.

Transactions
============

Transactions make programs easier to reason about.

Transactions are atomic
  Changes made in a transaction are either saved in their entirety or
  not at all.

  This makes error handling a lot easier.  If you have an error, you
  just abort the current transaction. You don't have to worry about
  undoing previous database changes.

Transactions provide isolation
  Transactions allow multiple logical threads (threads or processes)
  to access databases and the database prevents the threads from
  making conflicting changes.

  This allows you to scale your application across multiple threads,
  processes or machines without having to use low-level locking
  primitives.

  You still have to deal with concurrency on some level. For
  timestamp-based systems like ZODB, you may have to retry conflicting
  transactions. With locking-based systems, you have to deal with
  possible deadlocks.

Transactions affect multiple objects
  Most NoSQL databases don't have transactions. Their notions of
  consistency are much weaker, typically applying to single documents.
  There can be good reasons to use NoSQL databases for their extreme
  scalability, but otherwise, think hard about giving up the benefits
  of transactions.

ZODB transaction support:

- `ACID <https://en.wikipedia.org/wiki/ACID>`_ transactions with
  `snapshot isolation
  <https://en.wikipedia.org/wiki/Snapshot_isolation>`_

- Distributed transaction support using two-phase commit

  This allows transactions to span multiple ZODB databases and to span
  ZODB and non-ZODB databases.

Other notable ZODB features
===========================

Database caching with invalidation
  Every database connection has a cache that is a consistent partial database
  replica. When accessing database objects, data already in the cache
  is accessed without any database interactions.  When data are
  modified, invalidations are sent to clients causing cached objects
  to be invalidated. The next time invalidated objects are accessed
  they'll be loaded from the database.

  Applications don't have to invalidate cache entries. The database
  invalidates cache entries automatically.

Pluggable layered storage
  ZODB has a pluggable storage architecture. This allows a variety of
  storage schemes including memory-based, file-based and distributed
  (client-server) storage.  Through storage layering, storage
  components provide compression, encryption, replication and more.

Easy testing
  Because application code rarely has database logic, it can
  usually be unit tested without a database.

  ZODB provides in-memory storage implementations as well as
  copy-on-write layered "demo storage" implementations that make testing
  database-related code very easy.

Garbage collection
  Removal of unused objects is automatic, so application developers
  don't have to worry about referential integrity.

Binary large objects, Blobs
  ZODB blobs are database-managed files.  This can be especially
  useful when serving media.  If you use AWS, there's a Blob
  implementation that stores blobs in S3 and caches them on disk.

Time travel
  ZODB storages typically add new records on write and remove old
  records on "pack" operations.  This allows limited time travel, back
  to the last pack time.  This can be very useful for forensic
  analysis.

When should you use ZODB?
=========================

You want to focus on your application without writing a lot of database code.
  ZODB provides highly transparent persistence.

Your application has complex relationships and data structures.
  In relational databases you have to join tables to model complex
  data structures and these joins can be tedious and expensive.  You
  can mitigate this to some extent in databases like Postgres by using
  more powerful data types like arrays and JSON columns, but when
  relationships extend across rows, you still have to do joins.

  In NoSQL databases, you can model complex data structures with
  documents, but if you have relationships across documents, then you
  have to do joins and join capabilities in NoSQL databases are
  typically far less powerful and transactional semantics typically don't
  cross documents, if they exist at all.

  In ZODB, you can make objects as complex as you want and cross
  object relationships are handled with Python object references.

You access data through object attributes and methods.
  If your primary object access is search, then other database
  technologies might be a better fit.

  ZODB has no query language other than Python. It's primary support
  for search is through mapping objects called BTrees.  People have
  build higher-level search APIs on top of ZODB. These work well
  enough to support some search.

You read data a lot more than you write it.
  ZODB caches aggressively, and if you're working set fits (or mostly
  fits) in memory, performance is very good because it rarely has to
  touch the database server.

  If your application is very write heavy (e.g. logging), then you're
  better off using something else.  Sometimes, you can use a database
  suitable for heavy writes in combination with ZODB.

Need to test logic that uses your database.
  ZODB has a number of storage implementations, including layered
  in-memory implementations that make testing very easy.

  A database without an in-memory storage option can make testing very
  complicated.

When should you *not* use ZODB?
===============================

- You have very high write volume.

  ZODB can commit thousands of transactions per second with suitable
  storage configuration and without conflicting changes.

  Internal search indexes can lead to lots of conflicts, and can
  therefore limit write capacity.  If you need high write volume and
  search beyond mapping access, consider using external indexes.

- You need to use non-Python tools to access your database.

  especially tools designed to work with relational databases

Newt DB addresses these issues to a significant degree. See
http://newtdb.org.

How does ZODB scale?
====================

Not as well as many technologies, but some fairly large applications
have been built on ZODB.

At Zope Corporation, several hundred newspaper content-management
systems and web sites were hosted using a multi-database configuration
with most data in a main database and a catalog database.  The
databases had several hundred gigabytes of ordinary database records
plus multiple terabytes of blob data.

ZODB is mature
==============

ZODB is very mature. Development started in 1996 and it has been used
in production in thousands of applications for many years.

ZODB is in heavy use in the `Pyramid <http://www.pylonsproject.org/>`_
and `Plone <https://plone.org/>`_ communities and in many other
applications.

Learning more
=============

.. toctree::
   :maxdepth: 1

   tutorial
   guide/index
   reference/index
   articles/index

* `The ZODB Book (in progress) <http://zodb.readthedocs.org/en/latest/>`_

Downloads
=========

ZODB is distributed through the `Python Package Index
<http://pypi.python.org/pypi/ZODB>`_.

You can install the ZODB using pip command::

    $ pip install ZODB

Community and contributing
==========================

Discussion occurs on the `ZODB mailing list
<https://groups.google.com/forum/#!forum/zodb>`_. (And for the
transaction system on the `transaction list
<https://groups.google.com/forum/#!forum/python-transaction>`_)

Bug reporting and feature requests are submitted through github issue
trackers for various ZODB components:

- `ZODB <https://github.com/zopefoundation/zodb>`_

- `persistent <https://github.com/zopefoundation/persistent>`_

- `transaction <https://github.com/zopefoundation/transaction>`_

- `BTrees <https://github.com/zopefoundation/BTrees>`_

- `ZEO (client-server framework) <https://github.com/zopefoundation/ZEO>`_

If you'd like to contribute then we'll gladly accept work on documentation,
helping out other developers and users at the mailing list, submitting bugs,
creating proposals and writing code.

ZODB is a project managed by the Zope Foundation so you can get write access
for contributing directly - check out the foundation's `Zope Developer Information <http://docs.zope.org/developer>`_.
