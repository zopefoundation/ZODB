:css: presentation.css
:skip-help: true

.. title: A tour of ZODB, a transactional object-oriented database

----

A tour of ZODB, a transactional object-oriented database
========================================================

ZODB was designed to provide a **very easy to use programming** model by
minimizing the impedance mismatch between programming and databases.

To learn more, visit http://zodb.org

----

Transactions
============

Transactions are wildly important for maintaining data integrity.

- Atomicity

- Consistency

- Isolation

- Durability

Transactional semantics shouldn't be sacrificed lightly (if at all).

----

Caching
=======

*There are only two hard things in Computer Science: cache invalidation
and naming things.*  -- Phil Karlton

ZODB object cache:

- In memory object graph

- Transactionally-consistent partial database replica.

----

ZODB orders transactions in time
================================

- Facilitates replication from a point in time.

- Facilitates caching, because ZODB's caching is based on replication.

- Facilitates updating external indexes.

- Allows time travel, depending on which storage components are used.

(There is a write performance cost.)

----

Pluggable storages
==================

ZODB includes a pluggable-storage framework.

- Variety of storage options available (file, in memory,
  client-server, RDBMS)

- Layering (compression, encryption, testing)

----

Testing support
===============

- First, Application logic has little or no database code, so most
  unit tests require no database.

- In-memory storage

- Demo storage combines a static base with writable changes, and can
  be layered to multiple levels.

- Tests can set up common (in-memory) base database.

- Individual tests can use in-memory demo storage that builds on
  common base.

----

Database-managed files
======================

ZODB supports Blobs for large opaque binary data.

- Database-managed files.

  Blobs are literally files.

- Copy-on-write semantics.

- Can be used with network file systems or in a sever mode with a
  local cache.

----

Asynchronous garbage collection
===============================

- Referential integrity

- Integrity constraints

Applications managing referential integrity is a bit like saddling
applications with memory management.

ZODB provides asynchronous tracing garbage collection.

----

Distributed transaction support
===============================

ZODB uses the transaction package, which provides
distributed-transaction support.

ZODB can be integrated with other databases through distributed
transactions.

----

Schemaless?
===========

There's always a schema.

- Python classes

- Informal

- Database independent.

- Migration patterns.


----

NoSQL databases
===============

- Emphasize speed

- Typically Sacrifice transactions

- Mitigate by supporting complex schema (documents).

  - No need for joins

  - Single-update

- Much much weaker notion of consistency (BASE vs ACID).

  The word "consistent" has wildly different meaning.

----

Speed
=====

ZODB is optimized for reads. Cached reads are simply memory accesses
and are extremely fast.

ZODB is slower at writing that many relational databases, which are
typically slower than some NoSQL databases.

But ZODB can still write thousands of transactions per second.

