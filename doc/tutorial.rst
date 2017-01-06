.. _tutorial-label:

========
Tutorial
========

This tutorial is intended to guide developers with a step-by-step introduction
of how to develop an application which stores its data in the ZODB.

Introduction
============

To save application data in ZODB, you'll generally define classes that
subclass ``persistent.Persistent``::

    # account.py

    import persistent

    class Account(persistent.Persistent):

        def __init__(self):
            self.balance = 0.0

        def deposit(self, amount):
            self.balance += amount

        def cash(self, amount):
            assert amount < self.balance
            self.balance -= amount

This code defines a simple class that holds the balance of a bank
account and provides two methods to manipulate the balance: deposit
and cash.

Subclassing ``Persistent`` provides a number of features:

- The database will automatically track object changes made by setting
  attributes [#changed]_.

- Data will be saved in its own database record.

  You can save data that doesn't subclass ``Persistent``, but it will be
  stored in the database record of whatever persistent object
  references it.

- Objects will have unique persistent identity.

  Multiple objects can refer to the same persistent object and they'll
  continue to refer to the same object even after being saved
  and loaded from the database.

  Non-persistent objects are essentially owned by their containing
  persistent object and if multiple persistent objects refer to the
  same non-persistent subobject, they'll (eventually) get their own
  copies.

Note that we put the class in a named module.  Classes aren't stored
in the ZODB [#persistentclasses]_.  They exist on the file system and
their names, consisting of their class and module names, are stored in
the database. It's sometimes tempting to create persistent classes in
scripts or in interactive sessions, but if you do, then their module
name will be ``'__main__'`` and you'll always have to define them that
way.

Installation
============

Before being able to use ZODB we have to install it. A common way to
do this is with pip::

    $ pip install ZODB

Creating Databases
==================

When a program wants to use the ZODB it has to establish a connection,
like any other database. For the ZODB we need 3 different parts: a
storage, a database and finally a connection::

    import ZODB, ZODB.FileStorage

    storage = ZODB.FileStorage.FileStorage('mydata.fs')
    db = ZODB.DB(storage)
    connection = db.open()
    root = connection.root

ZODB has a pluggable storage framework.  This means there are a
variety of storage implementations to meet different needs, from
in-memory databases, to databases stored in local files, to databases
on remote database servers, and specialized databases for compression,
encryption, and so on.  In the example above, we created a database
that stores its data in a local file, using the ``FileStorage``
class.

Having a storage, we then use it to instantiate a database, which we
then connect to by calling ``open()``.  A process with multiple
threads will often have multiple connections to the same database,
with different threads having different connections.

There are a number of convenient shortcuts you can use for some of the
commonly used storages:

- You can pass a file name to the ``DB`` constructor to have it construct
  a FileStorage for you::

    db = ZODB.DB('mydata.fs')

  You can pass None to create an in-memory database::

    memory_db = ZODB.DB(None)

- If you're only going to use one connection, you can call the
  ``connection`` function::

    connection = ZODB.connection('mydata.fs')
    memory_connection = ZODB.connection(None)

Storing objects
===============

To store an object in the ZODB we simply attach it to any other object
that already lives in the database. Hence, the root object functions
as a boot-strapping point.  The root object is meant to serve as a
namespace for top-level objects in your database.  We could store
account objects directly on the root object::

    import account

    # Probably a bad idea:
    root.account1 = account.Account()

But if you're going to store many objects, you'll want to use a
collection object [#root]_::

    import account, BTrees.OOBTree

    root.accounts = BTrees.OOBTree.BTree()
    root.accounts['account-1'] = Account()

Another common practice is to store a persistent object in the root of
the database that provides an application-specific root::

    root.accounts = AccountManagementApplication()

That can facilitate encapsulation of an application that shares a
database with other applications.  This is a little bit like using
modules to avoid namespace colisions in Python programs.

Containers and search
=====================

BTrees provide the core scalable containers and indexing facility for
ZODB. There are different families of BTrees.  The most general are
OOBTrees, which have object keys and values. There are specialized
BTrees that support integer keys and values.  Integers can be stored
more efficiently, and compared more quickly than objects and they're
often used as application-level object identifiers.  It's critical,
when using BTrees, to make sure that its keys have a stable ordering.

ZODB doesn't provide a query engine.  The primary way to access
objects in ZODB is by traversing (accessing attributes or items, or
calling methods) other objects.  Object traversal is typically much
faster than search.

You can use BTrees to build indexes for efficient search, when
necessary.  If your application is search centric, or if you prefer to
approach data access that way, then ZODB might not be the best
technology for you.

Transactions
============

You now have objects in your root object and in your database.
However, they are not permanently stored yet. The ZODB uses
transactions and to make your changes permanent, you have to commit
the transaction::

   import transaction

   transaction.commit()

Now you can stop and start your application and look at the root object again,
and you will find the data you saved.

If your application makes changes during a transaction and finds that it does
not want to commit those changes, then you can abort the transaction and have
the changes rolled back [#rollback]_ for you::

   transaction.abort()

Transactions are a very powerful way to protect the integrity of a
database.  Transactions have the property that all of the changes made
in a transaction are saved, or none of them are.  If in the midst of a
program, there's an error after making changes, you can simply abort
the transaction (or not commit it) and all of the intermediate changes
you make are automatically discarded.

Memory Management
=================

ZODB manages moving objects in and out of memory for you.  The unit of
storage is the persistent object.  When you access attributes of a
persistent object, they are loaded from the database automatically, if
necessary. If too many objects are in memory, then objects used least
recently are evicted [#eviction]_.  The maximum number of objects or
bytes in memory is configurable.

Summary
=======

You have seen how to install ZODB and how to open a database in your
application and to start storing objects in it. We also touched the
two simple transaction commands: ``commit`` and ``abort``. The
reference documentation contains sections with more information on the
individual topics.

.. [#changed] 
   You can manually mark an object as changed by setting its
   ``_p_changed__`` attribute to ``True``. You might do this if you
   update a subobject, such as a standard Python ``list`` or ``set``,
   that doesn't subclass ``Persistent``.

.. [#persistentclasses]
   Actually, there is semi-experimental support for storing classes in
   the database, but applications rarely do this.

.. [#root]
   The root object is a fairy simple persistent object that's stored
   in a single database record.  If you stored many objects in it,
   its database record would become very large, causing updates to be
   inefficient and causing memory to be used ineffeciently.

   Another reason not to store items directly in the root object is
   that doing so would make adding a second collection of objects
   later awkward.

.. [#rollback]
   A caveat is that ZODB can only roll back changes to objects that
   have been stored and committed to the database.  Objects not
   previously committed can't be rolled back because there's no
   previous state to roll back to.

.. [#eviction]
   Objects aren't actually evicted, but their state is released, so
   they take up much less memory and any objects they referenced can
   be removed from memory.
