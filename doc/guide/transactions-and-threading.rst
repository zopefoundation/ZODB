============================
Transactions and concurrency
============================

.. contents::

`Transactions <https://en.wikipedia.org/wiki/Database_transaction>`_
are a core feature of ZODB.  Much has been written about transactions,
and we won't go into much detail here.  Transactions provide two core
benefits:

Atomicity
  When a transaction executes, it succeeds or fails completely. If
  some data are updated and then an error occurs, causing the
  transaction to fail, the updates are rolled back automatically. The
  application using the transactional system doesn't have to undo
  partial changes.  This takes a significant burden from developers
  and increases the reliability of applications.

Concurrency
  Transactions provide a way of managing concurrent updates to data.
  Different programs operate on the data independently, without having
  to use low-level techniques to moderate their access. Coordination
  and synchronization happen via transactions.


.. _using-transactions-label:

Using transactions
==================

All activity in ZODB happens in the context of database connections
and transactions.  Here's a simple example::

  import ZODB, transaction
  db = ZODB.DB(None) # Use a mapping storage
  conn = db.open()

  conn.root.x = 1
  transaction.commit()

.. -> src

   >>> exec(src)

In the example above, we used ``transaction.commit()`` to commit a
transaction, making the change to ``conn.root`` permanent.  This is
the most common way to use ZODB, at least historically.

If we decide we don't want to commit a transaction, we can use
``abort``::

  conn.root.x = 2
  transaction.abort() # conn.root.x goes back to 1

.. -> src

   >>> exec(src)
   >>> conn.root.x
   1
   >>> conn.close()

In this example, because we aborted the transaction, the value of
``conn.root.x`` was rolled back to 1.

There are a number of things going on here that deserve some
explanation.  When using transactions, there are three kinds of
objects involved:

Transaction
   Transactions represent units of work.  Each transaction has a beginning and
   an end. Transactions provide the
   :interface:`~transaction.interfaces.ITransaction` interface.

Transaction manager
   Transaction managers create transactions and
   provide APIs to start and end transactions.  The transactions
   managed are always sequential. There is always exactly one active
   transaction associated with a transaction manager at any point in
   time. Transaction managers provide the
   :interface:`~transaction.interfaces.ITransactionManager` interface.

Data manager
   Data managers manage data associated with transactions.  ZODB
   connections are data managers.  The details of how they interact
   with transactions aren't important here.

Explicit transaction managers
-----------------------------

ZODB connections have transaction managers associated with them when
they're opened. When we call the database :meth:`~ZODB.DB.open` method
without an argument, a thread-local transaction manager is used. Each
thread has its own transaction manager.  When we called
``transaction.commit()`` above we were calling commit on the
thread-local transaction manager.

Because we used a thread-local transaction manager, all of the work in
the transaction needs to happen in the same thread.  Similarly, only
one transaction can be active in a thread.

If we want to run multiple simultaneous transactions in a single
thread, or if we want to spread the work of a transaction over
multiple threads [#bad-idea-using-multiple-threads-per-transaction]_,
then we can create transaction managers ourselves and pass them to
:meth:`~ZODB.DB.open`::

  my_transaction_manager = transaction.TransactionManager()
  conn = db.open(my_transaction_manager)
  conn.root.x = 2
  my_transaction_manager.commit()

.. -> src

   >>> exec(src)

In this example, to commit our work, we called ``commit()`` on the
transaction manager we created and passed to :meth:`~ZODB.DB.open`.

Context managers
----------------

In the examples above, the transaction beginnings were
implicit. Transactions were effectively
[#implicit-transaction-creation]_ created when the transaction
managers were created and when previous transactions were committed.
We can create transactions explicitly using
:meth:`~transaction.interfaces.ITransactionManager.begin`::

  my_transaction_manager.begin()

.. -> src

   >>> exec(src)

A more modern [#context-managers-are-new]_ way to manage transaction
boundaries is to use context managers and the Python ``with``
statement. Transaction managers are context managers, so we can use
them with the ``with`` statement directly::

  with my_transaction_manager as trans:
     trans.note(u"incrementing x")
     conn.root.x += 1

.. -> src

   >>> exec(src)
   >>> conn.root.x
   3


When used as a context manager, a transaction manager explicitly
begins a new transaction, executes the code block and commits the
transaction if there isn't an error and aborts it if there is an
error.

We used ``as trans`` above to get the transaction.

Databases provide the :meth:`~ZODB.DB.transaction` method to execute a code
block as a transaction::

  with db.transaction() as conn2:
     conn2.root.x += 1

.. -> src

   >>> exec(src)

This opens a connection, assignes it its own context manager, and
executes the nested code in a transaction.  We used ``as conn2`` to
get the connection.  The transaction boundaries are defined by the
``with`` statement.

Getting a connection's transaction manager
------------------------------------------

In the previous example, you may have wondered how one might get the
current transaction. Every connection has an associated transaction
manager, which is available as the ``transaction_manager`` attribute.
So, for example, if we wanted to set a transaction note::


  with db.transaction() as conn2:
     conn2.transaction_manager.get().note(u"incrementing x again")
     conn2.root.x += 1

.. -> src

   >>> exec(src)
   >>> (db.history(conn.root()._p_oid)[0]['description'] ==
   ...  u'incrementing x again')
   True

Here, we used the
:meth:`~transaction.interfaces.ITransactionManager.get` method to get
the current transaction.

Connection isolation
--------------------

In the last few examples, we used a connection opened using
:meth:`~ZODB.DB.transaction`.  This was distinct from and used a
different transaction manager than the original connection. If we
looked at the original connection, ``conn``, we'd see that it has the
same value for ``x`` that we set earlier:

  >>> conn.root.x
  3

This is because it's still in the same transaction that was begun when
a change was last committed against it.  If we want to see changes, we
have to begin a new transaction:

  >>> trans = my_transaction_manager.begin()
  >>> conn.root.x
  5

ZODB uses a timestamp-based commit protocol that provides `snapshot
isolation <https://en.wikipedia.org/wiki/Snapshot_isolation>`_.
Whenever we look at ZODB data, we see its state as of the time the
transaction began.

.. _conflicts-label:

Conflict errors
---------------

As mentioned in the previous section, each connection sees and
operates on a view of the database as of the transaction start time.
If two connections modify the same object at the same time, one of the
connections will get a conflict error when it tries to commit::

  with db.transaction() as conn2:
     conn2.root.x += 1

  conn.root.x = 9
  my_transaction_manager.commit() # will raise a conflict error

.. -> src

    >>> exec(src) # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    ZODB.POSException.ConflictError: ...

If we executed this code, we'd get a ``ConflictError`` exception on the
last line.  After a conflict error is raised, we'd need to abort the
transaction, or begin a new one, at which point we'd see the data as
written by the other connection:

    >>> my_transaction_manager.abort()
    >>> conn.root.x
    6

The timestamp-based approach used by ZODB is referred to as an
*optimistic* approach, because it works best if there are no
conflicts.

The best way to avoid conflicts is to design your application so that
multiple connections don't update the same object at the same time.
This isn't always easy.

Sometimes you may need to queue some operations that update shared
data structures, like indexes, so the updates can be made by a
dedicated thread or process, without making simultaneous updates.

Retrying transactions
~~~~~~~~~~~~~~~~~~~~~

The most common way to deal with conflict errors is to catch them and
retry transactions.  To do this manually involves code that looks
something like this::

  max_attempts = 3
  attempts = 0
  while True:
      try:
          with transaction.manager:
              ... code that updates a database
      except transaction.interfaces.TransientError:
          attempts += 1
          if attempts == max_attempts:
              raise
      else:
          break

In the example above, we used ``transaction.manager`` to refer to the
thread-local transaction manager, which we then used used with the
``with`` statement.  When a conflict error occurs, the transaction
must be aborted before retrying the update. Using the transaction
manager as a context manager in the ``with`` statement takes care of this
for us.

The example above is rather tedious.  There are a number of tools to
automate transaction retry.  The `transaction
<http://zodb.readthedocs.io/en/latest/transactions.html#retrying-transactions>`_
package provides a context-manager-based mechanism for retrying
transactions::

  for attempt in transaction.manager.attempts():
      with attempt:
          ... code that updates a database

Which is shorter and simpler [#but-obscure]_.

For Python web frameworks, there are WSGI [#wtf-wsgi]_ middle-ware
components, such as `repoze.tm2
<https://pypi.python.org/pypi/repoze.tm2>`_ that align transaction
boundaries with HTTP requests and retry transactions when there are
transient errors.

For applications like queue workers or `cron jobs
<https://en.wikipedia.org/wiki/Cron>`_, conflicts can sometimes be
allowed to fail, letting other queue workers or subsequent cron-job
runs retry the work.

Conflict resolution
~~~~~~~~~~~~~~~~~~~

ZODB provides a conflict-resolution framework for merging conflicting
changes.  When conflicts occur, conflict resolution is used, when
possible, to resolve the conflicts without raising a ConflictError to
the application.

Commonly used objects that implement conflict resolution are
buckets and ``Length`` objects provided by the `BTree
<https://pythonhosted.org/BTrees/>`_ package.

The main data structures provided by BTrees, BTrees and TreeSets,
spread their data over multiple objects.  The leaf-level objects,
called *buckets*, allow distinct keys to be updated without causing
conflicts [#usually-avoids-conflicts]_.

``Length`` objects are conflict-free counters that merge changes by
simply accumulating changes.

.. caution::
   Conflict resolution weakens consistency.  Resist the temptation to
   try to implement conflict resolution yourself.  In the future, ZODB
   will provide greater control over conflict resolution, including
   the option of disabling it.

   It's generally best to avoid conflicts in the first place, if possible.

ZODB and atomicity
==================

ZODB provides atomic transactions. When using ZODB, it's important to
align work with transactions.  Once a transaction is committed, it
can't be rolled back [#undo]_ automatically.  For applications, this
implies that work that should be atomic shouldn't be split over
multiple transactions.  This may seem somewhat obvious, but the rule
can be broken in non-obvious ways. For example a Web API that splits
logical operations over multiple web requests, as is often done in
`REST
<https://en.wikipedia.org/wiki/Representational_state_transfer>`_
APIs, violates this rule.

Partial transaction error recovery using savepoints
---------------------------------------------------

A transaction can be split into multiple steps that can be rolled back
individually.  This is done by creating savepoints.  Changes in a
savepoint can be rolled back without rolling back an entire
transaction::

  import ZODB
  db = ZODB.DB(None) # using a mapping storage
  with db.transaction() as conn:
      conn.root.x = 1
      conn.root.y = 0
      savepoint = conn.transaction_manager.savepoint()
      conn.root.y = 2
      savepoint.rollback()

  with db.transaction() as conn:
      print([conn.root.x, conn.root.y]) # prints 1 0

.. -> src

   >>> exec(src)
   [1, 0]

If we executed this code, it would print 1 and 0, because while the
initial changes were committed, the changes in the savepoint were
rolled back.

A secondary benefit of savepoints is that they save any changes made
before the savepoint to a file, so that memory of changed objects can
be freed if they aren't used later in the transaction.

Concurrency, threads and processes
==================================

ZODB supports concurrency through transactions.  Multiple programs
[#wtf-program]_ can operate independently in separate transactions.
They synchronize at transaction boundaries.

The most common way to run ZODB is with each program running in its
own thread.  Usually the thread-local transaction manager is used.

You can use multiple threads per transaction and you can run multiple
transactions in a single thread. To do this, you need to instantiate
and use your own transaction manager, as described in `Explicit
transaction managers`_.  To run multiple transaction managers
simultaneously in a thread, you need to use a separate transaction
manager for each transaction.

To spread a transaction over multiple threads, you need to keep in
mind that database connections, transaction managers and transactions
are **not thread-safe**.  You have to prevent simultaneous access from
multiple threads.  For this reason, **using multiple threads with a
single transaction is not recommended**, but it is possible with care.

Using multiple processes
------------------------

Using multiple Python processes is a good way to scale an application
horizontally, especially given Python's `global interpreter lock
<https://wiki.python.org/moin/GlobalInterpreterLock>`_.

Some things to keep in mind when utilizing multiple processes:

- If using the :mod:`multiprocessing` module, you can't
  [#cant-share-now]_ share databases or connections between
  processes. When you launch a subprocess, you'll need to
  re-instantiate your storage and database.

- You'll need to use a storage such as `ZEO
  <https://github.com/zopefoundation/ZEO>`_, `RelStorage
  <http://relstorage.readthedocs.io/en/latest/>`_, or `NEO
  <http://www.neoppod.org/>`_, that supports multiple processes.  None
  of the included storages do.

.. [#but-obscure] But also a bit obscure.  The Python context-manager
   mechanism isn't a great fit for the transaction-retry use case.

.. [#wtf-wsgi] `Web Server Gateway Interface
   <http://wsgi.readthedocs.io/en/latest/>`_

.. [#usually-avoids-conflicts] Conflicts can still occur when buckets
   split due to added objects causing them to exceed their maximum size.

.. [#undo] Transactions can't be rolled back, but they may be undone
   in some cases, especially if subsequent transactions
   haven't modified the same objects.

.. [#bad-idea-using-multiple-threads-per-transaction] While it's
   possible to spread transaction work over multiple threads, **it's
   not a good idea**. See `Concurrency, threads and processes`_

.. [#implicit-transaction-creation] Transactions are implicitly
   created when needed, such as when data are first modified.

.. [#context-managers-are-new] ZODB and the transaction package
   predate context managers and the Python ``with`` statement.

.. [#wtf-program] We're using *program* here in a fairly general
   sense, meaning some logic that we want to run to
   perform some function, as opposed to an operating system program.

.. [#cant-share-now] at least not now.
