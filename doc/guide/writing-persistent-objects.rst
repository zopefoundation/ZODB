============================
 Writing persistent objects
============================

In the :ref:`Tutorial <tutorial-label>`, we discussed the basics of
implementing persistent objects by subclassing
``persistent.Persistent``.  This is probably enough for 80% of
persistent-object classes you write, but there are some other aspects
of writing persistent classes you should be aware of.

Access and modification
=======================

Two of the main jobs of the ``Persistent`` base class are to detect
when an object has been accessed and when it has been modified.  When
an object is accessed, its state may need to be loaded from the
database.  When an object is modified, the modification needs to be
saved if a transaction is committed.

``Persistent`` detects object accesses by hooking into object
attribute access and update.  In the case of object update, there
may be other ways of modifying state that we need to make provision for.

Rules of persistence
====================

When implementing persistent objects, be aware that an object's
attributes should be :

- immutable (such as strings or integers),

- persistent (subclass Persistent), or

- You need to take special precautions.

If you modify a non-persistent mutable value of a persistent-object
attribute, you need to mark the persistent object as changed yourself
by setting ``_p_changed`` to True::

  import persistent

  class Book(persistent.Persistent):

     def __init__(self, title):
         self.title = title
         self.authors = []

     def add_author(self, author):
         self.authors.append(author)
         self._p_changed = True

.. -> src

   >>> exec(src)
   >>> db = ZODB.DB(None)
   >>> with db.transaction() as conn:
   ...     conn.root.book = Book("ZODB")
   >>> conn = db.open()
   >>> book = conn.root.book
   >>> bool(book._p_changed)
   False
   >>> book.authors.append('Jim')
   >>> bool(book._p_changed)
   False
   >>> book.add_author('Carlos')
   >>> bool(book._p_changed)
   True
   >>> db.close()

In this example, ``Book`` objects have an ``authors`` object that's a
regular Python list, so it's mutable and non-persistent.  When we add
an author, we append it to the ``authors`` attribute's value.  Because
we didn't set an attribute on the book, it's not marked as changed, so
we set ``_p_changed`` ourselves.

Using standard Python lists, dicts, or sets is a common thing to do,
so this pattern of setting ``_p_changed`` is common.

Let's look at some alternatives.

Using tuples for small sequences instead of lists
-------------------------------------------------

If objects contain sequences that are small or that don't change
often, you can use tuples instead of lists::

  import persistent

  class Book(persistent.Persistent):

     def __init__(self, title):
         self.title = title
         self.authors = ()

     def add_author(self, author):
         self.authors += (author, )

.. -> src

   >>> exec(src)
   >>> db = ZODB.DB(None)
   >>> with db.transaction() as conn:
   ...     conn.root.book = Book("ZODB")
   >>> conn = db.open()
   >>> book = conn.root.book
   >>> bool(book._p_changed)
   False
   >>> book.add_author('Carlos')
   >>> bool(book._p_changed)
   True
   >>> db.close()

Because tuples are immutable, they satisfy the rules of persistence
without any special handling.

Using persistent data structures
--------------------------------

The ``persistent`` package provides persistent versions of ``list``
and ``dict``, namely ``persistent.list.PersistentList`` and
``persistent.mapping.PersistentMapping``. We can update our example to
use ``PersistentList``::

  import persistent
  import persistent.list

  class Book(persistent.Persistent):

     def __init__(self, title):
         self.title = title
         self.authors = persistent.list.PersistentList()

     def add_author(self, author):
         self.authors.append(author)

.. -> src

   >>> exec(src)
   >>> db = ZODB.DB(None)
   >>> with db.transaction() as conn:
   ...     conn.root.book = Book("ZODB")
   >>> conn = db.open()
   >>> book = conn.root.book
   >>> bool(book._p_changed)
   False
   >>> book.add_author('Carlos')
   >>> bool(book._p_changed)
   False
   >>> bool(book.authors._p_changed)
   True
   >>> db.close()

Note that in this example, when we added an author, the book itself
didn't change, but the ``authors`` attribute value did.  Because
``authors`` is a persistent object, it's stored in a separate database
record from the book record and is managed by ZODB independent of the
management of the book.

In addition to ``PersistentList`` and ``PersistentMapping``, general
persistent data structures are provided by the BTrees_ package,
most notably ``BTree`` and ``TreeSet`` objects.  Unlike
``PersistentList`` and ``PersistentMapping``, ``BTree`` and
``TreeSet`` objects are scalable and can easily hold millions of
objects, because their data are spread over many subobjects.

It's generally better to use ``BTree`` objects than
``PersistentMapping`` objects, because they're scalable and because
they handle :ref:`conflicts <conflicts-label>` better. ``TreeSet``
objects are the only ZODB-provided persistent set implementation.
``BTree`` and ``TreeSets`` come in a number of families provided via
different modules and differ in their internal implementations:

===============  ===============  ================
Module           Key type         Value Type
===============  ===============  ================
BTrees.OOBTree   object           object
BTrees.IOBTree   integer          Object
BTrees.OIBTree   object           integer
BTrees.IIBTree   integer          integer
BTrees.IFBTree   integer          float
BTrees.LOBTree   64-bit integer   Object
BTrees.OLBTree   object           64-bit integer
BTrees.LLBTree   64-bit integer   64-bit integer
BTrees.LFBTree   64-bit integer   float
===============  ===============  ================

Here's a version of the example that uses a ``TreeSet``::

  import persistent
  from BTrees.OOBTree import TreeSet

  class Book(persistent.Persistent):

     def __init__(self, title):
         self.title = title
         self.authors = TreeSet()

     def add_author(self, author):
         self.authors.add(author)

.. -> src

   >>> exec(src)
   >>> db = ZODB.DB(None)
   >>> with db.transaction() as conn:
   ...     conn.root.book = Book("ZODB")
   >>> conn = db.open()
   >>> book = conn.root.book
   >>> bool(book._p_changed)
   False
   >>> book.add_author('Carlos')
   >>> bool(book._p_changed)
   False
   >>> bool(book.authors._p_changed)
   True
   >>> db.close()

If you're going to use custom classes as keys in a ``BTree`` or
entries in a ``TreeSet``, they must provide a `total ordering
<https://pythonhosted.org/BTrees/#total-ordering-and-persistence>`_.
The builtin python `str` class is always safe to use as BTree key. You
can use `zope.keyreference
<https://pypi.python.org/pypi/zope.keyreference>`_ to treat arbitrary
persistent objects as totally orderable based on their persistent
object identity.

Scalable sequences are a bit more challenging. The `zc.blist
<https://pypi.python.org/pypi/zc.blist/>`_ package provides a scalable
list implementation that works well for some sequence use cases.

Properties
==========

If you implement some attributes using Python properties (or other
types of descriptors), they are treated just like any other attributes
by the persistence machinery.  When you set an attribute through a
property, the object is considered changed, even if the property
didn't actually modify the object state.

Special attributes
==================

There are some attributes that are treated specially.

Attributes with names starting with ``_p_`` are reserved for use by
the persistence machinery and by ZODB.  These include (but aren't
limited to):

_p_changed
  The ``_p_changed`` attribute has the value ``None`` if the
  object is a :ref:`ghost <ghost-label>`, True if it's changed, and
  False if it's not a ghost and not changed.

_p_oid
  The object's unique id in the database.

_p_serial
  The object's revision identifier also know as the object serial
  number, also known as the object transaction id. It's a timestamp
  and if not set has the value 0 encoded as string of 8 zero bytes.

_p_jar
  The database connection the object was accessed through.  This is
  commonly used by database-aware application code to get hold of an
  object's database connection.

Attributes with names starting with ``_v_`` are treated as volatile.
They aren't saved to the database.  They're useful for caching data
that can be computed from saved data and shouldn't be saved [#cache]_.
They should be treated as though they can disappear between
transactions.  Setting a volatile attribute doesn't cause an object to
be considered to be modified.

An object's ``__dict__`` attribute is treated specially in that
getting it doesn't cause an object's state to be loaded.  It may have
the value ``None`` rather than a dictionary for :ref:`ghosts
<ghost-label>`.


Object storage and management
=============================

Every persistent object is stored in its own database record. Some
storages maintain multiple object revisions, in which case each
persistent object is stored in its own set of records.  Data for
different persistent objects are stored separately.

The database manages each object separately, according to a :ref:`life
cycle <object-life-cycle-label>`.

This is important when considering how to distribute data across your
objects.  If you use lots of small persistent objects, then more
objects may need to be loaded or saved and you may incur more memory
overhead. On the other hand, if objects are too big, you may load or
save more data than would otherwise be needed.

You can't change your mind in subclassing persistent
====================================================

Currently, you can't change your mind about whether a class is
persistent (subclasses ``persistent.Persistent``) or not.  If you save
objects in a database who's classes subclass ``persistent.Persistent``,
you can't change your mind later and make them non-persistent, and the
other way around.  This may be a `bug or misfeature
<https://github.com/zopefoundation/ZODB/issues/99>`_.

.. _schema-migration-label:

Schema migration
================

Object requirements and implementations tend to evolve over time.
This isn't a problem for objects that are short lived, but persistent
objects may have lifetimes that extend for years.  There needs to be
some way of making sure that state for an older object schema can
still be loaded into an object with the new schema.

Adding attributes
-----------------

Perhaps the commonest schema change is to add attributes.  This is
usually accomplished easily by adding a default value in a class
definition::

  class Book(persistent.Persistent):

     publisher = 'UNKNOWN'

     def __init__(self, title, publisher):
         self.title = title
         self.publisher = publisher
         self.authors = TreeSet()

     def add_author(self, author):
         self.authors.add(author)

Removing attributes
-------------------

Removing attributes generally doesn't require any action, assuming
that their presence in older objects doesn't do any harm.

Renaming/moving classes
-----------------------

The easiest way to handle renaming or moving classes is to leave
aliases for the old name.  For example, if we have a class,
``library.Book``, and want to move it to ``catalog.Publication``, we
can keep a ``library`` module that contains::

  from catalog import Publication as Book # XXX deprecated name

A downside of this approach is that it clutters code and may even
cause us to keep modules solely to hold aliases. (`zope.deferredimport
<http://zopedeferredimport.readthedocs.io/en/latest/narrative.html>`_
can help with this by making these aliases a little more efficient and
by generating deprecation warnings.)

Migration scripts
-----------------

If the simple approaches above aren't enough, then migration scripts
can be used.  How these scripts are written is usually application
dependent, as the application usually determines where objects of a
given type reside in the database. (There are also some low-level
interfaces for iterating over all of the objects of a database, but
these are usually impractical for large databases.)

An improvement to running migration scripts manually is to use a
generational framework like `zope.generations
<https://pypi.python.org/pypi/zope.generations>`_. With a generational
framework, each migration is assigned a migration number and the
number is recorded in the database as each migration is run.  This is
useful because remembering what migrations are needed is automated.

Upgrading multiple clients without down time
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Production applications typically have multiple clients for
availability and load balancing.  This means an active application may
be committing transactions using multiple software and schema
versions.  In this situation, you may need to plan schema migrations
in multiple steps:

#. Upgrade software on all clients to a version that works with the old and new
   version of the schema and that writes data using the old schema.

#. Upgrade software on all clients to a version that works with the old and new
   version of the schema and that writes data using the new schema.

#. Migrate objects written with the old schema to the new schema.

#. Remove support for the old schema from the software.

.. _object-life-cycle-label:

Object life cycle states and special attributes (advanced)
==========================================================

Persistent objects typically transition through a collection of
states. Most of the time, you don't need to think too much about this.

Unsaved
   When an object is created, it's said to be in an *unsaved* state
   until it's associated with a database.

Added
   When an unsaved object is added to a database, but hasn't been
   saved by committing a transaction, it's in the *added* state.

   Note that most objects are added implicitly by being set as
   subobjects (attribute values or items) of objects already in the
   database.

Saved
   When an object is added and saved through a transaction commit, the
   object is in the *saved* state.

Changed
   When a saved object is updated, it enters the *changed* state to
   indicate that there are changes that need to be committed. It
   remains in this state until either:

   - The current transaction is committed, and the object transitions to
     the saved state, or

   - The current transaction is aborted, and the object transitions to
     the ghost state.

.. _ghost-label:

Ghost
   An object in the *ghost* state is an empty shell. It has no
   state. When it's accessed, its state will be loaded automatically,
   and it will enter the saved state.  A saved object can become a
   ghost if it hasn't been accessed in a while and the database
   releases its state to make room for other objects.  A changed
   object can also become a ghost if the transaction it's modified in is
   aborted.

   An object that's loaded from the database is loaded as a
   ghost. This typically happens when the object is a subobject of
   another object who's state is loaded.

We can interrogate and control an object's state, although somewhat
indirectly.  To do this, we'll look at some special persistent-object
attributes, described in `Special attributes`_, above.

Let's look at some state transitions with an example. First, we create
an unsaved book::

    >>> book = Book("ZODB")
    >>> from ZODB.utils import z64
    >>> book._p_changed, bool(book._p_oid)
    (False, False)

We can tell that it's unsaved because it doesn't have an object id, ``_p_oid``.

If we add it to a database::

    >>> import ZODB
    >>> connection = ZODB.connection(None)
    >>> connection.add(book)
    >>> book._p_changed, bool(book._p_oid), book._p_serial == z64
    (False, True, True)

We know it's added because it has an oid, but its serial (object
revision timestamp), ``_p_serial``, is the special zero value, and it's
value for ``_p_changed`` is False.

If we commit the transaction that added it::

    >>> import transaction
    >>> transaction.commit()
    >>> book._p_changed, bool(book._p_oid), book._p_serial == z64
    (False, True, False)

We see that the object is in the saved state because it has an object
id and serial, and is unchanged.

Now if we modify the object, it enters the changed state:

    >>> book.title = "ZODB Explained"
    >>> book._p_changed, bool(book._p_oid), book._p_serial == z64
    (True, True, False)

If we abort the transaction, the object becomes a ghost:

    >>> transaction.abort()
    >>> book._p_changed, bool(book._p_oid)
    (None, True)

We can see it's a ghost because ``_p_changed`` is None.
(``_p_serial`` isn't meaningful for ghosts.)

If we access the object, it will be loaded into the saved state, which
is indicated by a false ``_p_changed`` and an object id and non-zero serial.

    >>> book.title
    'ZODB'
    >>> book._p_changed, bool(book._p_oid), book._p_serial == z64
    (False, True, False)

Note that accessing ``_p_`` attributes didn't cause the object's state
to be loaded.

We've already seen how modifying ``_p_changed`` can cause an object to
be marked as modified.  We can also use it to make an object into a
ghost:

    >>> book._p_changed = None
    >>> book._p_changed, bool(book._p_oid)
    (None, True)

Things you can do, but need to carefully consider (advanced)
============================================================

While you can do anything with a persistent subclass that you can with
a normal subclass, certain things have additional implications for
persistent objects. These often show up as performance issues, or the
result may become hard to maintain.

Implement ``__eq__`` and ``__hash__``
-------------------------------------

When you store an entry into a Python ``dict`` (or the persistent
variant ``PersistentMapping``, or a ``set`` or ``frozenset``), the
key's ``__eq__`` and ``__hash__`` methods are used to determine where
to store the value. Later they are used to look it up via ``in`` or
``__getitem__``.

When that ``dict`` is later loaded from the database, the internal
storage is rebuilt from scratch. This means that every key has its
``__hash__`` method called at least once, and may have its ``__eq__``
method called many times.

By default, every object, including persistent objects, inherits an
implementation of ``__eq__`` and ``__hash__`` from :class:`object`.
These default implementations are based on the object's *identity*,
that is, its unique identifier within the current Python process.
Calling them, therefore, is very fast, even on :ref:`ghosts
<ghost-label>`, and doesn't cause a ghost to load its state.

If you override ``__eq__`` and ``__hash__`` in a custom persistent
subclass, however, when you use instances of that class as a key
in a ``dict``, then the instance will have to be unghosted before it
can be put in the dictionary. If you're building a large dictionary
with many such keys that are ghosts, you may find that loading all the
object states takes a considerable amount of time. If you were to
store that dictionary in the database and load it later, *all* the
keys will have to be unghosted at the same time before the dictionary
can be accessed, again, possibly taking a long time.

For example, a class that defines ``__eq__`` and ``__hash__`` like this::

  class BookEq(persistent.Persistent):

     def __init__(self, title):
         self.title = title
         self.authors = ()

     def add_author(self, author):
         self.authors += (author, )

     def __eq__(self, other):
         return self.title == other.title and self.authors == other.authors

     def __hash__(self):
         return hash((self.title, self.authors))

.. -> src

   >>> exec(src)

is going to be much slower to use as a key in a persistent dictionary,
or in a new dictionary when the key is a ghost, than the class that
inherits identity-based ``__eq__`` and ``__hash__``.

.. Example of the above.

    Here's what that class would look like::

    >>> class Book(persistent.Persistent):
    ...    def __init__(self, title):
    ...        self.title = title
    ...        self.authors = ()
    ...
    ...    def add_author(self, author):
    ...        self.authors += (author, )

    Lets see an example of how these classes behave when stored in a
    dictionary. First, lets store some dictionaries::

    >>> import ZODB
    >>> db = ZODB.DB(None)
    >>> conn1 = db.open()
    >>> conn1.root.with_hashes = {BookEq(str(i)) for i in range(5000)}
    >>> conn1.root.with_ident =  {Book(str(i)) for i in range(5000)}
    >>> transaction.commit()

    Now, in a new connection (so we don't have any objects cached), lets
    load the dictionaries::

    >>> conn2 = db.open()
    >>> all((book._p_status == 'ghost' for book in conn2.root.with_ident))
    True
    >>> all((book._p_status == 'ghost' for book in conn2.root.with_hashes))
    False


   We can see that all the objects that did have a custom ``__eq__``
   and ``__hash__`` were loaded into memory, while those that did weren't.

There are some alternatives:

- Avoiding the use of persistent objects as keys in dictionaries or
  entries in sets sidesteps the issue.

- If your application can tolerate identity based comparisons, simply
  don't implement the two methods. This means that objects will be
  compared only by identity, but because persistent objects are
  persistent, the same object will have the same identity in each
  connection, so that often works out.

  It is safe to remove ``__eq__`` and ``__hash__`` methods from a
  class even if you already have dictionaries in a database using
  instances of those classes as keys.

- Make your classes `orderable
  <https://pythonhosted.org/BTrees/#total-ordering-and-persistence>`_
  and use them as keys in a BTree or entries in a TreeSet instead of a
  dictionary or set. Even though your custom comparison methods will
  have to unghost the objects, the nature of a BTree means that only a
  small number of objects will have to be loaded in most cases.

- Any persistent object can be wrapped in a ``zope.keyreferenece`` to
  make it orderable and hashable based on persistent identity. This
  can be an alternative for some dictionaries if you can't alter the
  class definition but can accept identity comparisons in some
  dictionaries or sets. You must remember to wrap all keys, though.


Implement ``__getstate__`` and ``__setstate__``
-----------------------------------------------

When an object is saved in a database, its ``__getstate__`` method is
called without arguments to get the object's state.  The default
implementation simply returns a copy of an object's instance
dictionary. (It's a little more complicated for objects with slots.)

An object's state is loaded by loading the state from the database and
passing it to the object's ``__setstate__`` method.  The default
implementation expects a dictionary, which it uses to populate the
object's instance dictionary.

Early on, we thought that overriding these methods would be useful for
tasks like providing more efficient state representations or for
:ref:`schema migration <schema-migration-label>`, but we found that
the result was to make object implementations brittle and/or complex
and the benefit usually wasn't worth it.

Implement ``__getattr__``, ``__getattribute__``, or ``__setattribute__``
------------------------------------------------------------------------

This is something extremely clever people might attempt, but it's
probably never worth the bother. It's possible, but it requires such
deep understanding of persistence and internals that we're not even
going to document it. :)


Links
=====

`persistent.Persistent
<http://persistent.readthedocs.io/en/latest/index.html>`_ provides
additional documentation on the ``Persistent`` base class.

The `zc.blist <https://pypi.python.org/pypi/zc.blist/>`_ package provides
a scalable sequence implementation for many use cases.

The `zope.cachedescriptors
<https://pypi.python.org/pypi/zope.cachedescriptors>`_ package
provides descriptor implementations that facilitate implementing
caching attributes, especially ``_v_`` volatile attributes.

The `zope.deferredimport
<http://zopedeferredimport.readthedocs.io/en/latest/narrative.html>`_
package provides lazy import and support for deprecating import
location, which is helpful when moving classes, especially persistent
classes.

The `zope.generations
<https://pypi.python.org/pypi/zope.generations>`_ package provides a
framework for managing schema-migration scripts.


.. [#cache] The `zope.cachedescriptors
   <https://pypi.python.org/pypi/zope.cachedescriptors>`_ package
   provides some descriptors that help implement attributes that cache
   data.

.. _BTrees: https://pythonhosted.org/BTrees/
