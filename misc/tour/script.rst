A tour of ZODB, a transactional object-oriented database
========================================================

<first slide>

In this web-cast, I'm going give a tour of ZODB to help you decide
whether you want to learn more.

ZODB is a general-purpose transactional database designed to make
working with persistent data as easy and transparent as practical.

ZODB has a number of interesting features, which we'll review in a
bit, but first, we'll look at working with ZODB by looking at a small
Web application.

<show board>

The application we'll use as an example is a two-tiered Kanban board.

The application supports a number of sites, which contain a number of
Kanban boards.  The site used is determined by the domain used to
access the application.  Boards are user-selectable.  An organization
using the application might have a board for each development team.

Each board is used to manage features, which are units of value.  A
feature has a collection of development tasks.

When a feature is dragged to a development state, a nested Kanban
board is displayed to support management of development tasks.

<drag a feature to development>

Features and tasks have data.

<edit a task>

When features are completed, they can be dragged to the bag, for
archival.  We can search the bag for old features.

<drag to bag, open bag, search>

To get a feel for working with objects, we'll access the database in
interactive mode.

<start a python interpreter>

The first step in accessing a database is getting a database
connection. ZODB has a pluggable storage architecture, and the details
of getting a connection vary, depending on the storage approach used.

This application uses newt.db, which is a thin wrapper around ZODB
that provides Postgres-based storage and also allows data to be
indexed and searched with PostgreSQL.

::
   import newt.db

We're using a Postgres database and we create a connection using a
Postgres connection string for our Postgres database.

::
   url = '...'
   conn = newt.db.connection(url)

To access data we start with a database root and walk objects from the
root.  If we print the root object, we'll see that it has a sites
attribute.

<print root>

The root object is usually small, and is used as an anchor
for application-specific objects.

The sites object is a mapping object that contains sites by domain.

::

   list(conn.root.sites)

Let's look at one of the sites:

::
   site = conn.root.sites['dogfood.valuenator.com']
   site.title

The `Site` class is pretty simple.

https://github.com/feature-flow/twotieredkanban/blob/90018a656f2200ef9ed1ef886b5c9bb53bfdbfe3/server/twotieredkanban/site.py#L26

The `Site` class subclasses `persistent.Persistent`.  User-defined classes
stored in ZODB typically subclass Persistent.  Doing so causes them to
have their own database records and provides logic for automatically
loading objects when accessed and saving objects when they change.

Site objects have a title and a collection of Kanban boards.

::
   list(site.boards)

They also have a changes object that tracks site changes.  This is
part of a framework for providing real-time user interfaces.  While
that framework is interesting, we won't delve into it here.

Let's look at a board:

::
   board = site.board['Valuenator']
   board.name
   [state.title for state in board.states]
   [task.title for task in board.tasks]


https://github.com/feature-flow/twotieredkanban/blob/90018a656f2200ef9ed1ef886b5c9bb53bfdbfe3/server/twotieredkanban/board.py#L15

Note that we're accessing data by just accessing objects. No queries.

Let's add a feature.

::
   f = board.new_feature('Demo feature', 0, 'This is a demo feature')
   f.title
   import transaction
   transaction.commit()

If we look at the board, we'll see the feature.

<look at board>

We were able to see the feature without reloading the board thanks to
the real-time user-interface framework mentioned earlier.

To update the board we had to commit a transaction. ZODB is
transactional.  Nothing is saved until a transaction is
committed.

Let's change the feature title

::

   f.title = 'Awesome demo feature'
   f.title

If we decide this is a bad change, or if some other change fails, we
can choose to abort the current transaction.

::
   transaction.abort()
   f.title

When we abort a transaction, the data are returned to the state they
were in at the start of the transaction. This feature of transactions
is called "atomicity".  It is a wildly important feature of
transactional databases.

Without atomicity, recovering from errors is extremely difficult,
because it's up to the application to keep track of changes and unwind
them.  I can't emphasize the importance of this enough.

In the examples above, we defined transaction boundaries
manually. Lot's of database applications are request oriented, meaning
the application does work in discrete units. For these applications,
the application framework will often manage transactions. For example,
let's look at the REST API method for updating a task.

https://github.com/feature-flow/twotieredkanban/blob/90018a656f2200ef9ed1ef886b5c9bb53bfdbfe3/server/twotieredkanban/apiboard.py#L46

The details of the web framework used aren't important. We have a REST
endpoint that updates tasks given a task id.  Note that internally,
both features and tasks are just tasks. This allows features to be
demoted to tasks and tasks to be promoted to features.

The REST method is just a thin wrapper that translates from the WEB
framework to the application framework. It calls an application method
for updating the task.

https://github.com/feature-flow/twotieredkanban/blob/90018a656f2200ef9ed1ef886b5c9bb53bfdbfe3/server/twotieredkanban/board.py#L116

Note that the changed call is an application-level operation needed by
the real-time user interface framework. It has nothing to do with ZODB
change management.

The important thing to note about this example is that we haven't seen
any obvious database logic. The code could just be operating on
non-persistent objects in memory.

The web framework has been configured to begin a transaction at the
start of a request, abort the transaction if an exception is raised
and commit if there isn't an error.

So far, we haven't seen obvious database queries.  We've simply
traversed objects.  When we accessed a board by name, that was a kind
of a query.  The collections of boards and sites, as well as some
other collections used in the application use BTrees rather than
ordinary dictionaries.

::
   site.boards

BTrees are very efficient for working with mapping objects with
ordered keys.  They can spread data over multiple database records, so
very large collections can be handled without loading the entire
collection into memory to access a subset of keys.

Search
======

This application doesn't need search beyond mapping access except in
an important case, which is searching the Bag.

In ZODB, search is viewed as something to be provided at the
application level.

In this application, we leverage PostgreSQL for search.  Newt DB
replicates object data to JSONB column and the application uses a
Postgres full-text index to support task search in the bag.

More commonly, "catalog" objects are used. These provide
application-level search engines.  There are a variety of these to
select from, which can be a bit confusing.  They typically require the
use of event frameworks to cause indexes to be updated when objects
change.

Because of ZODB's powerful caching, catalogs are often faster than
using external indexes, like Postgres or ElasticSearch.

Now that you've seen ZODB in action, let's review some of it's notable
features.



