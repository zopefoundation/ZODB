=========================
Cross-Database References
=========================

Persistent references to objects in different databases within a
multi-database are allowed.

Lets set up a multi-database with 2 databases:

    >>> import ZODB.tests.util, transaction, persistent
    >>> databases = {}
    >>> db1 = ZODB.tests.util.DB(databases=databases, database_name='1')
    >>> db2 = ZODB.tests.util.DB(databases=databases, database_name='2')

And create a persistent object in the first database:

    >>> tm = transaction.TransactionManager()
    >>> conn1 = db1.open(transaction_manager=tm)
    >>> p1 = MyClass()
    >>> conn1.root()['p'] = p1
    >>> tm.commit()

First, we get a connection to the second database.  We get the second
connection using the first connection's `get_connection` method.  This
is important.  When using multiple databases, we need to make sure we
use a consistent set of connections so that the objects in the
connection caches are connected in a consistent manner.

    >>> conn2 = conn1.get_connection('2')

Now, we'll create a second persistent object in the second database.
We'll have a reference to the first object:

    >>> p2 = MyClass()
    >>> conn2.root()['p'] = p2
    >>> p2.p1 = p1
    >>> tm.commit()

Now, let's open a separate connection to database 2.  We use it to
read `p2`, use `p2` to get to `p1`, and verify that it is in database 1:

    >>> conn = db2.open()
    >>> p2x = conn.root()['p']
    >>> p1x = p2x.p1

    >>> p2x is p2, p2x._p_oid == p2._p_oid, p2x._p_jar.db() is db2
    (False, True, True)

    >>> p1x is p1, p1x._p_oid == p1._p_oid, p1x._p_jar.db() is db1
    (False, True, True)

It isn't valid to create references outside a multi database:

    >>> db3 = ZODB.tests.util.DB()
    >>> conn3 = db3.open(transaction_manager=tm)
    >>> p3 = MyClass()
    >>> conn3.root()['p'] = p3
    >>> tm.commit()

    >>> p2.p3 = p3
    >>> tm.commit() # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    Traceback (most recent call last):
    ...
    InvalidObjectReference:
      ('Attempt to store an object from a foreign database connection',
       <Connection at ...>,
       <ZODB.tests.testcrossdatabasereferences.MyClass...>)

    >>> tm.abort()

Databases for new objects
=========================

Objects are normally added to a database by making them reachable from
an object already in the database.  This is unambiguous when there is
only one database.  With multiple databases, it is not so clear what
happens.  Consider:

    >>> p4 = MyClass()
    >>> p1.p4 = p4
    >>> p2.p4 = p4

In this example, the new object is reachable from both `p1` in database
1 and `p2` in database 2.  If we commit, which database should `p4` end up
in?  This sort of ambiguity could lead to subtle bugs.  For that reason,
an error is generated if we commit changes when new objects are
reachable from multiple databases:

    >>> tm.commit() # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    Traceback (most recent call last):
    ...
    InvalidObjectReference:
    ("A new object is reachable from multiple databases. Won't try to
    guess which one was correct!",
    <Connection at ...>,
    <ZODB.tests.testcrossdatabasereferences.MyClass...>)

    >>> tm.abort()

To resolve this ambiguity, we can commit before an object becomes
reachable from multiple databases.

    >>> p4 = MyClass()
    >>> p1.p4 = p4
    >>> tm.commit()
    >>> p2.p4 = p4
    >>> tm.commit()
    >>> p4._p_jar.db().database_name
    '1'

This doesn't work with a savepoint:

    >>> p5 = MyClass()
    >>> p1.p5 = p5
    >>> s = tm.savepoint()
    >>> p2.p5 = p5
    >>> tm.commit() # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    Traceback (most recent call last):
    ...
    InvalidObjectReference:
    ("A new object is reachable from multiple databases. Won't try to guess
    which one was correct!",
    <Connection at ...>,
    <ZODB.tests.testcrossdatabasereferences.MyClass...>)

    >>> tm.abort()

(Maybe it should.)

We can disambiguate this situation by using the connection add method
to explicitly say what database an object belongs to:

    >>> p5 = MyClass()
    >>> p1.p5 = p5
    >>> p2.p5 = p5
    >>> conn1.add(p5)
    >>> tm.commit()
    >>> p5._p_jar.db().database_name
    '1'

This the most explicit and thus the best way, when practical, to avoid
the ambiguity.

Dissallowing implicit cross-database references
===============================================

The database constructor accepts a xrefs keyword argument that defaults
to True.  If False is passed, the implicit cross database references
are disallowed. (Note that currently, implicit cross references are
the only kind of cross references allowed.)

    >>> databases = {}
    >>> db1 = ZODB.tests.util.DB(databases=databases, database_name='1')
    >>> db2 = ZODB.tests.util.DB(databases=databases, database_name='2',
    ...                          xrefs=False)

In this example, we allow cross-references from db1 to db2, but not
the other way around.

    >>> c1 = db1.open()
    >>> c2 = c1.get_connection('2')
    >>> c1.root.x = c2.root()
    >>> transaction.commit()
    >>> c2.root.x = c1.root()
    >>> transaction.commit() # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    Traceback (most recent call last):
    ...
    InvalidObjectReference:
    ("Database '2' doesn't allow implicit cross-database references",
    <Connection at ...>,
    {'x': {}})

    >>> transaction.abort()

NOTE
====

This implementation is incomplete.  It allows creating and using
cross-database references, however, there are a number of facilities
missing:

cross-database garbage collection

    Garbage collection is done on a database by database basis.
    If an object on a database only has references to it from other
    databases, then the object will be garbage collected when its
    database is packed.  The cross-database references to it will be
    broken.

cross-database undo

    Undo is only applied to a single database.  Fixing this for
    multiple databases is going to be extremely difficult.  Undo
    currently poses consistency problems, so it is not (or should not
    be) widely used.

Cross-database aware (tolerant) export/import

    The export/import facility needs to be aware, at least, of cross-database
    references.
