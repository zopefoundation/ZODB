==================
Persistent Classes
==================

NOTE: persistent classes are EXPERIMENTAL and, in some sense,
      incomplete.  This module exists largely to test changes made to
      support Zope 2 ZClasses, with their historical flaws.

The persistentclass module provides a meta class that can be used to implement
persistent classes.

Persistent classes have the following properties:

- They cannot be turned into ghosts

- They can only contain picklable subobjects

- They don't live in regular file-system modules

Let's look at an example:

    >>> def __init__(self, name):
    ...     self.name = name

    >>> def foo(self):
    ...     return self.name, self.kind

    >>> import ZODB.persistentclass
    >>> C = ZODB.persistentclass.PersistentMetaClass(
    ...     'C', (object, ), dict(
    ...     __init__ = __init__,
    ...     __module__ = '__zodb__',
    ...     foo = foo,
    ...     kind = 'sample',
    ...     ))

This example is obviously a bit contrived. In particular, we defined
the methods outside of the class. Why?  Because all of the items in a
persistent class must be picklable.  We defined the methods as global
functions to make them picklable.

Also note that we explicitly set the module.  Persistent classes don't
live in normal Python modules. Rather, they live in the database.  We
use information in ``__module__`` to record where in the database.  When
we want to use a database, we will need to supply a custom class
factory to load instances of the class.

The class we created works a lot like other persistent objects.  It
has standard standard persistent attributes:

    >>> C._p_oid
    >>> C._p_jar
    >>> C._p_serial
    >>> C._p_changed
    False

Because we haven't saved the object, the jar, oid, and serial are all
None and it's not changed.

We can create and use instances of the class:

    >>> c = C('first')
    >>> c.foo()
    ('first', 'sample')

We can modify the class and none of the persistent attributes will
change because the object hasn't been saved.

    >>> import six
    >>> def bar(self):
    ...     six.print_('bar', self.name)
    >>> C.bar = bar
    >>> c.bar()
    bar first

    >>> C._p_oid
    >>> C._p_jar
    >>> C._p_serial
    >>> C._p_changed
    False

Now, we can store the class in a database. We're going to use an
explicit transaction manager so that we can show parallel transactions
without having to use threads.

    >>> import transaction
    >>> tm = transaction.TransactionManager()
    >>> connection = some_database.open(transaction_manager=tm)
    >>> connection.root()['C'] = C
    >>> tm.commit()

Now, if we look at the persistence variables, we'll see that they have
values:

    >>> C._p_oid
    '\x00\x00\x00\x00\x00\x00\x00\x01'
    >>> C._p_jar is not None
    True
    >>> C._p_serial is not None
    True
    >>> C._p_changed
    False

Now, if we modify the class:

    >>> def baz(self):
    ...     six.print_('baz', self.name)
    >>> C.baz = baz
    >>> c.baz()
    baz first

We'll see that the class has changed:

    >>> C._p_changed
    True

If we abort the transaction:

    >>> tm.abort()

Then the class will return to it's prior state:

    >>> c.baz()
    Traceback (most recent call last):
    ...
    AttributeError: 'C' object has no attribute 'baz'...

    >>> c.bar()
    bar first

We can open another connection and access the class there.

    >>> tm2 = transaction.TransactionManager()
    >>> connection2 = some_database.open(transaction_manager=tm2)

    >>> C2 = connection2.root()['C']
    >>> c2 = C2('other')
    >>> c2.bar()
    bar other

If we make changes without committing them:

    >>> C.bar = baz
    >>> c.bar()
    baz first

    >>> C is C2
    False

Other connections are unaffected:

    >>> connection2.sync()
    >>> c2.bar()
    bar other

Until we commit:

    >>> tm.commit()
    >>> connection2.sync()
    >>> c2.bar()
    baz other

Similarly, we don't see changes made in other connections:

    >>> C2.color = 'red'
    >>> tm2.commit()

    >>> c.color
    Traceback (most recent call last):
    ...
    AttributeError: 'C' object has no attribute 'color'...

until we sync:

    >>> connection.sync()
    >>> c.color
    'red'

Instances of Persistent Classes
===============================

We can, of course, store instances of persistent classes in the
database:

    >>> c.color = 'blue'
    >>> connection.root()['c'] = c
    >>> tm.commit()

    >>> connection2.sync()
    >>> connection2.root()['c'].color
    'blue'

NOTE: If a non-persistent instance of a persistent class is copied,
      the class may be copied as well. This is usually not the desired
      result.


Persistent instances of persistent classes
==========================================

Persistent instances of persistent classes are handled differently
than normal instances.  When we copy a persistent instances of a
persistent class, we want to avoid copying the class.

Lets create a persistent class that subclasses Persistent:

    >>> import persistent
    >>> class P(persistent.Persistent, C):
    ...     __module__ = '__zodb__'
    ...     color = 'green'

    >>> connection.root()['P'] = P

    >>> import persistent.mapping
    >>> connection.root()['obs'] = persistent.mapping.PersistentMapping()
    >>> p = P('p')
    >>> connection.root()['obs']['p'] = p
    >>> tm.commit()

You might be wondering why we didn't just stick 'p' into the root
object. We created an intermediate persistent object instead.  We are
storing persistent classes in the root object. To create a ghost for a
persistent instance of a persistent class, we need to be able to be
able to access the root object and it must be loaded first.  If the
instance was in the root object, we'd be unable to create it while
loading the root object.

Now, if we try to load it, we get a broken object:

    >>> connection2.sync()
    >>> connection2.root()['obs']['p']
    <persistent broken __zodb__.P instance '\x00\x00\x00\x00\x00\x00\x00\x04'>

because the module, `__zodb__` can't be loaded.  We need to provide a
class factory that knows about this special module. Here we'll supply a
sample class factory that looks up a class name in the database root
if the module is `__zodb__`.  It falls back to the normal class lookup
for other modules:

    >>> from ZODB.broken import find_global
    >>> def classFactory(connection, modulename, globalname):
    ...     if modulename == '__zodb__':
    ...        return connection.root()[globalname]
    ...     return find_global(modulename, globalname)

    >>> some_database.classFactory = classFactory

Normally, the classFactory should be set before a database is opened.
We'll reopen the connections we're using.  We'll assign the old
connections to a variable first to prevent getting them from the
connection pool:

    >>> old = connection, connection2
    >>> connection = some_database.open(transaction_manager=tm)
    >>> connection2 = some_database.open(transaction_manager=tm2)

Now, we can read the object:

    >>> connection2.root()['obs']['p'].color
    'green'
    >>> connection2.root()['obs']['p'].color = 'blue'
    >>> tm2.commit()

    >>> connection.sync()
    >>> p = connection.root()['obs']['p']
    >>> p.color
    'blue'

Copying
=======

If we copy an instance via export/import, the copy and the original
share the same class:

    >>> file = connection.exportFile(p._p_oid)
    >>> _ = file.seek(0)
    >>> cp = connection.importFile(file)
    >>> file.close()
    >>> cp.color
    'blue'

    >>> cp is not p
    True

    >>> cp.__class__ is p.__class__
    True

    >>> tm.abort()


XXX test abort of import
