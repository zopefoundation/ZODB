========
Tutorial
========

This tutorial is intended to guide developers with a step-by-step introduction
of how to develop an application which stores its data in the ZODB.

Introduction
============

Lets have a look at a simple piece of code that we want to turn into using
ZODB::

    class Account(object):
        def __init__(self):
            self.balance = 0.0

        def deposit(self, amount):
            self.balance += amount

        def cash(self, amount):
            assert amount < self.balance
            self.balance -= amount 

This code defines a simple class that holds the balance of a bank account and provides two methods to manipulate the balance: deposit and cash.

Installation
============

Before being able to use ZODB we have to install it, using easy_install. Note
that the actual package name is called "ZODB3"::

    $ easy_install ZODB3
    ...
    $ python
    >>> import ZODB

ZODB is now installed and can be imported from your Python installation.

    If you do not have easy_install available on your system, follow the
    `EasyInstall
    <http://peak.telecommunity.com/DevCenter/EasyInstall#installation-instructions>`_
    installation instructions.

    There are other installation mechanisms available to manage the
    installation of Python packages. This tutorial assumes that you are using
    a plain Python installation and install ZODB globally.


Configuration
=============

When a program wants to use the ZODB it has to establish a connection, like any other database. For the ZODB we need 3 different parts: a storage, a database and finally a connection:

    >>> from ZODB.FileStorage import FileStorage
    >>> from ZODB.DB import DB
    >>> storage = FileStorage('Data.fs')
    >>> db = DB(storage)
    >>> connection = db.open()
    >>> root = connection.root()

We create a storage called FileStorage, which is the current standard storage
used by virtually everyone. It keeps track of all data in a single file as
stated by the first parameter. From this storage we create a database and
finally open up a connection. Finally, we retrieve the database root object
from the connection that we opened.

Storing objects
===============

To store an object in the ZODB we simply attach it to any other object that
already lives in the database. Hence, the root object functions as a
boot-strapping point. The root object is a dictionary and you can start storing
your objects directly in there:

>>> root['account-1'] = Account()
>>> root['account-2'] = Account()

    Frameworks like Zope only
    create a single object in the ZODB root representing the application itself and
    then let all sub-objects be referenced from there on. They choose names like
    'app' for the first object they place in the ZODB.


Transactions
============

You now have two objects placed in your root object and in your database.
However, they are not permanently stored yet. The ZODB uses transactions and to
make your changes permanent, you have to commit the transaction:

>>> import transaction
>>> transaction.commit()
>>> root.keys()
['account-1', 'account-2']

Now you can stop and start your application and look at the root object again,
and you will find the entries 'account-1' and 'account-2' to be still be
present and be the objects you created.

    Objects that have not been stored in
    the ZODB yet are not rolled back by an abort.

If your application makes changes during a transaction and finds that it does
not want to commit those changes, then you can abort the transaction and have
the changes rolled back for you:

>>> del root['account-1']
>>> root.keys()
['account-2']
>>> transaction.abort()
>>> root.keys()
['account-1', 'account-2']

Persistent objects
==================

One last aspect that we need to cover are persistent objects themselves. The
ZODB will be happy to store almost any Python object that you pass to it (it
won't store files for example). But in order for noticing which objects have
changed the ZODB needs those objects to cooperate with the database. In
general, you just subclass `persistent.Persistent` to make this happen. So our
example class from above would read like::

    import persistent

    class Account(persistent.Persistent):
        # ... same code as above ...

..

    Have a look at the reference documentation about the rules of persistency and
    to find out more about specialised persistent objects like BTrees.

Summary
=======

You have seen how to install ZODB and how to  open a database in your
application and to start storing objects in it. We also touched the two simple
transaction commands: commit and abort. The reference documentation contains
sections with more information on the individual topics.
