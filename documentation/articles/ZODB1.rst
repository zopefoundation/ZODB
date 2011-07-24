Introduction to the ZODB (by Michel Pelletier)
==============================================

In this article, we cover the very basics of the Zope Object
Database (ZODB) for Python programmers.  This short article
documents almost everything you need to know about using this
powerful object database in Python. In a later article, I will
cover some of the more advanced features of ZODB for Python
programmers.

ZODB is a database for Python objects that comes with
`Zope <http://www.zope.org>`_.  If you've ever worked with a
relational database, like PostgreSQL, MySQL, or Oracle, than you
should be familiar with the role of a database.  It's a long term
or short term storage for your application data.

For many tasks, relational databases are clearly a good solution,
but sometimes relational databases don't fit well with your object
model.  If you have lots of different kinds of interconnected
objects with complex relationships, and changing schemas then ZODB
might be worth giving a try.

A major feature of ZODB is transparency.  You do not need to write
any code to explicitly read or write your objects to or from a
database.  You just put your *persistent* objects into a container
that works just like a Python dictionary.  Everything inside this
dictionary is saved in the database.  This dictionary is said to
be the "root" of the database. It's like a magic bag; any Python
object that you put inside it becomes persistent.

Actually there are a few restrictions on what you can store in the
ZODB. You can store any objects that can be "pickled" into a
standard, cross-platform serial format.  Objects like lists,
dictionaries, and numbers can be pickled.  Objects like files,
sockets, and Python code objects, cannot be stored in the database
because they cannot be pickled.  For more information on
"pickling", see the Python pickle module documentation at
http://www.python.org/doc/current/lib/module-pickle.html

A Simple Example
----------------

The first thing you need to do to start working with ZODB is to
create a "root object".  This process involves first opening a
connection to a "storage", which is the actual back-end that stores
your data.

ZODB supports many pluggable storage back-ends, but for the
purposes of this article I'm going to show you how to use the
'FileStorage' back-end storage, which stores your object data in a
file.  Other storages include storing objects in relational
databases, Berkeley databases, and a client to server storage that
stores objects on a remote storage server.

To set up a ZODB, you must first install it.  ZODB comes with
Zope, so the easiest way to install ZODB is to install Zope and
use the ZODB that comes with your Zope installation.  For those of
you who don't want all of Zope, but just ZODB, see the
instructions for downloading StandaloneZODB from the `ZODB web
page <http://www.zope.org/Wikis/ZODB/FrontPage>`_.

StandaloneZODB can be installed into your system's Python
libraries using the standard 'distutils' Python module.

After installing ZODB, you can start to experiment with it right
from the Python command line interpreter.  For example, try the
following python code in your interpreter::

      >>> from ZODB import FileStorage, DB
      >>> storage = FileStorage.FileStorage('mydatabase.fs')
      >>> db = DB(storage)
      >>> connection = db.open()
      >>> root = connection.root()

Here, you create storage and use the 'mydatabse.fs' file to store
the object information.  Then, you create a database that uses
that storage.

Next, the database needs to be "opened" by calling the 'open()'
method.  This will return a connection object to the database.
The connection object then gives you access to the 'root' of the
database with the 'root()' method.

The 'root' object is the dictionary that holds all of your
persistent objects.  For example, you can store a simple list of
strings in the root object::
   
      >>> root['employees'] = ['Mary', 'Jo', 'Bob']

Now, you have changed the persistent database by adding a new
object, but this change is so far only temporary.  In order to
make the change permanent, you must commit the current
transaction::

      >>> import transaction
      >>> transaction.commit()

Transactions group of lots of changes in one atomic operation.  In
a later article, I'll show you how this is a very powerful
feature.  For now, you can think of committing transactions as
"checkpoints" where you save the changes you've made to your
objects so far.  Later on, I'll show you how to abort those
changes, and how to undo them after they are committed.

Now let's find out if our data was actually saved. First close the
database connection::

      >>> connection.close()

Then quit Python. Now start the Python interpreter up again, and
connect to the database you just created::

      >>> from ZODB import FileStorage, DB
      >>> storage = FileStorage.FileStorage('mydatabase.fs')
      >>> db = DB(storage)
      >>> connection = db.open()
      >>> root = connection.root()

Now, let's see what's in the root::

      >>> root.items()
      [('employees', ['Mary', 'Jo', 'Bob'])]  

There's your list.  If you had used a relational database, you
would have had to issue a SQL query to save even a simple Python
list like the above example.  You would have also needed some code
to convert a SQL query back into the list when you wanted to use
it again.  You don't have to do any of this work when using ZODB.
Using ZODB is almost completely transparent, in fact, ZODB based
programs often look suspiciously simple!

Keep in mind that ZODB's persistent dictionary is just the tip of
the persistent iceberg.  Persistent objects can have attributes
that are themselves persistent.  In other words, even though you
may have only one or two "top level" persistent objects as values
in the persistent dictionary, you can still have thousands of
sub-objects below them.  This is, in fact, how Zope does it.  In
Zope, there is only *one* top level object that is the root
"application" object for all other objects in Zope.

Detecting Changes
-----------------

One thing that makes ZODB so easy to use is that it doesn't
require you to keep track of your changes. All you have to do is
to make changes to persistent objects and then commit a
transaction. Anything that has changed will be stored in the
database. 

There is one exception to this rule when it comes to simple
mutable Python types like lists and dictionaries.  If you change a
list or dictionary that is already stored in the database, then
the change will *not* take effect.  Consider this example::

      >>> root['employees'].append('Bill')
      >>> transaction.commit()
    
You would expect this to work, but it doesn't.  The reason for
this is that ZODB cannot detect that the 'employees' list
changed. The 'employees' list is a mutable object that does not
notify ZODB when it changes.

There are a couple of very simple ways around this problem.  The
simplest is to re-assign the changed object::

      >>> employees = root['employees']
      >>> employees.append('Bill')
      >>> root['employees'] = employees
      >>> transaction.commit()

Here, you move the employees list to a local variable, change the
list, and then *reassign* the list back into the database and
commit the transaction.  This reassignment notifies the database
that the list changed and needs to be saved to the database.

Later in this article, we'll show you another technique for
notifying the ZODB that your objects have changed.  Also, in a
later article, we'll show you how to use simple, ZODB-aware list
and dictionary classes that come pre-packaged with ZODB for your
convenience. 

Persistent Classes
------------------

The easiest way to create mutable objects that notify the ZODB of
changes is to create a persistent class.  Persistent classes let
you store your own kinds of objects in the database.  For example,
consider a class that represents a employee::

      import ZODB
      from Persistence import Persistent

      class Employee(Persistent):
        
          def setName(self, name):
              self.name = name

To create a persistent class, simply subclass from
'Persistent.Persistent'. Because of some special magic that ZODB
does, you must first import ZODB before you can import Persistent.
The 'Persistent' module is actually *created* when you import
'ZODB'.

Now, you can put Employee objects in your database::

      >>> employees=[]
      >>> for name in ['Mary', 'Joe', 'Bob']:
      ...     employee = Employee()
      ...     employee.setName(name)
      ...     employees.append(employee)
      >>> root['employees']=employees
      >>> transaction.commit()

Don't forget to call 'commit()', so that the changes you have made
so far are committed to the database, and a new transaction is
begun.

Now you can change your employees and they will be saved in the
database. For example you can change Bob's name to "Robert"::

      >>> bob=root['employees'][2]
      >>> bob.setName('Robert')
      >>> transaction.commit()

You can even change attributes of persistent instaces without
calling methods::

      >>> bob=root['employees'][2]
      >>> bob._coffee_prefs=('Cream', 'Sugar')
      >>> transaction.commit()

It doesn't matter whether you change an attribute directly, or
whether it's changed by a method.  As you can tell, all of the
normal Python language rules still work as you'd expect.

Mutable Attributes
------------------

Earlier you saw how ZODB can't detect changes to normal mutable
objects like Python lists. This issue still affects you when using
persistent instances. This is because persistent instances can
have attributes which are normal mutable objects. For example,
consider this class::

      class Employee(Persistent):

          def __init__(self):
              self.tasks = []
        
          def setName(self, name):
              self.name = name

          def addTask(self, task):
              self.task.append(task)

When you call 'addTask', the ZODB won't know that the mutable
attribute 'self.tasks' has changed.  As you saw earlier, you can
reassign 'self.tasks' after you change it to get around this
problem. However, when you're using persistent instances, you have
another choice. You can signal the ZODB that your instance has
changed with the '_p_changed' attribute::

      class Employee(Persistent):
          ...

          def addTask(self, task):
              self.task.append(task)
              self._p_changed = 1

To signal that this object has change, set the '_p_changed'
attribute to 1. You only need to signal ZODB once, even if you
change many mutable attributes.

The '_p_changed' flag leads us to one of the few rules of you must
follow when creating persistent classes: your instances *cannot*
have attributes that begin with '_p_', those names are reserved
for use by the ZODB.

A Complete Example
------------------

Here's a complete example program. It builds on the employee
examples used so far::

      from ZODB import DB
      from ZODB.FileStorage import FileStorage
      from ZODB.PersistentMapping import PersistentMapping
      from Persistence import Persistent
      import transaction

      class Employee(Persistent):
          """An employee"""

          def __init__(self, name, manager=None):
              self.name=name
              self.manager=manager

      # setup the database
      storage=FileStorage("employees.fs")
      db=DB(storage)
      connection=db.open()
      root=connection.root()

      # get the employees mapping, creating an empty mapping if
      # necessary
      if not root.has_key("employees"):
          root["employees"] = {}
      employees=root["employees"]


      def listEmployees():
          if len(employees.values())==0:
              print "There are no employees."
              print
              return
          for employee in employees.values():
              print "Name: %s" % employee.name
              if employee.manager is not None:
                  print "Manager's name: %s" % employee.manager.name
              print

      def addEmployee(name, manager_name=None):
          if employees.has_key(name):
              print "There is already an employee with this name."
              return
          if manager_name:
              try:
                  manager=employees[manager_name]
              except KeyError:
                  print
                  print "No such manager"
                  print
                  return
              employees[name]=Employee(name, manager)
          else:
              employees[name]=Employee(name)

          root['employees'] = employees  # reassign to change
          transaction.commit()
          print "Employee %s added." % name
          print


      if __name__=="__main__":
          while 1:
              choice=raw_input("Press 'L' to list employees, 'A' to add"
                               "an employee, or 'Q' to quit:")
              choice=choice.lower()
              if choice=="l":
                  listEmployees()
              elif choice=="a":
                  name=raw_input("Employee name:")
                  manager_name=raw_input("Manager name:")
                  addEmployee(name, manager_name)
              elif choice=="q":
                  break

          # close database
          connection.close()

This program demonstrates a couple interesting things. First, this
program shows how persistent objects can refer to each other. The
'self.manger' attribute of 'Employee' instances can refer to other
'Employee' instances. Unlike a relational database, there is no
need to use indirection such as object ids when referring from one
persistent object to another. You can just use normal Python
references. In fact, you can even use circular references.

A final trick used by this program is to look for a persistent
object and create it if it is not present. This allows you to just
run this program without having to run a setup script to build the
database first. If there is not database present, the program will
create one and initialize it.

Conclusion
----------

ZODB is a very simple, transparent object database for Python that
is a freely available component of the Zope application server.
As these examples illustrate, only a few lines of code are needed
to start storing Python objects in ZODB, with no need to write SQL
queries.  In the next article on ZODB, we'll show you some more
advanced techniques for using ZODB, like using ZODB's distributed
object protocol to distribute your persistent objects across many
machines.  

ZODB Resources

- `Andrew Kuchling's "ZODB pages" <http://web.archive.org/web/20030606003753/http://amk.ca/zodb/>`_ (archived)

- `Zope.org "ZODB Wiki" <http://www.zope.org/Wikis/ZODB/FrontPage>`_

- `Jim Fulton's "Introduction to the Zope Object Database" <http://www.python.org/workshops/2000-01/proceedings/papers/fulton/zodb3.html>`_








