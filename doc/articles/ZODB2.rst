Advanced ZODB for Python Programmers
====================================

In the first article in this series, "ZODB for Python
Programmers":ZODB1 I covered some of the simpler aspects of Python
object persistence.  In this article, I'll go over some of the more
advanced features of ZODB.

In addition to simple persistence, ZODB offers some very useful
extras for the advanced Python application.  Specificly, we'll cover
the following advanced features in this article:

-   Persistent-Aware Types -- ZODB comes with some special,
    "persistent-aware" data types for storing data in a ZODB.  The
    most useful of these is the "BTree", which is a fast, efficient
    storage object for lots of data.

-   Voalitile Data -- Not all your data is meant to be stored in the
    database, ZODB let's you have volatile data on your objects that
    does not get saved.

-   Pluggable Storages -- ZODB offers you the ability to use many
    different storage back-ends to store your object data, including
    files, relational databases and a special client-server storage
    that stores objects on a remote server.

-   Conflict Resolution -- When many threads try to write to the same
    object at the same time, you can get conflicts.  ZODB offers a
    conflict resolution protocol that allows you to mitigate most
    conflicting writes to your data.

-   Transactions -- When you want your changes to be "all or nothing"
    transactions come to the rescue.  

Persistent-Aware Types
----------------------

You can also get around the mutable attribute problem discussed in
the first article by using special types that are "persistent
aware".  ZODB comes with the following persistent aware mutable
object types:

-     PersistentList -- This type works just like a list, except that
      changing it does not require setting _p_changed or explicitly
      re-assigning the attribute.

-     PersistentMapping -- A persistent aware dictionary, much like
      PersistentList.
      
-     BTree -- A dictionary-like object that can hold large
      collections of objects in an ordered, fast, efficient way.

BTrees offer a very powerful facility to the Python programmer:

-   BTrees can hold a large collection of information in an
    efficient way; more objects than your computer has enough
    memory to hold at one time.  

-   BTrees are integrated into the persistence machinery to work
    effectively with ZODB's object cache.  Recently, or heavily
    used objects are kept in a memory cache for speed.

-   BTrees can be searched very quickly, because they are stored
    in an fast, balanced tree data structure.

-   BTrees come in three flavors, OOBTrees, IOBTrees, OIBTrees, and
    IIBTrees.  The last three are optimized for integer keys, values,
    and key-value pairs, respectively.  This means that, for example,
    an IOBTree is meant to map an integer to an object, and is
    optimized for having integers keys.

Using BTrees
------------

Suppose you track the movement of all your employees with
heat-seeking cameras hidden in the ceiling tiles.  Since your
employees tend to frequently congregate against you, all of the
tracking information could end up to be a lot of data, possibly
thousands of coordinates per day per employee.  Further, you want
to key the coordinate on the time that it was taken, so that you
can only look at where your employees were during certain times::

      from BTrees import IOBTree
      from time import time

      class Employee(Persistent):

          def __init__(self):
              self.movements = IOBTree()
        
          def fix(self, coords):
              "get a fix on the employee"
              self.movements[int(time())] = coords

          def trackToday(self): 
              "return all the movements of the
              employee in the last 24 hours"
              current_time = int(time())
              return self.movements.items(current_time - 86400, 
                                          current_time)


In this example, the 'fix' method is called every time one of your
cameras sees that employee.  This information is then stored in a
BTree, with the current 'time()' as the key and the 'coordinates'
as the value.

Because BTrees store their information is a ordered structure,
they can be quickly searched for a range of key values.  The
'trackToday' method uses this feature to return a sequence of
coordinates from 24 hours hence to the present.

This example shows how BTrees can be quickly searched for a range
of values from a minimum to a maximum, and how you can use this
technique to oppress your workforce.  BTrees have a very rich API,
including doing unions and intersections of result sets.

Not All Objects are Persistent
------------------------------

You don't have to make all of your objects persistent.
Non-persistent objects are often useful to represent either
"canned" behavior (classes that define methods but no state), or
objects that are useful only as a "cache" that can be thrown away
when your persistent object is deactivated (removed from memory
when not used).

ZODB provides you with the ability to have *volatile* attributes.
Volatile attributes are attributes of persistent objects that are
never saved in the database, even if they are capable of being
persistent.  Volatile attributes begin with '_v_' are good for
keeping cached information around for optimization.  ZODB also
provides you with access to special pickling hooks that allow you
to set volatile information when an object is activated.

Imagine you had a class that stored a complex image that you
needed to calculate.  This calculation is expensive.  Instead of
calculating the image every time you called a method, it would be
better to calculate it *once* and then cache the result in a
volatile attribute::

      def image(self):
          "a large and complex image of the terrain"
          if hasattr(self, '_v_image'):
              return self._v_image
          image=expensive_calculation()
          self._v_image=image
          return image

Here, calling 'image' the first time the object is activated will
cause the method to do the expensive calculation.  After the first
call, the image will be cached in a volatile attribute.  If the
object is removed from memory, the '_v_image' attribute is not
saved, so the cached image is thrown away, only to be recalculated
the next time you call 'image'.
 
ZODB and Concurrency 
--------------------

Different, threads, processes, and computers on a network can open
connections to a single ZODB object database.  Each of these
different processes keeps its own copy of the objects that it uses
in memory.

The problem with allowing concurrent access is that conflicts can
occur.  If different threads try to commit changes to the same
objects at the same time, one of the threads will raise a
ConflictError.  If you want, you can write your application to
either resolve or retry conflicts a reasonable number of times.

Zope will retry a conflicting ZODB operation three times.  This is
usually pretty reasonable behavior.  Because conflicts only happen
when two threads write to the same object, retrying a conflict
means that one thread will win the conflict and write itself, and
the other thread will retry a few seconds later.

Pluggable Storages
------------------

Different processes and computers can connection to the same
database using a special kind of storage called a 'ClientStorage'.
A 'ClientStorage' connects to a 'StorageServer' over a network.

In the very beginning, you created a connection to the database by
first creating a storage.  This was of the type 'FileStorage'.
Zope comes with several different back end storage objects, but
one of the most interesting is the 'ClientStorage' from the Zope
Enterprise Objects product (ZEO).

The 'ClientStorage' storage makes a TCP/IP connection to a
'StorageServer' (also provided with ZEO).  This allows many
different processes on one or machines to work with the same
object database and, hence, the same objects.  Each process gets a
cached "copy" of a particular object for speed.  All of the
'ClientStorages' connected to a 'StorageServer' speak a special
object transport and cache invalidation protocol to keep all of
your computers synchronized.

Opening a 'ClientStorage' connection is simple.  The following
code creates a database connection and gets the root object for a
'StorageServer' listening on "localhost:12345"::

      from ZODB import DB
      from ZEO import ClientStorage
      storage = ClientStorage.ClientStorage('localhost', 12345)
      db = DB( storage )
      connection = db.open()
      root = connection.root()

In the rare event that two processes (or threads) modify the same
object at the same time, ZODB provides you with the ability to
retry or resolve these conflicts yourself. 

Resolving Conflicts
-------------------

If a conflict happens, you have two choices. The first choice is
that you live with the error and you try again.  Statistically,
conflicts are going to happen, but only in situations where objects
are "hot-spots".  Most problems like this can be "designed away";
if you can redesign your application so that the changes get
spread around to many different objects then you can usually get
rid of the hot spot.

Your second choice is to try and *resolve* the conflict. In many
situations, this can be done. For example, consider the following
persistent object::

      class Counter(Persistent):

          self.count = 0

          def hit(self):
              self.count = self.count + 1

This is a simple counter.  If you hit this counter with a lot of
requests though, it will cause conflict errors as different threads
try to change the count attribute simultaneously.

But resolving the conflict between conflicting threads in this
case is easy.  Both threads want to increment the self.count
attribute by a value, so the resolution is to increment the
attribute by the sum of the two values and make both commits
happy.

To resolve a conflict, a class should define an
'_p_resolveConflict' method. This method takes three arguments:

-  'oldState' -- The state of the object that the changes made by
   the current transaction were based on. The method is permitted
   to modify this value.

-  'savedState' -- The state of the object that is currently
   stored in the database. This state was written after 'oldState'
   and reflects changes made by a transaction that committed
   before the current transaction. The method is permitted to
   modify this value.

-  'newState' -- The state after changes made by the current
   transaction.  The method is *not* permitted to modify this
   value. This method should compute a new state by merging
   changes reflected in 'savedState' and 'newState', relative to
   'oldState'.

The method should return the state of the object after resolving
the differences.  

Here is an example of a '_p_resolveConflict' in the 'Counter'
class::

      class Counter(Persistent):

          self.count = 0

          def hit(self):
              self.count = self.count + 1

          def _p_resolveConflict(self, oldState, savedState, newState):

              # Figure out how each state is different:
              savedDiff= savedState['count'] - oldState['count']
              newDiff= newState['count']- oldState['count']

              # Apply both sets of changes to old state:
              return oldState['count'] + savedDiff + newDiff

In the above example, '_p_resolveConflict' resolves the difference
between the two conflicting transactions.

Transactions and Subtransactions
--------------------------------

Transactions are a very powerful concept in databases.
Transactions let you make many changes to your information as if
they were all one big change.  Imagine software that did online
banking and allowed you to transfer money from one account to
another.  You would do this by deducting the amount of the
transfer from one account, and adding  that amount onto the
other.  

If an error happened while you were adding the money to the
receiving account (say, the bank's computers were unavailable),
then you would want to abort the transaction so that the state of
the accounts went back to the way they were before you changed
anything.

To abort a transaction, you need to call the 'abort' method of the
transactions object::

    >>> import transaction
    >>> transaction.abort()

    This will throw away all the currently changed objects and start a
    new, empty transaction.

Subtransactions, sometimes called "inner transactions", are
transactions that happen inside another transaction.
Subtransactions can be commited and aborted like regular "outer"
transactions.  Subtransactions mostly provide you with an
optimization technique.

Subtransactions can be commited and aborted.  Commiting or
aborting a subtransaction does not commit or abort its outer
transaction, just the subtransaction.  This lets you use many,
fine-grained transactions within one big transaction.

Why is this important?  Well, in order for a transaction to be
"rolled back" the changes in the transaction must be stored in
memory until commit time.  By commiting a subtransaction, you are
telling Zope that "I'm pretty sure what I've done so far is
permenant, you can store this subtransaction somewhere other than
in memory".  For very, very large transactions, this can be a big
memory win for you.

If you abort an outer transaction, then all of its inner
subtransactions will also be aborted and not saved.  If you abort
an inner subtransaction, then only the changes made during that
subtransaction are aborted, and the outer transaction is *not*
aborted and more changes can be made and commited, including more
subtransactions.

You can commit or abort a subtransaction by calling either
commit() or abort() with an argument of 1::

      transaction.commit(1) # or
      transaction.abort(1)

Subtransactions offer you a nice way to "batch" all of your "all
or none" actions into smaller "all or none" actions while still
keeping the outer level "all or none" transaction intact.  As a
bonus, they also give you much better memory resource performance.

Conclusion
----------

ZODB offers many advanced features to help you develop simple, but
powerful python programs.  In this article, you used some of the
more advanced features of ZODB to handle different application
needs, like storing information in large sets, using the database
concurrently, and maintaining transactional integrity.  For more
information on ZODB, join the discussion list at zodb-dev@zope.org
where you can find out more about this powerful component of Zope.



