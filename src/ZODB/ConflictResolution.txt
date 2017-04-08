===================
Conflict Resolution
===================

Overview
========

Conflict resolution is a way to resolve transaction conflicts that would
otherwise abort a transaction.  As such, it risks data integrity in order to
try to avoid throwing away potentially computationally expensive transactions.

The risk of harming data integrity should not be underestimated. Writing
conflict resolution code takes some responsibility for transactional
integrity away from the ZODB, and puts it in the hands of the developer
writing the conflict resolution code.

The current conflict resolution code is implemented with a storage mix-in
found in ZODB/ConflictResolution.py.  The idea's proposal, and an explanation
of the interface, can be found here:
http://www.zope.org/Members/jim/ZODB/ApplicationLevelConflictResolution

Here is the most pertinent section, somewhat modified for this document's
use:

    A new interface is proposed to allow object authors to provide a method
    for resolving conflicts. When a conflict is detected, then the database
    checks to see if the class of the object being saved defines the method,
    _p_resolveConflict. If the method is defined, then the method is called
    on the object. If the method succeeds, then the object change can be
    committed, otherwise a ConflictError is raised as usual.

    def _p_resolveConflict(oldState, savedState, newState):
        Return the state of the object after resolving different changes.

        Arguments:

        oldState
            The state of the object that the changes made by the current
            transaction were based on.

            The method is permitted to modify this value.

        savedState
            The state of the object that is currently stored in the
            database. This state was written after oldState and reflects
            changes made by a transaction that committed before the
            current transaction.

            The method is permitted to modify this value.

        newState
            The state after changes made by the current transaction.

            The method is not permitted to modify this value.

            This method should compute a new state by merging changes
            reflected in savedState and newState, relative to oldState.

        If the method cannot resolve the changes, then it should raise
        ZODB.POSException.ConflictError.


        Consider an extremely simple example, a counter::

            from persistent import Persistent
            class PCounter(Persistent):
                '`value` is readonly; increment it with `inc`.'

                # Fool BTree checks for sane comparison :/
                def __cmp__(self, other):
                    return object.__cmp__(self, other)
                def __lt__(self, other):
                    return object.__lt__(self, other)

                _val = 0
                def inc(self):
                    self._val += 1
                @property
                def value(self):
                    return self._val
                def _p_resolveConflict(self, oldState, savedState, newState):
                    oldState['_val'] = (
                        savedState.get('_val', 0) +
                        newState.get('_val', 0) -
                        oldState.get('_val', 0))
                    return oldState

        .. -> src

            >>> import ConflictResolution_txt
            >>> exec(src, ConflictResolution_txt.__dict__)
            >>> PCounter = ConflictResolution_txt.PCounter
            >>> PCounter.__module__ = 'ConflictResolution_txt'


By "state", the excerpt above means the value used by __getstate__ and
__setstate__: a dictionary, in most cases.  We'll look at more details below,
but let's continue the example above with a simple successful resolution
story.

First we create a storage and a database, and put a PCounter in the database.

    >>> import ZODB
    >>> db = ZODB.DB('Data.fs')
    >>> import transaction
    >>> tm_A = transaction.TransactionManager()
    >>> conn_A = db.open(transaction_manager=tm_A)
    >>> p_A = conn_A.root()['p'] = PCounter()
    >>> p_A.value
    0
    >>> tm_A.commit()

Now get another copy of 'p' so we can make a conflict.  Think of `conn_A`
(connection A) as one thread, and `conn_B` (connection B) as a concurrent
thread.  `p_A` is a view on the object in the first connection, and `p_B`
is a view on *the same persistent object* in the second connection.

    >>> tm_B = transaction.TransactionManager()
    >>> conn_B = db.open(transaction_manager=tm_B)
    >>> p_B = conn_B.root()['p']
    >>> p_B.value
    0
    >>> p_A._p_oid == p_B._p_oid
    True

Now we can make a conflict, and see it resolved.

    >>> p_A.inc()
    >>> p_A.value
    1
    >>> p_B.inc()
    >>> p_B.value
    1
    >>> tm_B.commit()
    >>> p_B.value
    1
    >>> tm_A.commit()
    >>> p_A.value
    2

We need to synchronize connection B, in any of a variety of ways, to see the
change from connection A.

    >>> p_B.value
    1
    >>> trans = tm_B.begin()
    >>> p_B.value
    2

A very similar class found in real world use is BTrees.Length.Length.

This conflict resolution approach is simple, yet powerful.  However, it
has a few caveats and rough edges in practice.  The simplicity, then, is
a bit of a disguise. Again, be warned, writing conflict resolution code
means that you claim significant responsibilty for your data integrity.

Because of the rough edges, the current conflict resolution approach is slated
for change (as of this writing, according to Jim Fulton, the ZODB
primary author and maintainer).  Others have talked about different approaches
as well (see, for instance, http://www.python.org/~jeremy/weblog/031031c.html).
But for now, the _p_resolveConflict method is what we have.

Caveats and Dangers
===================

Here are caveats for working with this conflict resolution approach.
Each sub-section has a "DANGERS" section that outlines what might happen
if you ignore the warning.  We work from the least danger to the most.

Conflict Resolution Is on the Server
------------------------------------

If you are using ZEO or ZRS, be aware that the classes for which you have
conflict resolution code *and* the classes of the non-persistent objects
they reference must be available to import by the *server* (or ZRS
primary).

DANGERS: You think you are going to get conflict resolution, but you won't.

Ignore `self`
-------------

Even though the _p_resolveConflict method has a "self", ignore it.
Don't change it.  You make changes by returning the state.  This is
effectively a class method.

DANGERS: The changes you make to the instance will be discarded.  The
instance is not initialized, so other methods that depend on instance
attributes will not work.

Here's an example of a broken _p_resolveConflict method::

    class PCounter2(PCounter):
        def __init__(self):
            self.data = []
        def _p_resolveConflict(self, oldState, savedState, newState):
            self.data.append('bad idea')
            return super(PCounter2, self)._p_resolveConflict(
                oldState, savedState, newState)

.. -> src

    >>> exec(src, ConflictResolution_txt.__dict__)
    >>> PCounter2 = ConflictResolution_txt.PCounter2
    >>> PCounter2.__module__ = 'ConflictResolution_txt'

Now we'll prepare for the conflict again.

    >>> p2_A = conn_A.root()['p2'] = PCounter2()
    >>> p2_A.value
    0
    >>> tm_A.commit()
    >>> trans = tm_B.begin() # sync
    >>> p2_B = conn_B.root()['p2']
    >>> p2_B.value
    0
    >>> p2_A._p_oid == p2_B._p_oid
    True

And now we will make a conflict.

    >>> p2_A.inc()
    >>> p2_A.value
    1
    >>> p2_B.inc()
    >>> p2_B.value
    1
    >>> tm_B.commit()
    >>> p2_B.value
    1
    >>> tm_A.commit() # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    ConflictError: database conflict error...

oops!

    >>> tm_A.abort()
    >>> p2_A.value
    1
    >>> trans = tm_B.begin()
    >>> p2_B.value
    1

Watch Out for Persistent Objects in the State
---------------------------------------------

If the object state has a reference to Persistent objects (instances
of classes that inherit from persistent.Persistent) then these references
*will not be loaded and are inaccessible*.  Instead, persistent objects
in the state dictionary are ZODB.ConflictResolution.PersistentReference
instances.  These objects have the following interface::

    class IPersistentReference(zope.interface.Interface):
        '''public contract for references to persistent objects from an object
        with conflicts.'''

        oid = zope.interface.Attribute(
            'The oid of the persistent object that this reference represents')

        database_name = zope.interface.Attribute(
            '''The name of the database of the reference, *if* different.

            If not different, None.''')

        klass = zope.interface.Attribute(
            '''class meta data.  Presence is not reliable.''')

        weak = zope.interface.Attribute(
            '''bool: whether this reference is weak''')

        def __cmp__(other):
            '''if other is equivalent reference, return 0; else raise ValueError.

            Equivalent in this case means that oid and database_name are the same.

            If either is a weak reference, we only support `is` equivalence, and
            otherwise raise a ValueError even if the datbase_names and oids are
            the same, rather than guess at the correct semantics.

            It is impossible to sort reliably, since the actual persistent
            class may have its own comparison, and we have no idea what it is.
            We assert that it is reasonably safe to assume that an object is
            equivalent to itself, but that's as much as we can say.

            We don't compare on 'is other', despite the
            PersistentReferenceFactory.data cache, because it is possible to
            have two references to the same object that are spelled with different
            data (for instance, one with a class and one without).'''

So let's look at one of these.  Let's assume we have three, `old`,
`saved`, and `new`, each representing a persistent reference to the same
object within a _p_resolveConflict call from the oldState, savedState,
and newState [#get_persistent_reference]_.  They have an oid, `weak` is
False, and `database_name` is None.  `klass` happens to be set but this is
not always the case.

    >>> isinstance(new.oid, bytes)
    True
    >>> new.weak
    False
    >>> print(new.database_name)
    None
    >>> new.klass is PCounter
    True

There are a few subtleties to highlight here.  First, notice that the
database_name is only present if this is a cross-database reference
(see cross-database-references.txt in this directory, and examples
below). The database name and oid is sometimes a reasonable way to
reliably sort Persistent objects (see zope.app.keyreference, for
instance) but if your code compares one PersistentReference with a
database_name and another without, you need to refuse to give an answer
and raise an exception, because you can't know how the unknown
database_name sorts.

We already saw a persistent reference with a database_name of None.  Now
let's suppose `new` is an example of a cross-database reference from a
database named '2' [#cross-database]_.

    >>> new.database_name
    '2'

As seen, the database_name is available for this cross-database reference,
and not for others.  References to persistent objects, as defined in
seialize.py, have other variations, such as weak references, which are
handled but not discussed here [#instantiation_test]_

Second, notice the __cmp__ behavior [#cmp_test]_.  This is new behavior
after ZODB 3.8 and addresses a serious problem for when persistent
objects are compared in an _p_resolveConflict, such as that in the ZODB
BTrees code.  Prior to this change, it was not safe to use Persistent
objects as keys in a BTree. You needed to define a __cmp__ for them to
be sorted reliably out of the context of conflict resolution, but then
during conflict resolution the sorting would be arbitrary, on the basis
of the persistent reference's memory location.  This could have lead to
inconsistent state for BTrees (or BTree module buckets or tree sets or sets).

Here's an example of how the new behavior stops potentially incorrect
resolution.

    >>> import BTrees
    >>> treeset_A = conn_A.root()['treeset'] = BTrees.family32.OI.TreeSet()
    >>> tm_A.commit()
    >>> trans = tm_B.begin() # sync
    >>> treeset_B = conn_B.root()['treeset']
    >>> treeset_A.insert(PCounter())
    1
    >>> treeset_B.insert(PCounter())
    1
    >>> tm_B.commit()
    >>> tm_A.commit() # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    ConflictError: database conflict error...
    >>> tm_A.abort()

Third, note that, even if the persistent object to which the reference refers
changes in the same transaction, the reference is still the same.

DANGERS: subtle and potentially serious.  Beyond the two subtleties above,
which should now be addressed, there is a general problem for objects that
are composites of smaller persistent objects--for instance, a BTree, in
which the BTree and each bucket is a persistent object; or a
zc.queue.CompositePersistentQueue, which is a persistent queue of
persistent queues.  Consider the following situation.  It is actually solved,
but it is a concrete example of what might go wrong.

A BTree (persistent object) has a two buckets (persistent objects).  The
second bucket has one persistent object in it.  Concurrently, one thread
deletes the one object in the second bucket, which causes the BTree to dump
the bucket; and another thread puts an object in the second bucket.  What
happens during conflict resolution?  Remember, each persistent object cannot
see the other.  From the perspective of the BTree object, it has no
conflicts: one transaction modified it, causing it to lose a bucket; and the
other transaction did not change it.  From the perspective of the bucket,
one transaction deleted an object and the other added it: it will resolve
conflicts and say that the bucket has the new object and not the old one.
However, it will be garbage collected, and effectively the addition of the
new object will be lost.

As mentioned, this story is actually solved for BTrees.  As
BTrees/MergeTemplate.c explains, whenever savedState or newState for a bucket
shows an empty bucket, the code refuses to resolve the conflict: this avoids
the situation above.

    >>> bucket_A = conn_A.root()['bucket'] = BTrees.family32.II.Bucket()
    >>> bucket_A[0] = 255
    >>> tm_A.commit()
    >>> trans = tm_B.begin() # sync
    >>> bucket_B = conn_B.root()['bucket']
    >>> bucket_B[1] = 254
    >>> del bucket_A[0]
    >>> tm_B.commit()
    >>> tm_A.commit() # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    ConflictError: database conflict error...
    >>> tm_A.abort()

However, the story highlights the kinds of subtle problems that units
made up of multiple composite Persistent objects need to contemplate.
Any structure made up of objects that contain persistent objects with
conflict resolution code, as a catalog index is made up of multiple
BTree Buckets and Sets, each with conflict resolution, needs to think
through these kinds of problems or be faced with potential data
integrity issues.

.. cleanup

    >>> db.close()
    >>> db1.close()
    >>> db2.close()

.. ......... ..
.. FOOTNOTES ..
.. ......... ..

.. [#get_persistent_reference] We'll catch persistent references with a class
    mutable.

    ::

        class PCounter3(PCounter):
            data = []
            def _p_resolveConflict(self, oldState, savedState, newState):
                PCounter3.data.append(
                    (oldState.get('other'),
                     savedState.get('other'),
                     newState.get('other')))
                return super(PCounter3, self)._p_resolveConflict(
                    oldState, savedState, newState)

    .. -> src

        >>> exec(src, ConflictResolution_txt.__dict__)
        >>> PCounter3 = ConflictResolution_txt.PCounter3
        >>> PCounter3.__module__ = 'ConflictResolution_txt'

    >>> p3_A = conn_A.root()['p3'] = PCounter3()
    >>> p3_A.other = conn_A.root()['p']
    >>> tm_A.commit()
    >>> trans = tm_B.begin() # sync
    >>> p3_B = conn_B.root()['p3']
    >>> p3_A.inc()
    >>> p3_B.inc()
    >>> tm_B.commit()
    >>> tm_A.commit()
    >>> old, saved, new = PCounter3.data[-1]

.. [#cross-database] We need a whole different set of databases for this.
    See cross-database-references.txt in this directory for a discussion of
    what is going on here.

    >>> databases = {}
    >>> db1 = ZODB.DB('1', databases=databases, database_name='1')
    >>> db2 = ZODB.DB('2', databases=databases, database_name='2')
    >>> tm_multi_A = transaction.TransactionManager()
    >>> conn_1A = db1.open(transaction_manager=tm_multi_A)
    >>> conn_2A = conn_1A.get_connection('2')
    >>> p4_1A = conn_1A.root()['p4'] = PCounter3()
    >>> p5_2A = conn_2A.root()['p5'] = PCounter3()
    >>> conn_2A.add(p5_2A)
    >>> p4_1A.other = p5_2A
    >>> tm_multi_A.commit()
    >>> tm_multi_B = transaction.TransactionManager()
    >>> conn_1B = db1.open(transaction_manager=tm_multi_B)
    >>> p4_1B = conn_1B.root()['p4']
    >>> p4_1A.inc()
    >>> p4_1B.inc()
    >>> tm_multi_B.commit()
    >>> tm_multi_A.commit()
    >>> old, saved, new = PCounter3.data[-1]

.. [#instantiation_test] We'll simply instantiate PersistentReferences
    with examples of types described in ZODB/serialize.py.

    >>> from ZODB.ConflictResolution import PersistentReference

    >>> ref1 = PersistentReference(b'my_oid')
    >>> ref1.oid
    'my_oid'
    >>> print(ref1.klass)
    None
    >>> print(ref1.database_name)
    None
    >>> ref1.weak
    False

    >>> ref2 = PersistentReference((b'my_oid', 'my_class'))
    >>> ref2.oid
    'my_oid'
    >>> ref2.klass
    'my_class'
    >>> print(ref2.database_name)
    None
    >>> ref2.weak
    False

    >>> ref3 = PersistentReference(['w', (b'my_oid',)])
    >>> ref3.oid
    'my_oid'
    >>> print(ref3.klass)
    None
    >>> print(ref3.database_name)
    None
    >>> ref3.weak
    True

    >>> ref3a = PersistentReference(['w', (b'my_oid', 'other_db')])
    >>> ref3a.oid
    'my_oid'
    >>> print(ref3a.klass)
    None
    >>> ref3a.database_name
    'other_db'
    >>> ref3a.weak
    True

    >>> ref4 = PersistentReference(['m', ('other_db', b'my_oid', 'my_class')])
    >>> ref4.oid
    'my_oid'
    >>> ref4.klass
    'my_class'
    >>> ref4.database_name
    'other_db'
    >>> ref4.weak
    False

    >>> ref5 = PersistentReference(['n', ('other_db', b'my_oid')])
    >>> ref5.oid
    'my_oid'
    >>> print(ref5.klass)
    None
    >>> ref5.database_name
    'other_db'
    >>> ref5.weak
    False

    >>> ref6 = PersistentReference([b'my_oid']) # legacy
    >>> ref6.oid
    'my_oid'
    >>> print(ref6.klass)
    None
    >>> print(ref6.database_name)
    None
    >>> ref6.weak
    True

.. [#cmp_test] All references are equal to themselves.

    >>> ref1 == ref1 and ref2 == ref2 and ref4 == ref4 and ref5 == ref5
    True
    >>> ref3 == ref3 and ref3a == ref3a and ref6 == ref6 # weak references
    True

    Non-weak references with the same oid and database_name are equal.

    >>> ref1 == ref2 and ref4 == ref5
    True

    Everything else raises a ValueError: weak references with the same oid and
    database, and references with a different database_name or oid.

    >>> ref3 == ref6
    Traceback (most recent call last):
    ...
    ValueError: can't reliably compare against different PersistentReferences

    >>> ref1 == PersistentReference(('another_oid', 'my_class'))
    Traceback (most recent call last):
    ...
    ValueError: can't reliably compare against different PersistentReferences

    >>> ref4 == PersistentReference(
    ...     ['m', ('another_db', 'my_oid', 'my_class')])
    Traceback (most recent call last):
    ...
    ValueError: can't reliably compare against different PersistentReferences
