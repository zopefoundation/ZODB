##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""ZODB-based persistent weakrefs

$Id$
"""

__docformat__ = "reStructuredText"

from persistent import Persistent
import transaction

WeakRefMarker = object()

class WeakRef(object):
    """Persistent weak references

    Persistent weak references are used much like Python weak
    references.  The major difference is that you can't specify an
    object to be called when the object is removed from the database.

    Here's an example. We'll start by creating a persistent object and
    a reference to it:

    >>> import persistent, ZODB.tests.MinPO
    >>> import ZODB.tests.util
    >>> ob = ZODB.tests.MinPO.MinPO()
    >>> ref = WeakRef(ob)
    >>> ref() is ob
    True

    The hash of the ref if the same as the hash of the referenced object:

    >>> hash(ref) == hash(ob)
    True

    Two refs to the same object are equal:

    >>> WeakRef(ob) == ref
    True

    >>> ob2 = ZODB.tests.MinPO.MinPO(1)
    >>> WeakRef(ob2) == ref
    False

    Lets save the reference and the referenced object in a database:

    >>> db = ZODB.tests.util.DB()

    >>> conn1 = db.open()
    >>> conn1.root()['ob'] = ob
    >>> conn1.root()['ref'] = ref
    >>> transaction.commit()

    If we open a new connection, we can use the reference:

    >>> conn2 = db.open()
    >>> conn2.root()['ref']() is conn2.root()['ob']
    True
    >>> hash(conn2.root()['ref']) == hash(conn2.root()['ob'])
    True

    But if we delete the referenced object and pack:

    >>> del conn2.root()['ob']
    >>> transaction.commit()
    >>> ZODB.tests.util.pack(db)

    And then look in a new connection:

    >>> conn3 = db.open()
    >>> conn3.root()['ob']
    Traceback (most recent call last):
    ...
    KeyError: 'ob'

    Trying to dereference the reference returns None:

    >>> conn3.root()['ref']()

    Trying to get a hash, raises a type error:

    >>> hash(conn3.root()['ref'])
    Traceback (most recent call last):
    ...
    TypeError: Weakly-referenced object has gone away

    Always explicitly close databases: :)

    >>> db.close()
    >>> del ob, ref, db, conn1, conn2, conn3

    When multiple databases are in use, a weakref in one database may
    point to an object in a different database.  Let's create two new
    databases to demonstrate this.

    >>> dbA = ZODB.tests.util.DB(
    ...     database_name = 'dbA',
    ...     )
    >>> dbB = ZODB.tests.util.DB(
    ...     database_name = 'dbB',
    ...     databases = dbA.databases,
    ...     )
    >>> connA1 = dbA.open()
    >>> connB1 = connA1.get_connection('dbB')

    Now create and add a new object and a weak reference, and add them
    to different databases.

    >>> ob = ZODB.tests.MinPO.MinPO()
    >>> ref = WeakRef(ob)
    >>> connA1.root()['ob'] = ob
    >>> connA1.add(ob)
    >>> connB1.root()['ref'] = ref
    >>> transaction.commit()

    After a succesful commit, the reference should know the oid,
    database name and connection of the object.

    >>> ref.oid == ob._p_oid
    True
    >>> ref.database_name == 'dbA'
    True
    >>> ref.dm is ob._p_jar is connA1
    True

    If we open new connections, we should be able to use the reference.

    >>> connA2 = dbA.open()
    >>> connB2 = connA2.get_connection('dbB')
    >>> ref2 = connB2.root()['ref']
    >>> ob2 = connA2.root()['ob']
    >>> ref2() is ob2
    True
    >>> ref2.oid == ob2._p_oid
    True
    >>> ref2.database_name == 'dbA'
    True
    >>> ref2.dm is ob2._p_jar is connA2
    True

    Always explicitly close databases: :)

    >>> dbA.close()
    >>> dbB.close()

    """

    # We set _p_oid to a marker so that the serialization system can
    # provide special handling of weakrefs.
    _p_oid = WeakRefMarker

    def __init__(self, ob):
        self._v_ob = ob
        self.oid = ob._p_oid
        self.dm = ob._p_jar
        if self.dm is not None:
            self.database_name = self.dm.db().database_name

    def __call__(self):
        try:
            return self._v_ob
        except AttributeError:
            try:
                self._v_ob = self.dm[self.oid]
            except (KeyError, AttributeError):
                return None
            return self._v_ob

    def __hash__(self):
        self = self()
        if self is None:
            raise TypeError('Weakly-referenced object has gone away')
        return hash(self)

    def __eq__(self, other):
        self = self()
        if self is None:
            raise TypeError('Weakly-referenced object has gone away')
        other = other()
        if other is None:
            raise TypeError('Weakly-referenced object has gone away')

        return self == other


class PersistentWeakKeyDictionary(Persistent):
    """Persistent weak key dictionary

    This is akin to WeakKeyDictionaries. Note, however, that removal
    of items is extremely lazy. See below.

    We'll start by creating a PersistentWeakKeyDictionary and adding
    some persistent objects to it.

    >>> d = PersistentWeakKeyDictionary()
    >>> import ZODB.tests.util
    >>> p1 = ZODB.tests.util.P('p1')
    >>> p2 = ZODB.tests.util.P('p2')
    >>> p3 = ZODB.tests.util.P('p3')
    >>> d[p1] = 1
    >>> d[p2] = 2
    >>> d[p3] = 3

    We'll create an extra persistent object that's not in the dict:

    >>> p4 = ZODB.tests.util.P('p4')

    Now we'll excercise iteration and item access:

    >>> l = [(str(k), d[k], d.get(k)) for k in d]
    >>> l.sort()
    >>> l
    [('P(p1)', 1, 1), ('P(p2)', 2, 2), ('P(p3)', 3, 3)]

    And the containment operator:

    >>> [p in d for p in [p1, p2, p3, p4]]
    [True, True, True, False]

    We can add the dict and the referenced objects to a database:

    >>> db = ZODB.tests.util.DB()

    >>> conn1 = db.open()
    >>> conn1.root()['p1'] = p1
    >>> conn1.root()['d'] = d
    >>> conn1.root()['p2'] = p2
    >>> conn1.root()['p3'] = p3
    >>> transaction.commit()

    And things still work, as before:

    >>> l = [(str(k), d[k], d.get(k)) for k in d]
    >>> l.sort()
    >>> l
    [('P(p1)', 1, 1), ('P(p2)', 2, 2), ('P(p3)', 3, 3)]
    >>> [p in d for p in [p1, p2, p3, p4]]
    [True, True, True, False]

    Likewise, we can read the objects from another connection and
    things still work.

    >>> conn2 = db.open()
    >>> d = conn2.root()['d']
    >>> p1 = conn2.root()['p1']
    >>> p2 = conn2.root()['p2']
    >>> p3 = conn2.root()['p3']
    >>> l = [(str(k), d[k], d.get(k)) for k in d]
    >>> l.sort()
    >>> l
    [('P(p1)', 1, 1), ('P(p2)', 2, 2), ('P(p3)', 3, 3)]
    >>> [p in d for p in [p1, p2, p3, p4]]
    [True, True, True, False]

    Now, we'll delete one of the objects from the database, but *not*
    from the dictionary:

    >>> del conn2.root()['p2']
    >>> transaction.commit()

    And pack the database, so that the no-longer referenced p2 is
    actually removed from the database.

    >>> ZODB.tests.util.pack(db)

    Now if we access the dictionary in a new connection, it no longer
    has p2:

    >>> conn3 = db.open()
    >>> d = conn3.root()['d']
    >>> l = [(str(k), d[k], d.get(k)) for k in d]
    >>> l.sort()
    >>> l
    [('P(p1)', 1, 1), ('P(p3)', 3, 3)]

    It's worth nothing that that the versions of the dictionary in
    conn1 and conn2 still have p2, because p2 is still in the caches
    for those connections.

    Always explicitly close databases: :)

    >>> db.close()

    """
    # TODO:  It's expensive trying to load dead objects from the database.
    # It would be helpful if the data manager/connection cached these.

    def __init__(self, adict=None, **kwargs):
        self.data = {}
        if adict is not None:
            keys = getattr(adict, "keys", None)
            if keys is None:
                adict = dict(adict)
            self.update(adict)
        if kwargs:
            self.update(kwargs)

    def __getstate__(self):
        state = Persistent.__getstate__(self)
        state['data'] = state['data'].items()
        return state

    def __setstate__(self, state):
        state['data'] = dict([
            (k, v) for (k, v) in state['data']
            if k() is not None
            ])
        Persistent.__setstate__(self, state)

    def __setitem__(self, key, value):
        self.data[WeakRef(key)] = value

    def __getitem__(self, key):
        return self.data[WeakRef(key)]

    def __delitem__(self, key):
        del self.data[WeakRef(key)]

    def get(self, key, default=None):
        """D.get(k[, d]) -> D[k] if k in D, else d.

        >>> import ZODB.tests.util
        >>> key = ZODB.tests.util.P("key")
        >>> missing = ZODB.tests.util.P("missing")
        >>> d = PersistentWeakKeyDictionary([(key, 1)])
        >>> d.get(key)
        1
        >>> d.get(missing)
        >>> d.get(missing, 12)
        12
        """
        return self.data.get(WeakRef(key), default)

    def __contains__(self, key):
        return WeakRef(key) in self.data

    def __iter__(self):
        for k in self.data:
            yield k()

    def update(self, adict):
        if isinstance(adict, PersistentWeakKeyDictionary):
            self.data.update(adict.update)
        else:
            for k, v in adict.items():
                self.data[WeakRef(k)] = v

    # TODO:  May need more methods, and tests.
