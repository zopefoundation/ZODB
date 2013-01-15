##############################################################################
#
# Copyright (c) 2007 Zope Foundation and Contributors.
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
import unittest


def _setUp(test):
    from ZODB.ConflictResolution import _class_cache
    from ZODB.ConflictResolution import _unresolvable
    from zope.testing.module import setUp
    from ZODB.tests.util import setUp as util_setUp
    util_setUp(test)
    setUp(test, 'ConflictResolution_txt')
    _class_cache.clear()
    _unresolvable.clear()

def _tearDown(test):
    from ZODB.ConflictResolution import _class_cache
    from ZODB.ConflictResolution import _unresolvable
    from zope.testing.module import tearDown
    from ZODB.tests.util import tearDown as util_tearDown
    tearDown(test)
    util_tearDown(test)
    _class_cache.clear()
    _unresolvable.clear()


def succeed_with_resolution_when_state_is_unchanged():
    """
    If a conflicting change doesn't change the state, then don't even
    bother calling _p_resolveConflict

    >>> import transaction
    >>> from ZODB.DB import DB
    >>> db = DB('t.fs') # FileStorage!
    >>> storage = db.storage
    >>> conn = db.open()
    >>> from ZODB.tests.examples import ResolveableWhenStateDoesNotChange
    >>> conn.root.x = ResolveableWhenStateDoesNotChange()
    >>> conn.root.x.v = 1
    >>> transaction.commit()
    >>> serial1 = conn.root.x._p_serial
    >>> conn.root.x.v = 2
    >>> transaction.commit()
    >>> serial2 = conn.root.x._p_serial
    >>> oid = conn.root.x._p_oid

So, let's try resolving when the old and committed states are the same
bit the new state (pickle) is different:

    >>> p = storage.tryToResolveConflict(
    ...         oid, serial1, serial1, storage.loadSerial(oid, serial2))

    >>> p == storage.loadSerial(oid, serial2)
    True


And when the old and new states are the same bit the committed state
is different:

    >>> p = storage.tryToResolveConflict(
    ...         oid, serial2, serial1, storage.loadSerial(oid, serial1))

    >>> p == storage.loadSerial(oid, serial2)
    True

But we still conflict if both the committed and new are different than
the original:

    >>> p = storage.tryToResolveConflict(
    ...         oid, serial2, serial1, storage.loadSerial(oid, serial2))
    ... # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    ConflictError: database conflict error (oid 0x01, ...


Of course, none of this applies if content doesn't support conflict resolution.

    >>> from ZODB.tests.examples import Unresolvable
    >>> conn.root.y = Unresolvable()
    >>> conn.root.y.v = 1
    >>> transaction.commit()
    >>> oid = conn.root.y._p_oid
    >>> serial = conn.root.y._p_serial

    >>> p = storage.tryToResolveConflict(
    ...         oid, serial, serial, storage.loadSerial(oid, serial))
    ... # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    ConflictError: database conflict error (oid 0x02, ...

    >>> db.close()
    """

def resolve_even_when_referenced_classes_are_absent():
    """

We often want to be able to resolve even when there are pesistent
references to classes that can't be imported.

    >>> from persistent import Persistent
    >>> class P(Persistent):
    ...     pass

    >>> import transaction
    >>> from ZODB.DB import DB
    >>> from ZODB.tests.examples import Resolveable
    >>> db = DB('t.fs') # FileStorage!
    >>> storage = db.storage
    >>> conn = db.open()
    >>> conn.root.x = Resolveable()
    >>> transaction.commit()
    >>> oid = conn.root.x._p_oid
    >>> serial = conn.root.x._p_serial

    >>> conn.root.x.a = P()
    >>> transaction.commit()
    >>> aid = conn.root.x.a._p_oid
    >>> serial1 = conn.root.x._p_serial

    >>> del conn.root.x.a
    >>> conn.root.x.b = P()
    >>> transaction.commit()
    >>> serial2 = conn.root.x._p_serial

Bwahaha:

    >>> P_aside = P
    >>> del P

Now, even though we can't import P, we can still resolve the conflict:

    >>> p = storage.tryToResolveConflict(
    ...         oid, serial1, serial, storage.loadSerial(oid, serial2))

And load the pickle:

    >>> conn2 = db.open()
    >>> P = P_aside
    >>> p = conn2._reader.getState(p)
    >>> sorted(p), p['a'] is conn2.get(aid), p['b'] is conn2.root.x.b
    (['a', 'b'], True, True)

    >>> isinstance(p['a'], P) and isinstance(p['b'], P)
    True


Oooooof course, this won't work if the subobjects aren't persistent:

    >>> class NP:
    ...     pass


    >>> conn.root.x = Resolveable()
    >>> transaction.commit()
    >>> oid = conn.root.x._p_oid
    >>> serial = conn.root.x._p_serial

    >>> conn.root.x.a = a = NP()
    >>> transaction.commit()
    >>> serial1 = conn.root.x._p_serial

    >>> del conn.root.x.a
    >>> conn.root.x.b = b = NP()
    >>> transaction.commit()
    >>> serial2 = conn.root.x._p_serial

Bwahaha:

    >>> del NP


    >>> storage.tryToResolveConflict(
    ...         oid, serial1, serial, storage.loadSerial(oid, serial2))
    ... # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    ConflictError: database conflict error (oid ...

    >>> db.close()
    """


def resolve_even_when_xdb_referenced_classes_are_absent():
    """Cross-database persistent refs!

    >>> from persistent import Persistent
    >>> class P(Persistent):
    ...     pass

    >>> import transaction
    >>> from ZODB.DB import DB
    >>> databases = {}
    >>> db = DB('t.fs', databases=databases, database_name='')
    >>> db2 = DB('o.fs', databases=databases, database_name='o')
    >>> storage = db.storage
    >>> conn = db.open()
    >>> from ZODB.tests.examples import Resolveable
    >>> conn.root.x = Resolveable()
    >>> transaction.commit()
    >>> oid = conn.root.x._p_oid
    >>> serial = conn.root.x._p_serial

    >>> p = P(); conn.get_connection('o').add(p)
    >>> conn.root.x.a = p
    >>> transaction.commit()
    >>> aid = conn.root.x.a._p_oid
    >>> serial1 = conn.root.x._p_serial

    >>> del conn.root.x.a
    >>> p = P(); conn.get_connection('o').add(p)
    >>> conn.root.x.b = p
    >>> transaction.commit()
    >>> serial2 = conn.root.x._p_serial

    >>> del p

Bwahaha:

    >>> P_aside = P
    >>> del P

Now, even though we can't import P, we can still resolve the conflict:

    >>> p = storage.tryToResolveConflict(
    ...         oid, serial1, serial, storage.loadSerial(oid, serial2))

And load the pickle:

    >>> conn2 = db.open()
    >>> conn2o = conn2.get_connection('o')
    >>> P = P_aside
    >>> p = conn2._reader.getState(p)
    >>> sorted(p), p['a'] is conn2o.get(aid), p['b'] is conn2.root.x.b
    (['a', 'b'], True, True)

    >>> isinstance(p['a'], P) and isinstance(p['b'], P)
    True

    >>> db.close()
    >>> db2.close()
    """

def test_suite():
    import doctest
    import manuel.doctest
    import manuel.footnote
    import manuel.capture
    import manuel.testing
    return unittest.TestSuite((
        manuel.testing.TestSuite(
            manuel.doctest.Manuel()
            + manuel.footnote.Manuel()
            + manuel.capture.Manuel(),
            '../ConflictResolution.txt',
            setUp=_setUp,
            tearDown=_tearDown,
            ),
        doctest.DocTestSuite(
            setUp=_setUp,
            tearDown=_tearDown),
    ))
