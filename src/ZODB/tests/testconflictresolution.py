##############################################################################
#
# Copyright (c) 2007 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
import manuel.doctest
import manuel.footnote
import doctest
import manuel.capture
import manuel.testing
import persistent
import transaction
import unittest
import ZODB.ConflictResolution
import ZODB.tests.util
import ZODB.POSException
import zope.testing.module

def setUp(test):
    ZODB.tests.util.setUp(test)
    zope.testing.module.setUp(test, 'ConflictResolution_txt')
    ZODB.ConflictResolution._class_cache.clear()
    ZODB.ConflictResolution._unresolvable.clear()

def tearDown(test):
    zope.testing.module.tearDown(test)
    ZODB.tests.util.tearDown(test)
    ZODB.ConflictResolution._class_cache.clear()
    ZODB.ConflictResolution._unresolvable.clear()


class ResolveableWhenStateDoesNotChange(persistent.Persistent):

    def _p_resolveConflict(old, committed, new):
        raise ZODB.POSException.ConflictError

class Unresolvable(persistent.Persistent):
    pass

def succeed_with_resolution_when_state_is_unchanged():
    """
    If a conflicting change doesn't change the state, then don't even
    bother calling _p_resolveConflict

    >>> db = ZODB.DB('t.fs') # FileStorage!
    >>> storage = db.storage
    >>> conn = db.open()
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

class Resolveable(persistent.Persistent):

    def _p_resolveConflict(self, old, committed, new):

        resolved = {}
        for k in old:
            if k not in committed:
                if k in new and new[k] == old[k]:
                    continue
                raise ZODB.POSException.ConflictError
            if k not in new:
                if k in committed and committed[k] == old[k]:
                    continue
                raise ZODB.POSException.ConflictError
            if committed[k] != old[k]:
                if new[k] == old[k]:
                    resolved[k] = committed[k]
                    continue
                raise ZODB.POSException.ConflictError
            if new[k] != old[k]:
                if committed[k] == old[k]:
                    resolved[k] = new[k]
                    continue
                raise ZODB.POSException.ConflictError
            resolved[k] = old[k]

        for k in new:
            if k in old:
                continue
            if k in committed:
                raise ZODB.POSException.ConflictError
            resolved[k] = new[k]

        for k in committed:
            if k in old:
                continue
            if k in new:
                raise ZODB.POSException.ConflictError
            resolved[k] = committed[k]

        return resolved

def resolve_even_when_referenced_classes_are_absent():
    """

We often want to be able to resolve even when there are pesistent
references to classes that can't be imported.

    >>> class P(persistent.Persistent):
    ...     pass

    >>> db = ZODB.DB('t.fs') # FileStorage!
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





def test_suite():
    return unittest.TestSuite([
        manuel.testing.TestSuite(
            manuel.doctest.Manuel()
            + manuel.footnote.Manuel()
            + manuel.capture.Manuel(),
            '../ConflictResolution.txt',
            setUp=setUp, tearDown=tearDown,
            ),
        doctest.DocTestSuite(
            setUp=setUp, tearDown=tearDown),
        ])

