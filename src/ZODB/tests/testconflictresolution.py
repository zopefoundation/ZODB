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
import doctest
import unittest

import manuel.capture
import manuel.doctest
import manuel.footnote
import manuel.testing
import persistent
import transaction
import zope.testing.module

import ZODB.ConflictResolution
import ZODB.POSException
import ZODB.tests.util


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

    def _p_resolveConflict(self, old, committed, new):
        if new == old:
            # old -> new diff is empty, so merge is trivial
            committed['resolved'] = 'committed'
            return committed
        elif committed == old:
            # old -> committed diff is empty, so merge is trivial
            new['resolved'] = 'new'
            return new
        # 3-way merge
        raise ZODB.POSException.ConflictError


class Unresolvable(persistent.Persistent):
    pass


def succeed_with_resolution_when_state_is_unchanged():
    """
    If a conflicting change doesn't change the state, then we must still call
    _p_resolveConflict, even if in most cases the result would be either
    committed or new (as shown above in ResolveableWhenStateDoesNotChange).
    One use case is to implement an "asynchronous" cache:
    - Initially, a cache value is not filled (e.g. None is used to describe
      this state).
    - A transaction fills the cache (actually done by a background application)
      (None -> "foo").
    - A concurrent transaction invalidates the cache due to some user action
      (None -> None), and pushes a new background task to fill the cache.
    Then the expected resolved value is None, and not "foo".

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
but the new state (pickle) is different:

    >>> p = storage.tryToResolveConflict(
    ...         oid, serial1, serial1, storage.loadSerial(oid, serial2))

    >>> conn._reader.getState(p)['resolved']
    'new'


And when the old and new states are the same but the committed state
is different:

    >>> p = storage.tryToResolveConflict(
    ...         oid, serial2, serial1, storage.loadSerial(oid, serial1))

    >>> conn._reader.getState(p)['resolved']
    'committed'

But we still conflict if both the committed and new are different than
the original:

    >>> p = storage.tryToResolveConflict(
    ...         oid, serial2, serial1, storage.loadSerial(oid, serial2))
    ... # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    ConflictError: database conflict error (oid 0x01, ...


Of course, there's also no automatic trivial merge if content doesn't support
conflict resolution. Touching an object without change is a common locking
mechanism.

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

    >>> class NP(object):
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

    >>> class P(persistent.Persistent):
    ...     pass

    >>> databases = {}
    >>> db = ZODB.DB('t.fs', databases=databases, database_name='')
    >>> db2 = ZODB.DB('o.fs', databases=databases, database_name='o')
    >>> storage = db.storage
    >>> conn = db.open()
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


class FailHard(persistent.Persistent):

    def _p_resolveConflict(self, old, committed, new):
        raise RuntimeError("epic fail")


def show_tryToResolveConflict_log_output():
    """
    Verify output generated by tryToResolveConflict in the logs

    >>> db = ZODB.DB('t.fs') # FileStorage!
    >>> storage = db.storage
    >>> conn = db.open()
    >>> conn.root.x = FailHard()
    >>> conn.root.x.v = 1
    >>> transaction.commit()
    >>> serial1 = conn.root.x._p_serial
    >>> conn.root.x.v = 2
    >>> transaction.commit()
    >>> serial2 = conn.root.x._p_serial
    >>> oid = conn.root.x._p_oid

Install a log handler to be able to show log entries

    >>> import logging
    >>> from zope.testing.loggingsupport import InstalledHandler
    >>> handler = InstalledHandler('ZODB.ConflictResolution',
    ...     level=logging.DEBUG)

Content fails hard on conflict resolution:

    >>> p = storage.tryToResolveConflict(
    ...         oid, serial2, serial1, storage.loadSerial(oid, serial2))
    ... # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    ConflictError: database conflict error (oid 0x01, ...

Content doesn't support conflict resolution:

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

Let's see what went into the log:

    >>> len(handler.records)
    2

    >>> import six

    >>> msg = handler.records[0]
    >>> six.print_(msg.name, msg.levelname, msg.getMessage())
    ZODB.ConflictResolution ERROR Unexpected error while trying to resolve conflict on <class 'ZODB.tests.testconflictresolution.FailHard'>

    >>> msg = handler.records[1]
    >>> six.print_(msg.name, msg.levelname, msg.getMessage())
    ZODB.ConflictResolution DEBUG Conflict resolution on <class 'ZODB.tests.testconflictresolution.Unresolvable'> failed with ConflictError: database conflict error

Cleanup:

    >>> handler.uninstall()
    >>> db.close()
    """  # noqa: E501 line too long


def test_suite():
    return unittest.TestSuite([
        manuel.testing.TestSuite(
            manuel.doctest.Manuel(checker=ZODB.tests.util.checker)
            + manuel.footnote.Manuel()
            + manuel.capture.Manuel(),
            '../ConflictResolution.rst',
            setUp=setUp, tearDown=tearDown
        ),
        doctest.DocTestSuite(
            setUp=setUp, tearDown=tearDown,
            checker=ZODB.tests.util.checker),
    ])
