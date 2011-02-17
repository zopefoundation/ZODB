##############################################################################
#
# Copyright (c) 2003 Zope Foundation and Contributors.
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

class DummyConnection:

    def setklassstate(self, obj):
        """Method used by PickleCache."""


def test_delitem():
    """
    >>> from persistent import PickleCache
    >>> conn = DummyConnection()
    >>> cache = PickleCache(conn)
    >>> del cache[''] # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    KeyError:...
    >>> from persistent import Persistent
    >>> class Derived(Persistent):
    ...    pass
    >>> p = Derived()
    >>> p._p_oid = 'foo'
    >>> p._p_jar = conn
    >>> cache['foo'] = p
    >>> del cache['foo']

    """

def new_ghost():
    """
Creating ghosts (from scratch, as opposed to ghostifying a non-ghost)
in the curremt implementation is rather tricky. IPeristent doesn't
really provide the right interface given that:

- _p_deactivate and _p_invalidate are overridable and could assume
  that the object's state is properly initialized.

- Assigning _p_changed to None or deleting it just calls _p_deactivate
  or _p_invalidate.

The current cache implementation is intimately tied up with the
persistence implementation and has internal access to the persistence
state.  The cache implementation can update the persistence state for
newly created and ininitialized objects directly.

The future persistence and cache implementations will be far more
decoupled. The persistence implementation will only manage object
state and generate object-usage events.  The cache implemnentation(s)
will be rersponsible for managing persistence-related (meta-)state,
such as _p_state, _p_changed, _p_oid, etc.  So in that future
implemention, the cache will be more central to managing object
persistence information.

Caches have a new_ghost method that:

- adds an object to the cache, and
- initializes its persistence data.

    >>> import persistent

    >>> class C(persistent.Persistent):
    ...     pass

    >>> from persistent.tests.utils import ResettingJar
    >>> jar = ResettingJar()
    >>> cache = persistent.PickleCache(jar, 10, 100)
    >>> ob = C.__new__(C)
    >>> cache.new_ghost('1', ob)

    >>> ob._p_changed
    >>> ob._p_jar is jar
    True
    >>> ob._p_oid
    '1'

    >>> cache.cache_non_ghost_count
    0

    <<< cache.total_estimated_size # WTF?
    0


Peristent meta classes work too:

    >>> import ZODB.persistentclass
    >>> class PC:
    ...     __metaclass__ = ZODB.persistentclass.PersistentMetaClass

    >>> PC._p_oid
    >>> PC._p_jar
    >>> PC._p_serial
    >>> PC._p_changed
    False

    >>> cache.new_ghost('2', PC)
    >>> PC._p_oid
    '2'
    >>> PC._p_jar is jar
    True
    >>> PC._p_serial
    >>> PC._p_changed
    False

    """

if 0: # this test doesn't belong here!
    def cache_invalidate_and_minimize_used_to_leak_None_ref():
        """Persistent weak references

        >>> import transaction
        >>> import ZODB.tests.util

        >>> db = ZODB.tests.util.DB()

        >>> conn = db.open()
        >>> conn.root.p = p = conn.root().__class__()
        >>> transaction.commit()

        >>> import sys
        >>> old = sys.getrefcount(None)
        >>> conn._cache.invalidate(p._p_oid)
        >>> sys.getrefcount(None) - old
        0

        >>> _ = conn.root.p.keys()
        >>> old = sys.getrefcount(None)
        >>> conn._cache.minimize()
        >>> sys.getrefcount(None) - old
        0

        >>> db.close()

        """


import os
if os.environ.get('USE_ZOPE_TESTING_DOCTEST'):
    from zope.testing.doctest import DocTestSuite
else:
    from doctest import DocTestSuite

import unittest

def test_suite():
    return unittest.TestSuite((
        DocTestSuite(),
        ))

if __name__ == '__main__':
    unittest.main()
