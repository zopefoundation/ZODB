##############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
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
"""Test behavior of Connection plus cPickleCache.

Methods involved:
get()
add()
cacheFullSweep()
cacheMinimize()
invalidate()

Other:
resetCache()

cache internal issues:
cache gets full
when incrgc is called
gc

Need to cover various transaction boundaries:
commit
abort
sub-transaction commit / abort
-- can provide our own txn implementation
"""

import doctest
import unittest

from persistent import Persistent
from ZODB.config import databaseFromString

class RecalcitrantObject(Persistent):
    """A Persistent object that will not become a ghost."""

    deactivations = 0

    def _p_deactivate(self):
        self.__class__.deactivations += 1

class RegularObject(Persistent):

    deactivations = 0
    invalidations = 0

    def _p_deactivate(self):
        self.__class__.deactivations += 1
        super(RegularObject, self)._p_deactivate()

    def _p_invalidate(self):
        self.__class__.invalidations += 1
        super(RegularObject, self)._p_invalidate()

class CacheTests(unittest.TestCase):

    def test_cache(self):
        r"""Test basic cache methods.
        
        >>> db = databaseFromString("<zodb>\n"
        ...                         "cache-size 4\n"
        ...                         "<mappingstorage/>\n"
        ...                         "</zodb>")
        >>> cn = db.open()
        >>> r = cn.root()
        >>> L = []
        >>> for i in range(5):
        ...     o = RegularObject()
        ...     L.append(o)
        ...     r[i] = o
        >>> get_transaction().commit()

        After committing a transaction and calling cacheGC(), there
        should be cache-size (4) objects in the cache.  One of the
        RegularObjects was deactivated.
        
        >>> cn._cache.ringlen()
        4
        >>> RegularObject.deactivations
        1

        If we explicitly activate the objects again, the ringlen
        should go back up to 5.
        
        >>> for o in L:
        ...     o._p_activate()
        >>> cn._cache.ringlen()
        5

        >>> cn.cacheGC()
        >>> cn._cache.ringlen()
        4
        >>> RegularObject.deactivations
        2
        
        >>> cn.cacheMinimize()
        >>> cn._cache.ringlen()
        0
        >>> RegularObject.deactivations
        6

        If we activate all the objects again and mark one as modified,
        then the one object should not be deactivated even by a
        minimize.
        
        >>> for o in L:
        ...     o._p_activate()
        >>> o.attr = 1  
        >>> cn._cache.ringlen()
        5
        >>> cn.cacheMinimize()
        >>> cn._cache.ringlen()
        1
        >>> RegularObject.deactivations
        10
        
        """

    def test_cache_gc_recalcitrant(self):
        r"""Test that a cacheGC() call will return.

        It's possible for a particular object to ignore the
        _p_deactivate() call.  We want to check several things in this
        case.  The cache should called the real _p_deactivate() method
        not the one provided by Persistent.  The cacheGC() call should
        also return when it's looked at each item, regardless of whether
        it became a ghost.

        >>> db = databaseFromString("<zodb>\n"
        ...                         "cache-size 4\n"
        ...                         "<mappingstorage/>\n"
        ...                         "</zodb>")
        >>> cn = db.open()
        >>> r = cn.root()
        >>> L = []
        >>> for i in range(5):
        ...     o = RecalcitrantObject()
        ...     L.append(o)
        ...     r[i] = o
        >>> get_transaction().commit()
        >>> [o._p_state for o in L]
        [0, 0, 0, 0, 0]

        The Connection calls cacheGC() after it commits a transaction.
        Since the cache will now have more objects that it's target size,
        it will call _p_deactivate() on each RecalcitrantObject.
        
        >>> RecalcitrantObject.deactivations
        5
        >>> [o._p_state for o in L]
        [0, 0, 0, 0, 0]

        An explicit call to cacheGC() has the same effect.

        >>> cn.cacheGC()
        >>> RecalcitrantObject.deactivations
        10
        >>> [o._p_state for o in L]
        [0, 0, 0, 0, 0]
        """

def test_suite():
    return doctest.DocTestSuite()
