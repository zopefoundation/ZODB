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

class CacheTests(unittest.TestCase):

    def test_cache(self):
        pass

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
