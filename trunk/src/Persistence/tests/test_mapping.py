##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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
"""XXX short summary goes here.

$Id: test_mapping.py,v 1.2 2003/12/29 22:40:46 tim_one Exp $
"""
import unittest
from doctest import DocTestSuite
from Persistence import PersistentMapping

def test_basic_functionality():
    """
    >>> m = PersistentMapping({'x': 1}, a=2, b=3)
    >>> m['name'] = 'bob'
    >>> m['fred']
    Traceback (most recent call last):
    ...
    KeyError: 'fred'
    >>> m.get('fred')
    >>> m.get('fred', 42)
    42
    >>> m.get('name', 42)
    'bob'
    >>> m.get('name')
    'bob'
    >>> m['name']
    'bob'

    >>> keys = m.keys()
    >>> keys.sort()
    >>> keys
    ['a', 'b', 'name', 'x']

    >>> values = m.values()
    >>> values.sort()
    >>> values
    [1, 2, 3, 'bob']

    >>> items = m.items()
    >>> items.sort()
    >>> items
    [('a', 2), ('b', 3), ('name', 'bob'), ('x', 1)]

    >>> keys = list(m.iterkeys())
    >>> keys.sort()
    >>> keys
    ['a', 'b', 'name', 'x']

    >>> values = list(m.itervalues())
    >>> values.sort()
    >>> values
    [1, 2, 3, 'bob']

    >>> items = list(m.iteritems())
    >>> items.sort()
    >>> items
    [('a', 2), ('b', 3), ('name', 'bob'), ('x', 1)]

    >>> 'name' in m
    True

    """

def test_old_pickles():
    """
    >>> m = PersistentMapping()
    >>> m.__setstate__({'_container': {'x': 1, 'y': 2}})
    >>> items = m.items()
    >>> items.sort()
    >>> items
    [('x', 1), ('y', 2)]

    """

def test_suite():
    return unittest.TestSuite((
        DocTestSuite(),
        ))

if __name__ == '__main__': unittest.main()
