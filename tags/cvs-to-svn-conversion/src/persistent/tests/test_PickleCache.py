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
"""Unit tests for PickleCache

$Id: test_PickleCache.py,v 1.2 2004/02/19 02:59:32 jeremy Exp $
"""

class DummyConnection:

    def setklassstate(self, obj):
        """Method used by PickleCache."""


def test_delitem():
    """
    >>> from persistent import PickleCache
    >>> conn = DummyConnection()
    >>> cache = PickleCache(conn)
    >>> del cache['']
    Traceback (most recent call last):
    ...
    KeyError: ''
    >>> from persistent import Persistent
    >>> p = Persistent()
    >>> p._p_oid = 'foo'
    >>> p._p_jar = conn
    >>> cache['foo'] = p
    >>> del cache['foo']

    """

from doctest import DocTestSuite
import unittest

def test_suite():
    return unittest.TestSuite((
        DocTestSuite(),
        ))

if __name__ == '__main__':
    unittest.main()
