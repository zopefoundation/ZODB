##############################################################################
#
# Copyright (c) 2004 Zope Foundation and Contributors.
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
"""ZClass tests

$Id$
"""

import os, sys
import unittest
import ZODB.tests.util
import transaction
from zope.testing import doctest
import ZODB.persistentclass

def class_with_circular_ref_to_self():
    """
It should be possible for a class to reger to itself.

    >>> class C:
    ...     __metaclass__ = ZODB.persistentclass.PersistentMetaClass

    >>> C.me = C
    >>> db = ZODB.tests.util.DB()
    >>> conn = db.open()
    >>> conn.root()['C'] = C
    >>> transaction.commit()

    >>> conn2 = db.open()
    >>> C2 = conn2.root()['C']
    >>> c = C2()
    >>> c.__class__.__name__
    'C'
    
"""

# XXX need to update files to get newer testing package
class FakeModule:
    def __init__(self, name, dict):
        self.__dict__ = dict
        self.__name__ = name


def setUp(test):
    test.globs['some_database'] = ZODB.tests.util.DB()
    module = FakeModule('ZODB.persistentclass_txt', test.globs)
    sys.modules[module.__name__] = module

def tearDown(test):
    transaction.abort()
    test.globs['some_database'].close()
    del sys.modules['ZODB.persistentclass_txt']

def test_suite():
    return unittest.TestSuite((
        doctest.DocFileSuite("../persistentclass.txt",
                             setUp=setUp, tearDown=tearDown),
        doctest.DocTestSuite(setUp=setUp, tearDown=tearDown),
        ))

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')

