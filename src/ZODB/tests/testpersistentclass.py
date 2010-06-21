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
import doctest
import os
import sys
import transaction
import unittest
import ZODB.persistentclass
import ZODB.tests.util

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
    ZODB.tests.util.setUp(test)
    test.globs['some_database'] = ZODB.tests.util.DB()
    module = FakeModule('ZODB.persistentclass_txt', test.globs)
    sys.modules[module.__name__] = module

def tearDown(test):
    test.globs['some_database'].close()
    del sys.modules['ZODB.persistentclass_txt']
    ZODB.tests.util.tearDown(test)

def test_suite():
    return unittest.TestSuite((
        doctest.DocFileSuite("../persistentclass.txt",
                             setUp=setUp, tearDown=tearDown),
        doctest.DocTestSuite(setUp=setUp, tearDown=tearDown),
        ))

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')

