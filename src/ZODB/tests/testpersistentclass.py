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
import unittest

def class_with_circular_ref_to_self():
    """
It should be possible for a class to refer to itself.

    >>> import transaction
    >>> from ZODB.persistentclass import PersistentMetaClass
    >>> from ZODB.DB import DB
    >>> class C:
    ...     __metaclass__ = PersistentMetaClass

    >>> C.me = C
    >>> db = DB(None)
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


def _setUp(test):
    import sys
    from ZODB.DB import DB
    from ZODB.tests.util import setUp as util_setUp
    util_setUp(test)
    test.globs['some_database'] = DB(None)
    module = FakeModule('ZODB.persistentclass_txt', test.globs)
    sys.modules[module.__name__] = module

def _tearDown(test):
    import sys
    from ZODB.tests.util import tearDown as util_tearDown
    test.globs['some_database'].close()
    del sys.modules['ZODB.persistentclass_txt']
    util_tearDown(test)

def test_suite():
    import doctest
    return unittest.TestSuite((
        doctest.DocFileSuite("../persistentclass.txt",
                             setUp=_setUp, tearDown=_tearDown),
        doctest.DocTestSuite(setUp=_setUp, tearDown=_tearDown),
    ))
