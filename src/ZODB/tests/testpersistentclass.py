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
import sys
import unittest

import transaction

import ZODB.persistentclass
import ZODB.tests.util


def class_with_circular_ref_to_self():
    """
It should be possible for a class to reger to itself.

    >>> C = ZODB.persistentclass.PersistentMetaClass('C', (object,), {})

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


def test_new_ghost_w_persistent_class():
    """
    Peristent meta classes work with PickleCache.new_ghost:

    >>> import ZODB.persistentclass

    >>> PC = ZODB.persistentclass.PersistentMetaClass('PC', (object,), {})

    >>> PC._p_oid
    >>> PC._p_jar
    >>> PC._p_serial
    >>> PC._p_changed
    False

    >>> import persistent
    >>> jar = object()
    >>> cache = persistent.PickleCache(jar, 10, 100)
    >>> cache.new_ghost(b'1', PC)

    >>> PC._p_oid == b'1'
    True
    >>> PC._p_jar is jar
    True
    >>> PC._p_serial
    >>> PC._p_changed
    False
    """

# XXX need to update files to get newer testing package


class FakeModule(object):
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
        doctest.DocFileSuite(
            "../persistentclass.rst",
            setUp=setUp, tearDown=tearDown,
            checker=ZODB.tests.util.checker,
            optionflags=doctest.ELLIPSIS),
        doctest.DocTestSuite(setUp=setUp, tearDown=tearDown),
    ))
