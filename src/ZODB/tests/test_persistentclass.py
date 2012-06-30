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

from doctest import DocTestSuite 
import unittest


def test_new_ghost_w_persistent_class():
    """
    Peristent meta classes work with PickleCache.new_ghost:

    >>> import ZODB.persistentclass

    >>> class PC:
    ...     __metaclass__ = ZODB.persistentclass.PersistentMetaClass

    >>> PC._p_oid
    >>> PC._p_jar
    >>> PC._p_serial
    >>> PC._p_changed
    False

    >>> import persistent
    >>> jar = object()
    >>> cache = persistent.PickleCache(jar, 10, 100)
    >>> cache.new_ghost('1', PC)

    >>> PC._p_oid
    '1'
    >>> PC._p_jar is jar
    True
    >>> PC._p_serial
    >>> PC._p_changed
    False
    """


def test_suite():
    return unittest.TestSuite((
        DocTestSuite(),
    ))
