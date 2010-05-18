##############################################################################
#
# Copyright (c) 2010 Zope Foundation and Contributors.
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
import unittest

def test_fsbucket_string_conversion():
    """
fsBuckets have toString and fromString methods that can be used to
get and set their state very efficiently:

    >>> from BTrees.fsBTree import fsBucket
    >>> b = fsBucket([(c*2, c*6) for c in 'abcdef'])
    >>> import pprint
    >>> b.toString()
    'aabbccddeeffaaaaaabbbbbbccccccddddddeeeeeeffffff'

    >>> b2 = fsBucket().fromString(b.toString())
    >>> b.__getstate__() == b2.__getstate__()
    True

    """

def test_suite():
    return doctest.DocTestSuite()

