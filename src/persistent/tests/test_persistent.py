##############################################################################
#
# Copyright (c) Zope Foundation and Contributors.
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
import unittest
from zope.testing import doctest
from persistent import Persistent

class P(Persistent):
    def __init__(self):
        self.x = 0
    def inc(self):
        self.x += 1

def cpersistent_setstate_pointer_sanity():
    """
    >>> Persistent().__setstate__({})
    Traceback (most recent call last):
    ...
    TypeError: this object has no instance dictionary

    >>> class C(Persistent): __slots__ = 'x', 'y'
    >>> C().__setstate__(({}, {}))
    Traceback (most recent call last):
    ...
    TypeError: this object has no instance dictionary
    """


def test_suite():
    return unittest.TestSuite((
        doctest.DocFileSuite("persistent.txt", globs={"P": P}),
        doctest.DocTestSuite(),
        ))
