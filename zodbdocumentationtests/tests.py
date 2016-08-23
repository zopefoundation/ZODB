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
import os
import doctest
import unittest
import manuel.capture
import manuel.doctest
import manuel.testing
import zope.testing.module

from os.path import join

def setUp(test):
    import ZODB
    test.globs.update(
        ZODB=ZODB,
        )
    zope.testing.module.setUp(test)

def tearDown(test):
    zope.testing.module.tearDown(test)

def test_suite():
    here = os.path.dirname(__file__)
    guide = join(here, '..', 'documentation', 'guide')

    return unittest.TestSuite((
        manuel.testing.TestSuite(
            manuel.doctest.Manuel() + manuel.capture.Manuel(),
            join(guide, 'writing-persistent-objects.rst'),
            setUp=setUp, tearDown=tearDown,
            ),
        ))

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')

