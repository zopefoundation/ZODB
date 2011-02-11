##############################################################################
#
# Copyright (c) 2007 Zope Foundation and Contributors.
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
import manuel.doctest
import manuel.footnote
import manuel.capture
import manuel.testing
import ZODB.ConflictResolution
import ZODB.tests.util
import zope.testing.module

def setUp(test):
    ZODB.tests.util.setUp(test)
    zope.testing.module.setUp(test, 'ConflictResolution_txt')

def tearDown(test):
    zope.testing.module.tearDown(test)
    ZODB.tests.util.tearDown(test)
    ZODB.ConflictResolution._class_cache.clear()

def test_suite():
    return manuel.testing.TestSuite(
        manuel.doctest.Manuel()
        + manuel.footnote.Manuel()
        + manuel.capture.Manuel(),
        '../ConflictResolution.txt',
        setUp=setUp, tearDown=tearDown,
        )

