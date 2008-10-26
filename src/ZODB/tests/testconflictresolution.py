##############################################################################
#
# Copyright (c) 2007 Zope Corporation and Contributors.
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
"""
$Id$
"""
import unittest
from zope.testing import doctest, module, setupstack

def setUp(test):
    module.setUp(test, 'ConflictResolution_txt')
    setupstack.setUpDirectory(test)

def tearDown(test):
    test.globs['db'].close()
    test.globs['db1'].close()
    test.globs['db2'].close()
    module.tearDown(test)
    setupstack.tearDown(test)

def test_suite():
    return unittest.TestSuite((
        doctest.DocFileSuite('../ConflictResolution.txt',
                             setUp=setUp,
                             tearDown=tearDown,
                             optionflags=doctest.INTERPRET_FOOTNOTES,
                             ),
        ))

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')

