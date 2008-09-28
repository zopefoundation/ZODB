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
from zope.testing import doctest, module

def setUp(test):
    module.setUp(test, 'historical_connections_txt')

def tearDown(test):
    test.globs['db'].close()
    test.globs['db2'].close()
    test.globs['storage'].close()
    test.globs['storage'].cleanup()
    # the DB class masks the module because of __init__ shenanigans
    DB_module = __import__('ZODB.DB', globals(), locals(), ['chicken'])
    DB_module.time = test.globs['original_time']
    module.tearDown(test)

def test_suite():
    return unittest.TestSuite((
        doctest.DocFileSuite('../historical_connections.txt',
                             setUp=setUp,
                             tearDown=tearDown,
                             optionflags=doctest.INTERPRET_FOOTNOTES,
                             ),
        ))

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')

