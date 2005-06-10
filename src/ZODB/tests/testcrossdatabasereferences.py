##############################################################################
#
# Copyright (c) 2005 Zope Corporation and Contributors.
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
from zope.testing import doctest
import persistent

class MyClass(persistent.Persistent):
    pass

class MyClass_w_getnewargs(persistent.Persistent):

    def __getnewargs__(self):
        return ()

def test_suite():
    return unittest.TestSuite((
        doctest.DocFileSuite('../cross-database-references.txt',
                             globs=dict(MyClass=MyClass),
                             ),
        doctest.DocFileSuite('../cross-database-references.txt',
                             globs=dict(MyClass=MyClass_w_getnewargs),
                             ),
        ))

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')

