##############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
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
"""XXX short summary goes here.

$Id$
"""
import re, unittest
from zope.testing import doctest, renormalizing

def test_suite():
    return unittest.TestSuite((
        doctest.DocFileSuite(
            'zeopack.test',
            checker=renormalizing.RENormalizing([
                (re.compile('usage: Usage: '), 'Usage: '), # Py 2.4
                (re.compile('options:'), 'Options:'), # Py 2.4
                ])
            ),
        ))

