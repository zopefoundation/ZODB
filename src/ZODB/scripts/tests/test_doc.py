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
"""Test harness for scripts.

$Id$
"""
import unittest
import re
from zope.testing import doctest, renormalizing
import ZODB.tests.util

checker = renormalizing.RENormalizing([
    (re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+'),
     '2007-11-10 15:18:48.543001'),
    (re.compile('hash=[0-9a-f]{40}'),
     'hash=b16422d09fabdb45d4e4325e4b42d7d6f021d3c3')])

def test_suite():
    return unittest.TestSuite((
        doctest.DocFileSuite(
            'referrers.txt', 'fstail.txt',
            setUp=ZODB.tests.util.setUp, tearDown=ZODB.tests.util.tearDown,
            checker=checker),
        ))
