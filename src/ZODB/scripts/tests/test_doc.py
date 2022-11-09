##############################################################################
#
# Copyright (c) 2004 Zope Foundation and Contributors.
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
import re
import unittest

import zope.testing.renormalizing

import ZODB.tests.util


checker = zope.testing.renormalizing.RENormalizing([
    (re.compile(
        r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+'),
     '2007-11-10 15:18:48.543001'),
    (re.compile('hash=[0-9a-f]{40}'),
     'hash=b16422d09fabdb45d4e4325e4b42d7d6f021d3c3'),
    # Python 3 bytes add a "b".
    (re.compile("b('.*?')"), r"\1"),
    (re.compile('b(".*?")'), r"\1"),
    # Python 3 produces larger pickles, even when we use zodbpickle :(
    # this changes all the offsets and sizes in fstail.txt
    (re.compile("user='' description='' "
                r"length=[0-9]+ offset=[0-9]+ \(\+23\)"),
     "user='' description='' "
     "length=<LENGTH> offset=<OFFSET> (+23)"),
    (re.compile("user='' description='initial database creation' "
                r"length=[0-9]+ offset=4 \(\+48\)"),
     "user='' description='initial database creation' "
     "length=<LENGTH> offset=4 (+48)"),
])


def test_suite():
    return unittest.TestSuite((
        doctest.DocFileSuite(
            'referrers.txt',
            'fstail.txt',
            setUp=ZODB.tests.util.setUp, tearDown=ZODB.tests.util.tearDown,
            checker=checker),
    ))
