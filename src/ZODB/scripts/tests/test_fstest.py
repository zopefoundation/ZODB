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
import re
import unittest

from zope.testing import setupstack
from zope.testing.renormalizing import RENormalizing

import ZODB


def test_fstest_verbose():
    r"""
    >>> db = ZODB.DB('data.fs')
    >>> db.close()
    >>> import ZODB.scripts.fstest
    >>> ZODB.scripts.fstest.main(['data.fs'])

    >>> ZODB.scripts.fstest.main(['data.fs'])

    >>> ZODB.scripts.fstest.main(['-v', 'data.fs'])
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
             4: transaction tid ... #0
    no errors detected

    >>> ZODB.scripts.fstest.main(['-vvv', 'data.fs'])
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
            52: object oid 0x0000000000000000 #0
             4: transaction tid ... #0
    no errors detected

    """


def test_suite():
    checker = RENormalizing([
        # Python 3 drops the u'' prefix on unicode strings
        (re.compile(r"u('[^']*')"), r"\1"),
    ])
    return unittest.TestSuite([
        doctest.DocTestSuite('ZODB.scripts.fstest', checker=checker),
        doctest.DocTestSuite(setUp=setupstack.setUpDirectory,
                             tearDown=setupstack.tearDown),
    ])
