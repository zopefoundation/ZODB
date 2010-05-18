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
import manuel.doctest
import manuel.footnote
import manuel.testing
import ZODB.tests.util

def test_suite():
    return manuel.testing.TestSuite(
        manuel.doctest.Manuel() + manuel.footnote.Manuel(),
        '../historical_connections.txt',
        setUp=ZODB.tests.util.setUp, tearDown=ZODB.tests.util.tearDown,
        )
