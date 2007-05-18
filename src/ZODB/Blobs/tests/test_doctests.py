##############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
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

from zope.testing import doctest
import ZODB.tests.util

def test_suite():
    return doctest.DocFileSuite(
        "basic.txt",  "connection.txt", "transaction.txt",
        "packing.txt", "importexport.txt", "consume.txt",
        setUp=ZODB.tests.util.setUp,
        tearDown=ZODB.tests.util.tearDown,
        )
