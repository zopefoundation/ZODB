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
"""Tests of savepoint feature

$Id$
"""
import unittest
from zope.testing import doctest
import persistent.dict, transaction

def testAddingThenModifyThenAbort():
    """
    >>> import ZODB.tests.util
    >>> db = ZODB.tests.util.DB()
    >>> connection = db.open()
    >>> root = connection.root()

    >>> ob = persistent.dict.PersistentDict()
    >>> root['ob'] = ob
    >>> sp = transaction.savepoint()
    >>> ob.x = 1
    >>> transaction.abort()
  
"""

def testModifyThenSavePointThenModifySomeMoreThenCommit():
    """
    >>> import ZODB.tests.util
    >>> db = ZODB.tests.util.DB()
    >>> connection = db.open()
    >>> root = connection.root()
    >>> sp = transaction.savepoint()
    >>> root['a'] = 1
    >>> sp = transaction.savepoint()
    >>> root['a'] = 2
    >>> transaction.commit()
  
"""

def test_suite():
    return unittest.TestSuite((
        doctest.DocFileSuite('testConnectionSavepoint.txt'),
        doctest.DocTestSuite(),
        ))

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')

