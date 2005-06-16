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

def test_must_use_consistent_connections():
    """

It's important to use consistent connections.  References to to
separate connections to the ssme database or multi-database won't
work.

For example, it's tempting to open a second database using the
database open function, but this doesn't work:

    >>> import ZODB.tests.util, transaction, persistent
    >>> databases = {}
    >>> db1 = ZODB.tests.util.DB(databases=databases, database_name='1')
    >>> db2 = ZODB.tests.util.DB(databases=databases, database_name='2')

    >>> tm = transaction.TransactionManager()
    >>> conn1 = db1.open(transaction_manager=tm)
    >>> p1 = MyClass()
    >>> conn1.root()['p'] = p1
    >>> tm.commit()

    >>> conn2 = db2.open(transaction_manager=tm)

    >>> p2 = MyClass()
    >>> conn2.root()['p'] = p2
    >>> p2.p1 = p1
    >>> tm.commit() # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    InvalidObjectReference: Attempt to store a reference to an object
    from a separate onnection to the same database or multidatabase

    >>> tm.abort()

Even without multi-databases, a common mistake is to mix objects in
different connections to the same database.

    >>> conn2 = db1.open(transaction_manager=tm)

    >>> p2 = MyClass()
    >>> conn2.root()['p'] = p2
    >>> p2.p1 = p1
    >>> tm.commit() # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    InvalidObjectReference: Attempt to store a reference to an object
    from a separate onnection to the same database or multidatabase

    >>> tm.abort()

"""


def test_suite():
    return unittest.TestSuite((
        doctest.DocFileSuite('../cross-database-references.txt',
                             globs=dict(MyClass=MyClass),
                             ),
        doctest.DocFileSuite('../cross-database-references.txt',
                             globs=dict(MyClass=MyClass_w_getnewargs),
                             ),
        doctest.DocTestSuite(),
        ))

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')

