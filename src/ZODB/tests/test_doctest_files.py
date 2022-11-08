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
import unittest


__test__ = dict(
    cross_db_refs_to_blank_db_name="""

    There was a bug that caused bad refs to be generated is a database
    name was blank.

    >>> import ZODB.tests.util, persistent.mapping, transaction
    >>> dbs = {}
    >>> db1 = ZODB.tests.util.DB(database_name='', databases=dbs)
    >>> db2 = ZODB.tests.util.DB(database_name='2', databases=dbs)
    >>> conn1 = db1.open()
    >>> conn2 = conn1.get_connection('2')
    >>> for i in range(10):
    ...     conn1.root()[i] = persistent.mapping.PersistentMapping()
    ...     transaction.commit()
    >>> conn2.root()[0] = conn1.root()[9]
    >>> transaction.commit()
    >>> conn2.root()._p_deactivate()
    >>> conn2.root()[0] is conn1.root()[9]
    True

    >>> list(conn2.root()[0].keys())
    []

    >>> db2.close()
    >>> db1.close()
    """,
)


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(doctest.DocFileSuite("dbopen.txt",
                                       "multidb.txt",
                                       "synchronizers.txt",
                                       ))
    suite.addTest(doctest.DocTestSuite())
    return suite
