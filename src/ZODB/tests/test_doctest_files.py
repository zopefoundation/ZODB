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
import unittest

def cross_db_refs_to_blank_db_name():
    """
    There was a bug that caused bad refs to be generated is a database
    name was blank.

    >>> import persistent.mapping
    >>> import transaction
    >>> from ZODB.DB import DB
    >>> dbs = {}
    >>> db1 = DB(None, database_name='', databases=dbs)
    >>> db2 = DB(None, database_name='2', databases=dbs)
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

    """


def test_suite():
    import doctest
    return unittest.TestSuite((
        doctest.DocFileSuite("dbopen.txt",
                             "multidb.txt",
                             "synchronizers.txt"),
        doctest.DocTestSuite(),
    ))
