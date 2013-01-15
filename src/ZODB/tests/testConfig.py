##############################################################################
#
# Copyright (c) 2003 Zope Foundation and Contributors.
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

from ZODB.tests.util import TestCase as utilTestCase


class ConfigTestBase(utilTestCase):
    def _opendb(self, s):
        from ZODB.config import databaseFromString
        return databaseFromString(s)

    def tearDown(self):
        utilTestCase.tearDown(self)
        if getattr(self, "storage", None) is not None:
            self.storage.cleanup()

    def _test(self, s):
        import transaction
        db = self._opendb(s)
        self.storage = db._storage
        # Do something with the database to make sure it works
        cn = db.open()
        rt = cn.root()
        rt["test"] = 1
        transaction.commit()
        db.close()


class ZODBConfigTest(ConfigTestBase):
    def test_map_config1(self):
        self._test(
            """
            <zodb>
              <mappingstorage/>
            </zodb>
            """)

    def test_map_config2(self):
        self._test(
            """
            <zodb>
              <mappingstorage/>
              cache-size 1000
            </zodb>
            """)

    def test_file_config1(self):
        import tempfile
        path = tempfile.mktemp()
        self._test(
            """
            <zodb>
              <filestorage>
                path %s
              </filestorage>
            </zodb>
            """ % path)

    def test_file_config2(self):
        import tempfile
        from ZODB.POSException import ReadOnlyError
        path = tempfile.mktemp()
        cfg = """
        <zodb>
          <filestorage>
            path %s
            create false
            read-only true
          </filestorage>
        </zodb>
        """ % path
        self.assertRaises(ReadOnlyError, self._test, cfg)

    def test_demo_config(self):
        cfg = """
        <zodb unused-name>
          <demostorage>
            name foo
            <mappingstorage/>
          </demostorage>
        </zodb>
        """
        self._test(cfg)


def database_xrefs_config():
    r"""
    >>> from ZODB.config import databaseFromString
    >>> db = databaseFromString(
    ...    "<zodb>\n<mappingstorage>\n</mappingstorage>\n</zodb>\n")
    >>> db.xrefs
    True
    >>> db = databaseFromString(
    ...    "<zodb>\nallow-implicit-cross-references true\n"
    ...    "<mappingstorage>\n</mappingstorage>\n</zodb>\n")
    >>> db.xrefs
    True
    >>> db = databaseFromString(
    ...    "<zodb>\nallow-implicit-cross-references false\n"
    ...    "<mappingstorage>\n</mappingstorage>\n</zodb>\n")
    >>> db.xrefs
    False
    """

def multi_databases():
    r"""If there are multiple codb sections -> multidatabase

    >>> from ZODB.config import databaseFromString
    >>> db = databaseFromString('''
    ... <zodb>
    ...    <mappingstorage>
    ...    </mappingstorage>
    ... </zodb>
    ... <zodb Foo>
    ...    <mappingstorage>
    ...    </mappingstorage>
    ... </zodb>
    ... <zodb>
    ...    database-name Bar
    ...    <mappingstorage>
    ...    </mappingstorage>
    ... </zodb>
    ... ''')
    >>> sorted(db.databases)
    ['', 'Bar', 'foo']

    >>> db.database_name
    ''
    >>> db.databases[db.database_name] is db
    True
    >>> db.databases['foo'] is not db
    True
    >>> db.databases['Bar'] is not db
    True
    >>> db.databases['Bar'] is not db.databases['foo']
    True

    Can't have repeats:

    >>> databaseFromString('''
    ... <zodb 1>
    ...    <mappingstorage>
    ...    </mappingstorage>
    ... </zodb>
    ... <zodb 1>
    ...    <mappingstorage>
    ...    </mappingstorage>
    ... </zodb>
    ... <zodb 1>
    ...    <mappingstorage>
    ...    </mappingstorage>
    ... </zodb>
    ... ''') # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    ConfigurationSyntaxError:
    section names must not be re-used within the same container:'1' (line 9)

    >>> databaseFromString('''
    ... <zodb>
    ...    <mappingstorage>
    ...    </mappingstorage>
    ... </zodb>
    ... <zodb>
    ...    <mappingstorage>
    ...    </mappingstorage>
    ... </zodb>
    ... ''') # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    ValueError: database_name '' already in databases

    """

def test_suite():
    import doctest
    from ZODB.tests.util import setUp
    from ZODB.tests.util import tearDown
    return  unittest.TestSuite((
        doctest.DocTestSuite(setUp=setUp, tearDown=tearDown),
        unittest.makeSuite(ZODBConfigTest),
    ))
