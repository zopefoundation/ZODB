##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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
import tempfile
import unittest

import transaction
import ZODB.config
import ZODB.tests.util
from ZODB.POSException import ReadOnlyError


class ConfigTestBase(ZODB.tests.util.TestCase):
    def _opendb(self, s):
        return ZODB.config.databaseFromString(s)

    def tearDown(self):
        ZODB.tests.util.TestCase.tearDown(self)
        if getattr(self, "storage", None) is not None:
            self.storage.cleanup()

    def _test(self, s):
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


class ZEOConfigTest(ConfigTestBase):
    def test_zeo_config(self):
        # We're looking for a port that doesn't exist so a
        # connection attempt will fail.  Instead of elaborate
        # logic to loop over a port calculation, we'll just pick a
        # simple "random", likely to not-exist port number and add
        # an elaborate comment explaining this instead.  Go ahead,
        # grep for 9.
        from ZEO.ClientStorage import ClientDisconnected
        import ZConfig
        from ZODB.config import getDbSchema
        from StringIO import StringIO
        cfg = """
        <zodb>
          <zeoclient>
            server localhost:56897
            wait false
          </zeoclient>
        </zodb>
        """
        config, handle = ZConfig.loadConfigFile(getDbSchema(), StringIO(cfg))
        self.assertEqual(config.database[0].config.storage.config.blob_dir,
                         None)
        self.assertRaises(ClientDisconnected, self._test, cfg)

        cfg = """
        <zodb>
          <zeoclient>
            blob-dir blobs
            server localhost:56897
            wait false
          </zeoclient>
        </zodb>
        """
        config, handle = ZConfig.loadConfigFile(getDbSchema(), StringIO(cfg))
        self.assertEqual(config.database[0].config.storage.config.blob_dir,
                         'blobs')
        self.assertRaises(ClientDisconnected, self._test, cfg)

def database_xrefs_config():
    r"""
    >>> db = ZODB.config.databaseFromString(
    ...    "<zodb>\n<mappingstorage>\n</mappingstorage>\n</zodb>\n")
    >>> db.xrefs
    True
    >>> db = ZODB.config.databaseFromString(
    ...    "<zodb>\nallow-implicit-cross-references true\n"
    ...    "<mappingstorage>\n</mappingstorage>\n</zodb>\n")
    >>> db.xrefs
    True
    >>> db = ZODB.config.databaseFromString(
    ...    "<zodb>\nallow-implicit-cross-references false\n"
    ...    "<mappingstorage>\n</mappingstorage>\n</zodb>\n")
    >>> db.xrefs
    False
    """

def multi_atabases():
    r"""If there are multiple codb sections -> multidatabase

    >>> db = ZODB.config.databaseFromString('''
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

    >>> ZODB.config.databaseFromString('''
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

    >>> ZODB.config.databaseFromString('''
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
    suite = unittest.TestSuite()
    suite.addTest(doctest.DocTestSuite(
        setUp=ZODB.tests.util.setUp, tearDown=ZODB.tests.util.tearDown))
    suite.addTest(unittest.makeSuite(ZODBConfigTest))
    suite.addTest(unittest.makeSuite(ZEOConfigTest))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
