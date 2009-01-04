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

import os
import transaction
import unittest
import ZEO.ClientStorage
import ZODB.config
import ZODB.POSException
import ZODB.tests.util
from zope.testing import doctest

class ConfigTestBase(ZODB.tests.util.TestCase):

    def _opendb(self, s):
        return ZODB.config.databaseFromString(s)

    def _test(self, s):
        db = self._opendb(s)
        self.storage = db.storage
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
        self._test(
            """
            <zodb>
              <filestorage>
                path Data.fs
              </filestorage>
            </zodb>
            """)

    def test_file_config2(self):
        cfg = """
        <zodb>
          <filestorage>
            path Data.fs
            create false
            read-only true
          </filestorage>
        </zodb>
        """
        self.assertRaises(ZODB.POSException.ReadOnlyError, self._test, cfg)

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
        self.assertEqual(config.database.config.storage.config.blob_dir,
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
        self.assertEqual(
            os.path.abspath(config.database.config.storage.config.blob_dir),
            os.path.abspath('blobs'))
        self.assertRaises(ClientDisconnected, self._test, cfg)

def db_connection_pool_timeout():
    """
Test that the database pool timeout option works:

    >>> db = ZODB.config.databaseFromString('''
    ...     <zodb>
    ...       <mappingstorage/>
    ...     </zodb>
    ... ''')
    >>> db.pool._timeout == 1<<31
    True

    >>> db = ZODB.config.databaseFromString('''
    ...     <zodb>
    ...       pool-timeout 600
    ...       <mappingstorage/>
    ...     </zodb>
    ... ''')
    >>> db.pool._timeout == 600
    True

    """


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ZODBConfigTest))
    suite.addTest(unittest.makeSuite(ZEOConfigTest))
    suite.addTest(doctest.DocTestSuite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
