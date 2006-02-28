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
import tempfile, shutil, unittest
import os

from ZODB.tests.testConfig import ConfigTestBase
from ZConfig import ConfigurationSyntaxError

class BlobConfigTestBase(ConfigTestBase):

    def setUp(self):
        super(BlobConfigTestBase, self).setUp()

        self.blob_dir = tempfile.mkdtemp()

    def tearDown(self):
        super(BlobConfigTestBase, self).tearDown()

        shutil.rmtree(self.blob_dir)
    

class ZODBBlobConfigTest(BlobConfigTestBase):

    def test_map_config1(self):
        self._test(
            """
            <zodb>
              <blobstorage>
                blob-dir %s
                <mappingstorage/>
              </blobstorage>
            </zodb>
            """ % self.blob_dir)

    def test_file_config1(self):
        path = tempfile.mktemp()
        self._test(
            """
            <zodb>
              <blobstorage>
                blob-dir %s
                <filestorage>
                  path %s
                </filestorage>
              </blobstorage>
            </zodb>
            """ %(self.blob_dir, path))
        os.unlink(path)
        os.unlink(path+".index")
        os.unlink(path+".tmp")

    def test_blob_dir_needed(self):
        self.assertRaises(ConfigurationSyntaxError,
                          self._test,
                          """
                          <zodb>
                            <blobstorage>
                              <mappingstorage/>
                            </blobstorage>
                          </zodb>
                          """)


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ZODBBlobConfigTest))

    return suite

if __name__ == '__main__':
    unittest.main(defaultTest = 'test_suite')
