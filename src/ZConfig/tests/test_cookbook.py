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
"""Tests of examples from the online cookbook, so we don't break them
down the road.  Unless we really mean to.

The ZConfig Cookbook is available online at:

    http://dev.zope.org/Zope3/ZConfig

"""

import unittest

from ZConfig.tests.support import TestBase


def basic_key_mapping_password_to_passwd(key):
    # Lower-case the key since that's what basic-key does:
    key = key.lower()
    # Now map password to passwd:
    if key == "password":
        key = "passwd"
    return key

def user_info_conversion(section):
    return section


class CookbookTestCase(TestBase):

    def test_rewriting_key_names(self):
        schema = self.load_schema_text("""
            <schema prefix='%s'>
              <sectiontype name='userinfo' datatype='.user_info_conversion'
                           keytype='.basic_key_mapping_password_to_passwd'>
                <key name='userid' datatype='integer'/>
                <key name='username' datatype='identifier'/>
                <key name='password'/>
              </sectiontype>
              <section type='userinfo' name='*' attribute='userinfo'/>
            </schema>
            """ % __name__)
        config = self.load_config_text(schema, """\
            <userinfo>
              USERID 42
              USERNAME foouser
              PASSWORD yeah-right
            </userinfo>
            """)
        self.assertEqual(config.userinfo.userid, 42)
        self.assertEqual(config.userinfo.username, "foouser")
        self.assertEqual(config.userinfo.passwd, "yeah-right")
        self.assert_(not hasattr(config.userinfo, "password"))


def test_suite():
    return unittest.makeSuite(CookbookTestCase)

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
