##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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

"""Tests of the 'basic' section types provided as part of
ZConfig.components.basic."""

import unittest

from ZConfig.tests import support


SIMPLE_SCHEMA = '''\
<schema>
  <import package="ZConfig.components.basic" file="mapping.xml" />

  <sectiontype name="dict"
               extends="ZConfig.basic.mapping" />

  <sectiontype name="intkeys"
               extends="ZConfig.basic.mapping"
               keytype="integer" />

  <section name="*"
           type="dict"
           attribute="simple_dict" />

  <section name="*"
           type="intkeys"
           attribute="int_dict" />

</schema>
'''


class BasicSectionTypeTestCase(support.TestBase):
    schema = None

    def setUp(self):
        if self.schema is None:
            self.__class__.schema = self.load_schema_text(SIMPLE_SCHEMA)

    def test_simple_empty_dict(self):
        conf = self.load_config_text(self.schema, "<dict/>")
        self.assertEqual(conf.simple_dict, {})
        conf = self.load_config_text(self.schema, """\
            <dict foo>
            # comment
            </dict>
            """)
        self.assertEqual(conf.simple_dict, {})

    def test_simple_dict(self):
        conf = self.load_config_text(self.schema, """\
           <dict foo>
           key-one value-one
           key-two value-two
           </dict>
           """)
        L = conf.simple_dict.items()
        L.sort()
        self.assertEqual(L, [("key-one", "value-one"),
                             ("key-two", "value-two")])

    def test_derived_dict(self):
        conf = self.load_config_text(self.schema, """\
            <intkeys>
            1 foo
            2 bar
            42 question?
            </intkeys>
            """)
        L = conf.int_dict.items()
        L.sort()
        self.assertEqual(L, [(1, "foo"), (2, "bar"), (42, "question?")])


def test_suite():
    return unittest.makeSuite(BasicSectionTypeTestCase)
