##############################################################################
#
# Copyright (c) 2002, 2003 Zope Corporation and Contributors.
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
"""Tests of the configuration data structures and loader."""

import os
import StringIO
import tempfile
import unittest

import ZConfig

from ZConfig.tests.support import CONFIG_BASE


class ConfigurationTestCase(unittest.TestCase):

    schema = None

    def get_schema(self):
        if self.schema is None:
            ConfigurationTestCase.schema = ZConfig.loadSchema(
                CONFIG_BASE + "simple.xml")
        return self.schema

    def load(self, relurl, context=None):
        url = CONFIG_BASE + relurl
        self.conf, self.handlers = ZConfig.loadConfig(self.get_schema(), url)
        conf = self.conf
        #self.assertEqual(conf.url, url)
        self.assert_(conf.getSectionName() is None)
        self.assert_(conf.getSectionType() is None)
        #self.assert_(conf.delegate is None)
        return conf

    def loadtext(self, text):
        sio = StringIO.StringIO(text)
        return self.loadfile(sio)

    def loadfile(self, file):
        schema = self.get_schema()
        self.conf, self.handlers = ZConfig.loadConfigFile(schema, file)
        return self.conf

    def check_simple_gets(self, conf):
        self.assertEqual(conf.empty, '')
        self.assertEqual(conf.int_var, 12)
        self.assertEqual(conf.neg_int, -2)
        self.assertEqual(conf.float_var, 12.02)
        self.assertEqual(conf.var1, 'abc')
        self.assert_(conf.true_var_1)
        self.assert_(conf.true_var_2)
        self.assert_(conf.true_var_3)
        self.assert_(not conf.false_var_1)
        self.assert_(not conf.false_var_2)
        self.assert_(not conf.false_var_3)
        self.assertEqual(conf.list_1, [])
        self.assertEqual(conf.list_2, ['abc'])
        self.assertEqual(conf.list_3, ['abc', 'def', 'ghi'])
        self.assertEqual(conf.list_4, ['[', 'what', 'now?', ']'])

    def test_simple_gets(self):
        conf = self.load("simple.conf")
        self.check_simple_gets(conf)

    def test_type_errors(self):
        Error = ZConfig.DataConversionError
        raises = self.assertRaises
        raises(Error, self.loadtext, "int-var true")
        raises(Error, self.loadtext, "float-var true")
        raises(Error, self.loadtext, "neg-int false")
        raises(Error, self.loadtext, "true-var-1 0")
        raises(Error, self.loadtext, "true-var-1 1")
        raises(Error, self.loadtext, "true-var-1 -1")

    def test_simple_sections(self):
        self.schema = ZConfig.loadSchema(CONFIG_BASE + "simplesections.xml")
        conf = self.load("simplesections.conf")
        self.assertEqual(conf.var, "foo")
        # check each interleaved position between sections
        for c in "0123456":
            self.assertEqual(getattr(conf, "var_" +c), "foo-" + c)
        sect = [sect for sect in conf.sections
                if sect.getSectionName() == "name"][0]
        self.assertEqual(sect.var, "bar")
        self.assertEqual(sect.var_one, "splat")
        self.assert_(sect.var_three is None)
        sect = [sect for sect in conf.sections
                if sect.getSectionName() == "delegate"][0]
        self.assertEqual(sect.var, "spam")
        self.assertEqual(sect.var_two, "stuff")
        self.assert_(sect.var_three is None)

    def test_include(self):
        conf = self.load("include.conf")
        self.assertEqual(conf.var1, "abc")
        self.assertEqual(conf.var2, "value2")
        self.assertEqual(conf.var3, "value3")
        self.assertEqual(conf.var4, "value")

    def test_includes_with_defines(self):
        self.schema = ZConfig.loadSchemaFile(StringIO.StringIO("""\
            <schema>
              <key name='refinner' />
              <key name='refouter' />
            </schema>
            """))
        conf = self.load("outer.conf")
        self.assertEqual(conf.refinner, "inner")
        self.assertEqual(conf.refouter, "outer")

    def test_define(self):
        conf = self.load("simple.conf")
        self.assertEqual(conf.getname, "value")
        self.assertEqual(conf.getnametwice, "valuevalue")
        self.assertEqual(conf.getdollars, "$$")
        self.assertEqual(conf.getempty, "xy")
        self.assertEqual(conf.getwords, "abc two words def")

    def test_define_errors(self):
        self.assertRaises(ZConfig.ConfigurationSyntaxError,
                          self.loadtext, "%define\n")
        self.assertRaises(ZConfig.ConfigurationSyntaxError,
                          self.loadtext, "%define abc-def\n")
        self.assertRaises(ZConfig.ConfigurationSyntaxError,
                          self.loadtext, "%define a value\n%define a value\n")

    def test_fragment_ident_disallowed(self):
        self.assertRaises(ZConfig.ConfigurationError,
                          self.load, "simplesections.conf#another")

    def test_load_from_fileobj(self):
        sio = StringIO.StringIO("%define name value\n"
                                "getname x $name y \n")
        cf = self.loadfile(sio)
        self.assertEqual(cf.getname, "x value y")

    def test_load_from_abspath(self):
        fn = self.write_tempfile()
        try:
            self.check_load_from_path(fn)
        finally:
            os.unlink(fn)

    def test_load_from_relpath(self):
        fn = self.write_tempfile()
        dir, name = os.path.split(fn)
        pwd = os.getcwd()
        try:
            os.chdir(dir)
            self.check_load_from_path(name)
        finally:
            os.chdir(pwd)
            os.unlink(fn)

    def write_tempfile(self):
        fn = tempfile.mktemp()
        fp = open(fn, "w")
        fp.write("var1 value\n")
        fp.close()
        return fn

    def check_load_from_path(self, path):
        schema = self.get_schema()
        ZConfig.loadConfig(schema, path)


def test_suite():
    return unittest.makeSuite(ConfigurationTestCase)

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
