##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################

"""Test suite for ZEO.runsvr."""

import os
import sys
import tempfile
import unittest
from StringIO import StringIO

import ZEO.runsvr


class TestOptions(unittest.TestCase):

    OptionsClass = ZEO.runsvr.Options

    def save_streams(self):
        self.save_stdout = sys.stdout
        self.save_stderr = sys.stderr
        sys.stdout = self.stdout = StringIO()
        sys.stderr = self.stderr = StringIO()

    def restore_streams(self):
        sys.stdout = self.save_stdout
        sys.stderr = self.save_stderr

    input_args = ["arg1", "arg2"]
    output_opts = []
    output_args = ["arg1", "arg2"]

    def test_basic(self):
        progname = "progname"
        doc = "doc"
        options = self.OptionsClass(self.input_args, progname, doc)
        self.assertEqual(options.progname, "progname")
        self.assertEqual(options.doc, "doc\n")
        self.assertEqual(options.options, self.output_opts)
        self.assertEqual(options.args, self.output_args)

    def test_configure(self):
        class MyOptions(self.OptionsClass):
            def load_configuration(self):
                pass
        for arg in "-C", "--c", "--configure":
            options = MyOptions(["-C", "foobar"])
            self.assertEqual(options.configuration, "foobar")

    def test_help(self):
        for arg in "-h", "--h", "--help":
            try:
                self.save_streams()
                try:
                    options = self.OptionsClass([arg])
                finally:
                    self.restore_streams()
            except SystemExit, err:
                self.assertEqual(err.code, 0)
            else:
                self.fail("%s didn't call sys.exit()" % repr(arg))


class TestZEOOptions(TestOptions):

    OptionsClass = ZEO.runsvr.ZEOOptions

    input_args = ["-f", "Data.fs", "-a", "5555"]
    output_opts = [("-f", "Data.fs"), ("-a", "5555")]
    output_args = []

    configdata = """
        <zeo>
          address 5555
        </zeo>
        <filestorage fs>
          path Data.fs
        </filestorage>
        """

    def setUp(self):
        self.tempfilename = tempfile.mktemp()
        f = open(self.tempfilename, "w")
        f.write(self.configdata)
        f.close()

    def tearDown(self):
        try:
            os.remove(self.tempfilename)
        except os.error:
            pass

    def test_configure(self):
        # Hide the base class test_configure
        pass

    def test_defaults_with_schema(self):
        options = self.OptionsClass(["-C", self.tempfilename])
        self.assertEqual(options.address, ("", 5555))
        import ZODB.config
        opener = options.storages["fs"]
        self.assertEqual(opener.__class__, ZODB.config.FileStorage)
        self.assertEqual(options.read_only, 0)
        self.assertEqual(options.transaction_timeout, None)
        self.assertEqual(options.invalidation_queue_size, 100)

    def test_defaults_without_schema(self):
        options = self.OptionsClass(["-a", "5555", "-f", "Data.fs"])
        self.assertEqual(options.address, ("", 5555))
        import ZODB.config
        opener = options.storages["1"]
        self.assertEqual(opener.__class__, ZODB.config.FileStorage)
        self.assertEqual(opener.config.path, "Data.fs")
        self.assertEqual(options.read_only, 0)
        self.assertEqual(options.transaction_timeout, None)
        self.assertEqual(options.invalidation_queue_size, 100)

    def test_commandline_overrides(self):
        options = self.OptionsClass(["-C", self.tempfilename,
                                     "-a", "6666", "-f", "Wisdom.fs"])
        self.assertEqual(options.address, ("", 6666))
        import ZODB.config
        opener = options.storages["1"]
        self.assertEqual(opener.__class__, ZODB.config.FileStorage)
        self.assertEqual(opener.config.path, "Wisdom.fs")
        self.assertEqual(options.read_only, 0)
        self.assertEqual(options.transaction_timeout, None)
        self.assertEqual(options.invalidation_queue_size, 100)


def test_suite():
    suite = unittest.TestSuite()
    for cls in TestOptions, TestZEOOptions:
        suite.addTest(unittest.makeSuite(cls))
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest='test_suite')
