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

"""Test suite for zdaemon.zdoptions."""

import os
import sys
import tempfile
import unittest
from StringIO import StringIO

import ZConfig
import zdaemon
from zdaemon.zdoptions import ZDOptions

class ZDOptionsTestBase(unittest.TestCase):

    OptionsClass = ZDOptions

    def save_streams(self):
        self.save_stdout = sys.stdout
        self.save_stderr = sys.stderr
        sys.stdout = self.stdout = StringIO()
        sys.stderr = self.stderr = StringIO()

    def restore_streams(self):
        sys.stdout = self.save_stdout
        sys.stderr = self.save_stderr

    def check_exit_code(self, options, args):
        save_sys_stderr = sys.stderr
        try:
            sys.stderr = StringIO()
            try:
                options.realize(args)
            except SystemExit, err:
                self.assertEqual(err.code, 2)
            else:
                self.fail("SystemExit expected")
        finally:
            sys.stderr = save_sys_stderr


class TestZDOptions(ZDOptionsTestBase):

    input_args = ["arg1", "arg2"]
    output_opts = []
    output_args = ["arg1", "arg2"]

    def test_basic(self):
        progname = "progname"
        doc = "doc"
        options = self.OptionsClass()
        options.positional_args_allowed = 1
        options.schemadir = os.path.dirname(zdaemon.__file__)
        options.realize(self.input_args, progname, doc)
        self.assertEqual(options.progname, "progname")
        self.assertEqual(options.doc, "doc")
        self.assertEqual(options.options, self.output_opts)
        self.assertEqual(options.args, self.output_args)

    def test_configure(self):
        configfile = os.path.join(os.path.dirname(zdaemon.__file__),
                                  "sample.conf")
        for arg in "-C", "--c", "--configure":
            options = self.OptionsClass()
            options.realize([arg, configfile])
            self.assertEqual(options.configfile, configfile)

    def test_help(self):
        for arg in "-h", "--h", "--help":
            options = self.OptionsClass()
            try:
                self.save_streams()
                try:
                    options.realize([arg])
                finally:
                    self.restore_streams()
            except SystemExit, err:
                self.assertEqual(err.code, 0)
            else:
                self.fail("%s didn't call sys.exit()" % repr(arg))

    def test_unrecognized(self):
        # Check that we get an error for an unrecognized option
        self.check_exit_code(self.OptionsClass(), ["-/"])


class TestBasicFunctionality(TestZDOptions):

    def test_no_positional_args(self):
        # Check that we get an error for positional args when they
        # haven't been enabled.
        self.check_exit_code(self.OptionsClass(), ["A"])

    def test_positional_args(self):
        options = self.OptionsClass()
        options.positional_args_allowed = 1
        options.realize(["A", "B"])
        self.assertEqual(options.args, ["A", "B"])

    def test_positional_args_empty(self):
        options = self.OptionsClass()
        options.positional_args_allowed = 1
        options.realize([])
        self.assertEqual(options.args, [])

    def test_positional_args_unknown_option(self):
        # Make sure an unknown option doesn't become a positional arg.
        options = self.OptionsClass()
        options.positional_args_allowed = 1
        self.check_exit_code(options, ["-o", "A", "B"])

    def test_conflicting_flags(self):
        # Check that we get an error for flags which compete over the
        # same option setting.
        options = self.OptionsClass()
        options.add("setting", None, "a", flag=1)
        options.add("setting", None, "b", flag=2)
        self.check_exit_code(options, ["-a", "-b"])

    def test_handler_simple(self):
        # Test that a handler is called; use one that doesn't return None.
        options = self.OptionsClass()
        options.add("setting", None, "a:", handler=int)
        options.realize(["-a2"])
        self.assertEqual(options.setting, 2)

    def test_handler_side_effect(self):
        # Test that a handler is called and conflicts are not
        # signalled when it returns None.
        options = self.OptionsClass()
        L = []
        options.add("setting", None, "a:", "append=", handler=L.append)
        options.realize(["-a2", "--append", "3"])
        self.assert_(options.setting is None)
        self.assertEqual(L, ["2", "3"])

    def test_handler_with_bad_value(self):
        options = self.OptionsClass()
        options.add("setting", None, "a:", handler=int)
        self.check_exit_code(options, ["-afoo"])

    def test_raise_getopt_errors(self):
        options = self.OptionsClass()
        # note that we do not add "a" to the list of options;
        # if raise_getopt_errors was true, this test would error
        options.realize(["-afoo"], raise_getopt_errs=False)
        # check_exit_code realizes the options with raise_getopt_errs=True
        self.check_exit_code(options, ['-afoo'])


class EnvironmentOptions(ZDOptionsTestBase):

    saved_schema = None

    class OptionsClass(ZDOptions):
        def __init__(self):
            ZDOptions.__init__(self)
            self.add("opt", "opt", "o:", "opt=",
                     default=42, handler=int, env="OPT")

        def load_schema(self):
            # Doing this here avoids needing a separate file for the schema:
            if self.schema is None:
                if EnvironmentOptions.saved_schema is None:
                    schema = ZConfig.loadSchemaFile(StringIO("""\
                        <schema>
                          <key name='opt' datatype='integer' default='12'/>
                        </schema>
                        """))
                    EnvironmentOptions.saved_schema = schema
                self.schema = EnvironmentOptions.saved_schema

        def load_configfile(self):
            if getattr(self, "configtext", None):
                self.configfile = tempfile.mktemp()
                f = open(self.configfile, 'w')
                f.write(self.configtext)
                f.close()
                try:
                    ZDOptions.load_configfile(self)
                finally:
                    os.unlink(self.configfile)
            else:
                ZDOptions.load_configfile(self)

    # Save and restore the environment around each test:

    def setUp(self):
        self._oldenv = os.environ
        env = {}
        for k, v in os.environ.items():
            env[k] = v
        os.environ = env

    def tearDown(self):
        os.environ = self._oldenv

    def create_with_config(self, text):
        options = self.OptionsClass()
        zdpkgdir = os.path.dirname(os.path.abspath(zdaemon.__file__))
        options.schemadir = os.path.join(zdpkgdir, 'tests')
        options.schemafile = "envtest.xml"
        # configfile must be set for ZDOptions to use ZConfig:
        if text:
            options.configfile = "not used"
            options.configtext = text
        return options


class TestZDOptionsEnvironment(EnvironmentOptions):

    def test_with_environment(self):
        os.environ["OPT"] = "2"
        self.check_from_command_line()
        options = self.OptionsClass()
        options.realize([])
        self.assertEqual(options.opt, 2)

    def test_without_environment(self):
        self.check_from_command_line()
        options = self.OptionsClass()
        options.realize([])
        self.assertEqual(options.opt, 42)

    def check_from_command_line(self):
        for args in (["-o1"], ["--opt", "1"]):
            options = self.OptionsClass()
            options.realize(args)
            self.assertEqual(options.opt, 1)

    def test_with_bad_environment(self):
        os.environ["OPT"] = "Spooge!"
        # make sure the bad value is ignored if the command-line is used:
        self.check_from_command_line()
        options = self.OptionsClass()
        try:
            self.save_streams()
            try:
                options.realize([])
            finally:
                self.restore_streams()
        except SystemExit, e:
            self.assertEqual(e.code, 2)
        else:
            self.fail("expected SystemExit")

    def test_environment_overrides_configfile(self):
        options = self.create_with_config("opt 3")
        options.realize([])
        self.assertEqual(options.opt, 3)

        os.environ["OPT"] = "2"
        options = self.create_with_config("opt 3")
        options.realize([])
        self.assertEqual(options.opt, 2)


class TestCommandLineOverrides(EnvironmentOptions):

    def test_simple_override(self):
        options = self.create_with_config("# empty config")
        options.realize(["-X", "opt=-2"])
        self.assertEqual(options.opt, -2)

    def test_error_propogation(self):
        self.check_exit_code(self.create_with_config("# empty"),
                             ["-Xopt=1", "-Xopt=2"])
        self.check_exit_code(self.create_with_config("# empty"),
                             ["-Xunknown=foo"])


def test_suite():
    suite = unittest.TestSuite()
    for cls in [TestBasicFunctionality,
                TestZDOptionsEnvironment,
                TestCommandLineOverrides]:
        suite.addTest(unittest.makeSuite(cls))
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest='test_suite')
