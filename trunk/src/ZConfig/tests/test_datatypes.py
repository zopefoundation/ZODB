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
"""Tests of standard ZConfig datatypes."""

import os
import sys
import shutil
import socket
import datetime
import tempfile
import unittest

import ZConfig.datatypes

try:
    here = __file__
except NameError:
    here = sys.argv[0]

here = os.path.abspath(here)

try:
    unicode
except NameError:
    have_unicode = False
else:
    have_unicode = True


class DatatypeTestCase(unittest.TestCase):
    types = ZConfig.datatypes.Registry()

    def test_datatype_basickey(self):
        convert = self.types.get("basic-key")
        eq = self.assertEqual
        raises = self.assertRaises

        eq(convert("abc"), "abc")
        eq(convert("ABC_DEF.123"), "abc_def.123")
        eq(convert("Abc-Def-456"), "abc-def-456")
        eq(convert("Abc.Def"), "abc.def")

        raises(ValueError, convert, "_abc")
        raises(ValueError, convert, "-abc")
        raises(ValueError, convert, "123")
        raises(ValueError, convert, "")

    def test_datatype_boolean(self):
        convert = self.types.get("boolean")
        check = self.assert_
        raises = self.assertRaises

        check(convert("on"))
        check(convert("true"))
        check(convert("yes"))
        check(not convert("off"))
        check(not convert("false"))
        check(not convert("no"))
        raises(ValueError, convert, '0')
        raises(ValueError, convert, '1')
        raises(ValueError, convert, '')
        raises(ValueError, convert, 'junk')

    def test_datatype_float(self):
        convert = self.types.get("float")
        eq = self.assertEqual
        raises = self.assertRaises

        eq(convert("1"), 1.0)
        self.assert_(type(convert(1)) is type(1.0))
        eq(convert("1.1"), 1.1)
        eq(convert("50.50"), 50.50)
        eq(convert("-50.50"), -50.50)
        eq(convert(0), 0.0)
        eq(convert("0"), 0.0)
        eq(convert("-0"), 0.0)
        eq(convert("0.0"), 0.0)

        raises(ValueError, convert, "junk")
        raises(ValueError, convert, "0x234.1.9")
        raises(ValueError, convert, "0.9-")

        # These are not portable representations; make sure they are
        # disallowed everywhere for consistency.
        raises(ValueError, convert, "inf")
        raises(ValueError, convert, "-inf")
        raises(ValueError, convert, "nan")

        if have_unicode:
            raises(ValueError, convert, unicode("inf"))
            raises(ValueError, convert, unicode("-inf"))
            raises(ValueError, convert, unicode("nan"))

    def test_datatype_identifier(self):
        convert = self.types.get("identifier")
        raises = self.assertRaises
        self.check_names(convert)
        self.check_never_namelike(convert)
        raises(ValueError, convert, ".abc")

    def check_names(self, convert):
        eq = self.assert_ascii_equal
        eq(convert, "AbcDef")
        eq(convert, "a________")
        eq(convert, "abc_def")
        eq(convert, "int123")
        eq(convert, "_abc")
        eq(convert, "_123")
        eq(convert, "__dict__")

    def assert_ascii_equal(self, convert, value):
        v = convert(value)
        self.assertEqual(v, value)
        self.assert_(isinstance(v, str))
        if have_unicode:
            unicode_value = unicode(value)
            v = convert(unicode_value)
            self.assertEqual(v, value)
            self.assert_(isinstance(v, str))

    def check_never_namelike(self, convert):
        raises = self.assertRaises
        raises(ValueError, convert, "2345")
        raises(ValueError, convert, "23.45")
        raises(ValueError, convert, ".45")
        raises(ValueError, convert, "23.")
        raises(ValueError, convert, "abc.")
        raises(ValueError, convert, "-abc")
        raises(ValueError, convert, "-123")
        raises(ValueError, convert, "abc-")
        raises(ValueError, convert, "123-")
        raises(ValueError, convert, "-")
        raises(ValueError, convert, ".")
        raises(ValueError, convert, "&%$*()")
        raises(ValueError, convert, "")

    def test_datatype_dotted_name(self):
        convert = self.types.get("dotted-name")
        raises = self.assertRaises
        self.check_names(convert)
        self.check_dotted_names(convert)
        self.check_never_namelike(convert)
        raises(ValueError, convert, "abc.")
        raises(ValueError, convert, ".abc.")
        raises(ValueError, convert, "abc.def.")
        raises(ValueError, convert, ".abc.def.")
        raises(ValueError, convert, ".abc.def")

    def test_datatype_dotted_suffix(self):
        convert = self.types.get("dotted-suffix")
        eq = self.assert_ascii_equal
        raises = self.assertRaises
        self.check_names(convert)
        self.check_dotted_names(convert)
        self.check_never_namelike(convert)
        eq(convert, ".a")
        eq(convert, ".a.b")
        eq(convert, ".a.b.c.d.e.f.g.h.i.j.k.l.m.n.o")
        raises(ValueError, convert, "abc.")
        raises(ValueError, convert, ".abc.")
        raises(ValueError, convert, "abc.def.")
        raises(ValueError, convert, ".abc.def.")

    def check_dotted_names(self, convert):
        eq = self.assert_ascii_equal
        eq(convert, "abc.def")
        eq(convert, "abc.def.ghi")
        eq(convert, "a.d.g.g.g.g.g.g.g")

    def test_datatype_inet_address(self):
        convert = self.types.get("inet-address")
        eq = self.assertEqual
        defhost = ZConfig.datatypes.DEFAULT_HOST
        eq(convert("Host.Example.Com:80"), ("host.example.com", 80))
        eq(convert(":80"),                 (defhost, 80))
        eq(convert("80"),                  (defhost, 80))
        eq(convert("host.EXAMPLE.com"),    ("host.example.com", None))
        self.assertRaises(ValueError, convert, "40 # foo")

    def test_datatype_integer(self):
        convert = self.types.get("integer")
        eq = self.assertEqual
        raises = self.assertRaises

        eq(convert('-100'), -100)
        eq(convert('-1'), -1)
        eq(convert('-0'), 0)
        eq(convert('0'), 0)
        eq(convert('1'), 1)
        eq(convert('100'), 100)
        eq(convert('65535'), 65535)
        eq(convert('65536'), 65536)

        big = sys.maxint + 1L  # Python 2.1 needs the L suffix here
        s = str(big)           # s won't have the suffix
        eq(convert(s), big)
        eq(convert("-" + s), -big)

        raises(ValueError, convert, 'abc')
        raises(ValueError, convert, '-0xabc')
        raises(ValueError, convert, '')
        raises(ValueError, convert, '123 456')
        raises(ValueError, convert, '123-')

    def test_datatype_locale(self):
        convert = self.types.get("locale")
        # Python supports "C" even when the _locale module is not available
        self.assertEqual(convert("C"), "C")
        self.assertRaises(ValueError, convert, "locale-does-not-exist")

    def test_datatype_port(self):
        convert = self.types.get("port-number")
        eq = self.assertEqual
        raises = self.assertRaises

        raises(ValueError, convert, '-1')
        raises(ValueError, convert, '0')
        eq(convert('1'), 1)
        eq(convert('80'), 80)
        eq(convert('1023'), 1023)
        eq(convert('1024'), 1024)
        eq(convert('60000'), 60000)
        eq(convert('65535'), 0xffff)
        raises(ValueError, convert, '65536')

    def test_datatype_socket_address(self):
        convert = self.types.get("socket-address")
        eq = self.assertEqual
        AF_INET = socket.AF_INET
        defhost = ZConfig.datatypes.DEFAULT_HOST

        def check(value, family, address, self=self, convert=convert):
            a = convert(value)
            self.assertEqual(a.family, family)
            self.assertEqual(a.address, address)

        check("Host.Example.Com:80", AF_INET, ("host.example.com", 80))
        check(":80",                 AF_INET, (defhost, 80))
        check("80",                  AF_INET, (defhost, 80))
        check("host.EXAMPLE.com",    AF_INET, ("host.example.com",None))
        a1 = convert("/tmp/var/@345.4")
        a2 = convert("/tmp/var/@345.4:80")
        self.assertEqual(a1.address, "/tmp/var/@345.4")
        self.assertEqual(a2.address, "/tmp/var/@345.4:80")
        if hasattr(socket, "AF_UNIX"):
            self.assertEqual(a1.family, socket.AF_UNIX)
            self.assertEqual(a2.family, socket.AF_UNIX)
        else:
            self.assert_(a1.family is None)
            self.assert_(a2.family is None)

    def test_ipaddr_or_hostname(self):
        convert = self.types.get('ipaddr-or-hostname')
        eq = self.assertEqual
        raises = self.assertRaises
        eq(convert('hostname'),          'hostname')
        eq(convert('hostname.com'),      'hostname.com')
        eq(convert('www.hostname.com'),  'www.hostname.com')
        eq(convert('HOSTNAME'),          'hostname')
        eq(convert('HOSTNAME.COM'),      'hostname.com')
        eq(convert('WWW.HOSTNAME.COM'),  'www.hostname.com')
        eq(convert('127.0.0.1'),         '127.0.0.1')
        raises(ValueError, convert,  '1hostnamewithleadingnumeric')
        raises(ValueError, convert,  '255.255')
        raises(ValueError, convert,  '12345678')
        raises(ValueError, convert,  '999.999.999.999')
        raises(ValueError, convert,  'a!badhostname')

    def test_existing_directory(self):
        convert = self.types.get('existing-directory')
        eq = self.assertEqual
        raises = self.assertRaises
        eq(convert('.'), '.')
        eq(convert(os.path.dirname(here)), os.path.dirname(here))
        raises(ValueError, convert, tempfile.mktemp())

    def test_existing_file(self):
        convert = self.types.get('existing-file')
        eq = self.assertEqual
        raises = self.assertRaises
        eq(convert('.'), '.')
        eq(convert(here), here)
        raises(ValueError, convert, tempfile.mktemp())

    def test_existing_path(self):
        convert = self.types.get('existing-path')
        eq = self.assertEqual
        raises = self.assertRaises
        eq(convert('.'), '.')
        eq(convert(here), here)
        eq(convert(os.path.dirname(here)), os.path.dirname(here))
        raises(ValueError, convert, tempfile.mktemp())

    def test_existing_dirpath(self):
        convert = self.types.get('existing-dirpath')
        eq = self.assertEqual
        raises = self.assertRaises
        eq(convert('.'), '.')
        eq(convert(here), here)
        raises(ValueError, convert, '/a/hopefully/nonexistent/path')
        raises(ValueError, convert, here + '/bogus')

    def test_byte_size(self):
        eq = self.assertEqual
        raises = self.assertRaises
        convert = self.types.get('byte-size')
        eq(convert('128'), 128)
        eq(convert('128KB'), 128*1024)
        eq(convert('128MB'), 128*1024*1024)
        eq(convert('128GB'), 128*1024*1024*1024L)
        raises(ValueError, convert, '128TB')
        eq(convert('128'), 128)
        eq(convert('128kb'), 128*1024)
        eq(convert('128mb'), 128*1024*1024)
        eq(convert('128gb'), 128*1024*1024*1024L)
        raises(ValueError, convert, '128tb')

    def test_time_interval(self):
        eq = self.assertEqual
        raises = self.assertRaises
        convert = self.types.get('time-interval')
        eq(convert('120'), 120)
        eq(convert('120S'), 120)
        eq(convert('120M'), 120*60)
        eq(convert('120H'), 120*60*60)
        eq(convert('120D'), 120*60*60*24)
        raises(ValueError, convert, '120W')
        eq(convert('120'), 120)
        eq(convert('120s'), 120)
        eq(convert('120m'), 120*60)
        eq(convert('120h'), 120*60*60)
        eq(convert('120d'), 120*60*60*24)
        raises(ValueError, convert, '120w')

    def test_timedelta(self):
        eq = self.assertEqual
        raises = self.assertRaises
        convert = self.types.get('timedelta')
        eq(convert('4w'), datetime.timedelta(weeks=4))
        eq(convert('2d'), datetime.timedelta(days=2))
        eq(convert('7h'), datetime.timedelta(hours=7))
        eq(convert('12m'), datetime.timedelta(minutes=12))
        eq(convert('14s'), datetime.timedelta(seconds=14))
        eq(convert('4w 2d 7h 12m 14s'),
           datetime.timedelta(2, 14, minutes=12, hours=7, weeks=4))


class RegistryTestCase(unittest.TestCase):

    def test_registry_does_not_mask_toplevel_imports(self):
        old_sys_path = sys.path[:]
        tmpdir = tempfile.mkdtemp(prefix="test_datatypes_")
        fn = os.path.join(tmpdir, "datatypes.py")
        f = open(fn, "w")
        f.write(TEST_DATATYPE_SOURCE)
        f.close()
        registry = ZConfig.datatypes.Registry()

        # we really want the temp area to override everything else:
        sys.path.insert(0, tmpdir)
        try:
            datatype = registry.get("datatypes.my_sample_datatype")
        finally:
            shutil.rmtree(tmpdir)
            sys.path[:] = old_sys_path
        self.assertEqual(datatype, 42)

TEST_DATATYPE_SOURCE = """
# sample datatypes file

my_sample_datatype = 42
"""


def test_suite():
    suite = unittest.makeSuite(DatatypeTestCase)
    suite.addTest(unittest.makeSuite(RegistryTestCase))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
