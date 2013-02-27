##############################################################################
#
# Copyright (c) 2004 Zope Foundation and Contributors.
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
import sys
import unittest

import ZODB.tests.util
from ZODB import serialize
from ZODB._compat import Pickler, BytesIO


class ClassWithNewargs(int):
    def __new__(cls, value):
        return int.__new__(cls, value)

    def __getnewargs__(self):
        return int(self),

class ClassWithoutNewargs(object):
    def __init__(self, value):
        self.value = value

def make_pickle(ob):
    sio = BytesIO()
    p = Pickler(sio, 1)
    p.dump(ob)
    return sio.getvalue()


def test_factory(conn, module_name, name):
    return globals()[name]

class SerializerTestCase(unittest.TestCase):

    # old format:  (module, name), None
    old_style_without_newargs = make_pickle(
        ((__name__, "ClassWithoutNewargs"), None))

    # old format:  (module, name), argtuple
    old_style_with_newargs = make_pickle(
        ((__name__, "ClassWithNewargs"), (1,)))

    # new format:  klass
    new_style_without_newargs = make_pickle(
        ClassWithoutNewargs)

    # new format:  klass, argtuple
    new_style_with_newargs = make_pickle(
        (ClassWithNewargs, (1,)))

    def test_getClassName(self):
        r = serialize.ObjectReader(factory=test_factory)
        eq = self.assertEqual
        eq(r.getClassName(self.old_style_with_newargs),
           __name__ + ".ClassWithNewargs")
        eq(r.getClassName(self.new_style_with_newargs),
           __name__ + ".ClassWithNewargs")
        eq(r.getClassName(self.old_style_without_newargs),
           __name__ + ".ClassWithoutNewargs")
        eq(r.getClassName(self.new_style_without_newargs),
           __name__ + ".ClassWithoutNewargs")

    def test_getGhost(self):
        # Use a TestObjectReader since we need _get_class() to be
        # implemented; otherwise this is just a BaseObjectReader.

        class TestObjectReader(serialize.ObjectReader):
            # A production object reader would optimize this, but we
            # don't need to in a test
            def _get_class(self, module, name):
                __import__(module)
                return getattr(sys.modules[module], name)

        r = TestObjectReader(factory=test_factory)
        g = r.getGhost(self.old_style_with_newargs)
        self.assertTrue(isinstance(g, ClassWithNewargs))
        self.assertEqual(g, 1)
        g = r.getGhost(self.old_style_without_newargs)
        self.assertTrue(isinstance(g, ClassWithoutNewargs))
        g = r.getGhost(self.new_style_with_newargs)
        self.assertTrue(isinstance(g, ClassWithNewargs))
        g = r.getGhost(self.new_style_without_newargs)
        self.assertTrue(isinstance(g, ClassWithoutNewargs))

    def test_myhasattr(self):

        class OldStyle:
            bar = "bar"
            def __getattr__(self, name):
                if name == "error":
                    raise ValueError("whee!")
                else:
                    raise AttributeError(name)

        class NewStyle(object):
            bar = "bar"
            def _raise(self):
                raise ValueError("whee!")
            error = property(_raise)

        self.assertRaises(ValueError,
                          serialize.myhasattr, OldStyle(), "error")
        self.assertRaises(ValueError,
                          serialize.myhasattr, NewStyle(), "error")
        self.assertTrue(serialize.myhasattr(OldStyle(), "bar"))
        self.assertTrue(serialize.myhasattr(NewStyle(), "bar"))
        self.assertTrue(not serialize.myhasattr(OldStyle(), "rat"))
        self.assertTrue(not serialize.myhasattr(NewStyle(), "rat"))


def test_suite():
    suite = unittest.makeSuite(SerializerTestCase)
    suite.addTest(
        doctest.DocTestSuite("ZODB.serialize",
                             checker=ZODB.tests.util.checker))
    return suite
