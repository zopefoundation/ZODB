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
import unittest


class ClassWithNewargs(int):
    def __new__(cls, value):
        return int.__new__(cls, value)

    def __getnewargs__(self):
        return int(self),


class ClassWithoutNewargs(object):
    def __init__(self, value):
        self.value = value


def make_pickle(ob):
    import cPickle
    import cStringIO as StringIO
    sio = StringIO.StringIO()
    p = cPickle.Pickler(sio, 1)
    p.dump(ob)
    return sio.getvalue()


def _test_factory(conn, module_name, name):
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
        from ZODB.serialize import ObjectReader
        r = ObjectReader(factory=_test_factory)
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
        import sys
        from ZODB.serialize import ObjectReader

        class TestObjectReader(ObjectReader):
            # A production object reader would optimize this, but we
            # don't need to in a test
            def _get_class(self, module, name):
                __import__(module)
                return getattr(sys.modules[module], name)

        r = TestObjectReader(factory=_test_factory)
        g = r.getGhost(self.old_style_with_newargs)
        self.assert_(isinstance(g, ClassWithNewargs))
        self.assertEqual(g, 1)
        g = r.getGhost(self.old_style_without_newargs)
        self.assert_(isinstance(g, ClassWithoutNewargs))
        g = r.getGhost(self.new_style_with_newargs)
        self.assert_(isinstance(g, ClassWithNewargs))
        g = r.getGhost(self.new_style_without_newargs)
        self.assert_(isinstance(g, ClassWithoutNewargs))

    def test_myhasattr(self):
        from ZODB.serialize import myhasattr

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

        self.assertRaises(ValueError, myhasattr, OldStyle(), "error")
        self.assertRaises(ValueError, myhasattr, NewStyle(), "error")
        self.assert_(myhasattr(OldStyle(), "bar"))
        self.assert_(myhasattr(NewStyle(), "bar"))
        self.assert_(not myhasattr(OldStyle(), "rat"))
        self.assert_(not myhasattr(NewStyle(), "rat"))


def test_suite():
    import doctest
    return unittest.TestSuite((
        unittest.makeSuite(SerializerTestCase),
        doctest.DocTestSuite("ZODB.serialize"),
    ))
