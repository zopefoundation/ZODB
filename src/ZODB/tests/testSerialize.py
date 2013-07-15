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
from ZODB._compat import Pickler, BytesIO, _protocol


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
    p = Pickler(sio, _protocol)
    p.dump(ob)
    return sio.getvalue()


def _factory(conn, module_name, name):
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
        r = serialize.ObjectReader(factory=_factory)
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

        r = TestObjectReader(factory=_factory)
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


class SerializerFunctestCase(unittest.TestCase):

    def setUp(self):
        import tempfile
        self._tempdir = tempfile.mkdtemp(suffix='serializerfunc')

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tempdir)

    def test_funky_datetime_serialization(self):
        import os
        import subprocess
        fqn = os.path.join(self._tempdir, 'Data.fs')
        prep_args = [sys.executable, '-c',
                     'from ZODB.tests.testSerialize import _functest_prep; '
                     '_functest_prep(%s)' % repr(fqn)]
        # buildout doesn't arrange for the sys.path to be exported,
        # so force it ourselves
        environ = os.environ.copy()
        environ['PYTHONPATH'] = os.pathsep.join(sys.path)
        subprocess.check_call(prep_args, env=environ)
        load_args = [sys.executable, '-c',
                     'from ZODB.tests.testSerialize import _functest_load; '
                     '_functest_load(%s)' % repr(fqn)]
        subprocess.call(load_args, env=environ)

def _working_failing_datetimes():
    import datetime
    WORKING = datetime.datetime(5375, 12, 31, 23, 59, 59)
    # Any date after 5375 A.D. appears to trigger this bug.
    FAILING = datetime.datetime(5376, 12, 31, 23, 59, 59)
    return WORKING, FAILING

def _functest_prep(fqn):
    # Prepare the database with a BTree which won't deserialize
    # if the bug is present.
    # run in separate process)
    import transaction
    from BTrees.OOBTree import OOBTree
    from ZODB import DB
    WORKING, FAILING = _working_failing_datetimes()
    db = DB(fqn)
    conn = db.open()
    try:
        root = conn.root()
        tree = root['tree'] = OOBTree()
        tree[WORKING] = 'working'
        tree[FAILING] = 'failing'
        transaction.commit()
    finally: # Windoze
        conn.close()
        db.close()

def _functest_load(fqn):
    # Open the database and attempt to deserialize the tree
    # (run in separate process)
    from ZODB import DB
    WORKING, FAILING = _working_failing_datetimes()
    db = DB(fqn)
    conn = db.open()
    try:
        root = conn.root()
        tree = root['tree']
        assert tree[WORKING] == 'working'
        assert tree[FAILING] == 'failing'
    finally: # Windoze
        conn.close()
        db.close()

def test_suite():
    return unittest.TestSuite((
        unittest.makeSuite(SerializerTestCase),
        unittest.makeSuite(SerializerFunctestCase),
        doctest.DocTestSuite("ZODB.serialize",
                             checker=ZODB.tests.util.checker),
    ))
