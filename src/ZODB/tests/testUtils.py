##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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
"""Test the routines to convert between long and 64-bit strings"""
import doctest
import random
import re
import unittest

from persistent import Persistent
from zope.testing import renormalizing

from ZODB._compat import loads
from ZODB.utils import U64
from ZODB.utils import p64
from ZODB.utils import u64


NUM = 100


checker = renormalizing.RENormalizing([
    # Python 3 bytes add a "b".
    (re.compile("b('.*?')"), r"\1"),
    # Windows shows result from 'u64' as long?
    (re.compile(r"(\d+)L"), r"\1"),
])


class TestUtils(unittest.TestCase):

    small = [random.randrange(1, 1 << 32)
             for i in range(NUM)]
    large = [random.randrange(1 << 32, 1 << 64)
             for i in range(NUM)]
    all = small + large

    def test_LongToStringToLong(self):
        for num in self.all:
            s = p64(num)
            n = U64(s)
            self.assertEqual(num, n, "U64() failed")
            n2 = u64(s)
            self.assertEqual(num, n2, "u64() failed")

    def test_KnownConstants(self):
        self.assertEqual(b"\000\000\000\000\000\000\000\001", p64(1))
        self.assertEqual(b"\000\000\000\001\000\000\000\000", p64(1 << 32))
        self.assertEqual(u64(b"\000\000\000\000\000\000\000\001"), 1)
        self.assertEqual(U64(b"\000\000\000\000\000\000\000\001"), 1)
        self.assertEqual(u64(b"\000\000\000\001\000\000\000\000"), 1 << 32)
        self.assertEqual(U64(b"\000\000\000\001\000\000\000\000"), 1 << 32)

    def test_PersistentIdHandlesDescriptor(self):
        from ZODB.serialize import ObjectWriter

        class P(Persistent):
            pass

        writer = ObjectWriter(None)
        self.assertEqual(writer.persistent_id(P), None)

    # It's hard to know where to put this test.  We're checking that the
    # ConflictError constructor uses utils.py's get_pickle_metadata() to
    # deduce the class path from a pickle, instead of actually loading
    # the pickle (and so also trying to import application module and
    # class objects, which isn't a good idea on a ZEO server when avoidable).
    def test_ConflictErrorDoesntImport(self):
        from ZODB.POSException import ConflictError
        from ZODB.serialize import ObjectWriter
        from ZODB.tests.MinPO import MinPO

        obj = MinPO()
        data = ObjectWriter().serialize(obj)

        # The pickle contains a GLOBAL ('c') opcode resolving to MinPO's
        # module and class.
        self.assertTrue(b'cZODB.tests.MinPO\nMinPO\n' in data)

        # Fiddle the pickle so it points to something "impossible" instead.
        data = data.replace(b'cZODB.tests.MinPO\nMinPO\n',
                            b'cpath.that.does.not.exist\nlikewise.the.class\n')
        # Pickle can't resolve that GLOBAL opcode -- gets ImportError.
        self.assertRaises(ImportError, loads, data)

        # Verify that building ConflictError doesn't get ImportError.
        try:
            raise ConflictError(object=obj, data=data)
        except ConflictError as detail:
            # And verify that the msg names the impossible path.
            self.assertTrue(
                'path.that.does.not.exist.likewise.the.class' in str(detail))
        else:
            self.fail("expected ConflictError, but no exception raised")

    def test_get_pickle_metadata_w_protocol_0_class_pickle(self):
        from ZODB._compat import dumps
        from ZODB.utils import get_pickle_metadata
        pickle = dumps(ExampleClass, protocol=0)
        self.assertEqual(get_pickle_metadata(pickle),
                         (__name__, ExampleClass.__name__))

    def test_get_pickle_metadata_w_protocol_1_class_pickle(self):
        from ZODB._compat import dumps
        from ZODB.utils import get_pickle_metadata
        pickle = dumps(ExampleClass, protocol=1)
        self.assertEqual(get_pickle_metadata(pickle),
                         (__name__, ExampleClass.__name__))

    def test_get_pickle_metadata_w_protocol_2_class_pickle(self):
        from ZODB._compat import dumps
        from ZODB.utils import get_pickle_metadata
        pickle = dumps(ExampleClass, protocol=2)
        self.assertEqual(get_pickle_metadata(pickle),
                         (__name__, ExampleClass.__name__))

    def test_get_pickle_metadata_w_protocol_3_class_pickle(self):
        from ZODB._compat import HIGHEST_PROTOCOL
        from ZODB._compat import dumps
        from ZODB.utils import get_pickle_metadata
        if HIGHEST_PROTOCOL >= 3:
            pickle = dumps(ExampleClass, protocol=3)
            self.assertEqual(get_pickle_metadata(pickle),
                             (__name__, ExampleClass.__name__))

    def test_p64_bad_object(self):
        with self.assertRaises(ValueError) as exc:
            p64(2 ** 65)

        e = exc.exception
        # The args will be whatever the struct.error args were,
        # which vary from version to version and across implementations,
        # followed by the bad value
        self.assertEqual(e.args[-1], 2 ** 65)

    def test_u64_bad_object(self):
        with self.assertRaises(ValueError) as exc:
            u64(b'123456789')

        e = exc.exception
        # The args will be whatever the struct.error args were,
        # which vary from version to version and across implementations,
        # followed by the bad value
        self.assertEqual(e.args[-1], b'123456789')


class ExampleClass(object):
    pass


def test_suite():
    suite = unittest.defaultTestLoader.loadTestsFromName(__name__)
    suite.addTest(
        doctest.DocFileSuite('../utils.rst', checker=checker)
    )
    return suite
