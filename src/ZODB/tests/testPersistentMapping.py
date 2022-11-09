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
"""Verify that PersistentMapping works with old versions of Zope.

The comments in PersistentMapping.py address the issue in some detail.
The pickled form of a PersistentMapping must use _container to store
the actual mapping, because old versions of Zope used this attribute.
If the new code doesn't generate pickles that are consistent with the
old code, developers will have a hard time testing the new code.
"""

import sys
import unittest

from six import PY2

import ZODB
from ZODB.Connection import TransactionMetaData
from ZODB.MappingStorage import MappingStorage


# This pickle contains a persistent mapping pickle created from the
# old code.
pickle = ('((U\x0bPersistenceq\x01U\x11PersistentMappingtq\x02Nt.}q\x03U\n'
          '_containerq\x04}q\x05U\x07versionq\x06U\x03oldq\x07ss.\n')


class PMTests(unittest.TestCase):

    def checkOldStyleRoot(self):
        # The Persistence module doesn't exist in Zope3's idea of what ZODB
        # is, but the global `pickle` references it explicitly.  So just
        # bail if Persistence isn't available.
        try:
            import Persistence  # noqa:  F401 'Persistence' imported but unused
        except ImportError:
            return
        # insert the pickle in place of the root
        s = MappingStorage()
        t = TransactionMetaData()
        s.tpc_begin(t)
        s.store('\000' * 8, None, pickle, '', t)
        s.tpc_vote(t)
        s.tpc_finish(t)

        db = ZODB.DB(s)
        # If the root can be loaded successfully, we should be okay.
        r = db.open().root()
        # But make sure it looks like a new mapping
        self.assertTrue(hasattr(r, 'data'))
        self.assertTrue(not hasattr(r, '_container'))

    def checkBackwardCompat(self):
        # Verify that the sanest of the ZODB 3.2 dotted paths still works.
        from persistent.mapping import PersistentMapping as newPath

        from ZODB.PersistentMapping import PersistentMapping as oldPath

        self.assertTrue(oldPath is newPath)

    def checkBasicOps(self):
        from persistent.mapping import PersistentMapping
        m = PersistentMapping({'x': 1}, a=2, b=3)
        m['name'] = 'bob'
        self.assertEqual(m['name'], "bob")
        self.assertEqual(m.get('name', 42), "bob")
        self.assertTrue('name' in m)

        try:
            m['fred']
        except KeyError:
            pass
        else:
            self.fail("expected KeyError")
        self.assertTrue('fred' not in m)
        self.assertEqual(m.get('fred'), None)
        self.assertEqual(m.get('fred', 42), 42)

        keys = sorted(m.keys())
        self.assertEqual(keys, ['a', 'b', 'name', 'x'])

        values = set(m.values())
        self.assertEqual(values, set([1, 2, 3, 'bob']))

        items = sorted(m.items())
        self.assertEqual(items,
                         [('a', 2), ('b', 3), ('name', 'bob'), ('x', 1)])

        if PY2:
            keys = sorted(m.iterkeys())
            self.assertEqual(keys, ['a', 'b', 'name', 'x'])

            values = sorted(m.itervalues())
            self.assertEqual(values, [1, 2, 3, 'bob'])

            items = sorted(m.iteritems())
            self.assertEqual(
                items, [('a', 2), ('b', 3), ('name', 'bob'), ('x', 1)])

    # PersistentMapping didn't have an __iter__ method before ZODB 3.4.2.
    # Check that it plays well now with the Python iteration protocol.
    def checkIteration(self):
        from persistent.mapping import PersistentMapping
        m = PersistentMapping({'x': 1}, a=2, b=3)
        m['name'] = 'bob'

        def check(keylist):
            keylist.sort()
            self.assertEqual(keylist, ['a', 'b', 'name', 'x'])

        check(list(m))
        check([key for key in m])

        i = iter(m)
        keylist = []
        while 1:
            try:
                key = next(i)
            except StopIteration:
                break
            keylist.append(key)
        check(keylist)


def find_global(modulename, classname):
    """Helper for this test suite to get special PersistentMapping"""

    if classname == "PersistentMapping":
        class PersistentMapping(object):
            def __setstate__(self, state):
                self.__dict__.update(state)
        return PersistentMapping
    else:
        __import__(modulename)
        mod = sys.modules[modulename]
        return getattr(mod, classname)


def test_suite():
    return unittest.makeSuite(PMTests, 'check')
