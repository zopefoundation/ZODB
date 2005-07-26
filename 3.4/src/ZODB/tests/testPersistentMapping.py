##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
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

import unittest

import transaction
from transaction import Transaction
import ZODB
from ZODB.MappingStorage import MappingStorage
import cPickle
import cStringIO
import sys

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
            import Persistence
        except ImportError:
            return
        # insert the pickle in place of the root
        s = MappingStorage()
        t = Transaction()
        s.tpc_begin(t)
        s.store('\000' * 8, None, pickle, '', t)
        s.tpc_vote(t)
        s.tpc_finish(t)

        db = ZODB.DB(s)
        # If the root can be loaded successfully, we should be okay.
        r = db.open().root()
        # But make sure it looks like a new mapping
        self.assert_(hasattr(r, 'data'))
        self.assert_(not hasattr(r, '_container'))

    # TODO:  This test fails in ZODB 3.3a1.  It's making some assumption(s)
    # about pickles that aren't true.  Hard to say when it stopped working,
    # because this entire test suite hasn't been run for a long time, due to
    # a mysterious "return None" at the start of the test_suite() function
    # below.  I noticed that when the new checkBackwardCompat() test wasn't
    # getting run.
    def TODO_checkNewPicklesAreSafe(self):
        s = MappingStorage()
        db = ZODB.DB(s)
        r = db.open().root()
        r[1] = 1
        r[2] = 2
        r[3] = r
        transaction.commit()
        # MappingStorage stores serialno + pickle in its _index.
        root_pickle = s._index['\000' * 8][8:]

        f = cStringIO.StringIO(root_pickle)
        u = cPickle.Unpickler(f)
        klass_info = u.load()
        klass = find_global(*klass_info[0])
        inst = klass.__new__(klass)
        state = u.load()
        inst.__setstate__(state)

        self.assert_(hasattr(inst, '_container'))
        self.assert_(not hasattr(inst, 'data'))

    def checkBackwardCompat(self):
        # Verify that the sanest of the ZODB 3.2 dotted paths still works.
        from persistent.mapping import PersistentMapping as newPath
        from ZODB.PersistentMapping import PersistentMapping as oldPath

        self.assert_(oldPath is newPath)

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

if __name__ == "__main__":
    unittest.main()
