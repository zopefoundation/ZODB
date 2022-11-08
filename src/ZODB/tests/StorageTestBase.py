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
"""Provide a mixin base class for storage tests.

The StorageTestBase class provides basic setUp() and tearDown()
semantics (which you can override), and it also provides a helper
method _dostore() which performs a complete store transaction for a
single object revision.
"""
from __future__ import print_function

import sys
import time

import ZODB.tests.util
from ZODB._compat import BytesIO
from ZODB._compat import PersistentPickler
from ZODB._compat import Unpickler
from ZODB._compat import _protocol
from ZODB.Connection import TransactionMetaData
from ZODB.tests.MinPO import MinPO
from ZODB.utils import u64
from ZODB.utils import z64


ZERO = z64


def snooze():
    # In Windows, it's possible that two successive time.time() calls return
    # the same value.  Tim guarantees that time never runs backwards.  You
    # usually want to call this before you pack a storage, or must make other
    # guarantees about increasing timestamps.
    now = time.time()
    while now == time.time():
        time.sleep(0.1)


def _persistent_id(obj):
    oid = getattr(obj, "_p_oid", None)
    if getattr(oid, "__get__", None) is not None:
        return None
    else:
        return oid


def zodb_pickle(obj):
    """Create a pickle in the format expected by ZODB."""
    f = BytesIO()
    p = PersistentPickler(_persistent_id, f, _protocol)
    klass = obj.__class__
    assert not hasattr(obj, '__getinitargs__'), "not ready for constructors"
    args = None

    mod = getattr(klass, '__module__', None)
    if mod is not None:
        klass = mod, klass.__name__

    state = obj.__getstate__()

    p.dump((klass, args))
    p.dump(state)
    return f.getvalue()


def persistent_load(pid):
    # helper for zodb_unpickle
    return "ref to %s.%s oid=%s" % (pid[1][0], pid[1][1], u64(pid[0]))


def zodb_unpickle(data):
    """Unpickle an object stored using the format expected by ZODB."""
    f = BytesIO(data)
    u = Unpickler(f)
    u.persistent_load = persistent_load
    klass_info = u.load()
    if isinstance(klass_info, tuple):
        if isinstance(klass_info[0], type):
            # Unclear:  what is the second part of klass_info?
            klass, xxx = klass_info
            assert not xxx
        else:
            if isinstance(klass_info[0], tuple):
                modname, klassname = klass_info[0]
            else:
                modname, klassname = klass_info
            if modname == "__main__":
                ns = globals()
            else:
                mod = import_helper(modname)
                ns = mod.__dict__
            try:
                klass = ns[klassname]
            except KeyError:
                print("can't find %s in %r" % (klassname, ns), file=sys.stderr)
        inst = klass()
    else:
        raise ValueError("expected class info: %s" % repr(klass_info))
    state = u.load()
    inst.__setstate__(state)
    return inst


def import_helper(name):
    __import__(name)
    return sys.modules[name]


class StorageTestBase(ZODB.tests.util.TestCase):

    # It would be simpler if concrete tests didn't need to extend
    # setUp() and tearDown().

    _storage = None

    def _close(self):
        # You should override this if closing your storage requires additional
        # shutdown operations.
        if self._storage is not None:
            self._storage.close()

    def tearDown(self):
        self._close()
        ZODB.tests.util.TestCase.tearDown(self)

    def _dostore(self, oid=None, revid=None, data=None,
                 already_pickled=0, user=None, description=None,
                 extension=None):
        """Do a complete storage transaction.  The defaults are:

         - oid=None, ask the storage for a new oid
         - revid=None, use a revid of ZERO
         - data=None, pickle up some arbitrary data (the integer 7)

        Returns the object's new revision id.
        """
        if oid is None:
            oid = self._storage.new_oid()
        if revid is None:
            revid = ZERO
        if data is None:
            data = MinPO(7)
        if type(data) == int:
            data = MinPO(data)
        if not already_pickled:
            data = zodb_pickle(data)
        # Begin the transaction
        t = TransactionMetaData(extension=extension)
        if user is not None:
            t.user = user
        if description is not None:
            t.description = description
        try:
            self._storage.tpc_begin(t)
            # Store an object
            self._storage.store(oid, revid, data, '', t)
            # Finish the transaction
            self._storage.tpc_vote(t)
            revid = self._storage.tpc_finish(t)
        except:  # noqa: E722 do not use bare 'except'
            self._storage.tpc_abort(t)
            raise
        return revid

    def _dostoreNP(self, oid=None, revid=None, data=None,
                   user=None, description=None):
        return self._dostore(oid, revid, data, 1, user, description)

    # The following methods depend on optional storage features.

    def _undo(self, tid, expected_oids=None, note=None):
        # Undo a tid that affects a single object (oid).
        # This is very specialized.
        t = TransactionMetaData()
        t.note(note or u"undo")
        self._storage.tpc_begin(t)
        undo_result = self._storage.undo(tid, t)
        vote_result = self._storage.tpc_vote(t)
        if expected_oids is not None:
            oids = set(undo_result[1]) if undo_result else set()
            if vote_result:
                oids.update(vote_result)
            self.assertEqual(oids, set(expected_oids))
        return self._storage.tpc_finish(t)
