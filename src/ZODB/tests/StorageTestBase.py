"""Provide a mixin base class for storage tests.

The StorageTestBase class provides basic setUp() and tearDown()
semantics (which you can override), and it also provides a helper
method _dostore() which performs a complete store transaction for a
single object revision.
"""

import pickle
import string
import sys
import types
import unittest
from cPickle import Pickler, Unpickler
from cStringIO import StringIO

from ZODB.Transaction import Transaction

from ZODB.tests.MinPO import MinPO

ZERO = '\0'*8

def zodb_pickle(obj):
    """Create a pickle in the format expected by ZODB."""
    f = StringIO()
    p = Pickler(f, 1)
    klass = obj.__class__
    assert not hasattr(obj, '__getinitargs__'), "not ready for constructors"
    args = None

    mod = getattr(klass, '__module__', None)
    if mod is not None:
        klass = mod, klass.__name__

    state = obj.__getstate__()

    p.dump((klass, args))
    p.dump(state)
    return f.getvalue(1)

def zodb_unpickle(data):
    """Unpickle an object stored using the format expected by ZODB."""
    f = StringIO(data)
    u = Unpickler(f)
    klass_info = u.load()
    if isinstance(klass_info, types.TupleType):
        if isinstance(klass_info[0], types.TupleType):
            modname, klassname = klass_info[0]
            args = klass_info[1]
        else:
            modname, klassname = klass_info
            args = None
        if modname == "__main__":
            ns = globals()
        else:
            mod = import_helper(modname)
            ns = mod.__dict__
        try:
            klass = ns[klassname]
        except KeyError:
            sys.stderr.write("can't find %s in %s" % (klassname,
                                                      repr(ns)))
        inst = klass()
    else:
        raise ValueError, "expected class info: %s" % repr(klass_info)
    state = u.load()
    inst.__setstate__(state)
    return inst

def import_helper(name):
    mod = __import__(name)
    for part in string.split(name, ".")[1:]:
        mod = getattr(mod, part)
    return mod


class StorageTestBase(unittest.TestCase):
    def setUp(self):
        # You need to override this with a setUp that creates self._storage
        self._transaction = Transaction()

    def _close(self):
        # You should override this if closing your storage requires additional
        # shutdown operations.
        self._transaction.abort()
        self._storage.close()

    def tearDown(self):
        self._close()

    def _dostore(self, oid=None, revid=None, data=None, version=None,
                 already_pickled=0):
        """Do a complete storage transaction.  The defaults are:
        
         - oid=None, ask the storage for a new oid
         - revid=None, use a revid of ZERO
         - data=None, pickle up some arbitrary data (the integer 7)
         - version=None, use the empty string version
        
        Returns the object's new revision id.
        """
        if oid is None:
            oid = self._storage.new_oid()
        if revid is None:
            revid = ZERO
        if data is None:
            data = MinPO(7)
        if type(data) == types.IntType:
            data = MinPO(data)
        if not already_pickled:
            data = zodb_pickle(data)
        if version is None:
            version = ''
        # Begin the transaction
        self._storage.tpc_begin(self._transaction)
        # Store an object
        newrevid = self._storage.store(oid, revid, data, version,
                                       self._transaction)
        # Finish the transaction
        self._storage.tpc_vote(self._transaction)
        self._storage.tpc_finish(self._transaction)
        return newrevid
        
    def _dostoreNP(self, oid=None, revid=None, data=None, version=None):
        return self._dostore(oid, revid, data, version, already_pickled=1)
