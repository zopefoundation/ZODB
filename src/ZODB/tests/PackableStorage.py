# Run some tests relevant for storages that support pack()

try:
    import cPickle
    pickle = cPickle
    #import cPickle as pickle
except ImportError:
    import pickle

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

import time

from ZODB.referencesf import referencesf
from Persistence import Persistent



class Object:
    def __init__(self, oid):
        self._oid = oid

    def getoid(self):
        return self._oid



class PackableStorage:
    def _newobj(self):
        if not hasattr(self, '_cache'):
            self._cache = {}
        obj = Object(self._storage.new_oid())
        self._cache[obj.getoid()] = obj
        return obj

    # Override StorageTestBase's method, since we're generating the pickles
    # in this class explicitly.
    def _massagedata(self, data):
        return data

    def checkOldRevisionPacked(self):
        # The initial revision has an object graph like so:
        # o1 -> o2 -> o3
        o1 = self._newobj()
        o2 = self._newobj()
        o3 = self._newobj()
        o1.object = o2
        o2.object = o3
        # Pickle these objects
        def dumps(obj):
            def getpersid(obj):
                return obj.getoid()
            s = StringIO()
            p = pickle.Pickler(s)
            p.persistent_id = getpersid
            p.dump(obj)
            return s.getvalue()
        p1, p2, p3 = map(dumps, (o1, o2, o3))
        # Now commit these objects
        revid1 = self._dostore(oid=o1.getoid(), data=p1)
        revid2 = self._dostore(oid=o2.getoid(), data=p2)
        revid3 = self._dostore(oid=o3.getoid(), data=p3)
        # Record this moment in history so we can pack everything before it
        t0 = time.time()
        # Now change the object graph to look like so:
        # o1 -> o3
        # and note that o2 is no longer referenced
        o1.object = o3
        revid11 = self._dostore(oid=o1.getoid(), revid=revid1, data=dumps(o1))
        # Pack away transaction 2
        self._storage.pack(t0, referencesf)
        # Now, objects 1 and 3 should exist, but object 2 should have been
        # reference counted away.  First, we need a custom unpickler.
        def loads(str, persfunc=self._cache.get):
            fp = StringIO(str)
            u = pickle.Unpickler(fp)
            u.persistent_load = persfunc
            return u.load()
        # Get object 1
        data, revid = self._storage.load(o1.getoid(), '')
        assert revid == revid11
        from ZODB import utils
        assert loads(loads(data)).getoid() == o1.getoid()
        # Get object 3
        data, revid = self._storage.load(o3.getoid(), '')
        assert revid == revid2
        assert loads(loads(data)).getoid() == o3.getoid()
        # Object 2 should fail
        self.assertRaises(KeyError,
                          self._storage.load, o2.getoid(), '')
