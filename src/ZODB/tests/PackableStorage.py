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


def dumps(obj):
    def getpersid(obj):
        return obj.getoid()
    s = StringIO()
    p = pickle.Pickler(s)
    p.persistent_id = getpersid
    p.dump(obj)
    return s.getvalue()


def makeloader(persfunc):
    def loads(str, persfunc=persfunc):
        fp = StringIO(str)
        u = pickle.Unpickler(fp)
        u.persistent_load = persfunc
        return u.load()
    return loads



class PackableStorage:
    def _newobj(self):
        if not hasattr(self, '_cache'):
            self._cache = {}
        obj = Object(self._storage.new_oid())
        self._cache[obj.getoid()] = obj
        return obj

    def checkSimplePack(self):
        # Create the object
        obj = self._newobj()
        oid = obj.getoid()
        obj.value = 1
        # Commit three different revisions
        revid1 = self._dostoreNP(oid, data=pickle.dumps(obj))
        obj.value = 2
        revid2 = self._dostoreNP(oid, revid=revid1, data=pickle.dumps(obj))
        obj.value = 3
        revid3 = self._dostoreNP(oid, revid=revid2, data=pickle.dumps(obj))
        # Now make sure all three revisions can be extracted
        #loads = makeloader(self._cache.get)
        data = self._storage.loadSerial(oid, revid1)
        pobj = pickle.loads(data)
        assert pobj.getoid() == oid and pobj.value == 1
        data = self._storage.loadSerial(oid, revid2)
        pobj = pickle.loads(data)
        assert pobj.getoid() == oid and pobj.value == 2
        data = self._storage.loadSerial(oid, revid3)
        pobj = pickle.loads(data)
        assert pobj.getoid() == oid and pobj.value == 3
        # Now pack away all but the most current revision
        self._storage.pack(time.time(), referencesf)
        # Make sure the first two revisions are gone but the third (current)
        # still exists.
        self.assertRaises(KeyError,
                          self._storage.loadSerial, oid, revid1)
        self.assertRaises(KeyError,
                          self._storage.loadSerial, oid, revid2)
        data = self._storage.loadSerial(oid, revid3)
        pobj = pickle.loads(data)
        assert pobj.getoid() == oid and pobj.value == 3
        data, revid = self._storage.load(oid, '')
        assert revid == revid3
        pobj = pickle.loads(data)
        assert pobj.getoid() == oid and pobj.value == 3
