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


ZERO = '\0'*8



# This class is for the root object.  It must not contain a getoid() method
# (really, attribute).  The persistent pickling machinery -- in the dumps()
# function below -- will pickle Root objects as normal, but any attributes
# which reference persistent Object instances will get pickled as persistent
# ids, not as the object's state.  This makes the referencesf stuff work,
# because it pickle sniffs for persistent ids (so we have to get those
# persistent ids into the root object's pickle).
class Root:
    pass


# This is the persistent Object class.  Because it has a getoid() method, the
# persistent pickling machinery -- in the dumps() function below -- will
# pickle the oid string instead of the object's actual state.  Yee haw, this
# stuff is deep. ;)
class Object:
    def __init__(self, oid):
        self._oid = oid

    def getoid(self):
        return self._oid


# Here's where all the magic occurs.  Sadly, the pickle module is a bit
# underdocumented, but here's what happens: by setting the persistent_id
# attribute to getpersid() on the pickler, that function gets called for every
# object being pickled.  By returning None when the object has no getoid
# attribute, it signals pickle to serialize the object as normal.  That's how
# the Root instance gets pickled correctly.  But, if the object has a getoid
# attribute, then by returning that method's value, we tell pickle to
# serialize the persistent id of the object instead of the object's state.
# That sets the pickle up for proper sniffing by the referencesf machinery.
# Fun, huh?
def dumps(obj):
    def getpersid(obj):
        if hasattr(obj, 'getoid'):
            return obj.getoid()
        return None
    s = StringIO()
    p = pickle.Pickler(s)
    p.persistent_id = getpersid
    p.dump(obj)
    return s.getvalue()



class PackableStorageBase:
    # We keep a cache of object ids to instances so that the unpickler can
    # easily return any persistent object.
    _cache = {}

    def _newobj(self):
        # This is a convenience method to create a new persistent Object
        # instance.  It asks the storage for a new object id, creates the
        # instance with the given oid, populates the cache and returns the
        # object.
        oid = self._storage.new_oid()
        obj = Object(oid)
        self._cache[obj.getoid()] = obj
        return obj

    def _makeloader(self):
        # This is the other side of the persistent pickling magic.  We need a
        # custom unpickler to mirror our custom pickler above.  By setting the
        # persistent_load function of the unpickler to self._cache.get(),
        # whenever a persistent id is unpickled, it will actually return the
        # Object instance out of the cache.  As far as returning a function
        # with an argument bound to an instance attribute method, we do it
        # this way because it makes the code in the tests more succinct.
        #
        # BUT!  Be careful in your use of loads() vs. pickle.loads().  loads()
        # should only be used on the Root object's pickle since it's the only
        # special one.  All the Object instances should use pickle.loads().
        def loads(str, persfunc=self._cache.get):
            fp = StringIO(str)
            u = pickle.Unpickler(fp)
            u.persistent_load = persfunc
            return u.load()
        return loads



class PackableStorage(PackableStorageBase):
    def checkPackAllRevisions(self):
        # Create a `persistent' object
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
        data = self._storage.loadSerial(oid, revid1)
        pobj = pickle.loads(data)
        assert pobj.getoid() == oid and pobj.value == 1
        data = self._storage.loadSerial(oid, revid2)
        pobj = pickle.loads(data)
        assert pobj.getoid() == oid and pobj.value == 2
        data = self._storage.loadSerial(oid, revid3)
        pobj = pickle.loads(data)
        assert pobj.getoid() == oid and pobj.value == 3
        # Now pack all transactions
        self._storage.pack(time.time(), referencesf)
        # All revisions of the object should be gone, since there is no
        # reference from the root object to this object.
        self.assertRaises(KeyError,
                          self._storage.loadSerial, oid, revid1)
        self.assertRaises(KeyError,
                          self._storage.loadSerial, oid, revid2)
        self.assertRaises(KeyError,
                          self._storage.loadSerial, oid, revid3)

    def checkPackJustOldRevisions(self):
        loads = self._makeloader()
        # Create a root object.  This can't be an instance of Object,
        # otherwise the pickling machinery will serialize it as a persistent
        # id and not as an object that contains references (persistent ids) to
        # other objects.
        root = Root()
        # Create a persistent object, with some initial state
        obj = self._newobj()
        oid = obj.getoid()
        # Link the root object to the persistent object, in order to keep the
        # persistent object alive.  Store the root object.
        root.obj = obj
        root.value = 0
        revid0 = self._dostoreNP(ZERO, data=dumps(root))
        # Make sure the root can be retrieved
        data, revid = self._storage.load(ZERO, '')
        assert revid == revid0 and loads(data).value == 0
        # Commit three different revisions of the other object
        obj.value = 1
        revid1 = self._dostoreNP(oid, data=pickle.dumps(obj))
        obj.value = 2
        revid2 = self._dostoreNP(oid, revid=revid1, data=pickle.dumps(obj))
        obj.value = 3
        revid3 = self._dostoreNP(oid, revid=revid2, data=pickle.dumps(obj))
        # Now make sure all three revisions can be extracted
        data = self._storage.loadSerial(oid, revid1)
        pobj = pickle.loads(data)
        assert pobj.getoid() == oid and pobj.value == 1
        data = self._storage.loadSerial(oid, revid2)
        pobj = pickle.loads(data)
        assert pobj.getoid() == oid and pobj.value == 2
        data = self._storage.loadSerial(oid, revid3)
        pobj = pickle.loads(data)
        assert pobj.getoid() == oid and pobj.value == 3
        # Now pack just revisions 1 and 2.  The object's current revision
        # should stay alive because it's pointed to by the root.
        self._storage.pack(time.time(), referencesf)
        # Make sure the revisions are gone, but that object zero and revision
        # 3 are still there and correct
        data, revid = self._storage.load(ZERO, '')
        assert revid == revid0 and loads(data).value == 0
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
