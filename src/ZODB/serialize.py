##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Support for ZODB object serialization.

ZODB serializes objects using a custom format based on Python pickles.
When an object is unserialized, it can be loaded as either a ghost or
a real object.  A ghost is a persistent object of the appropriate type
but without any state.  The first time a ghost is accessed, the
persistence machinery traps access and loads the actual state.  A
ghost allows many persistent objects to be loaded while minimizing the
memory consumption of referenced but otherwise unused objects.

Pickle format
-------------

ZODB stores serialized objects using a custom format based on pickle.
Each serialized object has two parts: the class metadata and the
object state.  The class description must provide enough information
to call the class's ``__new__`` and create an empty object.  Once the
object exists as a ghost, its state is passed to ``__setstate__``.

The class metadata can be represented in two different ways, in order
to provide backwards compatibility with many earlier versions of ZODB.
The class metadata is always a two-tuple.  The first element may also
be a tuple, containing two string elements: name of a module and the
name of a class.  The second element of the class metadata tuple is a
tuple of arguments to pass to the class's ``__new__``.

Persistent references
---------------------

A persistent reference is a pair containing an oid and class metadata.
When one persistent object pickle refers to another persistent object,
the database uses a persistent reference.  The format allows a
significant optimization, because ghosts can be created directly from
persistent references.  If the reference was just an oid, a database
access would be required to determine the class of the ghost.

Because the persistent reference includes the class, it is not
possible to change the class of a persistent object.  If a transaction
changed the class of an object, a new record with new class metadata
would be written but all the old references would still include the
old class.

"""

import cPickle
import cStringIO

from ZODB.coptimizations import new_persistent_id

_marker = object()

def myhasattr(obj, attr):
    """Returns True or False or raises an exception."""
    val = getattr(obj, attr, _marker)
    return val is not _marker

def getClassMetadata(obj):
    klass = obj.__class__
    if issubclass(klass, type):
        # Handle ZClasses.
        d = obj.__dict__.copy()
        del d["_p_jar"]
        args = obj.__name__, obj.__bases__, d
        return klass, args
    else:
        getinitargs = getattr(klass, "__getinitargs__", None)
        if getinitargs is None:
            args = None
        else:
            args = getinitargs()
        mod = getattr(klass, "__module__", None)
        if mod is None:
            return klass, args
        else:
            return (mod, klass.__name__), args

class BaseObjectWriter:
    """Serializes objects for storage in the database.

    The ObjectWriter creates object pickles in the ZODB format.  It
    also detects new persistent objects reachable from the current
    object.

    The client is responsible for calling the close() method to avoid
    leaking memory.  The ObjectWriter uses a Pickler internally, and
    Pickler objects do not participate in garbage collection.  (Note
    that in Python 2.3 and higher, the close() method would be
    unnecessary because Picklers participate in garbage collection.)
    """

    def __init__(self, jar=None):
        self._file = cStringIO.StringIO()
        self._p = cPickle.Pickler(self._file, 1)
        self._stack = []
        self._p.persistent_id = new_persistent_id(jar, self._stack)
        if jar is not None:
            assert myhasattr(jar, "new_oid")
        self._jar = jar

    def serialize(self, obj):
        return self._dump(getClassMetadata(obj), obj.__getstate__())

    def _dump(self, classmeta, state):
        # To reuse the existing cStringIO object, we must reset
        # the file position to 0 and truncate the file after the
        # new pickle is written.
        self._file.seek(0)
        self._p.clear_memo()
        self._p.dump(classmeta)
        self._p.dump(state)
        self._file.truncate()
        return self._file.getvalue()

class ObjectWriter(BaseObjectWriter):

    def __init__(self, obj):
        BaseObjectWriter.__init__(self, obj._p_jar)
        self._stack.append(obj)

    def __iter__(self):
        return NewObjectIterator(self._stack)

class NewObjectIterator:

    # The pickler is used as a forward iterator when the connection
    # is looking for new objects to pickle.

    def __init__(self, stack):
        self._stack = stack

    def __iter__(self):
        return self

    def next(self):
        if self._stack:
            elt = self._stack.pop()
            return elt
        else:
            raise StopIteration

class BaseObjectReader:

    def _persistent_load(self, oid):
        # subclasses must define _persistent_load().
        raise NotImplementedError

    def _get_class(self, module, name):
        # subclasses must define _get_class()
        raise NotImplementedError

    def _get_unpickler(self, pickle):
        file = cStringIO.StringIO(pickle)
        unpickler = cPickle.Unpickler(file)
        unpickler.persistent_load = self._persistent_load
        return unpickler

    def _new_object(self, klass, args):
        if not args and not myhasattr(klass, "__getinitargs__"):
            obj = klass.__new__(klass)
        else:
            obj = klass(*args)
            if not isinstance(klass, type):
                obj.__dict__.clear()

        return obj

    def getClassName(self, pickle):
        unpickler = self._get_unpickler(pickle)
        klass, newargs = unpickler.load()
        if isinstance(klass, tuple):
            return "%s.%s" % klass
        else:
            return klass.__name__

    def getGhost(self, pickle):
        unpickler = self._get_unpickler(pickle)
        klass, args = unpickler.load()
        if isinstance(klass, tuple):
            klass = self._get_class(*klass)

        return self._new_object(klass, args)

    def getState(self, pickle):
        unpickler = self._get_unpickler(pickle)
        unpickler.load() # skip the class metadata
        return unpickler.load()

    def setGhostState(self, obj, pickle):
        state = self.getState(pickle)
        obj.__setstate__(state)

    def getObject(self, pickle):
        unpickler = self._get_unpickler(pickle)
        klass, args = unpickler.load()
        obj = self._new_object(klass, args)
        state = unpickler.load()
        obj.__setstate__(state)
        return obj

class ExternalReference(object):
    pass

class SimpleObjectReader(BaseObjectReader):
    """Can be used to inspect a single object pickle.

    It returns an ExternalReference() object for other persistent
    objects.  It can't instantiate the object.
    """

    ext_ref = ExternalReference()

    def _persistent_load(self, oid):
        return self.ext_ref

    def _get_class(self, module, name):
        return None

class ConnectionObjectReader(BaseObjectReader):

    def __init__(self, conn, cache, factory):
        self._conn = conn
        self._cache = cache
        self._factory = factory

    def _get_class(self, module, name):
        return self._factory(self._conn, module, name)

    def _persistent_load(self, oid):
        if isinstance(oid, tuple):
            # Quick instance reference.  We know all we need to know
            # to create the instance w/o hitting the db, so go for it!
            oid, klass_info = oid
            obj = self._cache.get(oid, None) # XXX it's not a dict
            if obj is not None:
                return obj

            klass = self._get_class(*klass_info)
            # XXX Why doesn't this have args?
            obj = self._new_object(klass, None)
            # XXX This doesn't address the last fallback that used to
            # exist:
##                    # Eek, we couldn't get the class. Hm.  Maybe there's
##                    # more current data in the object's actual record!
##                    return self._conn[oid]

            # XXX should be done by connection
            obj._p_oid = oid
            obj._p_jar = self._conn
            # When an object is created, it is put in the UPTODATE
            # state.  We must explicitly deactivate it to turn it into
            # a ghost.
            obj._p_changed = None

            self._cache[oid] = obj
            return obj

        obj = self._cache.get(oid)
        if obj is not None:
            return obj
        return self._conn[oid]
