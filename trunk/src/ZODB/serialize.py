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
Each serialized object has two parts: the class description and the
object state.  The class description must provide enough information
to call the class's ``__new__`` and create an empty object.  Once the
object exists as a ghost, its state is passed to ``__setstate__``.

The class description can be in a variety of formats, in part to
provide backwards compatibility with earlier versions of Zope.  The
two current formats for class description are:

    - type(obj)
    - type(obj), obj.__getnewargs__()

The second of these options is used if the object has a
__getnewargs__() method.  It is intended to support objects like
persistent classes that have custom C layouts that are determined by
arguments to __new__().

The type object is usually stored using the standard pickle mechanism,
which uses a string containing the class's module and name.  The type
may itself be a persistent object, in which case a persistent
reference (see below) is used.

Earlier versions of Zope supported several other kinds of class
descriptions.  The current serialization code reads these
descriptions, but does not write them.

The four formats are:

    1. (module name, class name), None
    2. (module name, class name), __getinitargs__()
    3. class, None
    4. class, __getinitargs__()

Formats 2 and 4 are used only if the class defines an
__getinitargs__() method.  Formats 3 and 4 are used if the class does
not have an __module__ attribute.  (I'm not sure when this applies,
but I think it occurs for some but not all ZClasses.)


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
import logging


from persistent import Persistent
from persistent.wref import WeakRefMarker, WeakRef
from ZODB import broken
from ZODB.broken import Broken
from ZODB.POSException import InvalidObjectReference

# Might to update or redo coptimizations to reflect weakrefs:
# from ZODB.coptimizations import new_persistent_id

def myhasattr(obj, name, _marker=object()):
    """Make sure we don't mask exceptions like hasattr().

    We don't want exceptions other than AttributeError to be masked,
    since that too often masks other programming errors.
    Three-argument getattr() doesn't mask those, so we use that to
    implement our own hasattr() replacement.
    """
    return getattr(obj, name, _marker) is not _marker


class BaseObjectWriter:
    """Serializes objects for storage in the database.

    The ObjectWriter creates object pickles in the ZODB format.  It
    also detects new persistent objects reachable from the current
    object.
    """

    def __init__(self, jar=None):
        self._file = cStringIO.StringIO()
        self._p = cPickle.Pickler(self._file, 1)
        self._stack = []
        self._p.persistent_id = self.persistent_id
        if jar is not None:
            assert myhasattr(jar, "new_oid")
        self._jar = jar

    def persistent_id(self, obj):
        """Return the persistent id for obj.

        >>> from ZODB.tests.util import P
        >>> class DummyJar:
        ...     def new_oid(self):
        ...         return 42
        >>> jar = DummyJar()
        >>> writer = BaseObjectWriter(jar)

        Normally, object references include the oid and a cached
        reference to the class.  Having the class available allows
        fast creation of the ghost, avoiding requiring an additional
        database lookup.

        >>> bob = P('bob')
        >>> oid, cls = writer.persistent_id(bob)
        >>> oid
        42
        >>> cls is P
        True

        If a persistent object does not already have an oid and jar,
        these will be assigned by persistent_id():

        >>> bob._p_oid
        42
        >>> bob._p_jar is jar
        True

        If the object already has a persistent id, the id is not changed:

        >>> bob._p_oid = 24
        >>> oid, cls = writer.persistent_id(bob)
        >>> oid
        24
        >>> cls is P
        True

        If the jar doesn't match that of the writer, an error is raised:

        >>> bob._p_jar = DummyJar()
        >>> writer.persistent_id(bob)
        Traceback (most recent call last):
          ...
        InvalidObjectReference: Attempt to store an object from a """ \
               """foreign database connection

        Constructor arguments used by __new__(), as returned by
        __getnewargs__(), can affect memory allocation, but may also
        change over the life of the object.  This makes it useless to
        cache even the object's class.

        >>> class PNewArgs(P):
        ...     def __getnewargs__(self):
        ...         return ()

        >>> sam = PNewArgs('sam')
        >>> writer.persistent_id(sam)
        42
        >>> sam._p_oid
        42
        >>> sam._p_jar is jar
        True

        Check that simple objects don't get accused of persistence:

        >>> writer.persistent_id(42)
        >>> writer.persistent_id(object())

        Check that a classic class doesn't get identified improperly:

        >>> class ClassicClara:
        ...    pass
        >>> clara = ClassicClara()

        >>> writer.persistent_id(clara)
        """

        # Most objects are not persistent. The following cheap test
        # identifies most of them.  For these, we return None,
        # signalling that the object should be pickled normally.
        if not isinstance(obj, (Persistent, type, WeakRef)):
            # Not persistent, pickle normally
            return None

        # Any persistent object must have an oid:
        try:
            oid = obj._p_oid
        except AttributeError:
            # Not persistent, pickle normally
            return None

        if not (oid is None or isinstance(oid, str)):
            # Deserves a closer look:

            # Make sure it's not a descr
            if hasattr(oid, '__get__'):
                # The oid is a decriptor.  That means obj is a non-persistent
                # class whose instances are persistent, so ...
                # Not persistent, pickle normally
                return None

            if oid is WeakRefMarker:
                # we have a weakref, see weakref.py

                oid = obj.oid
                if oid is None:
                    obj = obj() # get the referenced object
                    oid = obj._p_oid
                    if oid is None:
                        # Here we are causing the object to be saved in
                        # the database. One could argue that we shouldn't
                        # do this, because a weakref should not cause an object
                        # to be added.  We'll be optimistic, though, and
                        # assume that the object will be added eventually.

                        oid = self._jar.new_oid()
                        obj._p_jar = self._jar
                        obj._p_oid = oid
                        self._stack.append(obj)
                return [oid]


        # Since we have an oid, we have either a persistent instance
        # (an instance of Persistent), or a persistent class.

        # NOTE! Persistent classes don't (and can't) subclass persistent.

        if oid is None:
            oid = obj._p_oid = self._jar.new_oid()
            obj._p_jar = self._jar
            self._stack.append(obj)
        elif obj._p_jar is not self._jar:
            raise InvalidObjectReference(
                "Attempt to store an object from a foreign "
                "database connection"
                )

        klass = type(obj)
        if hasattr(klass, '__getnewargs__'):
            # We don't want to save newargs in object refs.
            # It's possible that __getnewargs__ is degenerate and
            # returns (), but we don't want to have to deghostify
            # the object to find out.
            return oid 

        return oid, klass

    def serialize(self, obj):
        # We don't use __class__ here, because obj could be a persistent proxy.
        # We don't want to be fooled by proxies.
        klass = type(obj)

        newargs = getattr(obj, "__getnewargs__", None)
        if newargs is None:
            meta = klass
        else:
            meta = klass, newargs()

        return self._dump(meta, obj.__getstate__())

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
        if not args and not myhasattr(klass, "__getnewargs__"):
            obj = klass.__new__(klass)
        else:
            obj = klass(*args)
            if not isinstance(klass, type):
                obj.__dict__.clear()

        return obj

    def getClassName(self, pickle):
        unpickler = self._get_unpickler(pickle)
        klass = unpickler.load()
        if isinstance(klass, tuple):
            klass, args = klass
            if isinstance(klass, tuple):
                # old style reference
                return "%s.%s" % klass
        return "%s.%s" % (klass.__module__, klass.__name__)

    def getGhost(self, pickle):
        unpickler = self._get_unpickler(pickle)
        klass = unpickler.load()
        if isinstance(klass, tuple):
            # Here we have a separate class and args.
            # This could be an old record, so the class module ne a named
            # refernce
            klass, args = klass
            if isinstance(klass, tuple):
                # Old module_name, class_name tuple
                klass = self._get_class(*klass)

            if args is None:
                args = ()
        else:
            # Definitely new style direct class reference
            args = ()

        if issubclass(klass, Broken):
            # We got a broken class. We might need to make it
            # PersistentBroken
            if not issubclass(klass, broken.PersistentBroken):
                klass = broken.persistentBroken(klass)

        return klass.__new__(klass, *args)

    def getState(self, pickle):
        unpickler = self._get_unpickler(pickle)
        try:
            unpickler.load() # skip the class metadata
            return unpickler.load()
        except EOFError, msg:
            log = logging.getLogger("zodb.serialize")
            log.exception("Unpickling error: %r", pickle)
            raise

    def setGhostState(self, obj, pickle):
        state = self.getState(pickle)
        obj.__setstate__(state)


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

    def _get_unpickler(self, pickle):
        unpickler = BaseObjectReader._get_unpickler(self, pickle)
        factory = self._factory
        conn = self._conn

        def find_global(modulename, name):
            return factory(conn, modulename, name)

        unpickler.find_global = find_global

        return unpickler

    def _persistent_load(self, oid):
        if isinstance(oid, tuple):
            # Quick instance reference.  We know all we need to know
            # to create the instance w/o hitting the db, so go for it!
            oid, klass = oid

            obj = self._cache.get(oid, None) # XXX it's not a dict
            if obj is not None:
                return obj

            if isinstance(klass, tuple):
                klass = self._get_class(*klass)

            if issubclass(klass, Broken):
                # We got a broken class. We might need to make it
                # PersistentBroken
                if not issubclass(klass, broken.PersistentBroken):
                    klass = broken.persistentBroken(klass)

            try:
                obj = klass.__new__(klass)
            except TypeError:
                # Couldn't create the instance.  Maybe there's more
                # current data in the object's actual record!
                return self._conn.get(oid)

            # XXX should be done by connection
            obj._p_oid = oid
            obj._p_jar = self._conn
            # When an object is created, it is put in the UPTODATE
            # state.  We must explicitly deactivate it to turn it into
            # a ghost.
            obj._p_changed = None

            self._cache[oid] = obj
            return obj

        elif isinstance(oid, list):
            # see weakref.py
            [oid] = oid
            obj = WeakRef.__new__(WeakRef)
            obj.oid = oid
            obj.dm = self._conn
            return obj

        obj = self._cache.get(oid, None)
        if obj is not None:
            return obj
        return self._conn.get(oid)

def referencesf(p, rootl=None):

    if rootl is None:
        rootl = []

    u = cPickle.Unpickler(cStringIO.StringIO(p))
    l = len(rootl)
    u.persistent_load = rootl
    u.noload()
    try:
        u.noload()
    except:
        # Hm.  We failed to do second load.  Maybe there wasn't a
        # second pickle.  Let's check:
        f = cStringIO.StringIO(p)
        u = cPickle.Unpickler(f)
        u.persistent_load = []
        u.noload()
        if len(p) > f.tell():
            raise ValueError, 'Error unpickling, %s' % p


    # References may be:
    #
    # - A tuple, in which case they are an oid and class.
    #   In this case, just extract the first element, which is
    #   the oid
    #
    # - A list, which is a weak reference. We skip those.
    #
    # - Anything else must be an oid. This means that an oid
    #   may not be a list or a tuple. This is a bit lame.
    #   We could avoid this lamosity by allowing single-element
    #   tuples, so that we wrap oids that are lists or tuples in
    #   tuples.
    #
    # - oids may *not* be False.  I'm not sure why. 

    out = []
    for v in rootl:
        assert v # Let's see if we ever get empty ones
        if type(v) is list:
            # skip wekrefs
            continue
        if type(v) is tuple:
            v = v[0]
        out.append(v)

    rootl[:] = out

    return rootl
