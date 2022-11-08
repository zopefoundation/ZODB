##############################################################################
#
# Copyright (c) 2003 Zope Foundation and Contributors.
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
four current formats for class description are:

    1. type(obj)
    2. type(obj), obj.__getnewargs__()
    3. (module name, class name), None
    7. (module name, class name), obj.__getnewargs__()

The second of these options is used if the object has a __getnewargs__()
method.  It is intended to support objects like persistent classes that have
custom C layouts that are determined by arguments to __new__().  The
third and fourth (#3 & #7) apply to instances of a persistent class (which
means the class itself is persistent, not that it's a subclass of
Persistent).

The type object is usually stored using the standard pickle mechanism, which
involves the pickle GLOBAL opcode (giving the type's module and name as
strings).  The type may itself be a persistent object, in which case a
persistent reference (see below) is used.

It's unclear what "usually" means in the last paragraph.  There are two
useful places to concentrate confusion about exactly which formats exist:

- ObjectReader.getClassName() below returns a dotted "module.class"
  string, via actually loading a pickle.  This requires that the
  implementation of application objects be available.

- ZODB/utils.py's get_pickle_metadata() tries to return the module and
  class names (as strings) without importing any application modules or
  classes, via analyzing the pickle.

Earlier versions of Zope supported several other kinds of class
descriptions.  The current serialization code reads these descriptions, but
does not write them.  The three earlier formats are:

    4. (module name, class name), __getinitargs__()
    5. class, None
    6. class, __getinitargs__()

Formats 4 and 6 are used only if the class defines a __getinitargs__()
method, but we really can't tell them apart from formats 7 and 2
(respectively).  Formats 5 and 6 are used if the class does not have a
__module__ attribute (I'm not sure when this applies, but I think it occurs
for some but not all ZClasses).


Persistent references
---------------------

When one persistent object pickle refers to another persistent object,
the database uses a persistent reference.

ZODB persistent references are of the form::

oid
    A simple object reference.

(oid, class meta data)
    A persistent object reference

[reference_type, args]
    An extended reference

    Extension references come in a number of subforms, based on the
    reference types.

    The following reference types are defined:

    'w'
        Persistent weak reference.  The arguments consist of an oid
        and optionally a database name.

    The following are planned for the future:

    'n'
        Multi-database simple object reference.  The arguments consist
        of a database name, and an object id.

    'm'
        Multi-database persistent object reference.  The arguments consist
        of a database name, an object id, and class meta data.

The following legacy format is also supported.

[oid]
    A persistent weak reference

Because the persistent object reference forms include class
information, it is not possible to change the class of a persistent
object for which this form is used.  If a transaction changed the
class of an object, a new record with new class metadata would be
written but all the old references would still use the old class.  (It
is possible that we could deal with this limitation in the future.)

An object id is used alone when a class requires arguments
to it's __new__ method, which is signalled by the class having a
__getnewargs__ attribute.

A number of legacyforms are defined:


"""
import logging

from persistent import Persistent
from persistent.wref import WeakRef
from persistent.wref import WeakRefMarker

from ZODB import broken
from ZODB._compat import BytesIO
from ZODB._compat import PersistentPickler
from ZODB._compat import PersistentUnpickler
from ZODB._compat import _protocol
from ZODB._compat import binary
from ZODB.POSException import InvalidObjectReference


_oidtypes = bytes, type(None)


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


class ObjectWriter(object):
    """Serializes objects for storage in the database.

    The ObjectWriter creates object pickles in the ZODB format.  It
    also detects new persistent objects reachable from the current
    object.
    """

    _jar = None

    def __init__(self, obj=None):
        self._file = BytesIO()
        self._p = PersistentPickler(self.persistent_id, self._file, _protocol)
        self._stack = []
        if obj is not None:
            self._stack.append(obj)
            jar = obj._p_jar
            assert myhasattr(jar, "new_oid")
            self._jar = jar

    def persistent_id(self, obj):
        """Return the persistent id for obj.

        >>> from ZODB.tests.util import P
        >>> class DummyJar(object):
        ...     xrefs = True
        ...     def new_oid(self):
        ...         return b'42'
        ...     def db(self):
        ...         return self
        ...     databases = {}

        >>> jar = DummyJar()
        >>> class O(object):
        ...     _p_jar = jar
        >>> writer = ObjectWriter(O)

        Normally, object references include the oid and a cached named
        reference to the class.  Having the class information
        available allows fast creation of the ghost, avoiding
        requiring an additional database lookup.

        >>> bob = P('bob')
        >>> oid, cls = writer.persistent_id(bob)
        >>> oid
        '42'
        >>> cls is P
        True

        To work with Python 3, the oid in the persistent id is of the
        zodbpickle binary type:

        >>> oid.__class__ is binary
        True


        If a persistent object does not already have an oid and jar,
        these will be assigned by persistent_id():

        >>> bob._p_oid
        '42'
        >>> bob._p_jar is jar
        True

        If the object already has a persistent id, the id is not changed:

        >>> bob._p_oid = b'24'
        >>> oid, cls = writer.persistent_id(bob)
        >>> oid
        '24'
        >>> cls is P
        True

        If the jar doesn't match that of the writer, an error is raised:

        >>> bob._p_jar = DummyJar()
        >>> writer.persistent_id(bob)
        ... # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
        Traceback (most recent call last):
          ...
        InvalidObjectReference:
        ('Attempt to store an object from a foreign database connection',
        <ZODB.serialize.DummyJar ...>, P(bob))

        Constructor arguments used by __new__(), as returned by
        __getnewargs__(), can affect memory allocation, but may also
        change over the life of the object.  This makes it useless to
        cache even the object's class.

        >>> class PNewArgs(P):
        ...     def __getnewargs__(self):
        ...         return ()

        >>> sam = PNewArgs('sam')
        >>> writer.persistent_id(sam)
        '42'
        >>> sam._p_oid
        '42'
        >>> sam._p_jar is jar
        True

        Check that simple objects don't get accused of persistence:

        >>> writer.persistent_id(42)
        >>> writer.persistent_id(object())

        Check that a classic class doesn't get identified improperly:

        >>> class ClassicClara(object):
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

        if not (oid is None or isinstance(oid, bytes)):
            # Deserves a closer look:

            # Make sure it's not a descriptor
            if hasattr(oid, '__get__'):
                # The oid is a descriptor.  That means obj is a non-persistent
                # class whose instances are persistent, so ...
                # Not persistent, pickle normally
                return None

            if oid is WeakRefMarker:
                # we have a weakref, see weakref.py

                oid = obj.oid
                if oid is None:
                    target = obj()  # get the referenced object
                    oid = target._p_oid
                    if oid is None:
                        # Here we are causing the object to be saved in
                        # the database. One could argue that we shouldn't
                        # do this, because a weakref should not cause an object
                        # to be added.  We'll be optimistic, though, and
                        # assume that the object will be added eventually.

                        oid = self._jar.new_oid()
                        target._p_jar = self._jar
                        target._p_oid = oid
                        self._stack.append(target)
                    obj.oid = oid
                    obj.dm = target._p_jar
                    obj.database_name = obj.dm.db().database_name

                oid = binary(oid)
                if obj.dm is self._jar:
                    return ['w', (oid, )]
                else:
                    return ['w', (oid, obj.database_name)]

        # Since we have an oid, we have either a persistent instance
        # (an instance of Persistent), or a persistent class.

        # NOTE! Persistent classes don't (and can't) subclass persistent.

        database_name = None

        if oid is None:
            oid = obj._p_oid = self._jar.new_oid()
            obj._p_jar = self._jar
            self._stack.append(obj)

        elif obj._p_jar is not self._jar:
            if not self._jar.db().xrefs:
                raise InvalidObjectReference(
                    "Database %r doesn't allow implicit cross-database "
                    "references" % self._jar.db().database_name,
                    self._jar, obj)

            try:
                otherdb = obj._p_jar.db()
                database_name = otherdb.database_name
            except AttributeError:
                otherdb = self

            if self._jar.db().databases.get(database_name) is not otherdb:
                raise InvalidObjectReference(
                    "Attempt to store an object from a foreign "
                    "database connection", self._jar, obj,
                )

            if self._jar.get_connection(database_name) is not obj._p_jar:
                raise InvalidObjectReference(
                    "Attempt to store a reference to an object from "
                    "a separate connection to the same database or "
                    "multidatabase", self._jar, obj,
                )

            # OK, we have an object from another database.
            # Lets make sure the object ws not *just* loaded.

            if obj._p_jar._implicitlyAdding(oid):
                raise InvalidObjectReference(
                    "A new object is reachable from multiple databases. "
                    "Won't try to guess which one was correct!",
                    self._jar, obj,
                )

        oid = binary(oid)
        klass = type(obj)
        if hasattr(klass, '__getnewargs__'):
            # We don't want to save newargs in object refs.
            # It's possible that __getnewargs__ is degenerate and
            # returns (), but we don't want to have to deghostify
            # the object to find out.

            # Note that this has the odd effect that, if the class has
            # __getnewargs__ of its own, we'll lose the optimization
            # of caching the class info.

            if database_name is not None:
                return ['n', (database_name, oid)]

            return oid

        # Note that we never get here for persistent classes.
        # We'll use direct refs for normal classes.

        if database_name is not None:
            return ['m', (database_name, oid, klass)]

        return oid, klass

    def serialize(self, obj):
        # We don't use __class__ here, because obj could be a persistent proxy.
        # We don't want to be fooled by proxies.
        klass = type(obj)

        # We want to serialize persistent classes by name if they have
        # a non-None non-empty module so as not to have a direct
        # ref. This is important when copying.  We probably want to
        # revisit this in the future.
        newargs = getattr(obj, "__getnewargs__", None)
        if (isinstance(getattr(klass, '_p_oid', 0), _oidtypes)
                and klass.__module__):
            # This is a persistent class with a non-empty module.  This
            # uses pickle format #3 or #7.
            klass = klass.__module__, klass.__name__
            if newargs is None:
                meta = klass, None
            else:
                meta = klass, newargs()
        elif newargs is None:
            # Pickle format #1.
            meta = klass
        else:
            # Pickle format #2.
            meta = klass, newargs()

        return self._dump(meta, obj.__getstate__())

    def _dump(self, classmeta, state):
        # To reuse the existing BytesIO object, we must reset
        # the file position to 0 and truncate the file after the
        # new pickle is written.
        self._file.seek(0)
        self._p.clear_memo()
        self._p.dump(classmeta)
        self._p.dump(state)
        self._file.truncate()
        return self._file.getvalue()

    def __iter__(self):
        return NewObjectIterator(self._stack)


class NewObjectIterator(object):

    # The pickler is used as a forward iterator when the connection
    # is looking for new objects to pickle.

    def __init__(self, stack):
        self._stack = stack

    def __iter__(self):
        return self

    def __next__(self):
        if self._stack:
            elt = self._stack.pop()
            return elt
        else:
            raise StopIteration

    next = __next__


class ObjectReader(object):

    def __init__(self, conn=None, cache=None, factory=None):
        self._conn = conn
        self._cache = cache
        self._factory = factory

    def _get_class(self, module, name):
        return self._factory(self._conn, module, name)

    def _get_unpickler(self, pickle):
        file = BytesIO(pickle)

        factory = self._factory
        conn = self._conn

        def find_global(modulename, name):
            return factory(conn, modulename, name)
        unpickler = PersistentUnpickler(
            find_global, self._persistent_load, file)

        return unpickler

    loaders = {}

    def _persistent_load(self, reference):
        if isinstance(reference, tuple):
            return self.load_persistent(*reference)
        elif isinstance(reference, (bytes, str)):
            return self.load_oid(reference)
        else:
            try:
                reference_type, args = reference
            except ValueError:
                # weakref
                return self.loaders['w'](self, *reference)
            else:
                return self.loaders[reference_type](self, *args)

    def load_persistent(self, oid, klass):
        # Quick instance reference.  We know all we need to know
        # to create the instance w/o hitting the db, so go for it!

        if not isinstance(oid, bytes):
            assert isinstance(oid, str)
            # this happens on Python 3 when all bytes in the oid are < 0x80
            oid = oid.encode('ascii')

        obj = self._cache.get(oid, None)
        if obj is not None:
            return obj

        if isinstance(klass, tuple):
            klass = self._get_class(*klass)

        if issubclass(klass, broken.Broken):
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

        # TODO: should be done by connection
        self._cache.new_ghost(oid, obj)
        return obj

    def load_multi_persistent(self, database_name, oid, klass):
        conn = self._conn.get_connection(database_name)
        # TODO, make connection _cache attr public
        reader = ObjectReader(conn, conn._cache, self._factory)
        return reader.load_persistent(oid, klass)

    loaders['m'] = load_multi_persistent

    def load_persistent_weakref(self, oid, database_name=None):
        if not isinstance(oid, bytes):
            assert isinstance(oid, str)
            # this happens on Python 3 when all bytes in the oid are < 0x80
            oid = oid.encode('ascii')
        obj = WeakRef.__new__(WeakRef)
        obj.oid = oid
        if database_name is None:
            obj.dm = self._conn
        else:
            obj.database_name = database_name
            try:
                obj.dm = self._conn.get_connection(database_name)
            except KeyError:
                # XXX Not sure what to do here.  It seems wrong to
                # fail since this is a weak reference.  For now we'll
                # just pretend that the target object has gone.
                pass
        return obj

    loaders['w'] = load_persistent_weakref

    def load_oid(self, oid):
        if not isinstance(oid, bytes):
            assert isinstance(oid, str)
            # this happens on Python 3 when all bytes in the oid are < 0x80
            oid = oid.encode('ascii')
        obj = self._cache.get(oid, None)
        if obj is not None:
            return obj
        return self._conn.get(oid)

    def load_multi_oid(self, database_name, oid):
        conn = self._conn.get_connection(database_name)
        # TODO, make connection _cache attr public
        reader = ObjectReader(conn, conn._cache, self._factory)
        return reader.load_oid(oid)

    loaders['n'] = load_multi_oid

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

        if issubclass(klass, broken.Broken):
            # We got a broken class. We might need to make it
            # PersistentBroken
            if not issubclass(klass, broken.PersistentBroken):
                klass = broken.persistentBroken(klass)

        return klass.__new__(klass, *args)

    def getState(self, pickle):
        unpickler = self._get_unpickler(pickle)
        try:
            unpickler.load()  # skip the class metadata
            return unpickler.load()
        except EOFError:
            log = logging.getLogger("ZODB.serialize")
            log.exception("Unpickling error: %r", pickle)
            raise

    def setGhostState(self, obj, pickle):
        state = self.getState(pickle)
        obj.__setstate__(state)


def referencesf(p, oids=None):
    """Return a list of object ids found in a pickle

    A list may be passed in, in which case, information is
    appended to it.

    Only ordinary internal references are included.
    Weak and multi-database references are not included.
    """

    refs = []
    u = PersistentUnpickler(None, refs.append, BytesIO(p))
    u.noload()
    u.noload()

    # Now we have a list of referencs.  Need to convert to list of
    # oids:

    if oids is None:
        oids = []

    for reference in refs:
        if isinstance(reference, tuple):
            oid = reference[0]
        elif isinstance(reference, (bytes, str)):
            oid = reference
        else:
            assert isinstance(reference, list)
            continue

        if not isinstance(oid, bytes):
            assert isinstance(oid, str)
            # this happens on Python 3 when all bytes in the oid are < 0x80
            oid = oid.encode('ascii')

        oids.append(oid)

    return oids


oid_klass_loaders = {
    'w': lambda oid, database_name=None: None,
}


def get_refs(a_pickle):
    """Return oid and class information for references in a pickle

    The result of a list of oid and class information tuples.
    If the reference doesn't contain class information, then the
    klass information is None.
    """

    refs = []
    u = PersistentUnpickler(None, refs.append, BytesIO(a_pickle))
    u.noload()
    u.noload()

    # Now we have a list of references.  Need to convert to list of
    # oids and class info:

    result = []

    for reference in refs:
        if isinstance(reference, tuple):
            oid, klass = reference
        elif isinstance(reference, (bytes, str)):
            oid, klass = reference, None
        else:
            assert isinstance(reference, list)
            continue

        if not isinstance(oid, bytes):
            assert isinstance(oid, str)
            # this happens on Python 3 when all bytes in the oid are < 0x80
            oid = oid.encode('ascii')

        result.append((oid, klass))

    return result
