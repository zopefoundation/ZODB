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
# FOR A PARTICULAR PURPOSE
#
##############################################################################

import logging
from cStringIO import StringIO
from cPickle import Unpickler, Pickler
from pickle import PicklingError

import zope.interface

from ZODB.POSException import ConflictError
from ZODB.loglevels import BLATHER

logger = logging.getLogger('ZODB.ConflictResolution')

ResolvedSerial = 'rs'

class BadClassName(Exception):
    pass

_class_cache = {}
_class_cache_get = _class_cache.get
def find_global(*args):
    cls = _class_cache_get(args, 0)
    if cls == 0:
        # Not cached. Try to import
        try:
            module = __import__(args[0], {}, {}, ['cluck'])
        except ImportError:
            cls = 1
        else:
            cls = getattr(module, args[1], 1)
        _class_cache[args] = cls

        if cls == 1:
            logger.log(BLATHER, "Unable to load class", exc_info=True)

    if cls == 1:
        # Not importable
        raise BadClassName(*args)
    return cls

def state(self, oid, serial, prfactory, p=''):
    p = p or self.loadSerial(oid, serial)
    file = StringIO(p)
    unpickler = Unpickler(file)
    unpickler.find_global = find_global
    unpickler.persistent_load = prfactory.persistent_load
    unpickler.load() # skip the class tuple
    return unpickler.load()

class IPersistentReference(zope.interface.Interface):
    '''public contract for references to persistent objects from an object
    with conflicts.'''

    oid = zope.interface.Attribute(
        'The oid of the persistent object that this reference represents')

    database_name = zope.interface.Attribute(
        '''The name of the database of the reference, *if* different.

        If not different, None.''')

    klass = zope.interface.Attribute(
        '''class meta data.  Presence is not reliable.''')

    weak = zope.interface.Attribute(
        '''bool: whether this reference is weak''')

    def __cmp__(other):
        '''if other is equivalent reference, return 0; else raise ValueError.
        
        Equivalent in this case means that oid and database_name are the same.

        If either is a weak reference, we only support `is` equivalence, and
        otherwise raise a ValueError even if the datbase_names and oids are
        the same, rather than guess at the correct semantics.
        
        It is impossible to sort reliably, since the actual persistent
        class may have its own comparison, and we have no idea what it is.
        We assert that it is reasonably safe to assume that an object is
        equivalent to itself, but that's as much as we can say.

        We don't compare on 'is other', despite the
        PersistentReferenceFactory.data cache, because it is possible to
        have two references to the same object that are spelled with different
        data (for instance, one with a class and one without).'''

class PersistentReference(object):

    zope.interface.implements(IPersistentReference)

    weak = False
    oid = database_name = klass = None

    def __init__(self, data):
        self.data = data
        # see serialize.py, ObjectReader._persistent_load
        if isinstance(data, tuple):
            self.oid, self.klass = data
        elif isinstance(data, str):
            self.oid = data
        else: # a list
            reference_type = data[0]
            # 'm' = multi_persistent: (database_name, oid, klass)
            # 'n' = multi_oid: (database_name, oid)
            # 'w' = persistent weakref: (oid)
            #    or persistent weakref: (oid, database_name)
            # else it is a weakref: reference_type
            if reference_type == 'm':
                self.database_name, self.oid, self.klass = data[1]
            elif reference_type == 'n':
                self.database_name, self.oid = data[1]
            elif reference_type == 'w':
                try:
                    self.oid, = data[1]
                except ValueError:
                    self.oid, self.database_name = data[1]
                self.weak = True
            else:
                assert len(data) == 1, 'unknown reference format'
                self.oid = data[0]
                self.weak = True

    def __cmp__(self, other):
        if self is other or (
            isinstance(other, PersistentReference) and 
            self.oid == other.oid and
            self.database_name == other.database_name and
            not self.weak and
            not other.weak):
            return 0
        else:
            raise ValueError(
                "can't reliably compare against different "
                "PersistentReferences")

    def __repr__(self):
        return "PR(%s %s)" % (id(self), self.data)

    def __getstate__(self):
        raise PicklingError("Can't pickle PersistentReference")

class PersistentReferenceFactory:

    data = None

    def persistent_load(self, ref):
        if self.data is None:
            self.data = {}
        key = tuple(ref) # lists are not hashable; formats are different enough
        # even after eliminating list/tuple distinction
        r = self.data.get(key, None)
        if r is None:
            r = PersistentReference(ref)
            self.data[key] = r

        return r

def persistent_id(object):
    if getattr(object, '__class__', 0) is not PersistentReference:
        return None
    return object.data

_unresolvable = {}
def tryToResolveConflict(self, oid, committedSerial, oldSerial, newpickle,
                         committedData=''):
    # class_tuple, old, committed, newstate = ('',''), 0, 0, 0
    try:
        prfactory = PersistentReferenceFactory()
        file = StringIO(newpickle)
        unpickler = Unpickler(file)
        unpickler.find_global = find_global
        unpickler.persistent_load = prfactory.persistent_load
        meta = unpickler.load()
        if isinstance(meta, tuple):
            klass = meta[0]
            newargs = meta[1] or ()
            if isinstance(klass, tuple):
                klass = find_global(*klass)
        else:
            klass = meta
            newargs = ()

        if klass in _unresolvable:
            return None

        newstate = unpickler.load()
        inst = klass.__new__(klass, *newargs)

        try:
            resolve = inst._p_resolveConflict
        except AttributeError:
            _unresolvable[klass] = 1
            return None

        old = state(self, oid, oldSerial, prfactory)
        committed = state(self, oid, committedSerial, prfactory, committedData)

        resolved = resolve(old, committed, newstate)

        file = StringIO()
        pickler = Pickler(file,1)
        pickler.inst_persistent_id = persistent_id
        pickler.dump(meta)
        pickler.dump(resolved)
        return file.getvalue(1)
    except (ConflictError, BadClassName):
        return None
    except:
        # If anything else went wrong, catch it here and avoid passing an
        # arbitrary exception back to the client.  The error here will mask
        # the original ConflictError.  A client can recover from a
        # ConflictError, but not necessarily from other errors.  But log
        # the error so that any problems can be fixed.
        logger.error("Unexpected error", exc_info=True)
        return None

class ConflictResolvingStorage:
    "Mix-in class that provides conflict resolution handling for storages"

    tryToResolveConflict = tryToResolveConflict
