##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
import sys
import logging
from cStringIO import StringIO
from cPickle import Unpickler, Pickler
from pickle import PicklingError

from ZODB.POSException import ConflictError
from ZODB.loglevels import BLATHER

logger = logging.getLogger('zodb.ConflictResolution')

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

class PersistentReference:

    def __repr__(self):
        return "PR(%s %s)" % (id(self), self.data)

    def __getstate__(self):
        raise PicklingError, "Can't pickle PersistentReference"

class PersistentReferenceFactory:

    data = None

    def persistent_load(self, oid):
        if self.data is None:
            self.data = {}

        r = self.data.get(oid, None)
        if r is None:
            r = PersistentReference()
            r.data = oid
            self.data[oid] = r

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
        pickler.persistent_id = persistent_id
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
