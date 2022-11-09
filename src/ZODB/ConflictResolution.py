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
# FOR A PARTICULAR PURPOSE
#
##############################################################################

import logging
# Subtle: Python 2.x has pickle.PicklingError and cPickle.PicklingError,
# and these are unrelated classes!  So we shouldn't use pickle.PicklingError,
# since on Python 2, ZODB._compat.pickle is cPickle.
from pickle import PicklingError

import six

import zope.interface

from ZODB._compat import BytesIO
from ZODB._compat import PersistentPickler
from ZODB._compat import PersistentUnpickler
from ZODB._compat import _protocol
from ZODB.loglevels import BLATHER
from ZODB.POSException import ConflictError


logger = logging.getLogger('ZODB.ConflictResolution')


class BadClassName(Exception):
    pass


class BadClass(object):

    def __init__(self, *args):
        self.args = args

    def __reduce__(self):
        raise BadClassName(*self.args)


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
        if (isinstance(args, tuple) and len(args) == 2 and
                isinstance(args[0], six.string_types) and
                isinstance(args[1], six.string_types)):
            return BadClass(*args)
        else:
            raise BadClassName(*args)
    return cls


def state(self, oid, serial, prfactory, p=''):
    p = p or self.loadSerial(oid, serial)
    p = self._crs_untransform_record_data(p)
    file = BytesIO(p)
    unpickler = PersistentUnpickler(
        find_global, prfactory.persistent_load, file)
    unpickler.load()  # skip the class tuple
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


@zope.interface.implementer(IPersistentReference)
class PersistentReference(object):

    weak = False
    oid = database_name = klass = None

    def __init__(self, data):
        self.data = data
        # see serialize.py, ObjectReader._persistent_load
        if isinstance(data, tuple):
            self.oid, klass = data
            if isinstance(klass, BadClass):
                # We can't use the BadClass directly because, if
                # resolution succeeds, there's no good way to pickle
                # it.  Fortunately, a class reference in a persistent
                # reference is allowed to be a module+name tuple.
                self.data = self.oid, klass.args
        elif isinstance(data, (bytes, str)):
            self.oid = data
        else:  # a list
            reference_type = data[0]
            # 'm' = multi_persistent: (database_name, oid, klass)
            # 'n' = multi_oid: (database_name, oid)
            # 'w' = persistent weakref: (oid)
            #    or persistent weakref: (oid, database_name)
            # else it is a weakref: reference_type
            if reference_type == 'm':
                self.database_name, self.oid, klass = data[1]
                if isinstance(klass, BadClass):
                    # see above wrt BadClass
                    data[1] = self.database_name, self.oid, klass.args
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
        if not isinstance(self.oid, (bytes, type(None))):
            assert isinstance(self.oid, str)
            # this happens on Python 3 when all bytes in the oid are < 0x80
            self.oid = self.oid.encode('ascii')

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

    # Python 3 dropped __cmp__

    def __eq__(self, other):
        return self.__cmp__(other) == 0

    def __ne__(self, other):
        return self.__cmp__(other) != 0

    def __lt__(self, other):
        return self.__cmp__(other) < 0

    def __gt__(self, other):
        return self.__cmp__(other) > 0

    def __le__(self, other):
        return self.__cmp__(other) <= 0

    def __ge__(self, other):
        return self.__cmp__(other) >= 0

    def __repr__(self):
        return "PR(%s %s)" % (id(self), self.data)

    def __getstate__(self):
        raise PicklingError("Can't pickle PersistentReference")

    @property
    def klass(self):
        # for tests
        data = self.data
        if isinstance(data, tuple):
            return data[1]
        elif isinstance(data, list) and data[0] == 'm':
            return data[1][2]


class PersistentReferenceFactory(object):

    data = None

    def persistent_load(self, ref):
        if self.data is None:
            self.data = {}
        # lists are not hashable; formats are different enough
        key = tuple(ref)
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
                         committedData=b''):
    # class_tuple, old, committed, newstate = ('',''), 0, 0, 0
    klass = 'n/a'
    try:
        prfactory = PersistentReferenceFactory()
        newpickle = self._crs_untransform_record_data(newpickle)
        file = BytesIO(newpickle)
        unpickler = PersistentUnpickler(
            find_global, prfactory.persistent_load, file)
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
            raise ConflictError

        inst = klass.__new__(klass, *newargs)

        try:
            resolve = inst._p_resolveConflict
        except AttributeError:
            _unresolvable[klass] = 1
            raise ConflictError

        oldData = self.loadSerial(oid, oldSerial)
        if not committedData:
            committedData = self.loadSerial(oid, committedSerial)

        newstate = unpickler.load()
        old = state(self, oid, oldSerial, prfactory, oldData)
        committed = state(self, oid, committedSerial, prfactory, committedData)

        resolved = resolve(old, committed, newstate)

        file = BytesIO()
        pickler = PersistentPickler(persistent_id, file, _protocol)
        pickler.dump(meta)
        pickler.dump(resolved)
        return self._crs_transform_record_data(file.getvalue())
    except (ConflictError, BadClassName) as e:
        logger.debug(
            "Conflict resolution on %s failed with %s: %s",
            klass, e.__class__.__name__, str(e))
    except:  # noqa: E722 do not use bare 'except'
        # If anything else went wrong, catch it here and avoid passing an
        # arbitrary exception back to the client.  The error here will mask
        # the original ConflictError.  A client can recover from a
        # ConflictError, but not necessarily from other errors.  But log
        # the error so that any problems can be fixed.
        logger.exception(
            "Unexpected error while trying to resolve conflict on %s", klass)

    raise ConflictError(oid=oid, serials=(committedSerial, oldSerial),
                        data=newpickle)


class ConflictResolvingStorage(object):
    "Mix-in class that provides conflict resolution handling for storages"

    tryToResolveConflict = tryToResolveConflict

    _crs_transform_record_data = _crs_untransform_record_data = (
        lambda self, o: o)

    def registerDB(self, wrapper):
        self._crs_untransform_record_data = wrapper.untransform_record_data
        self._crs_transform_record_data = wrapper.transform_record_data
        try:
            m = super(ConflictResolvingStorage, self).registerDB
        except AttributeError:
            pass
        else:
            m(wrapper)
