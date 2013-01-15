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
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

from persistent import Persistent


class StubObject(Persistent):
    pass


class RecalcitrantObject(Persistent):
    """A Persistent object that will not become a ghost."""

    deactivations = 0

    def _p_deactivate(self):
        self.__class__.deactivations += 1

    def init(cls):
        cls.deactivations = 0

    init = classmethod(init)


class RegularObject(Persistent):

    deactivations = 0
    invalidations = 0

    def _p_deactivate(self):
        self.__class__.deactivations += 1
        super(RegularObject, self)._p_deactivate()

    def _p_invalidate(self):
        self.__class__.invalidations += 1
        super(RegularObject, self)._p_invalidate()

    def init(cls):
        cls.deactivations = 0
        cls.invalidations = 0

    init = classmethod(init)


class PersistentObject(Persistent):
    pass


class Unresolvable(Persistent):
    pass


class ResolveableWhenStateDoesNotChange(Persistent):

    def _p_resolveConflict(old, committed, new):
        from ZODB.POSException import ConflictError
        raise ConflictError


class Resolveable(Persistent):

    def _p_resolveConflict(self, old, committed, new):
        from ZODB.POSException import ConflictError
        resolved = {}
        for k in old:
            if k not in committed:
                if k in new and new[k] == old[k]:
                    continue
                raise ConflictError
            if k not in new:
                if k in committed and committed[k] == old[k]:
                    continue
                raise ConflictError
            if committed[k] != old[k]:
                if new[k] == old[k]:
                    resolved[k] = committed[k]
                    continue
                raise ConflictError
            if new[k] != old[k]:
                if committed[k] == old[k]:
                    resolved[k] = new[k]
                    continue
                raise ConflictError
            resolved[k] = old[k]

        for k in new:
            if k in old:
                continue
            if k in committed:
                raise ConflictError
            resolved[k] = new[k]

        for k in committed:
            if k in old:
                continue
            if k in new:
                raise ConflictError
            resolved[k] = committed[k]

        return resolved


class C_invalidations_of_new_objects_work_after_savepoint(Persistent):

    def __init__(self):
        self.settings = 1

    def _p_invalidate(self):
        print 'INVALIDATE', self.settings
        Persistent._p_invalidate(self)
        print self.settings   # POSKeyError here


class proper_ghost_initialization_with_empty__p_deactivate_class(Persistent):
    def _p_deactivate(self):
        pass


class Clp485456_setattr_in_getstate_doesnt_cause_multiple_stores(Persistent):
    def __getstate__(self):
        self.got = 1
        return self.__dict__.copy()


class Clp9460655(Persistent):
    def __init__(self, word, id):
        super(Clp9460655, self).__init__()
        self.id = id
        self._word = word
