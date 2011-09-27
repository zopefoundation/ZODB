##############################################################################
#
# Copyright (c) 2009 Zope Corporation and Contributors.
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
import gc
import weakref

from zope.interface import implements

from persistent.interfaces import CHANGED
from persistent.interfaces import GHOST
from persistent.interfaces import IPickleCache
from persistent.interfaces import STICKY

class RingNode(object):
    # 32 byte fixed size wrapper.
    __slots__ = ('object', 'next', 'prev')
    def __init__(self, object, next=None, prev=None):
        self.object = object
        self.next = next
        self.prev = prev

class PickleCache(object):
    implements(IPickleCache)

    def __init__(self, jar, target_size=0, cache_size_bytes=0):
        # TODO:  forward-port Dieter's bytes stuff
        self.jar = jar
        self.target_size = target_size
        self.drain_resistance = 0
        self.non_ghost_count = 0
        self.persistent_classes = {}
        self.data = weakref.WeakValueDictionary()
        self.ring = RingNode(None)
        self.ring.next = self.ring.prev = self.ring

    # IPickleCache API
    def __len__(self):
        """ See IPickleCache.
        """
        return (len(self.persistent_classes) +
                len(self.data))

    def __getitem__(self, oid):
        """ See IPickleCache.
        """
        value = self.data.get(oid)
        if value is not None:
            return value
        return self.persistent_classes[oid]

    def __setitem__(self, oid, value):
        """ See IPickleCache.
        """
        if not isinstance(oid, str): # XXX bytes
            raise ValueError('OID must be string: %s' % oid)
        # XXX
        if oid in self.persistent_classes or oid in self.data:
            if self.data[oid] is not value:
                raise KeyError('Duplicate OID: %s' % oid)
        if type(value) is type:
            self.persistent_classes[oid] = value
        else:
            self.data[oid] = value
            if value._p_state != GHOST:
                self.non_ghost_count += 1
                mru = self.ring.prev
                self.ring.prev = node = RingNode(value, self.ring, mru)
                mru.next = node

    def __delitem__(self, oid):
        """ See IPickleCache.
        """
        if not isinstance(oid, str):
            raise ValueError('OID must be string: %s' % oid)
        if oid in self.persistent_classes:
            del self.persistent_classes[oid]
        else:
            value = self.data.pop(oid)
            node = self.ring.next
            if node is None:
                return
            while node is not self.ring:
                if node.object is value:
                    node.prev.next, node.next.prev = node.next, node.prev
                    self.non_ghost_count -= 1
                    break
                node = node.next

    def get(self, oid, default=None):
        """ See IPickleCache.
        """
        value = self.data.get(oid, self)
        if value is not self:
            return value
        return self.persistent_classes.get(oid, default)

    def mru(self, oid):
        """ See IPickleCache.
        """
        node = self.ring.next
        while node is not self.ring and node.object._p_oid != oid:
            node = node.next
        if node is self.ring:
            value = self.data[oid]
            if value._p_state != GHOST:
                self.non_ghost_count += 1
                mru = self.ring.prev
                self.ring.prev = node = RingNode(value, self.ring, mru)
                mru.next = node
        else:
            # remove from old location
            node.prev.next, node.next.prev = node.next, node.prev
            # splice into new
            self.ring.prev.next, node.prev = node, self.ring.prev
            self.ring.prev, node.next = node, self.ring
        
    def ringlen(self):
        """ See IPickleCache.
        """
        result = 0
        node = self.ring.next
        while node is not self.ring:
            result += 1
            node = node.next
        return result

    def items(self):
        """ See IPickleCache.
        """
        return self.data.items()

    def lru_items(self):
        """ See IPickleCache.
        """
        result = []
        node = self.ring.next
        while node is not self.ring:
            result.append((node.object._p_oid, node.object))
            node = node.next
        return result

    def klass_items(self):
        """ See IPickleCache.
        """
        return self.persistent_classes.items()

    def incrgc(self, ignored=None):
        """ See IPickleCache.
        """
        target = self.target_size
        if self.drain_resistance >= 1:
            size = self.non_ghost_count
            target2 = size - 1 - (size / self.drain_resistance)
            if target2 < target:
                target = target2
        self._sweep(target)

    def full_sweep(self, target=None):
        """ See IPickleCache.
        """
        self._sweep(0)

    minimize = full_sweep

    def new_ghost(self, oid, obj):
        """ See IPickleCache.
        """
        if obj._p_oid is not None:
            raise ValueError('Object already has oid')
        if obj._p_jar is not None:
            raise ValueError('Object already has jar')
        if oid in self.persistent_classes or oid in self.data:
            raise KeyError('Duplicate OID: %s' % oid)
        obj._p_oid = oid
        obj._p_jar = self.jar
        if type(obj) is not type:
            if obj._p_state != GHOST:
                obj._p_invalidate()
        self[oid] = obj

    def reify(self, to_reify):
        """ See IPickleCache.
        """
        if isinstance(to_reify, str): #bytes
            to_reify = [to_reify]
        for oid in to_reify:
            value = self[oid]
            if value._p_state == GHOST:
                value._p_activate()
                self.non_ghost_count += 1
                mru = self.ring.prev
                self.ring.prev = node = RingNode(value, self.ring, mru)
                mru.next = node

    def invalidate(self, to_invalidate):
        """ See IPickleCache.
        """
        if isinstance(to_invalidate, str):
            self._invalidate(to_invalidate)
        else:
            for oid in to_invalidate:
                self._invalidate(oid)

    def debug_info(self):
        result = []
        for oid, klass in self.persistent_classes.items():
            result.append((oid,
                            len(gc.getreferents(klass)),
                            type(klass).__name__,
                            klass._p_state,
                            ))
        for oid, value in self.data.items():
            result.append((oid,
                            len(gc.getreferents(value)),
                            type(value).__name__,
                            value._p_state,
                            ))
        return result

    def update_object_size_estimation(self, oid, new_size):
        """ See IPickleCache.
        """
        pass

    cache_size = property(lambda self: self.target_size)
    cache_drain_resistance = property(lambda self: self.drain_resistance)
    cache_non_ghost_count = property(lambda self: self.non_ghost_count)
    cache_data = property(lambda self: dict(self.data.items()))
    cache_klass_count = property(lambda self: len(self.persistent_classes))

    # Helpers
    def _sweep(self, target):
        # lock
        node = self.ring.next
        while node is not self.ring and self.non_ghost_count > target:
            if node.object._p_state not in (STICKY, CHANGED):
                node.prev.next, node.next.prev = node.next, node.prev
                node.object = None
                self.non_ghost_count -= 1
            node = node.next

    def _invalidate(self, oid):
        value = self.data.get(oid)
        if value is not None and value._p_state != GHOST:
            value._p_invalidate()
            node = self.ring.next
            while node is not self.ring:
                if node.object is value:
                    node.prev.next, node.next.prev = node.next, node.prev
                    break
        elif oid in self.persistent_classes:
            del self.persistent_classes[oid]
