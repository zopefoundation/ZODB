##############################################################################
#
# Copyright 2011 Zope Foundation and Contributors.
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
"""Python BTree implementation
"""

from ZODB.POSException import ConflictError
import persistent

_marker = object()

class _Base(persistent.Persistent):

    _key_type = list
    _to_key = lambda x: x

    def __init__(self, items=None):
        self.clear()
        if items:
            self.update(items)

    def clear(self):
        self._keys = self._key_type()
        self._next = None

    def __len__(self):
        return len(self._keys)

    @property
    def size(self):
        return len(self._keys)

    def _deleteNextBucket(self):
        next = self._next
        if next is not None:
            self._next = next._next

    def _search(self, key):
        # Return non-negative index on success
        # return -(insertion_index+1) on fail
        low=0
        keys = self._keys
        high=len(keys)
        while low < high:
            i = (low+high)//2
            k = keys[i]
            if k == key:
                return i
            if k < key:
                low = i+1
            else:
                high = i
        return -1-low


    def minKey(self, key=_marker):
        if key is _marker:
            return self._keys[0]
        else:
            key = self._to_key(key)
            index = self._search(key)
            if index >- 0:
                return key
            else:
                index = -index-1
                if index < len(self._keys):
                    return self._keys[index]
                else:
                    raise ValueError("no key satisfies the conditions")

    def maxKey(self, key=_marker):
        if key is _marker:
            return self._keys[-1]
        else:
            key = self._to_key(key)
            index = self._search(key)
            if index >= 0:
                return key
            else:
                index = -index-1
                if index:
                    return self._keys[index-1]
                else:
                    raise ValueError("no key satisfies the conditions")

    def _range(min=_marker, max=_marker, excludemin=False, excludemax=False):
        if min is _marker:
            start = 0
            if excludemin:
                start = 1
        else:
            min = self._to_key(min)
            start = self._search(min)
            if start >= 0:
                if excludemin:
                    start += 1
            else:
                start = -start - 1
        if max is _marker:
            end = len(self._keys)
            if excludemax:
                end -= 1
        else:
            max = self._to_key(max)
            end = self._search(max)
            if end >= 0:
                if excludemax:
                    end -= 1
            else:
                end = -end - 1
                if excludemax:
                    end -= 1

        return start, end

    def keys(self, *args, **kw):
        start, end = self._range(*args, **kw)
        return self._keys[start:end]

    def iterkeys(self, *args, **kw):
        if not (args or kw):
            return iter(self._keys)
        keys = self._keys
        return (keys[i] for i in xrange(*self._range(*args, **kw)))

    def __iter__(self):
        return iter(self._keys)

    def has_key(self, key):
        return self._search(self._to_key(key)) >= 0

    __contains__ = has_key

    def _p_resolveConflict(self, *states):
        set = not hasattr(self, '_values')

        buckets = []
        for state in states:
            bucket = self.__class__()
            bucket.__setstate__(state)
            buckets.append(bucket)
        if (buckets[1]._next != buckets[0]._next or
            buckets[2]._next != buckets[0]._next):
            raise ConflictError(-1, -1, -1, 0)

        i1 = _SetIteration(buckets[0])
        i2 = _SetIteration(buckets[1])
        i3 = _SetIteration(buckets[2])

        def merge_error(reason):
            return ConflictError(i1.position, i2.position, i3.position, reason)

        result = self.__class__()

        if set:
            def merge_output(it):
                result._keys.append(it.key)
                it.advance()
        else:
            def merge_output(it):
                result._keys.append(it.key)
                result._values.append(it.value)
                it.advance()

        while i1.active and i2.active and i3.active:
            cmp12 = cmp(i1.key, i2.key)
            cmp13 = cmp(i1.key, i3.key)
            if cmp12==0:
                if cmp13==0:
                    if set:
                        result.add(i1.key)
                    elif i2.value == i1.value:
                        result[i1.key] = i3.value
                    elif i3.value == i1.value:
                        result[i1.key] = i2.value
                    else:
                        raise merge_error(1)
                    i1.advance()
                    i2.advance()
                    i3.advance()
                elif (cmp13 > 0): # insert in new
                    merge_output(i3)
                elif set or i1.value == i2.value: # deleted new
                    if i3.position == 1:
                        # Deleted the first item.  This will modify the
                        # parent node, so we don't know if merging will be
                        # safe
                        raise merge_error(13)
                    i1.advance()
                    i2.advance()
                else:
                    raise merge_error(2)
            elif cmp13 == 0:
                if cmp12 > 0: # insert committed
                    merge_output(i2)
                elif set or i1.value == i3.value: # delete committed
                    if i2.position == 1
                        # Deleted the first item.  This will modify the
                        # parent node, so we don't know if merging will be
                        # safe
                        raise merge_error(13)
                    i1.advance()
                    i3.advance()
                else:
                    raise merge_error(3)
            else: # both keys changed
                cmp23 = cmp(i2.key, i3.key)
                if cmp23 == 0:
                    raise merge_error(4)
                if cmp12 > 0: # insert committed
                    if cmp23 > 0: # insert i3 first
                        merge_output(i3)
                    else:
                        merge_output(i2)
                elif cmp13 > 0: # insert i3
                    merge_output(i3)
                else:
                    merge_error(5) # both deleted same key

        while i2.active and i3.active: # new inserts
            cmp23 = cmp(i2.key, i3.key)
            if cmp23 == 0:
                raise merge_error(6) # dueling insert
            if cmp23 > 0: # insert new
                merge_output(i3)
            else: # insert committed
                merge_output(i2)

        while i1.active and i2.active: # new deletes rest of original
            cmp12 = cmp(i1.key, i2.key)
            if cmp12 > 0: # insert committed
                merge_output(i2)
            elif cmp12 == 0 and (set or i1.value == i2.value): # deleted in new
                i1.advance()
                i2.advance()
            else: # dueling deletes or delete and change
                merge_error(7)

        while i1.active and i3.active: # committed deletes rest of original
            cmp13 = cmp(i1.key, i3.key)
            if cmp13 > 0: # insert new
                merge_output(i3)
            elif cmp13 == 0 and (set or i1.value == i3.value):
                # deleted in committed
                i1.advance()
                i3.advance()
            else: # dueling deletes or delete and change
                merge_error(8)

        if i1.active: # dueling deletes
            merge_error(9)

        while i2.active:
            merge_output(i2)

        while i3.active:
            merge_output(i2)

        if len(result._keys) == 0:
            # If the output bucket is empty, conflict resolution doesn't have
            # enough info to unlink it from its containing BTree correctly.
            raise merge_error(10)

        result._next = buckets[0]._next
        return result.__getstate__()

class _SetIteration:

    def __init__(self, set, useValues=False):
        if set is None:
            set = ()
        self.set = set
        if useValues:
            try:
                itmeth = set.iteritems
            except AttributeError:
                itmeth = set.__iter__
                useValues = False
        else:
            itmeth = set.__iter__

        self.useValues = useValues
        self._next = itmeth().next
        self.active
        self.position = 0
        self.advance()

    def advance(self):
        try:
            if self.useValues:
                self.key, self.value = self._next()
            else:
                self.key = self._next()
            self.position += 1
        except StopIteration:
            self.active = False
            self.position = -1

        return self

class _MappingBase:

    def setdefault(self, key, value):
        return self._set(self._to_key(key), self._to_value(value), True)[1]

    def pop(self, key):
        return self._del(self._to_key(key))[1]

    def update(self, items):
        if hasattr(items, 'iteritems'):
            items = items.iteritems()
        elif hasattr(items, 'items'):
            items = items.items()

        set = self.__setitem__
        for i in items:
            set(*i)

    def __setitem__(self, key, value):
        self._set(self._to_key(key), self._to_value(value))

    def __delitem__(self, key):
        self._del(self._to_key(key))

class _SetBase:

    def add(self, key):
        self._set(self._to_key(key))

    insert = add

    def remove(self, key):
        self._del(self._to_key(key))

    def update(self, items):
        add = self.add
        for i in items:
            add(i)

class Bucket(_Base, _MappingBase):

    _value_type = list
    _to_value = lambda x: x
    VALUE_SAME_CHECK = False

    def clear(self):
        _Base.clear(self)
        self._values = self._value_type()

    def get(self, key, default=None):
        index = self._search(self._to_key(key))
        if index < 0:
            return default
        return self._values[index]

    def __getitem__(self, key):
        index = self._search(self._to_key(key))
        if index < 0:
            raise KeyError(key)
        return self._values[index]

    def _set(self, key, value, ifunset=False):
        """Set a value

        Return: status, value

        Status is:
              None if no change
                  0 if change, but not size change
                  1 if change and size change
        """
        index = self._search(key)
        if index >= 0:
            if (ifunset or
                self.VALUE_SAME_CHECK and value == self._values[index]
                ):
                return None, self._values[index]
            self._p_changed = True
            self._values[index] = value
            return 0, value
        else:
            self._p_changed = True
            index = -index-1
            self._keys.insert(index, key)
            self._values.insert(index, value)
            return 1, value

    __setitem__ = _set

    def _del(self, key):
        index = self._search(key)
        if index >= 0:
            self._p_changed = True
            del self._keys[index]
            return 0, self._values.pop(index)
        else:
            raise KeyError(key)

    def _split(self, index=-1):
        if index < 0 or index >= len(self._keys):
            index = len(self._keys) / 2
        new_instance = self.__class__()
        new_instance._keys = self._keys[i:]
        new_instance._values = self._values[i:]
        del self._keys[i:]
        del self._values[i:]
        new_instance._next = self._next
        self._next = new_instance
        return new_instance

    def values(self, *args, **kw):
        start, end = self._range(*args, **kw)
        return self._values[start:end]

    def itervalues(self, *args, **kw):
        values = self._values
        return (values[i] for i in xrange(*self._range(*args, **kw)))

    def items(self, *args, **kw):
        keys = self._keys
        values = self._values
        return [(keys[i], values[i]) for i in xrange(*self._range(*args, **kw))]

    def iteritems(self, *args, **kw):
        keys = self._keys
        values = self._values
        return ((keys[i], values[i]) for i in xrange(*self._range(*args, **kw)))

    def __getstate__(self):
        keys = self._keys
        values = self._values
        data = tuple((keys[i], values[i]) for i in range(len(self._keys)))
        if self._next:
            return data, self._next
        else:
            return (data, )

    def __setstate__(self, state):
        self.clear()
        if len(state) == 2:
            state, self._next = state
        else:
            self._next = None

        keys = self._keys
        values = self._values
        for k, v in state:
            self._keys.append(k)
            self._values.append(v)

class Set(_Base, _SetBase):

    def __getstate__(self):
        data = tuple(self._keys)
        if self._next:
            return data, self._next
        else:
            return (data, )

    def __setstate__(self, state):
        self.clear()
        if len(state) == 2:
            state, self._next = state
        else:
            self._next = None

        self.keys.extend(data)


    def _set(self, key, value=None, ifunset=False):
        index = self._search(key)
        if index < 0:
            index = -index-1
            self._keys.insert(index, key)
            return True, None
        else:
            return False, None

    def _del(self, key):
        index = self._search(key)
        if index >= 0:
            self._p_changed = True
            del self._values[index]
            return 0, 0
        else:
            raise KeyError(key)

class _TreeItem(object):

    __slots__ = 'key', 'child'

    def __init__(self, key, child):
        self.key = key
        self.child = child

class _Tree(persistent.Persistent):

    def __init__(self, items):
        self.clear()

    def clear(self):
        self._data = []
        self._firstbucket = None

    def __nonzero__(self):
        return bool(self._data)

    def __len__(self):
        l = 0
        bucket = self._firstbucket
        while bucket is not None:
            l += len(bucket._keys)
        return l

    @property
    def size(self):
        return len(self._keys)

    def _search(self, key):
        data = self._data
        lo = 0
        hi = len(data)
        i = hi//2
        while i > lo:
            cmp_ = cmp(data[i].key, key)
            if cmp_ < 0:
                lo = i
            elif cmp_ > 0:
                hi = i
            else:
                break
            i = (lo+hi)//2
        return i

    def _findbucket(self, key):
        if self._data:
            child = self._data[self._search(key)].child
            if isinstance(child, self._bucket_type):
                return child
            return child._findbucket(key)

    def __contains__(self, key):
        return key in (self._findbucket(self._to_key(key)) or ())
    has_key = __contains__

    def iterkeys(min=_marker, max=_marker, excludemin=False, excludemax=False,
                 iter_type='iterkeys'):
        if not self._data:
            return

        any = 0
        if min != marker:
            min = self._to_key(min)
            bucket = self._findbucket(min)
            for k in getattr(bucket, iter_type)(min, max,
                                                excludemin, excludemax):
                yield k
                any = 1
            bucket = bucket._next
        else:
            bucket = self._firstbucket

        while bucket is not None:
            for k in getattr(bucket, iter_type)():
                yield k
                any = 1
            if not any:
                break
            any = 0
            bucket = bucket._next

    keys = __iter__ = iterkeys

    def minKey(self, min=_marker):
        if min is _marker:
            bucket = self._firstbucket
        else:
            min = self._to_key(min)
            bucket = self._findbucket(min)
        if bucket is not None:
            return bucket.minKey(min)
        else:
            raise ValueError('empty tree')

    def maxKey(self, max=_marker):
        data = self._data
        if not data:
            raise ValueError('empty tree')
        if max is _marker:
            return data[-1].child.maxKey()

        max = self._to_key(max)
        index = self._search(max)
        if index and data[index].child.minKey() > max:
            index -= 1
        return data[index].child.maxKey(max)


    def _set(self, key, value=None, ifunset=False):
        data = self._data
        if data:
            index = self._search(key)
            child = data[index].child
        else:
            index = 0
            child = self._bucket_type()
            self._firstbucket = child
            data.append(_TreeItem(None, child))

        result = child._set(key, value, ifunset)
        grew = result[0]
        if grew and child.size > child.MAX_SIZE:
            self._grow(child, index)
        elif (grew is not None and
              child.__class__ is self._bucket_type and
              len(data) == 1 and
              child._p_oid is None
              ):
            self._p_changed = 1
        return result

    def _grow(self, child, index):
        self._p_changed = True
        new_child = child.split()
        self._data.insert(index+1, _TreeItem(new_child.minKey(), new_child))
        if len(self._data) > self.MAX_SIZE * 2:
            self._split_root()

    def _split_root(self):
        child = self.__class__()
        child._data = self._data
        child._firstbucket = self._firstbucket
        self._data = [_TreeItem(None, child)]
        self._grow(child, 0)

    def _del(self, key):
        data = self._data
        if data:
            index = self._search(key)
            child = data[index].child
        else:
            raise KeyError(key)

        removed_first_bucket, value = child._del(key)

        if index and child.size and key == data[index].key:
            self._p_changed = True
            data[index].key = child.minKey()

        if removed_first_bucket:
            if index:
                data[index-1].child._deleteNextBucket()
                removed_first_bucket = False # clear flag
            else:
                self._firstbucket = child._firstbucket

        if not child.size:
            if child.__class__ is self._bucket_type:
                if index:
                    data[index-1].child._deleteNextBucket()
                else:
                    self._firstbucket = child._next
                    removed_first_bucket = True
            del data[index]

        return removed_first_bucket, value

    def _deleteNextBucket(self):
        self._data[-1].child._deleteNextBucket()

    def __getstate__(self):
        data = self._data

        if not data:
            return None

        if (len(data) == 1 and
            data[0].child.__class__ is not self.__class__ and
            data[0].child._p_oid is None
            ):
            return ((data[0].child.__getstate__(), ), )

        sdata = []
        for item in data:
            if sdata:
                sdata.append(item.key)
                sdata.append(item.value)
            else:
                sdata.append(item.value)

        return sdata, self._firstbucket

    def __setstate__(self, state):
        self.clear()
        if state is None:
            return

        if len(state) == 1:
            bucket = self._bucket_type()
            bucket.__setstate__(state[0][0])
            state = [bucket], bucket

        data, self._firstbucket = state
        data = reversed(data)

        self.data.append(_TreeItem(None, data.pop()))
        while data:
            key = data.pop()
            child = data.pop()
            self.data.append(_TreeItem(key, child))

    def _assert(self, condition, message):
        if not condition:
            raise AssertionError(message)

    def _check(self, nextbucket=None):
        data = self._data
        assert_ = self._assert
        if not data:
            assert_(self._firstbucket is None,
                    "Empty BTree has non-NULL firstbucket")
            return
        assert_(self._firstbucket is not None,
                "Non-empty BTree has NULL firstbucket")

        child_class = data[0].child.__class__
        for i in data:
            assert_(i.child is not None, "BTree has NULL child")
            assert_(i.child.__class__ is child_class,
                    "BTree children have different types");
            assert_(i.child.size, "Bucket length < 1")

        if child_class is self.__class__:
            assert_(self._firstbucket is data[0].child._firstbucket,
                    "BTree has firstbucket different than "
                    "its first child's firstbucket")
            for i in range(len(data)-1):
                data[i].child._check(data[i+1].child._firstbucket)
            data[-1].child._check(nextbucket)
        elif child_class is self._bucket_type:
            assert_(self._firstbucket is data[0].child,
                    "Bottom-level BTree node has inconsistent firstbucket "
                    "belief")
            for i in range(len(data)-1):
                assert_(data[i].child._next is data[i+1].child,
                       "Bucket next pointer is damaged")
            assert_(data[i].child._next is nextbucket,
                    "Bucket next pointer is damaged")
        else:
            assert_(False, "Incorrect child type")


class Tree(_Tree, _MappingBase):

    def get(self, key, default=None):
        bucket = self._findbucket(key)
        if bucket:
            return bucket.get(key, default)

    def __getitem__(self, key):
        bucket = self._findbucket(key)
        if bucket:
            return bucket[key]
        raise KeyError(key)

    def itervalues(self, min=_marker, max=_marker,
                   excludemin=False, excludemax=False):
        return self.iterkeys(min, max, excludemin, excludemax, 'itervalues')

    values = itervalues

    def iteritems(self, min=_marker, max=_marker,
                   excludemin=False, excludemax=False):
        return self.iterkeys(min, max, excludemin, excludemax, 'itervalues')

    items = iteritems

    def byValue(self, min):
        return sorted((v, k) for (k, v) in self.iteritems() if v >= min)

    def insert(self, key, value):
        return bool(self._set(key, value, True)[0])

class TreeSet(_Tree, _SetBase):
    pass


def _set_operation(s1, s2,
                   usevalues1=False, usevalues2=False,
                   w1=1, w2=1,
                   c1=True, c12=True, c2=True):
    i1 = _SetIteration(s1, usevalues1)
    i2 = _SetIteration(s2, usevalues2)
    merge = i1.useValues or i2.useValues
    MERGE = getattr(s1, 'MERGE', None)
    MERGE_WEIGHT = getattr(s1, 'MERGE_WEIGHT', None)
    if merge:
        if MERGE is None and c12 and i1.useValues and i2.useValues:
            raise TypeError("invalid set operation")

        if (not i1.useValues) and i2.useValues:
            t = i1; i1 = i2; i2 = t
            t = w1; w1 = w2; w2 = t
            t = c1; c1 = c2; c2 = t

        MERGE_DEFAULT = getattr(s1, 'MERGE_DEFAULT', None)
        if MERGE_DEFAULT is not None:
            i1.value = i2.value = MERGE_DEFAULT
        else:
            if i1.usesValue:
                if (not i2.usesValue) and c2:
                    raise TypeError("invalid set operation")
            else:
                if c1 or c12:
                    raise TypeError("invalid set operation")

        r = s1._mapping_type()

        def copy(i, w):
            r._keys.append(i.key)
            r._values.append(MERGE_WEIGHT(i, w))
            i.advance()
    else:
        r = s1._set_type()

    while i1.active and i2.active:
        cmp_ = cmp(i1.key, i2.key)
        if cmp_ < 0:
            if c1:
                copy(i1, w1)
        elif cmp_ == 0:
            if c12:
                r._keys.append(i1.key)
                if merge:
                    r._values.append(MERGE(i1.value, w1, i2.value, w2))
                i1.advance()
                i2.advance()
        else:
            if c2:
                copy(i2, w2)

    if c1:
        while i1.active:
            copy(i1, w1)
    if c2:
        while i2.active:
            copy(i2, w2)

    return r

def difference(o1, o2):
    if o1 is None or o2 is None:
        return o1
    return _set_operation(o1, o2, 1, 0, 1, 0, 1, 0, 0)

def union(o1, o2):
    if o1 is None:
        return o2
    if o2 is None:
        return o1
    return _set_operation(o1, o2, 0, 0, 1, 1, 1, 1, 1)

def intersection(o1, o2):
    if o1 is None:
        return o2
    if o2 is None:
        return o1
    return _set_operation(o1, o2, 0, 0, 1, 1, 0, 1, 0)

def weightedUnion(o1, o2, w1=1, w2=1):
    wtf?

def weightedIntersection(o1, o2):
    pass

def multiunion(o1, o2):
    pass



