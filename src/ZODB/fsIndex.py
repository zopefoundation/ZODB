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
"""Implement an OID to File-position (long integer) mapping."""

# To save space, we do two things:
#
#     1. We split the keys (OIDS) into 6-byte prefixes and 2-byte suffixes.
#        We use the prefixes as keys in a mapping from prefix to mappings
#        of suffix to data:
#
#           data is  {prefix -> {suffix -> data}}
#
#     2. We limit the data size to 48 bits. This should allow databases
#        as large as 256 terabytes.
#
# Most of the space is consumed by items in the mappings from 2-byte
# suffix to 6-byte data. This should reduce the overall memory usage to
# 8-16 bytes per OID.
#
# Because
#  - the mapping from suffix to data contains at most 65535 entries,
#  - this is an in-memory data structure
#  - new keys are inserted sequentially,
# we use a BTree bucket instead of a full BTree to store the results.
#
# We use p64 to convert integers to 8-byte strings and lop off the two
# high-order bytes when saving. On loading data, we add the leading
# bytes back before using u64 to convert the data back to (long)
# integers.
import struct

import six

from BTrees.fsBTree import fsBucket
from BTrees.OOBTree import OOBTree

from ZODB._compat import INT_TYPES
from ZODB._compat import Pickler
from ZODB._compat import Unpickler
from ZODB._compat import _protocol


# convert between numbers and six-byte strings

def num2str(n):
    return struct.pack(">Q", n)[2:]


def str2num(s):
    return struct.unpack(">Q", b"\000\000" + s)[0]


def prefix_plus_one(s):
    num = str2num(s)
    return num2str(num + 1)


def prefix_minus_one(s):
    num = str2num(s)
    return num2str(num - 1)


def ensure_bytes(s):
    # on Python 3 we might pickle bytes and unpickle unicode strings
    return s.encode('ascii') if not isinstance(s, bytes) else s


class fsIndex(object):

    def __init__(self, data=None):
        self._data = OOBTree()
        if data:
            self.update(data)

    def __getstate__(self):
        return dict(
            state_version=1,
            _data=[(k, v.toString())
                   for (k, v) in six.iteritems(self._data)
                   ]
        )

    def __setstate__(self, state):
        version = state.pop('state_version', 0)
        getattr(self, '_setstate_%s' % version)(state)

    def _setstate_0(self, state):
        self.__dict__.clear()
        self.__dict__.update(state)
        self._data = OOBTree([
            (ensure_bytes(k), v)
            for (k, v) in self._data.items()
        ])

    def _setstate_1(self, state):
        self._data = OOBTree([
            (ensure_bytes(k), fsBucket().fromString(ensure_bytes(v)))
            for (k, v) in state['_data']
        ])

    def __getitem__(self, key):
        assert isinstance(key, bytes)
        return str2num(self._data[key[:6]][key[6:]])

    def save(self, pos, fname):
        with open(fname, 'wb') as f:
            pickler = Pickler(f, _protocol)
            pickler.fast = True
            pickler.dump(pos)
            for k, v in six.iteritems(self._data):
                pickler.dump((k, v.toString()))
            pickler.dump(None)

    @classmethod
    def load(class_, fname):
        with open(fname, 'rb') as f:
            unpickler = Unpickler(f)
            pos = unpickler.load()
            if not isinstance(pos, INT_TYPES):
                # NB: this might contain OIDs that got unpickled
                # into Unicode strings on Python 3; hope the caller
                # will pipe the result to fsIndex().update() to normalize
                # the keys
                return pos                  # Old format
            index = class_()
            data = index._data
            while 1:
                v = unpickler.load()
                if not v:
                    break
                k, v = v
                data[ensure_bytes(k)] = fsBucket().fromString(ensure_bytes(v))
            return dict(pos=pos, index=index)

    def get(self, key, default=None):
        assert isinstance(key, bytes)
        tree = self._data.get(key[:6], default)
        if tree is default:
            return default
        v = tree.get(key[6:], default)
        if v is default:
            return default
        return str2num(v)

    def __setitem__(self, key, value):
        assert isinstance(key, bytes)
        value = num2str(value)
        treekey = key[:6]
        tree = self._data.get(treekey)
        if tree is None:
            tree = fsBucket()
            self._data[treekey] = tree
        tree[key[6:]] = value

    def __delitem__(self, key):
        assert isinstance(key, bytes)
        treekey = key[:6]
        tree = self._data.get(treekey)
        if tree is None:
            raise KeyError(key)
        del tree[key[6:]]
        if not tree:
            del self._data[treekey]

    def __len__(self):
        r = 0
        for tree in six.itervalues(self._data):
            r += len(tree)
        return r

    def update(self, mapping):
        for k, v in mapping.items():
            self[ensure_bytes(k)] = v

    def has_key(self, key):
        v = self.get(key, self)
        return v is not self

    def __contains__(self, key):
        assert isinstance(key, bytes)
        tree = self._data.get(key[:6])
        if tree is None:
            return False
        v = tree.get(key[6:], None)
        if v is None:
            return False
        return True

    def clear(self):
        self._data.clear()

    def __iter__(self):
        for prefix, tree in six.iteritems(self._data):
            for suffix in tree:
                yield prefix + suffix

    iterkeys = __iter__

    def keys(self):
        return list(self.iterkeys())

    def iteritems(self):
        for prefix, tree in six.iteritems(self._data):
            for suffix, value in six.iteritems(tree):
                yield (prefix + suffix, str2num(value))

    def items(self):
        return list(self.iteritems())

    def itervalues(self):
        for tree in six.itervalues(self._data):
            for value in six.itervalues(tree):
                yield str2num(value)

    def values(self):
        return list(self.itervalues())

    # Comment below applies for the following minKey and maxKey methods
    #
    # Obscure:  what if `tree` is actually empty?  We're relying here on
    # that this class doesn't implement __delitem__:  once a key gets
    # into an fsIndex, the only way it can go away is by invoking
    # clear().  Therefore nothing in _data.values() is ever empty.
    #
    # Note that because `tree` is an fsBTree, its minKey()/maxKey() methods are
    # very efficient.

    def minKey(self, key=None):
        if key is None:
            smallest_prefix = self._data.minKey()
        else:
            smallest_prefix = self._data.minKey(key[:6])

        tree = self._data[smallest_prefix]

        assert tree

        if key is None:
            smallest_suffix = tree.minKey()
        else:
            try:
                smallest_suffix = tree.minKey(key[6:])
            except ValueError:  # 'empty tree' (no suffix >= arg)
                next_prefix = prefix_plus_one(smallest_prefix)
                smallest_prefix = self._data.minKey(next_prefix)
                tree = self._data[smallest_prefix]
                assert tree
                smallest_suffix = tree.minKey()

        return smallest_prefix + smallest_suffix

    def maxKey(self, key=None):
        if key is None:
            biggest_prefix = self._data.maxKey()
        else:
            biggest_prefix = self._data.maxKey(key[:6])

        tree = self._data[biggest_prefix]

        assert tree

        if key is None:
            biggest_suffix = tree.maxKey()
        else:
            try:
                biggest_suffix = tree.maxKey(key[6:])
            except ValueError:  # 'empty tree' (no suffix <= arg)
                next_prefix = prefix_minus_one(biggest_prefix)
                biggest_prefix = self._data.maxKey(next_prefix)
                tree = self._data[biggest_prefix]
                assert tree
                biggest_suffix = tree.maxKey()

        return biggest_prefix + biggest_suffix
