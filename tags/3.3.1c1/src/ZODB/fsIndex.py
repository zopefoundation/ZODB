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
# Since the mapping from suffix to data contains at most 256 entries,
# we use a BTree bucket instead of a full BTree to store the results.
#
# We use p64 to convert integers to 8-byte strings and lop off the two
# high-order bytes when saving. On loading data, we add the leading
# bytes back before using u64 to convert the data back to (long)
# integers.

import struct

from BTrees._fsBTree import fsBucket

# convert between numbers and six-byte strings

def num2str(n):
    return struct.pack(">Q", n)[2:]

def str2num(s):
    return struct.unpack(">Q", "\000\000" + s)[0]

class fsIndex:

    def __init__(self):
        self._data = {}

    def __getitem__(self, key):
        return str2num(self._data[key[:6]][key[6:]])

    def get(self, key, default=None):
        tree = self._data.get(key[:6], default)
        if tree is default:
            return default
        v = tree.get(key[6:], default)
        if v is default:
            return default
        return str2num(v)

    def __setitem__(self, key, value):
        value = num2str(value)
        treekey = key[:6]
        tree = self._data.get(treekey)
        if tree is None:
            tree = fsBucket()
            self._data[treekey] = tree
        tree[key[6:]] = value

    def __len__(self):
        r = 0
        for tree in self._data.values():
            r += len(tree)
        return r

    def update(self, mapping):
        for k, v in mapping.items():
            self[k] = v

    def has_key(self, key):
        v=self.get(key, self)
        return v is not self

    def __contains__(self, key):
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
        for prefix, tree in self._data.items():
            for suffix in tree:
                yield prefix + suffix

    def keys(self):
        r = []
        for prefix, tree in self._data.items():
            for suffix in tree.keys():
                r.append(prefix + suffix)
        return r

    def items(self):
        r = []
        for prefix, tree in self._data.items():
            for suffix, v in tree.items():
                r.append(((prefix + suffix), str2num(v)))
        return r

    def values(self):
        r = []
        for prefix, tree in self._data.items():
            for v in tree.values():
                r.append(str2num(v))
        return r

    def maxKey(self):
        # This is less general than the BTree method of the same name:  we
        # only care about the largest key in the entire tree.  By
        # construction, that's the largest oid in use in the associated
        # FileStorage.

        keys = self._data.keys()
        if not keys:
            # This is the same exception a BTree maxKey() raises when the
            # tree is empty.
            raise ValueError("empty tree")

        # We expect that keys is small, since each fsBTree in _data.values()
        # can hold as many as 2**16 = 64K entries.  So this max() should go
        # fast too.  Regardless, there's no faster way to find the largest
        # prefix.
        biggest_prefix = max(keys)
        tree = self._data[biggest_prefix]

        # Obscure:  what if tree is actually empty?  We're relying here on
        # that this class doesn't implement __delitem__:  once a key gets
        # into an fsIndex, the only way it can go away is by invoking
        # clear().  Therefore nothing in _data.values() is ever empty.
        #
        # Note that because `tree` is an fsBTree, its maxKey() method is very
        # efficient.
        assert tree
        return biggest_prefix + tree.maxKey()