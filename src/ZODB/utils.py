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

import sys
import time
from struct import pack, unpack
from binascii import hexlify
import cPickle
import cStringIO
import weakref
import warnings

from persistent.TimeStamp import TimeStamp

__all__ = ['z64',
           't32',
           'p64',
           'u64',
           'U64',
           'cp',
           'newTimeStamp',
           'oid_repr',
           'serial_repr',
           'tid_repr',
           'positive_id',
           'get_refs',
           'readable_tid_repr',
           'WeakSet',
           'DEPRECATED_ARGUMENT',
           'deprecated36',
          ]

# A unique marker to give as the default value for a deprecated argument.
# The method should then do a
#
#     if that_arg is not DEPRECATED_ARGUMENT:
#         complain
#
# dance.
DEPRECATED_ARGUMENT = object()

# Raise DeprecationWarning, noting that the deprecated thing will go
# away in ZODB 3.6.  Point to the caller of our caller (i.e., at the
# code using the deprecated thing).
def deprecated36(msg):
    warnings.warn("This will be removed in ZODB 3.6:\n%s" % msg,
                  DeprecationWarning, stacklevel=3)

z64 = '\0'*8

# TODO The purpose of t32 is unclear.  Code that uses it is usually
# of the form:
#
#    if e < 0:
#        e = t32 - e
#
# Doesn't make sense (since e is negative, it creates a number larger than
# t32).  If users said "e += t32", *maybe* it would make sense.
t32 = 1L << 32

assert sys.hexversion >= 0x02030000

# The distinction between ints and longs is blurred in Python 2.2,
# so u64() are U64() really the same.

def p64(v):
    """Pack an integer or long into a 8-byte string"""
    return pack(">Q", v)

def u64(v):
    """Unpack an 8-byte string into a 64-bit long integer."""
    return unpack(">Q", v)[0]

U64 = u64

def cp(f1, f2, l):
    read = f1.read
    write = f2.write
    n = 8192

    while l > 0:
        if n > l:
            n = l
        d = read(n)
        if not d:
            break
        write(d)
        l = l - len(d)


def newTimeStamp(old=None,
                 TimeStamp=TimeStamp,
                 time=time.time, gmtime=time.gmtime):
    t = time()
    ts = TimeStamp(gmtime(t)[:5]+(t%60,))
    if old is not None:
        return ts.laterThan(old)
    return ts


def oid_repr(oid):
    if isinstance(oid, str) and len(oid) == 8:
        # Convert to hex and strip leading zeroes.
        as_hex = hexlify(oid).lstrip('0')
        # Ensure two characters per input byte.
        if len(as_hex) & 1:
            as_hex = '0' + as_hex
        elif as_hex == '':
            as_hex = '00'
        return '0x' + as_hex
    else:
        return repr(oid)

serial_repr = oid_repr
tid_repr = serial_repr

# For example, produce
#     '0x03441422948b4399 2002-04-14 20:50:34.815000'
# for 8-byte string tid '\x03D\x14"\x94\x8bC\x99'.
def readable_tid_repr(tid):
    result = tid_repr(tid)
    if isinstance(tid, str) and len(tid) == 8:
        result = "%s %s" % (result, TimeStamp(tid))
    return result

# Addresses can "look negative" on some boxes, some of the time.  If you
# feed a "negative address" to an %x format, Python 2.3 displays it as
# unsigned, but produces a FutureWarning, because Python 2.4 will display
# it as signed.  So when you want to prodce an address, use positive_id() to
# obtain it.
def positive_id(obj):
    """Return id(obj) as a non-negative integer."""

    result = id(obj)
    if result < 0:
        # This is a puzzle:  there's no way to know the natural width of
        # addresses on this box (in particular, there's no necessary
        # relation to sys.maxint).  Try 32 bits first (and on a 32-bit
        # box, adding 2**32 gives a positive number with the same hex
        # representation as the original result).
        result += 1L << 32
        if result < 0:
            # Undo that, and try 64 bits.
            result -= 1L << 32
            result += 1L << 64
            assert result >= 0 # else addresses are fatter than 64 bits
    return result

# So full of undocumented magic it's hard to fathom.
# The existence of cPickle.noload() isn't documented, and what it
# does isn't documented either.  In general it unpickles, but doesn't
# actually build any objects of user-defined classes.  Despite that
# persistent_load is documented to be a callable, there's an
# undocumented gimmick where if it's actually a list, for a PERSID or
# BINPERSID opcode cPickle just appends "the persistent id" to that list.
# Also despite that "a persistent id" is documented to be a string,
# ZODB persistent ids are actually (often? always?) tuples, most often
# of the form
#     (oid, (module_name, class_name))
# So the effect of the following is to dig into the object pickle, and
# return a list of the persistent ids found (which are usually nested
# tuples), without actually loading any modules or classes.
# Note that pickle.py doesn't support any of this, it's undocumented code
# only in cPickle.c.
def get_refs(pickle):
    # The pickle is in two parts.  First there's the class of the object,
    # needed to build a ghost,  See get_pickle_metadata for how complicated
    # this can get.  The second part is the state of the object.  We want
    # to find all the persistent references within both parts (although I
    # expect they can only appear in the second part).
    f = cStringIO.StringIO(pickle)
    u = cPickle.Unpickler(f)
    u.persistent_load = refs = []
    u.noload() # class info
    u.noload() # instance state info
    return refs

# A simple implementation of weak sets, supplying just enough of Python's
# sets.Set interface for our needs.

class WeakSet(object):
    """A set of objects that doesn't keep its elements alive.

    The objects in the set must be weakly referencable.
    The objects need not be hashable, and need not support comparison.
    Two objects are considered to be the same iff their id()s are equal.

    When the only references to an object are weak references (including
    those from WeakSets), the object can be garbage-collected, and
    will vanish from any WeakSets it may be a member of at that time.
    """

    def __init__(self):
        # Map id(obj) to obj.  By using ids as keys, we avoid requiring
        # that the elements be hashable or comparable.
        self.data = weakref.WeakValueDictionary()

    def __len__(self):
        return len(self.data)

    def __contains__(self, obj):
        return id(obj) in self.data

    # Same as a Set, add obj to the collection.
    def add(self, obj):
        self.data[id(obj)] = obj

    # Same as a Set, remove obj from the collection, and raise
    # KeyError if obj not in the collection.
    def remove(self, obj):
        del self.data[id(obj)]

    # Return a list of all the objects in the collection.
    # Because a weak dict is used internally, iteration
    # is dicey (the underlying dict may change size during
    # iteration, due to gc or activity from other threads).
    # as_list() attempts to be safe.
    def as_list(self):
        return self.data.values()
