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
import time
from persistent.TimeStamp import TimeStamp

from struct import pack, unpack
from types import StringType

z64 = '\0'*8
t32 = 1L << 32

assert sys.hexversion >= 0x02020000

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
    if isinstance(oid, StringType) and len(oid) == 8:
        return '%016x' % U64(oid)
    else:
        return repr(oid)

serial_repr = oid_repr

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
