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
import TimeStamp, time, struct

if sys.version >= (2, 2):

    # Note that the distinction between ints and longs is blurred in
    # Python 2.2.  So make u64() and U64() the same.

    def p64(v, pack=struct.pack):
        """Pack an integer or long into a 8-byte string"""
        return pack(">Q", v)

    def u64(v, unpack=struct.unpack):
        """Unpack an 8-byte string into a 64-bit long integer."""
        return unpack(">Q", v)[0]

    U64 = u64

else:

    t32 = 1L << 32

    def p64(v, pack=struct.pack):
        """Pack an integer or long into a 8-byte string"""
        if v < t32:
            h = 0
        else:
            h, v = divmod(v, t32)
        return pack(">II", h, v)

    def u64(v, unpack=struct.unpack):
        """Unpack an 8-byte string into a 64-bit (or long) integer."""
        h, v = unpack(">ii", v)
        if v < 0:
            v = t32 + v
        if h:
            if h < 0:
                h= t32 + h
            v = (long(h) << 32) + v
        return v

    def U64(v, unpack=struct.unpack):
        """Same as u64 but always returns a long."""
        h, v = unpack(">II", v)
        if h:
            v = (long(h) << 32) + v
        return v

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
                 TimeStamp=TimeStamp.TimeStamp,
                 time=time.time, gmtime=time.gmtime):
    t = time()
    ts = TimeStamp(gmtime(t)[:5]+(t%60,))
    if old is not None:
        return ts.laterThan(old)
    return ts
