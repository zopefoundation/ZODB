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
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

# originally zodb.utils

import struct

def p64(v):
    """Pack an integer or long into a 8-byte string"""
    return struct.pack(">Q", v)

def u64(v):
    """Unpack an 8-byte string into a 64-bit long integer."""
    return struct.unpack(">Q", v)[0]

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


# originally from zodb.storage.base

def splitrefs(refstr, oidlen=8):
    # refstr is a packed string of reference oids.  Always return a list of
    # oid strings.  Most storages use fixed oid lengths of 8 bytes, but if
    # the oids in refstr are a different size, use oidlen to specify.  This
    # does /not/ support variable length oids in refstr.
    if not refstr:
        return []
    num, extra = divmod(len(refstr), oidlen)
    fmt = '%ds' % oidlen
    assert extra == 0, refstr
    return list(struct.unpack('>' + (fmt * num), refstr))
