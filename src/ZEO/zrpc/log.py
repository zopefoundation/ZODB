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
import os
import types
import zLOG

_label = "zrpc:%s" % os.getpid()

def new_label():
    global _label
    _label = "zrpc:%s" % os.getpid()

def log(message, level=zLOG.BLATHER, label=None, error=None):
    zLOG.LOG(label or _label, level, message, error=error)

REPR_LIMIT = 40

def short_repr(obj):
    "Return an object repr limited to REPR_LIMIT bytes."

    # Some of the objects being repr'd are large strings.  It's wastes
    # a lot of memory to repr them and then truncate, so special case
    # them in this function.
    # Also handle short repr of a tuple containing a long string.

    # This strategy works well for arguments to StorageServer methods.
    # The oid is usually first and will get included in its entirety.
    # The pickle is near the beginning, too, and you can often fit the
    # module name in the pickle.

    if isinstance(obj, types.StringType):
        r = `obj[:REPR_LIMIT]`
    elif isinstance(obj, types.TupleType):
        elts = []
        size = 0
        for elt in obj:
            r = repr(elt)
            elts.append(r)
            size += len(r)
            if size > REPR_LIMIT:
                break
        r = "(%s)" % (", ".join(elts))
    else:
        r = repr(obj)
    if len(r) > REPR_LIMIT:
        return r[:REPR_LIMIT] + '...'
    else:
        return r
