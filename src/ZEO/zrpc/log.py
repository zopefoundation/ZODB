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
    if isinstance(obj, types.StringType):
        obj = obj[:REPR_LIMIT]
    elif isinstance(obj, types.TupleType):
        elts = []
        size = 0
        for elt in obj:
            r = repr(elt)
            elts.append(r)
            size += len(r)
            if size > REPR_LIMIT:
                break
        obj = tuple(elts)
    return repr(obj)[:REPR_LIMIT]
