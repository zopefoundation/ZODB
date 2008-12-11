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
import os
import threading
import logging

from ZODB.loglevels import BLATHER

LOG_THREAD_ID = 0 # Set this to 1 during heavy debugging

logger = logging.getLogger('ZEO.zrpc')

_label = "%s" % os.getpid()

def new_label():
    global _label
    _label = str(os.getpid())

def log(message, level=BLATHER, label=None, exc_info=False):
    label = label or _label
    if LOG_THREAD_ID:
        label = label + ':' + threading.currentThread().getName()
    logger.log(level, '(%s) %s' % (label, message), exc_info=exc_info)

REPR_LIMIT = 60

def short_repr(obj):
    "Return an object repr limited to REPR_LIMIT bytes."

    # Some of the objects being repr'd are large strings. A lot of memory
    # would be wasted to repr them and then truncate, so they are treated
    # specially in this function.
    # Also handle short repr of a tuple containing a long string.

    # This strategy works well for arguments to StorageServer methods.
    # The oid is usually first and will get included in its entirety.
    # The pickle is near the beginning, too, and you can often fit the
    # module name in the pickle.

    if isinstance(obj, str):
        if len(obj) > REPR_LIMIT:
            r = repr(obj[:REPR_LIMIT])
        else:
            r = repr(obj)
        if len(r) > REPR_LIMIT:
            r = r[:REPR_LIMIT-4] + '...' + r[-1]
        return r
    elif isinstance(obj, (list, tuple)):
        elts = []
        size = 0
        for elt in obj:
            r = short_repr(elt)
            elts.append(r)
            size += len(r)
            if size > REPR_LIMIT:
                break
        if isinstance(obj, tuple):
            r = "(%s)" % (", ".join(elts))
        else:
            r = "[%s]" % (", ".join(elts))
    else:
        r = repr(obj)
    if len(r) > REPR_LIMIT:
        return r[:REPR_LIMIT] + '...'
    else:
        return r
