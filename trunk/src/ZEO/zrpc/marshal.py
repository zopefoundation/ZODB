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
import cPickle
from cStringIO import StringIO
import types

import zLOG

from ZEO.zrpc.error import ZRPCError
from ZEO.zrpc.log import log

class Marshaller:
    """Marshal requests and replies to second across network"""

    def encode(self, msgid, flags, name, args):
        """Returns an encoded message"""
        pickler = cPickle.Pickler()
        pickler.fast = 1
        return pickler.dump((msgid, flags, name, args), 1)

    def decode(self, msg):
        """Decodes msg and returns its parts"""
        unpickler = cPickle.Unpickler(StringIO(msg))
        unpickler.find_global = find_global

        try:
            return unpickler.load() # msgid, flags, name, args
        except:
            log("can't decode message: %s" % repr(msg), level=zLOG.ERROR)
            raise

_globals = globals()
_silly = ('__doc__',)

def find_global(module, name):
    """Helper for message unpickler"""
    try:
        m = __import__(module, _globals, _globals, _silly)
    except ImportError, msg:
        raise ZRPCError("import error %s: %s" % (module, msg))

    try:
        r = getattr(m, name)
    except AttributeError:
        raise ZRPCError("module %s has no global %s" % (module, name))

    safe = getattr(r, '__no_side_effects__', 0)
    if safe:
        return r

    # XXX what's a better way to do this?  esp w/ 2.1 & 2.2
    if type(r) == types.ClassType and issubclass(r, Exception):
        return r

    raise ZRPCError("Unsafe global: %s.%s" % (module, name))
