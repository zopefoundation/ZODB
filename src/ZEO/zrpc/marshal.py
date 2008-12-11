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
import cPickle
from cStringIO import StringIO
import logging

from ZEO.zrpc.error import ZRPCError
from ZEO.zrpc.log import log, short_repr

class Marshaller:
    """Marshal requests and replies to second across network"""

    def encode(self, msgid, flags, name, args):
        """Returns an encoded message"""
        # (We used to have a global pickler, but that's not thread-safe. :-( )
        # Note that args may contain very large binary pickles already; for
        # this reason, it's important to use proto 1 (or higher) pickles here
        # too.  For a long time, this used proto 0 pickles, and that can
        # bloat our pickle to 4x the size (due to high-bit and control bytes
        # being represented by \xij escapes in proto 0).
        # Undocumented:  cPickle.Pickler accepts a lone protocol argument;
        # pickle.py does not.
        pickler = cPickle.Pickler(1)
        pickler.fast = 1

        # Undocumented:  pickler.dump(), for a cPickle.Pickler, takes
        # an optional boolean argument.  When true, it returns the pickle;
        # when false or unspecified, it returns the pickler object itself.
        # pickle.py does none of this.
        return pickler.dump((msgid, flags, name, args), 1)

    def decode(self, msg):
        """Decodes msg and returns its parts"""
        unpickler = cPickle.Unpickler(StringIO(msg))
        unpickler.find_global = find_global

        try:
            return unpickler.load() # msgid, flags, name, args
        except:
            log("can't decode message: %s" % short_repr(msg),
                level=logging.ERROR)
            raise

_globals = globals()
_silly = ('__doc__',)

exception_type_type = type(Exception)

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

    # TODO:  is there a better way to do this?
    if type(r) == exception_type_type and issubclass(r, Exception):
        return r

    raise ZRPCError("Unsafe global: %s.%s" % (module, name))
