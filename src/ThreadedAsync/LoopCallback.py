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
"""Manage the asyncore mainloop in a multi-threaded app.

In a multi-threaded application, only a single thread runs the
asyncore mainloop.  This thread (the "mainloop thread") may not start
the mainloop before another thread needs to perform an async action
that requires it.  As a result, other threads need to coordinate with
the mainloop thread to find out whether the mainloop is running.

This module implements a callback mechanism that allows other threads
to be notified when the mainloop starts.  A thread calls
register_loop_callback() to register interest.  When the mainloop
thread calls loop(), each registered callback will be called with the
socket map as its first argument.
"""

import asyncore
import thread

_original_asyncore_loop = asyncore.loop

_loop_lock = thread.allocate_lock()
_looping = None # changes to socket map when loop() starts
_loop_callbacks = []

def register_loop_callback(callback, args=(), kw=None):
    """Register callback function to be called when mainloop starts.

    The callable object callback will be invokved when the mainloop
    starts.  If the mainloop is currently running, the callback will
    be invoked immediately.

    The callback will be called with a single argument, the mainloop
    socket map, unless the optional args or kw arguments are used.
    args defines a tuple of extra arguments to pass after the socket
    map.  kw defines a dictionary of keyword arguments.
    """
    _loop_lock.acquire()
    try:
        if _looping is not None:
            callback(_looping, *args, **(kw or {}))
        else:
            _loop_callbacks.append((callback, args, kw))
    finally:
        _loop_lock.release()

def remove_loop_callback(callback):
    """Remove a callback function registered earlier.

    This is useful if loop() was never called.
    """
    for i, value in enumerate(_loop_callbacks):
        if value[0] == callback:
            del _loop_callbacks[i]
            return

# Caution:  the signature of asyncore.loop changed in Python 2.4.
# That's why we use `args` and `kws` instead of spelling out the
# "intended" arguments.  Since we _replace_ asyncore.loop with this
# loop(), we need to be compatible with all signatures.
def loop(*args, **kws):
    global _looping

    map = kws.get("map", asyncore.socket_map)
    _loop_lock.acquire()
    try:
        _looping = map
        while _loop_callbacks:
            cb, args, kw = _loop_callbacks.pop()
            cb(map, *args, **(kw or {}))
    finally:
        _loop_lock.release()

    result = _original_asyncore_loop(*args, **kws)

    _loop_lock.acquire()
    try:
        _looping = None
    finally:
        _loop_lock.release()

    return result


# Evil:  rebind asyncore.loop to the above loop() function.
#
# Code should explicitly call ThreadedAsync.loop() instead of asyncore.loop().
# Most of ZODB has been fixed, but ripping this out may break 3rd party code.
# Maybe we should issue a warning and let it continue for a while.  Or
# maybe we should get rid of this mechanism entirely, and have each ZEO
# piece that needs one run its own asyncore loop in its own thread.

##def deprecated_loop(*args, **kws):
##    import warnings
##    warnings.warn("""\
##ThreadedAsync.loop() called through sneaky asyncore.loop() rebinding.
##You should change your code to call ThreadedAsync.loop() explicitly.""",
##                  DeprecationWarning)
##    loop(*args, **kws)
##
##asyncore.loop = deprecated_loop

asyncore.loop = loop
