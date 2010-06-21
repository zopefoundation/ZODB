##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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

# Zope pokes a non-None value into exit_status when it wants the loop()
# function to exit.  Indeed, there appears to be no other way to tell
# Zope3 to shut down.
exit_status = None

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

# Because of the exit_status magic, we can't just invoke asyncore.loop(),
# and that's a shame.
# The signature of asyncore.loop changed between Python 2.3 and 2.4, and
# this loop() has 2.4's signature, which added the optional `count` argument.
# Since we physically replace asyncore.loop with this `loop`, and want
# compatibility with both Pythons, we need to support the most recent
# signature.  Applications running under 2.3 should (of course) avoid using
# the `count` argument, since 2.3 doesn't have it.
def loop(timeout=30.0, use_poll=False, map=None, count=None):
    global _looping
    global exit_status

    exit_status = None

    if map is None:
        map = asyncore.socket_map

    # This section is taken from Python 2.3's asyncore.loop, and is more
    # elaborate than the corresponding section of 2.4's:  in 2.4 poll2 and
    # poll3 are aliases for the same function, in 2.3 they're different
    # functions.
    if use_poll:
        if hasattr(select, 'poll'):
            poll_fun = asyncore.poll3
        else:
            poll_fun = asyncore.poll2
    else:
        poll_fun = asyncore.poll

    # The loop is about to start:  invoke any registered callbacks.
    _loop_lock.acquire()
    try:
        _looping = map
        while _loop_callbacks:
            cb, args, kw = _loop_callbacks.pop()
            cb(map, *args, **(kw or {}))
    finally:
        _loop_lock.release()

    # Run the loop.  This is 2.4's logic, with the addition that we stop
    # if/when this module's exit_status global is set to a non-None value.
    if count is None:
        while map and exit_status is None:
            poll_fun(timeout, map)
    else:
        while map and count > 0 and exit_status is None:
            poll_fun(timeout, map)
            count -= 1

    _loop_lock.acquire()
    try:
        _looping = None
    finally:
        _loop_lock.release()

# Evil:  rebind asyncore.loop to the above loop() function.
#
# Code should explicitly call ThreadedAsync.loop() instead of asyncore.loop().
# Most of ZODB has been fixed, but ripping this out may break 3rd party code.
# Maybe we should issue a warning and let it continue for a while (NOTE:  code
# to raise DeprecationWarning was written but got commented out below; don't
# know why it got commented out).  Or maybe we should get rid of this
# mechanism entirely, and have each piece that needs one run its own asyncore
# loop in its own thread.

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
