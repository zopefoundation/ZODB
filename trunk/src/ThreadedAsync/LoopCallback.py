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
"""Manage the asyncore mainloop in a multi-threaded app

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
__version__ = '$Revision: 1.9 $'[11:-2]

import asyncore
import select
import thread
import time
from errno import EINTR

_loop_lock = thread.allocate_lock()
_looping = None
_loop_callbacks = []

def register_loop_callback(callback, args=(), kw=None):
    """Register callback function to be called when mainloop starts

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
            apply(callback, (_looping,) + args, kw or {})
        else:
            _loop_callbacks.append((callback, args, kw))
    finally:
        _loop_lock.release()
        
def remove_loop_callback(callback):
    """Remove a callback function registered earlier.
  	 
    This is useful if loop() was never called.
    """
    for i in range(len(_loop_callbacks)):
        if _loop_callbacks[i][0] == callback:
            del _loop_callbacks[i]
            return
  	 
def _start_loop(map):
    _loop_lock.acquire()
    try:
        global _looping
        _looping = map
        while _loop_callbacks:
            cb, args, kw = _loop_callbacks.pop()
            apply(cb, (map,) + args, kw or {})
    finally:
        _loop_lock.release()

def _stop_loop():
    _loop_lock.acquire()
    try:
        global _looping
        _looping = None
    finally:
        _loop_lock.release()

def poll(timeout=0.0, map=None):
    """A copy of asyncore.poll() with a bug fixed (see comment).

    (asyncore.poll2() and .poll3() don't have this bug.)
    """
    if map is None:
        map = asyncore.socket_map
    if map:
        r = []; w = []; e = []
        for fd, obj in map.items():
            if obj.readable():
                r.append(fd)
            if obj.writable():
                w.append(fd)
        if [] == r == w == e:
            time.sleep(timeout)
        else:
            try:
                r, w, e = select.select(r, w, e, timeout)
            except select.error, err:
                if err[0] != EINTR:
                    raise
                else:
                    # This part is missing in asyncore before Python 2.3
                    return

        for fd in r:
            obj = map.get(fd)
            if obj is not None:
                try:
                    obj.handle_read_event()
                except asyncore.ExitNow:
                    raise asyncore.ExitNow
                except:
                    obj.handle_error()

        for fd in w:
            obj = map.get(fd)
            if obj is not None:
                try:
                    obj.handle_write_event()
                except asyncore.ExitNow:
                    raise asyncore.ExitNow
                except:
                    obj.handle_error()

def loop(timeout=30.0, use_poll=0, map=None):
    """Invoke asyncore mainloop

    This function functions like the regular asyncore.loop() function
    except that it also triggers ThreadedAsync callback functions
    before starting the loop.
    """
    if use_poll:
        if hasattr(select, 'poll'):
            poll_fun = asyncore.poll3
        else:
            poll_fun = asyncore.poll2
    else:
        poll_fun = poll

    if map is None:
        map = asyncore.socket_map

    _start_loop(map)
    while map:
        poll_fun(timeout, map)
    _stop_loop()


# This module used to do something evil -- it rebound asyncore.loop to the
# above loop() function.  What was evil about this is that if you added some
# debugging to asyncore.loop, you'd spend 6 hours debugging why your debugging
# code wasn't called!
#
# Code should instead explicitly call ThreadedAsync.loop() instead of
# asyncore.loop().  Most of ZODB has been fixed, but ripping this out may
# break 3rd party code.  So we'll issue a warning and let it continue -- for
# now.

##def deprecated_loop(*args, **kws):
##    import warnings
##    warnings.warn("""\
##ThreadedAsync.loop() called through sneaky asyncore.loop() rebinding.
##You should change your code to call ThreadedAsync.loop() explicitly.""",
##                  DeprecationWarning)
##    loop(*args, **kws)

##asyncore.loop = deprecated_loop

# XXX Remove this once we've updated ZODB4 since they share this package
asyncore.loop = loop
