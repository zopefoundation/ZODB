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

This module rebinds loop() in the asyncore module; i.e. once this
module is imported, any client of the asyncore module will get
ThreadedAsync.loop() when it calls asyncore.loop().
"""
__version__='$Revision: 1.3 $'[11:-2]

import asyncore
import select
import thread

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

def loop (timeout=30.0, use_poll=0, map=None):
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
        poll_fun = asyncore.poll

    if map is None:
        map = asyncore.socket_map

    _start_loop(map)
    while map:
        poll_fun(timeout, map)
    _stop_loop()

# Woo hoo!
asyncore.loop=loop

# What the heck did we just do?
# 
# Well, the thing is, we want to work with other asyncore aware
# code. In particular, we don't necessarily want to make someone
# import this module just to start or loop.
# 
