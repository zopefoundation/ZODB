##############################################################################
# 
# Zope Public License (ZPL) Version 1.0
# -------------------------------------
# 
# Copyright (c) Digital Creations.  All rights reserved.
# 
# This license has been certified as Open Source(tm).
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
# 1. Redistributions in source code must retain the above copyright
#    notice, this list of conditions, and the following disclaimer.
# 
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions, and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
# 
# 3. Digital Creations requests that attribution be given to Zope
#    in any manner possible. Zope includes a "Powered by Zope"
#    button that is installed by default. While it is not a license
#    violation to remove this button, it is requested that the
#    attribution remain. A significant investment has been put
#    into Zope, and this effort will continue if the Zope community
#    continues to grow. This is one way to assure that growth.
# 
# 4. All advertising materials and documentation mentioning
#    features derived from or use of this software must display
#    the following acknowledgement:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    In the event that the product being advertised includes an
#    intact Zope distribution (with copyright and license included)
#    then this clause is waived.
# 
# 5. Names associated with Zope or Digital Creations must not be used to
#    endorse or promote products derived from this software without
#    prior written permission from Digital Creations.
# 
# 6. Modified redistributions of any form whatsoever must retain
#    the following acknowledgment:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    Intact (re-)distributions of any official Zope release do not
#    require an external acknowledgement.
# 
# 7. Modifications are encouraged but must be packaged separately as
#    patches to official Zope releases.  Distributions that do not
#    clearly separate the patches from the original work must be clearly
#    labeled as unofficial distributions.  Modifications which do not
#    carry the name Zope may be packaged in any form, as long as they
#    conform to all of the clauses above.
# 
# 
# Disclaimer
# 
#   THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS ``AS IS'' AND ANY
#   EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
#   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL CREATIONS OR ITS
#   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
#   USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#   ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#   OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
#   OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
#   SUCH DAMAGE.
# 
# 
# This software consists of contributions made by Digital Creations and
# many individuals on behalf of Digital Creations.  Specific
# attributions are listed in the accompanying credits file.
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
__version__='$Revision: 1.1 $'[11:-2]

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
