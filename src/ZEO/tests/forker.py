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
"""Library for forking storage server and connecting client storage"""

import os
import sys
import time
import errno
import types
import random
import socket
import asyncore
import tempfile
import traceback
from cStringIO import StringIO

import zLOG

# Change value of PROFILE to enable server-side profiling
PROFILE = 0
if PROFILE:
    import hotshot

def get_port():
    """Return a port that is not in use.

    Checks if a port is in use by trying to connect to it.  Assumes it
    is not in use if connect raises an exception.

    Raises RuntimeError after 10 tries.
    """
    for i in range(10):
        port = random.randrange(20000, 30000)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            try:
                s.connect(('localhost', port))
            except socket.error:
                # XXX check value of error?
                return port
        finally:
            s.close()
    raise RuntimeError, "Can't find port"


def start_zeo_server(conf, addr=None, ro_svr=0):
    """Start a ZEO server in a separate process.

    Returns the ZEO port, the test server port, and the pid.
    """
    # Store the config info in a temp file.
    tmpfile = tempfile.mktemp()
    fp = open(tmpfile, 'w')
    fp.write(conf)
    fp.close()
    # Create the server
    import ZEO.tests.zeoserver
    if addr is None:
        port = get_port()
    else:
        port = addr[1]
    script = ZEO.tests.zeoserver.__file__
    if script.endswith('.pyc'):
        script = script[:-1]
    # Create a list of arguments, which we'll tuplify below
    args = [sys.executable, script, '-C', tmpfile]
    if ro_svr:
        args.append('-r')
    args.append(str(port))
    d = os.environ.copy()
    d['PYTHONPATH'] = os.pathsep.join(sys.path)
    pid = os.spawnve(os.P_NOWAIT, sys.executable, tuple(args), d)
    adminaddr = ('localhost', port+1)
    # We need to wait until the server starts, but not forever
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    for i in range(5):
        try:
            zLOG.LOG('forker', zLOG.DEBUG, 'connect %s' % i)
            s.connect(adminaddr)
            ack = s.recv(1)
            zLOG.LOG('forker', zLOG.DEBUG, 'acked: %s' % ack)
            break
        except socket.error, e:
            if e[0] <> errno.ECONNREFUSED: raise
            time.sleep(1)
    else:
        zLOG.LOG('forker', zLOG.DEBUG, 'boo hoo')
        raise
    return ('localhost', port), adminaddr, pid


def shutdown_zeo_server(adminaddr):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(adminaddr)
    ack = s.recv(1)
    zLOG.LOG('shutdownServer', zLOG.DEBUG, 'acked: %s' % ack)
    s.close()
