##############################################################################
#
# Copyright (c) 2002 Zope Corporation and Contributors.
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
"""Utilities for setting up the server environment."""

import os

def parentdir(p, n=1):
    """Return the parent of p, n levels up."""
    d = p
    while n:
        d = os.path.split(d)[0]
        if not d or d == '.':
            d = os.getcwd()
        n -= 1
    return d

class Environment:

    def __init__(self, argv0):
        v = os.environ.get("INSTANCE_HOME")
        if v is None:
            # looking for a Zope/var directory assuming that this code
            # is installed in Zope/lib/python/ZEO
            p = parentdir(argv0, 4)
            if os.path.isdir(os.path.join(p, "var")):
                v = p
            else:
                v = os.getcwd()
        self.home = v
        self.var = os.path.join(v, "var")
        if not os.path.isdir(self.var):
            self.var = self.home

        pid = os.environ.get("ZEO_SERVER_PID")
        if pid is None:
            pid = os.path.join(v, "ZEO_SERVER.pid")
        self.zeo_pid = pid    
        
        self.fs = os.path.join(self.var, "Data.fs")
