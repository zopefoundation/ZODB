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
import os
import types
import zLOG

_label = "zrpc:%s" % os.getpid()

def new_label():
    global _label
    _label = "zrpc:%s" % os.getpid()

def log(message, level=zLOG.BLATHER, label=None, error=None):
    zLOG.LOG(label or _label, level, message, error=error)

# There's a nice "short_repr" function in the repr module:
from repr import repr as short_repr
