##############################################################################
#
# Copyright (c) 2001 Zope Corporation and Contributors. All Rights Reserved.
# 
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
# 
##############################################################################
"""Thread abstraction module

With this, we can run with or wothout threads.

$Id: bpthread.py,v 1.3 2001/11/28 15:51:20 matt Exp $"""

try:
    from thread import *
except:
    class allocate_lock:
        def acquire(self, *args): return args and 1 or None
        def release(self): pass

    start_new_thread=apply
