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

def convert(old, new, threshold=200, f=None, None=None):
    "Utility for converting old btree to new"
    n=0
    for k, v in old.items():
        if f is not None: v=f(v)
        new[k]=v
        n=n+1
        if n > threshold:
            get_transaction().commit(1)
            old._p_jar.cacheMinimize(3)
            n=0

    get_transaction().commit(1)
    old._p_jar.cacheMinimize(3)
