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

# TODO:  does this script still serve a purpose?  Writing this in 2005,
# "old btree" doesn't mean much to me.

import transaction

def convert(old, new, threshold=200, f=None):
    "Utility for converting old btree to new"
    n=0
    for k, v in old.items():
        if f is not None: v=f(v)
        new[k]=v
        n=n+1
        if n > threshold:
            transaction.savepoint()
            old._p_jar.cacheMinimize()
            n=0

    transaction.savepoint()
    old._p_jar.cacheMinimize()
