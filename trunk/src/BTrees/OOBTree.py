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

# hack to overcome dynamic-linking headache.
from _OOBTree import *

# We don't really want _ names in pickles, so update all of the __module__
# references.
##for o in globals().values():
##    print o
##    if hasattr(o, '__module__'):
##        o.__module__=__name__

# XXX can't figure out why _reduce() won't call our __getstate__.

import copy_reg

def pickle_OOBTree(t):
    return t.__class__, t.__getstate__()

def unpickle_OOBTree(state):
    obj = OOBTree.__new__(OOBTree, None)
    obj.__setstate__(state)
    return obj

copy_reg.pickle(OOBTree, pickle_OOBTree)
