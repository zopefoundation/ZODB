##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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

import zope.interface
import BTrees.Interfaces

# hack to overcome dynamic-linking headache.
try:
    from _OIBTree import *
except ImportError:
    import ___BTree
    ___BTree._import(globals(), 'OI', 60, 250)

zope.interface.moduleProvides(BTrees.Interfaces.IObjectIntegerBTreeModule)
