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
    from _OOBTree import *
except ImportError:

    import ___BTree

    class _Base:
        pass

    class OOBucket(___BTree.Bucket, _Base):
        MAX_SIZE = 30

    _Base._mapping_type = OOBucket

    class OOSet(___BTree.Set, _Base)):
        MAX_SIZE = 30

    _Base._set_type = OOSet

    class OOBTree(___BTree.BTree, _Base)):
        _bucket_type = OOBucket
        MAX_SIZE = 250

    class OOBTreeSet(___BTree.BTree, _Base)):
        _bucket_type = OOSet
        MAX_SIZE = 250

zope.interface.moduleProvides(BTrees.Interfaces.IObjectObjectBTreeModule)
