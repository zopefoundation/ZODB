/*############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
############################################################################*/

#define MASTER_ID "$Id: _OIBTree.c 25186 2004-06-02 15:07:33Z jim $\n"

/* OIBTree - object key, int value BTree

   Implements a collection using object type keys
   and int type values
*/

#define PERSISTENT

#define MOD_NAME_PREFIX "OL"
#define INITMODULE init_OLBTree
#define DEFAULT_MAX_BUCKET_SIZE 60
#define DEFAULT_MAX_BTREE_SIZE 250

#define ZODB_64BIT_INTS

#include "objectkeymacros.h"
#include "intvaluemacros.h"
#include "BTreeModuleTemplate.c"
