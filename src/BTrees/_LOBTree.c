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

#define MASTER_ID "$Id: _IOBTree.c 25186 2004-06-02 15:07:33Z jim $\n"

/* IOBTree - int key, object value BTree

   Implements a collection using int type keys
   and object type values
*/

#define PERSISTENT

#define MOD_NAME_PREFIX "LO"
#define DEFAULT_MAX_BUCKET_SIZE 120
#define DEFAULT_MAX_BTREE_SIZE 500
#define INITMODULE init_LOBTree

#define ZODB_64BIT_INTS

#include "intkeymacros.h"
#include "objectvaluemacros.h"
#include "BTreeModuleTemplate.c"
