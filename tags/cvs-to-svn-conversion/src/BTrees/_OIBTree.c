/*############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
############################################################################*/

#define MASTER_ID "$Id: _OIBTree.c,v 1.4 2004/05/03 18:45:52 spascoe Exp $\n"

/* OIBTree - object key, int value BTree

   Implements a collection using object type keys
   and int type values
*/

#define PERSISTENT

#define MOD_NAME_PREFIX "OI"
#define INITMODULE init_OIBTree
#define DEFAULT_MAX_BUCKET_SIZE 60
#define DEFAULT_MAX_BTREE_SIZE 250
                                
#include "objectkeymacros.h"
#include "intvaluemacros.h"
#include "BTreeModuleTemplate.c"
