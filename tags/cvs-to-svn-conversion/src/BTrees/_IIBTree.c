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

#define MASTER_ID "$Id: _IIBTree.c,v 1.9 2004/05/03 18:45:52 spascoe Exp $\n"

/* IIBTree - int key, int value BTree

   Implements a collection using int type keys
   and int type values
*/

/* Setup template macros */

#define PERSISTENT

#define MOD_NAME_PREFIX "II"
#define INITMODULE init_IIBTree
#define DEFAULT_MAX_BUCKET_SIZE 120
#define DEFAULT_MAX_BTREE_SIZE 500

#include "intkeymacros.h"
#include "intvaluemacros.h"
#include "BTreeModuleTemplate.c"
