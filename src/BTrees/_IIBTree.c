/* Setup template macros */

#define MASTER_ID "$Id: _IIBTree.c,v 1.6 2002/05/30 21:00:30 tim_one Exp $\n"

#define PERSISTENT

#define MOD_NAME_PREFIX "II"
#define INITMODULE init_IIBTree
#define DEFAULT_MAX_BUCKET_SIZE 120
#define DEFAULT_MAX_BTREE_SIZE 500
#define MULTI_INT_UNION 1

#include "intkeymacros.h"
#include "intvaluemacros.h"
#include "cPersistence.h"
#ifndef EXCLUDE_INTSET_SUPPORT
#include "BTree/intSet.h"
#endif
#include "BTreeModuleTemplate.c"
