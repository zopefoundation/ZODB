
#define MASTER_ID "$Id: _IOBTree.c,v 1.5 2002/02/21 21:41:17 jeremy Exp $\n"

#define PERSISTENT

#define MOD_NAME_PREFIX "IO"
#define DEFAULT_MAX_BUCKET_SIZE 60
#define DEFAULT_MAX_BTREE_SIZE 500
#define INITMODULE init_IOBTree
                                
#include "intkeymacros.h"
#include "objectvaluemacros.h"
#include "cPersistence.h"
#ifndef EXCLUDE_INTSET_SUPPORT
#include "BTree/intSet.h"
#endif
#include "BTreeModuleTemplate.c"
