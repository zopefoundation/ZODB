
#define MASTER_ID "$Id: _OOBTree.c,v 1.3 2003/11/28 16:44:44 jim Exp $\n"

#define PERSISTENT

#define MOD_NAME_PREFIX "OO"
#define INITMODULE init_OOBTree
#define DEFAULT_MAX_BUCKET_SIZE 30
#define DEFAULT_MAX_BTREE_SIZE 250
                                
#include "objectkeymacros.h"
#include "objectvaluemacros.h"
#include "BTreeModuleTemplate.c"
