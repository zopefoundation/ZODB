
#define MASTER_ID "$Id: _OOBTree.c,v 1.2 2001/03/27 16:37:42 jim Exp $\n"

#define PERSISTENT

#define MOD_NAME_PREFIX "OO"
#define INITMODULE init_OOBTree
#define DEFAULT_MAX_BUCKET_SIZE 30
#define DEFAULT_MAX_BTREE_SIZE 250
                                
#include "objectkeymacros.h"
#include "objectvaluemacros.h"
#include "BTreeModuleTemplate.c"
