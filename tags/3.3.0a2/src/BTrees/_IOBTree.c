
#define MASTER_ID "$Id: _IOBTree.c,v 1.6 2003/11/28 16:44:44 jim Exp $\n"

#define PERSISTENT

#define MOD_NAME_PREFIX "IO"
#define DEFAULT_MAX_BUCKET_SIZE 60
#define DEFAULT_MAX_BTREE_SIZE 500
#define INITMODULE init_IOBTree
                                
#include "intkeymacros.h"
#include "objectvaluemacros.h"
#include "BTreeModuleTemplate.c"
