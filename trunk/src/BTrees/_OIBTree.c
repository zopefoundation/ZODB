
#define MASTER_ID "$Id: _OIBTree.c,v 1.2 2001/03/27 16:37:42 jim Exp $\n"

#define PERSISTENT

#define MOD_NAME_PREFIX "OI"
#define INITMODULE init_OIBTree
#define DEFAULT_MAX_BUCKET_SIZE 60
#define DEFAULT_MAX_BTREE_SIZE 250
                                
#include "objectkeymacros.h"
#include "intvaluemacros.h"
#include "BTreeModuleTemplate.c"
