/* Setup template macros */

#define MASTER_ID "$Id: _IIBTree.c,v 1.8 2003/11/28 16:44:44 jim Exp $\n"

#define PERSISTENT

#define MOD_NAME_PREFIX "II"
#define INITMODULE init_IIBTree
#define DEFAULT_MAX_BUCKET_SIZE 120
#define DEFAULT_MAX_BTREE_SIZE 500

#include "intkeymacros.h"
#include "intvaluemacros.h"
#include "BTreeModuleTemplate.c"
