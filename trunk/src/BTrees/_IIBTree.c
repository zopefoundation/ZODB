/* Setup template macros */

#define MASTER_ID "$Id: _IIBTree.c,v 1.2 2001/03/27 16:37:42 jim Exp $\n"

#define PERSISTENT

#define MOD_NAME_PREFIX "II"
#define INITMODULE init_IIBTree
#define DEFAULT_MAX_BUCKET_SIZE 120
#define DEFAULT_MAX_BTREE_SIZE 500
                
#include "intkeymacros.h"
#include "intvaluemacros.h"
#include "cPersistence.h"
#include "BTree/intSet.h"
#include "BTreeModuleTemplate.c"
