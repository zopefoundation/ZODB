/* Setup template macros */

#define MASTER_ID "$Id: IIBTree.c,v 1.8 2001/03/20 13:52:00 jim Exp $\n"

#define PERSISTENT

#define MOD_NAME_PREFIX "II"
#define INITMODULE initIIBTree
#define DEFAULT_MAX_BUCKET_SIZE 120
#define DEFAULT_MAX_BTREE_SIZE 500
                
#include "intkeymacros.h"
#include "intvaluemacros.h"
#include "cPersistence.h"
#include "BTree/intSet.h"
#include "BTreeModuleTemplate.c"
