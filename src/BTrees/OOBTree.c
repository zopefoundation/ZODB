
#define MASTER_ID "$Id: OOBTree.c,v 1.7 2001/03/20 13:52:00 jim Exp $\n"

#define PERSISTENT

#define MOD_NAME_PREFIX "OO"
#define INITMODULE initOOBTree
#define DEFAULT_MAX_BUCKET_SIZE 30
#define DEFAULT_MAX_BTREE_SIZE 250
                                
#include "objectkeymacros.h"
#include "objectvaluemacros.h"
#include "BTreeModuleTemplate.c"
