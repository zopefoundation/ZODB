
#define MASTER_ID "$Id: OIBTree.c,v 1.7 2001/03/20 13:52:00 jim Exp $\n"

#define PERSISTENT

#define MOD_NAME_PREFIX "OI"
#define INITMODULE initOIBTree
#define DEFAULT_MAX_BUCKET_SIZE 60
#define DEFAULT_MAX_BTREE_SIZE 250
                                
#include "objectkeymacros.h"
#include "intvaluemacros.h"
#include "BTreeModuleTemplate.c"
