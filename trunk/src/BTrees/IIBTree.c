/* Setup template macros */

#define PERSISTENT

#define PREFIX "II"
#define INITMODULE initIIBTree
#define DEFAULT_MAX_BUCKET_SIZE 120
#define DEFAULT_MAX_BTREE_SIZE 500
                
#include "intkeymacros.h"
#include "intvaluemacros.h"
#include "BTreeModuleTemplate.c"
