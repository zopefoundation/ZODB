/* Setup template macros */

#define PERSISTENT

#define PREFIX "II"
#define INITMODULE initIIBTree
#define DEFAULT_MAX_BUCKET_SIZE 100
                
#include "intkeymacros.h"
#include "intvaluemacros.h"
#include "BTreeModuleTemplate.c"
