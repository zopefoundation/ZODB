
#define PERSISTENT

#define PREFIX "IO"
#define DEFAULT_MAX_BUCKET_SIZE 60
#define DEFAULT_MAX_BTREE_SIZE 500
#define INITMODULE initIOBTree
                                
#include "intkeymacros.h"
#include "objectvaluemacros.h"
#include "cPersistence.h"
#include "BTree/intSet.h"
#include "BTreeModuleTemplate.c"
