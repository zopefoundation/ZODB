
#define MASTER_ID "$Id: IOBTree.c,v 1.9 2001/03/20 13:52:00 jim Exp $\n"

#define PERSISTENT

#define MOD_NAME_PREFIX "IO"
#define DEFAULT_MAX_BUCKET_SIZE 60
#define DEFAULT_MAX_BTREE_SIZE 500
#define INITMODULE initIOBTree
                                
#include "intkeymacros.h"
#include "objectvaluemacros.h"
#include "cPersistence.h"
#include "BTree/intSet.h"
#include "BTreeModuleTemplate.c"
