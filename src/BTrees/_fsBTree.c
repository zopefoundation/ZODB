/* fsBTree - FileStorage index BTree

   This BTree implments a mapping from 2-character strings
   to six-character strings. This allows us to effieciently store
   a FileStorage index as a nested mapping of 6-character oid prefix
   to mapping of 2-character oid suffix to 6-character (byte) file
   positions.
*/

#include <string.h>

typedef unsigned char char2[2];
typedef unsigned char char6[6];


/* Setup template macros */

#define MASTER_ID "$Id: _fsBTree.c,v 1.2 2002/02/12 22:33:07 gvanrossum Exp $\n"

#define PERSISTENT

#define MOD_NAME_PREFIX "fs"
#define INITMODULE init_fsBTree
#define DEFAULT_MAX_BUCKET_SIZE 500
#define DEFAULT_MAX_BTREE_SIZE 500
                
/*#include "intkeymacros.h"*/

#define KEYMACROS_H "$Id: _fsBTree.c,v 1.2 2002/02/12 22:33:07 gvanrossum Exp $\n"
#define KEY_TYPE char2
#define KEY_CHECK(K) (PyString_Check(K) && PyString_GET_SIZE(K)==2)
#define TEST_KEY(K, T) ((*(K) < *(T) || (*(K) == *(T) && (K)[1] < (T)[1])) ? -1 : ((*(K) == *(T) && (K)[1] == (T)[1]) ? 0 : 1))
#define DECREF_KEY(KEY)
#define INCREF_KEY(k)
#define COPY_KEY(KEY, E) (*(KEY)=*(E), (KEY)[1]=(E)[1])
#define COPY_KEY_TO_OBJECT(O, K) O=PyString_FromStringAndSize(K,2)
#define COPY_KEY_FROM_ARG(TARGET, ARG, STATUS) \
  if (KEY_CHECK(ARG)) memcpy(TARGET, PyString_AS_STRING(ARG), 2); else { \
      PyErr_SetString(PyExc_TypeError, "expected two-character string key"); \
      (STATUS)=0; } 

/*#include "intvaluemacros.h"*/
#define VALUEMACROS_H "$Id: _fsBTree.c,v 1.2 2002/02/12 22:33:07 gvanrossum Exp $\n"
#define VALUE_TYPE char6
#define TEST_VALUE(K, T) strncmp(K,T,6)
#define DECLARE_VALUE(NAME) VALUE_TYPE NAME
#define DECREF_VALUE(k)
#define INCREF_VALUE(k)
#define COPY_VALUE(V, E) (memcpy(V, E, 6))
#define COPY_VALUE_TO_OBJECT(O, K) O=PyString_FromStringAndSize(K,6)
#define COPY_VALUE_FROM_ARG(TARGET, ARG, STATUS) \
  if ((PyString_Check(ARG) && PyString_GET_SIZE(ARG)==6)) \
      memcpy(TARGET, PyString_AS_STRING(ARG), 6); else { \
      PyErr_SetString(PyExc_TypeError, "expected six-character string key"); \
      (STATUS)=0; } 
  
#define NORMALIZE_VALUE(V, MIN)
#include "BTreeModuleTemplate.c"
