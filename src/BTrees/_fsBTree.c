/*############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
############################################################################*/

#define MASTER_ID "$Id: _fsBTree.c,v 1.9 2004/05/01 01:15:45 spascoe Exp $\n"

/* fsBTree - FileStorage index BTree

   This BTree implements a mapping from 2-character strings
   to six-character strings. This allows us to efficiently store
   a FileStorage index as a nested mapping of 6-character oid prefix
   to mapping of 2-character oid suffix to 6-character (byte) file
   positions.
*/

typedef unsigned char char2[2];
typedef unsigned char char6[6];

/* Setup template macros */

#define PERSISTENT

#define MOD_NAME_PREFIX "fs"
#define INITMODULE init_fsBTree
#define DEFAULT_MAX_BUCKET_SIZE 500
#define DEFAULT_MAX_BTREE_SIZE 500

/*#include "intkeymacros.h"*/

#define KEYMACROS_H "$Id: _fsBTree.c,v 1.9 2004/05/01 01:15:45 spascoe Exp $\n"
#define KEY_TYPE char2
#undef KEY_TYPE_IS_PYOBJECT
#define KEY_CHECK(K) (PyString_Check(K) && PyString_GET_SIZE(K)==2)
#define TEST_KEY_SET_OR(V, K, T) if ( ( (V) = ((*(K) < *(T) || (*(K) == *(T) && (K)[1] < (T)[1])) ? -1 : ((*(K) == *(T) && (K)[1] == (T)[1]) ? 0 : 1)) ), 0 )
#define DECREF_KEY(KEY)
#define INCREF_KEY(k)
#define COPY_KEY(KEY, E) (*(KEY)=*(E), (KEY)[1]=(E)[1])
#define COPY_KEY_TO_OBJECT(O, K) O=PyString_FromStringAndSize(K,2)
#define COPY_KEY_FROM_ARG(TARGET, ARG, STATUS) \
  if (KEY_CHECK(ARG)) memcpy(TARGET, PyString_AS_STRING(ARG), 2); else { \
      PyErr_SetString(PyExc_TypeError, "expected two-character string key"); \
      (STATUS)=0; }

/*#include "intvaluemacros.h"*/
#define VALUEMACROS_H "$Id: _fsBTree.c,v 1.9 2004/05/01 01:15:45 spascoe Exp $\n"
#define VALUE_TYPE char6
#undef VALUE_TYPE_IS_PYOBJECT
#define TEST_VALUE(K, T) memcmp(K,T,6)
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
