/*****************************************************************************

  Copyright (c) 2001, 2002 Zope Corporation and Contributors.
  All Rights Reserved.
  
  This software is subject to the provisions of the Zope Public License,
  Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
  THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
  WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
  FOR A PARTICULAR PURPOSE
  
 ****************************************************************************/

#ifndef CPERSISTENCE_H
#define CPERSISTENCE_H

#include "ExtensionClass.h"
#include <time.h>

#define CACHE_HEAD \
    PyObject_HEAD \
    CPersistentRing ring_home; \
    int non_ghost_count;

struct ccobject_head_struct;

typedef struct ccobject_head_struct PerCache;

#define cPersistent_HEAD \
    PyObject_HEAD; \
    PyObject *jar; \
    PyObject *oid; \
    PerCache *cache; \
    CPersistentRing ring; \
    char serial[8]; \
    signed char state; \
    unsigned char reserved[3];

#define cPersistent_GHOST_STATE -1
#define cPersistent_UPTODATE_STATE 0
#define cPersistent_CHANGED_STATE 1
#define cPersistent_STICKY_STATE 2

typedef struct CPersistentRing_struct
{
    struct CPersistentRing_struct *prev;
    struct CPersistentRing_struct *next;
} CPersistentRing;

typedef struct {
    cPersistent_HEAD
} cPersistentObject;

typedef int (*persetattr)(PyObject *, PyObject*, PyObject *, setattrofunc);
typedef PyObject *(*pergetattr)(PyObject *, PyObject*, char *, getattrofunc);

typedef struct {
  PyMethodChain *methods;
  getattrofunc getattro;
  setattrofunc setattro;
  int (*changed)(cPersistentObject*);
  void (*accessed)(cPersistentObject*);
  void (*ghostify)(cPersistentObject*);
  void (*deallocated)(cPersistentObject*);
  int (*setstate)(PyObject*);
  pergetattr pergetattro;
  persetattr persetattro;
} cPersistenceCAPIstruct;

#ifndef DONT_USE_CPERSISTENCECAPI
static cPersistenceCAPIstruct *cPersistenceCAPI;
#endif

#define cPersistanceModuleName "cPersistence"

#define PERSISTENT_TYPE_FLAG EXTENSIONCLASS_USER_FLAG8

/* ExtensionClass class flags for persistent base classes should
   include PERSISTENCE_FLAGS. 
*/
#define PERSISTENCE_FLAGS EXTENSIONCLASS_BASICNEW_FLAG | PERSISTENT_TYPE_FLAG \
  | EXTENSIONCLASS_PYTHONICATTR_FLAG

#define PER_USE_OR_RETURN(O,R) {if((O)->state==cPersistent_GHOST_STATE && cPersistenceCAPI->setstate((PyObject*)(O)) < 0) return (R); else if ((O)->state==cPersistent_UPTODATE_STATE) (O)->state=cPersistent_STICKY_STATE;}

#define PER_CHANGED(O) (cPersistenceCAPI->changed((cPersistentObject*)(O)))

#define PER_GHOSTIFY(O) (cPersistenceCAPI->ghostify((cPersistentObject*)(O)))

#define PER_ALLOW_DEACTIVATION(O) ((O)->state==cPersistent_STICKY_STATE && ((O)->state=cPersistent_UPTODATE_STATE))

#define PER_PREVENT_DEACTIVATION(O)  ((O)->state==cPersistent_UPTODATE_STATE && ((O)->state=cPersistent_STICKY_STATE))

#define PER_DEL(O) (cPersistenceCAPI->deallocated((cPersistentObject*)(O)))

#define PER_USE(O) \
(((O)->state != cPersistent_GHOST_STATE \
  || (cPersistenceCAPI->setstate((PyObject*)(O)) >= 0)) \
 ? (((O)->state==cPersistent_UPTODATE_STATE) \
    ? ((O)->state=cPersistent_STICKY_STATE) : 1) : 0)

#define PER_ACCESSED(O)  (cPersistenceCAPI->accessed((cPersistentObject*)(O)))

#endif


