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

#include "Python.h"
#include "ring.h"

#define CACHE_HEAD \
    PyObject_HEAD \
    CPersistentRing ring_home; \
    int non_ghost_count;

struct ccobject_head_struct;

typedef struct ccobject_head_struct PerCache;

/* How big is a persistent object?

   12  PyGC_Head is two pointers and an int
    8  PyObject_HEAD is an int and a pointer
 
   12  jar, oid, cache pointers
    8  ring struct
    8  serialno
    4  state + extra

  (52) so far

    4  dict ptr
    4  weaklist ptr
  -------------------------
   64  only need 62, but obmalloc rounds up to multiple of eight

  Even a ghost requires 64 bytes.  It's possible to make a persistent
  instance with slots and no dict, which changes the storage needed.

*/

#define cPersistent_HEAD \
    PyObject_HEAD \
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

typedef struct {
    cPersistent_HEAD
} cPersistentObject;

typedef void (*percachedelfunc)(PerCache *, PyObject *);

typedef struct {
    PyTypeObject *pertype;
    getattrofunc getattro;
    setattrofunc setattro;
    int (*changed)(cPersistentObject*);
    void (*accessed)(cPersistentObject*);
    void (*ghostify)(cPersistentObject*);
    int (*setstate)(PyObject*);
    percachedelfunc percachedel;
} cPersistenceCAPIstruct;

#ifndef DONT_USE_CPERSISTENCECAPI
static cPersistenceCAPIstruct *cPersistenceCAPI;
#endif

#define cPersistanceModuleName "cPersistence"

#define PER_TypeCheck(O) PyObject_TypeCheck((O), cPersistenceCAPI->pertype)

#define PER_USE_OR_RETURN(O,R) {if((O)->state==cPersistent_GHOST_STATE && cPersistenceCAPI->setstate((PyObject*)(O)) < 0) return (R); else if ((O)->state==cPersistent_UPTODATE_STATE) (O)->state=cPersistent_STICKY_STATE;}

#define PER_CHANGED(O) (cPersistenceCAPI->changed((cPersistentObject*)(O)))

#define PER_GHOSTIFY(O) (cPersistenceCAPI->ghostify((cPersistentObject*)(O)))

#define PER_ALLOW_DEACTIVATION(O) ((O)->state==cPersistent_STICKY_STATE && ((O)->state=cPersistent_UPTODATE_STATE))

#define PER_PREVENT_DEACTIVATION(O)  ((O)->state==cPersistent_UPTODATE_STATE && ((O)->state=cPersistent_STICKY_STATE))

#define PER_USE(O) \
(((O)->state != cPersistent_GHOST_STATE \
  || (cPersistenceCAPI->setstate((PyObject*)(O)) >= 0)) \
 ? (((O)->state==cPersistent_UPTODATE_STATE) \
    ? ((O)->state=cPersistent_STICKY_STATE) : 1) : 0)

#define PER_ACCESSED(O)  (cPersistenceCAPI->accessed((cPersistentObject*)(O)))

#endif
