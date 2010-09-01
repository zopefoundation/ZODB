/*****************************************************************************

  Copyright (c) 2001, 2002 Zope Foundation and Contributors.
  All Rights Reserved.

  This software is subject to the provisions of the Zope Public License,
  Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
  THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
  WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
  FOR A PARTICULAR PURPOSE

 ****************************************************************************/

#ifndef CPERSISTENCE_H
#define CPERSISTENCE_H

#include "Python.h"
#include "py24compat.h"

#include "ring.h"

#define CACHE_HEAD \
    PyObject_HEAD \
    CPersistentRing ring_home; \
    int non_ghost_count; \
    PY_LONG_LONG total_estimated_size;

struct ccobject_head_struct;

typedef struct ccobject_head_struct PerCache;    

/* How big is a persistent object?

   12  PyGC_Head is two pointers and an int
    8  PyObject_HEAD is an int and a pointer
 
   12  jar, oid, cache pointers
    8  ring struct
    8  serialno
    4  state + extra
    4  size info

  (56) so far

    4  dict ptr
    4  weaklist ptr
  -------------------------
   68  only need 62, but obmalloc rounds up to multiple of eight

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
    signed state:8;                              \
    unsigned estimated_size:24;

/* We recently added estimated_size.  We originally added it as a new
   unsigned long field after a signed char state field and a
   3-character reserved field.  This didn't work because there
   are packages in the wild that have their own copies of cPersistence.h
   that didn't see the update.  

   To get around this, we used the reserved space by making
   estimated_size a 24-bit bit field in the space occupied by the old
   3-character reserved field.  To fit in 24 bits, we made the units
   of estimated_size 64-character blocks.  This allows is to handle up
   to a GB.  We should never see that, but to be paranoid, we also
   truncate sizes greater than 1GB.  We also set the minimum size to
   64 bytes.

   We use the _estimated_size_in_24_bits and _estimated_size_in_bytes
   macros both to avoid repetition and to make intent a little clearer.
*/
#define _estimated_size_in_24_bits(I) ((I) > 1073741696 ? 16777215 : (I)/64+1)
#define _estimated_size_in_bytes(I) ((I)*64)

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
    int (*readCurrent)(cPersistentObject*);
} cPersistenceCAPIstruct;

#define cPersistenceType cPersistenceCAPI->pertype

#ifndef DONT_USE_CPERSISTENCECAPI
static cPersistenceCAPIstruct *cPersistenceCAPI;
#endif

#define cPersistanceModuleName "cPersistence"

#define PER_TypeCheck(O) PyObject_TypeCheck((O), cPersistenceCAPI->pertype)

#define PER_USE_OR_RETURN(O,R) {if((O)->state==cPersistent_GHOST_STATE && cPersistenceCAPI->setstate((PyObject*)(O)) < 0) return (R); else if ((O)->state==cPersistent_UPTODATE_STATE) (O)->state=cPersistent_STICKY_STATE;}

#define PER_CHANGED(O) (cPersistenceCAPI->changed((cPersistentObject*)(O)))

#define PER_READCURRENT(O, E)                                     \
  if (cPersistenceCAPI->readCurrent((cPersistentObject*)(O)) < 0) { E; }

#define PER_GHOSTIFY(O) (cPersistenceCAPI->ghostify((cPersistentObject*)(O)))

/* If the object is sticky, make it non-sticky, so that it can be ghostified.
   The value is not meaningful
 */
#define PER_ALLOW_DEACTIVATION(O) ((O)->state==cPersistent_STICKY_STATE && ((O)->state=cPersistent_UPTODATE_STATE))

#define PER_PREVENT_DEACTIVATION(O)  ((O)->state==cPersistent_UPTODATE_STATE && ((O)->state=cPersistent_STICKY_STATE))

/* 
   Make a persistent object usable from C by:

   - Making sure it is not a ghost

   - Making it sticky.

   IMPORTANT: If you call this and don't call PER_ALLOW_DEACTIVATION, 
              your object will not be ghostified.

   PER_USE returns a 1 on success and 0 failure, where failure means
   error.
 */
#define PER_USE(O) \
(((O)->state != cPersistent_GHOST_STATE \
  || (cPersistenceCAPI->setstate((PyObject*)(O)) >= 0)) \
 ? (((O)->state==cPersistent_UPTODATE_STATE) \
    ? ((O)->state=cPersistent_STICKY_STATE) : 1) : 0)

#define PER_ACCESSED(O)  (cPersistenceCAPI->accessed((cPersistentObject*)(O)))

#endif
