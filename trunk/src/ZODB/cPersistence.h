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

#define cPersistent_HEAD PyObject_HEAD PyObject *jar, *oid; char serial[8]; unsigned short atime;  signed char state;  unsigned char reserved; 
#define cPersistent_GHOST_STATE -1
#define cPersistent_UPTODATE_STATE 0
#define cPersistent_CHANGED_STATE 1
#define cPersistent_STICKY_STATE 2

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

#define PER_ALLOW_DEACTIVATION(O) ((O)->state==cPersistent_STICKY_STATE && ((O)->state=cPersistent_UPTODATE_STATE)) 

#define PER_PREVENT_DEACTIVATION(O)  ((O)->state==cPersistent_UPTODATE_STATE && ((O)->state=cPersistent_STICKY_STATE)) 

#define PER_DEL(O) Py_XDECREF((O)->jar); Py_XDECREF((O)->oid);

#define PER_USE(O) \
(((O)->state != cPersistent_GHOST_STATE \
  || (cPersistenceCAPI->setstate((PyObject*)(O)) >= 0)) \
 ? (((O)->state==cPersistent_UPTODATE_STATE) \
    ? ((O)->state=cPersistent_STICKY_STATE) : 1) : 0)

#define PER_ACCESSED(O) ((O)->atime=((long)(time(NULL)/3))%65536) 

#endif


