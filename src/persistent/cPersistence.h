/*

  $Id: cPersistence.h,v 1.10 1998/11/11 02:00:56 jim Exp $

  Definitions to facilitate making cPersistent subclasses in C.


*/

#ifndef CPERSISTENCE_H
#define CPERSISTENCE_H

#include "ExtensionClass.h"
#include <time.h>

#define cPersistent_HEAD   PyObject_HEAD \
  PyObject *jar; \
  char oid[8]; \
  unsigned short atime; \
  signed char state; \

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

static cPersistenceCAPIstruct *cPersistenceCAPI;

#define PER_USE_OR_RETURN(O,R) {                        \
  if ((O)->state==cPersistent_GHOST_STATE &&            \
      cPersistenceCAPI->setstate((PyObject*)(O)) < 0)	\
    return (R);						\
  else if ((O)->state==cPersistent_UPTODATE_STATE)	\
    (O)->state=cPersistent_STICKY_STATE;		\
}

#define PER_CHANGED(O) (cPersistenceCAPI->changed((cPersistentObject*)(O)))

#define PER_ALLOW_DEACTIVATION(O)           \
((O)->state==cPersistent_STICKY_STATE &&    \
 ((O)->state=cPersistent_UPTODATE_STATE)) 

#define PER_DEL(O) Py_XDECREF((O)->jar)

#endif


