/*

  $Id: cPersistence.h,v 1.8 1997/12/10 22:19:24 jim Exp $

  Definitions to facilitate making cPersistent subclasses in C.

     Copyright 

       Copyright 1996 Digital Creations, L.C., 910 Princess Anne
       Street, Suite 300, Fredericksburg, Virginia 22401 U.S.A. All
       rights reserved.  Copyright in this software is owned by DCLC,
       unless otherwise indicated. Permission to use, copy and
       distribute this software is hereby granted, provided that the
       above copyright notice appear in all copies and that both that
       copyright notice and this permission notice appear. Note that
       any product, process or technology described in this software
       may be the subject of other Intellectual Property rights
       reserved by Digital Creations, L.C. and are not licensed
       hereunder.

     Trademarks 

       Digital Creations & DCLC, are trademarks of Digital Creations, L.C..
       All other trademarks are owned by their respective companies. 

     No Warranty 

       The software is provided "as is" without warranty of any kind,
       either express or implied, including, but not limited to, the
       implied warranties of merchantability, fitness for a particular
       purpose, or non-infringement. This software could include
       technical inaccuracies or typographical errors. Changes are
       periodically made to the software; these changes will be
       incorporated in new editions of the software. DCLC may make
       improvements and/or changes in this software at any time
       without notice.

     Limitation Of Liability 

       In no event will DCLC be liable for direct, indirect, special,
       incidental, economic, cover, or consequential damages arising
       out of the use of or inability to use this software even if
       advised of the possibility of such damages. Some states do not
       allow the exclusion or limitation of implied warranties or
       limitation of liability for incidental or consequential
       damages, so the above limitation or exclusion may not apply to
       you.
   
    If you have questions regarding this software,
    contact:
   
      Digital Creations L.C.  
      info@digicool.com
   
      (540) 371-6909


  $Log: cPersistence.h,v $
  Revision 1.8  1997/12/10 22:19:24  jim
  Added PER_USE macro.

  Revision 1.7  1997/07/18 14:15:39  jim
  Added PER_DEL so that subclasses can handle deallocation correctly.

  Revision 1.6  1997/06/06 19:13:32  jim
  Changed/fixed convenience macros.

  Revision 1.5  1997/05/19 17:51:20  jim
  Added macros to simplify C PO implementation.

  Revision 1.4  1997/05/19 13:49:36  jim
  Added include of time.h.

  Revision 1.3  1997/04/27 09:18:23  jim
  Added to the CAPI to support subtypes (like Record) that want to
  extend attr functions.

  Revision 1.2  1997/04/22 02:40:28  jim
  Changed object header layout.

  Revision 1.1  1997/04/01 17:15:48  jim
  *** empty log message ***


*/

#ifndef CPERSISTENCE_H
#define CPERSISTENCE_H

#include "ExtensionClass.h"
#include <time.h>

#define cPersistent_HEAD   PyObject_HEAD \
  PyObject *jar; \
  int oid; \
  int state; \
  time_t atime; \


#define cPersistent_GHOST_STATE -1
#define cPersistent_UPTODATE_STATE 0
#define cPersistent_CHANGED_STATE 1

typedef struct {
  cPersistent_HEAD
} cPersistentObject;

typedef struct {
  PyObject_HEAD
  cPersistentObject *object;
} PATimeobject;

typedef int (*persetattr)(PyObject *, PyObject*, PyObject *, setattrofunc);
typedef PyObject *(*pergetattr)(PyObject *, PyObject*, char *, getattrofunc);

typedef struct {
  PyMethodChain *methods;
  getattrofunc getattro;
  setattrofunc setattro;
  int (*changed)(PyObject*);
  int (*setstate)(PyObject*);
  pergetattr pergetattro;
  persetattr persetattro;
} cPersistenceCAPIstruct;

static cPersistenceCAPIstruct *cPersistenceCAPI;


#define PER_USE_OR_RETURN(O,R) \
  if(cPersistenceCAPI->setstate((PyObject*)(O)) < 0) return (R)

#define PER_USE(O) (cPersistenceCAPI->setstate((PyObject*)(O)))

#define PER_CHANGED(O) (cPersistenceCAPI->changed((PyObject*)(O)))

#define PER_PREVENT_DEACTIVATION(O) ((O)->atime=(time_t)1);
#define PER_ALLOW_DEACTIVATION(O) ((O)->atime=time(NULL));
#define PER_DEL(O) Py_XDECREF((O)->jar)

#endif


