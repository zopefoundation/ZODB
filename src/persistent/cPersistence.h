/*

  $Id: cPersistence.h,v 1.2 1997/04/22 02:40:28 jim Exp $

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
  Revision 1.2  1997/04/22 02:40:28  jim
  Changed object header layout.

  Revision 1.1  1997/04/01 17:15:48  jim
  *** empty log message ***


*/

#ifndef CPERSISTENCE_H
#define CPERSISTENCE_H

#include "ExtensionClass.h"

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


typedef struct {
  PyMethodChain *methods;
  getattrofunc getattro;
  setattrofunc setattro;
  int (*changed)(PyObject*);
  int (*setstate)(PyObject*);
} cPersistenceCAPIstruct;

static cPersistenceCAPIstruct *cPersistenceCAPI;

#endif


