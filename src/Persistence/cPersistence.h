/*****************************************************************************
  
  Zope Public License (ZPL) Version 1.0
  -------------------------------------
  
  Copyright (c) Digital Creations.  All rights reserved.
  
  This license has been certified as Open Source(tm).
  
  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are
  met:
  
  1. Redistributions in source code must retain the above copyright
     notice, this list of conditions, and the following disclaimer.
  
  2. Redistributions in binary form must reproduce the above copyright
     notice, this list of conditions, and the following disclaimer in
     the documentation and/or other materials provided with the
     distribution.
  
  3. Digital Creations requests that attribution be given to Zope
     in any manner possible. Zope includes a "Powered by Zope"
     button that is installed by default. While it is not a license
     violation to remove this button, it is requested that the
     attribution remain. A significant investment has been put
     into Zope, and this effort will continue if the Zope community
     continues to grow. This is one way to assure that growth.
  
  4. All advertising materials and documentation mentioning
     features derived from or use of this software must display
     the following acknowledgement:
  
       "This product includes software developed by Digital Creations
       for use in the Z Object Publishing Environment
       (http://www.zope.org/)."
  
     In the event that the product being advertised includes an
     intact Zope distribution (with copyright and license included)
     then this clause is waived.
  
  5. Names associated with Zope or Digital Creations must not be used to
     endorse or promote products derived from this software without
     prior written permission from Digital Creations.
  
  6. Modified redistributions of any form whatsoever must retain
     the following acknowledgment:
  
       "This product includes software developed by Digital Creations
       for use in the Z Object Publishing Environment
       (http://www.zope.org/)."
  
     Intact (re-)distributions of any official Zope release do not
     require an external acknowledgement.
  
  7. Modifications are encouraged but must be packaged separately as
     patches to official Zope releases.  Distributions that do not
     clearly separate the patches from the original work must be clearly
     labeled as unofficial distributions.  Modifications which do not
     carry the name Zope may be packaged in any form, as long as they
     conform to all of the clauses above.
  
  
  Disclaimer
  
    THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS ``AS IS'' AND ANY
    EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
    IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
    PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL CREATIONS OR ITS
    CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
    LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
    USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
    ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
    OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
    OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
    SUCH DAMAGE.
  
  
  This software consists of contributions made by Digital Creations and
  many individuals on behalf of Digital Creations.  Specific
  attributions are listed in the accompanying credits file.
  
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

#endif


