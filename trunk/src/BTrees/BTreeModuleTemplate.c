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

#ifdef PERSISTENT
#include "cPersistence.h"

/***************************************************************
   The following are macros that ought to be in cPersistence.h */
#ifndef PER_USE 

#define PER_USE(O) \
(((O)->state != cPersistent_GHOST_STATE \
  || (cPersistenceCAPI->setstate((PyObject*)(O)) >= 0)) \
 ? (((O)->state==cPersistent_UPTODATE_STATE) \
    ? ((O)->state=cPersistent_STICKY_STATE) : 1) : 0)

#define PER_ACCESSED(O) ((O)->atime=((long)(time(NULL)/3))%65536) 


#endif
/***************************************************************/

#else
#include "ExtensionClass.h"
#define PER_USE_OR_RETURN(self, NULL)
#define PER_ALLOW_DEACTIVATION(self)
#define PER_PREVENT_DEACTIVATION(self)
#define PER_DEL(self)
#define PER_USE(O) 1
#define PER_ACCESSED(O) 1
#define PER_CHANGED(O) 0
#endif


static PyObject *sort_str, *reverse_str, *items_str, *__setstate___str;

static void PyVar_Assign(PyObject **v, PyObject *e) { Py_XDECREF(*v); *v=e;}
#define ASSIGN(V,E) PyVar_Assign(&(V),(E))
#define ASSIGNC(V,E) (Py_INCREF((E)), PyVar_Assign(&(V),(E)))
#define UNLESS(E) if (!(E))
#define UNLESS_ASSIGN(V,E) ASSIGN(V,E); UNLESS(V)
#define LIST(O) ((PyListObject*)(O))
#define OBJECT(O) ((PyObject*)(O))

#define MIN_BUCKET_ALLOC 16
#define MAX_BTREE_SIZE(B) DEFAULT_MAX_BTREE_SIZE
#define MAX_BUCKET_SIZE(B) DEFAULT_MAX_BUCKET_SIZE

#define SameType_Check(O1, O2) ((O1)->ob_type==(O2)->ob_type)

#define ASSERT(C, S, R) if (! (C)) { \
  PyErr_SetString(PyExc_AssertionError, (S)); return (R); }

typedef struct BTreeItemStruct {
  KEY_TYPE key;
  PyObject *value;
} BTreeItem;

typedef struct Bucket_s {
#ifdef PERSISTENT
  cPersistent_HEAD
#else
  PyObject_HEAD
#endif
  int size, len;
  struct Bucket_s *next;
  KEY_TYPE *keys;
  VALUE_TYPE *values;
} Bucket;

#define BUCKET(O) ((Bucket*)(O))

static void PyVar_AssignB(Bucket **v, Bucket *e) { Py_XDECREF(*v); *v=e;}
#define ASSIGNB(V,E) PyVar_AssignB(&(V),(E))
#define ASSIGNBC(V,E) (Py_INCREF((E)), PyVar_AssignB(&(V),(E)))

typedef struct {
#ifdef PERSISTENT
  cPersistent_HEAD
#else
  PyObject_HEAD
#endif
  int size, len;
  Bucket *firstbucket;
  BTreeItem *data;
} BTree;

staticforward PyExtensionClass BTreeType;


#define BTREE(O) ((BTree*)(O))

typedef struct SetIteration_s 
{
  PyObject *set;
  int position;
  int hasValue;
  KEY_TYPE key;
  VALUE_TYPE value;
  int (*next)(struct SetIteration_s*);
} SetIteration;

static PyObject *
IndexError(int i)
{                              
  PyObject *v;

  v=PyInt_FromLong(i);
  UNLESS (v) {
    v=Py_None;
    Py_INCREF(v);
  }
  PyErr_SetObject(PyExc_IndexError, v);
  Py_DECREF(v);
  return NULL;
}

static Bucket *
PreviousBucket(Bucket *current, Bucket *first, int i)
{
  if (! first) return NULL;
  if (first==current)
    {
      IndexError(i);
      return NULL;
    }

  Py_INCREF(first);
  while (1)
    {
      PER_USE_OR_RETURN(first,NULL);
      if (first->next==current) 
        {
          PER_ALLOW_DEACTIVATION(first);
          PER_ACCESSED(first);
          return first;
        }
      else if (first->next)
        {
          Bucket *next = first->next;
          Py_INCREF(next);
          PER_ALLOW_DEACTIVATION(first);
          PER_ACCESSED(first);
          Py_DECREF(first);
          first=next;
        }
      else
        {
          PER_ALLOW_DEACTIVATION(first);
          PER_ACCESSED(first);
          Py_DECREF(first);
          IndexError(i);
          return NULL;
        }
    }
}

static int 
firstBucketOffset(Bucket **bucket, int *offset)
{
  Bucket *b;

  *offset = (*bucket)->len - 1;
  while ((*bucket)->len < 1)
    {
      b=(*bucket)->next;
      if (b==NULL) return 0;
      Py_INCREF(b);
      PER_ALLOW_DEACTIVATION((*bucket));
      ASSIGNB((*bucket), b);
      UNLESS (PER_USE(*bucket)) return -1;
      *offset = 0;
    }
}

static int 
lastBucketOffset(Bucket **bucket, int *offset, Bucket *firstbucket, int i)
{
  Bucket *b;

  *offset = (*bucket)->len - 1;
  while ((*bucket)->len < 1)
    {
      b=PreviousBucket((*bucket), firstbucket, i);
      if (b==NULL) return 0;
      PER_ALLOW_DEACTIVATION((*bucket));
      ASSIGNB((*bucket), b);
      UNLESS (PER_USE(*bucket)) return -1;
      *offset = (*bucket)->len - 1;
    }
}

static void *
PyMalloc(size_t sz)
{
  void *r;

  ASSERT(sz > 0, "non-positive size malloc", NULL);

  if (r=malloc(sz)) return r;

  PyErr_NoMemory();
  return NULL;
}

static void *
PyRealloc(void *p, size_t sz)
{
  void *r;

  ASSERT(sz > 0, "non-positive size realloc", NULL);

  if (p) r=realloc(p,sz);
  else r=malloc(sz);

  UNLESS (r) PyErr_NoMemory();

  return r;
}

#include "BTreeItemsTemplate.c"
#include "BucketTemplate.c"
#include "SetTemplate.c"
#include "BTreeTemplate.c"
#include "TreeSetTemplate.c"
#include "SetOpTemplate.c"
#include "MergeTemplate.c"

static struct PyMethodDef module_methods[] = {
  {"difference", (PyCFunction) difference_m,	METH_VARARGS,
   "difference(o1, o2) -- "
   "compute the difference between o1 and o2"
  },
  {"union", (PyCFunction) union_m,	METH_VARARGS,
   "union(o1, o2) -- compute the union of o1 and o2\n"
  },
  {"intersection", (PyCFunction) intersection_m,	METH_VARARGS,
   "intersection(o1, o2) -- "
   "compute the intersection of o1 and o2"
  },
#ifdef MERGE
  {"weightedUnion", (PyCFunction) wunion_m,	METH_VARARGS,
   "weightedUnion(o1, o2 [, w1, w2]) -- compute the union of o1 and o2\n"
   "\nw1 and w2 are weights."
  },
  {"weightedIntersection", (PyCFunction) wintersection_m,	METH_VARARGS,
   "weightedIntersection(o1, o2 [, w1, w2]) -- "
   "compute the intersection of o1 and o2\n"
   "\nw1 and w2 are weights."
  },
#endif
  {NULL,		NULL}		/* sentinel */
};

static char BTree_module_documentation[] = 
"\n"
MASTER_ID
BTREEITEMSTEMPLATE_C
"$Id: BTreeModuleTemplate.c,v 1.8 2001/03/27 16:37:42 jim Exp $\n"
BTREETEMPLATE_C
BUCKETTEMPLATE_C
KEYMACROS_H
MERGETEMPLATE_C
SETOPTEMPLATE_C
SETTEMPLATE_C
TREESETTEMPLATE_C
VALUEMACROS_H
BTREEITEMSTEMPLATE_C
;

void 
INITMODULE ()
{
  PyObject *m, *d;

  UNLESS (sort_str=PyString_FromString("sort")) return;
  UNLESS (reverse_str=PyString_FromString("reverse")) return;
  UNLESS (items_str=PyString_FromString("items")) return;
  UNLESS (__setstate___str=PyString_FromString("__setstate__")) return;

  UNLESS (PyExtensionClassCAPI=PyCObject_Import("ExtensionClass","CAPI"))
      return;

#ifdef PERSISTENT
  if (cPersistenceCAPI=PyCObject_Import("cPersistence","CAPI"))
    {
	BucketType.methods.link=cPersistenceCAPI->methods;
	BucketType.tp_getattro=cPersistenceCAPI->getattro;
	BucketType.tp_setattro=cPersistenceCAPI->setattro;

	SetType.methods.link=cPersistenceCAPI->methods;
	SetType.tp_getattro=cPersistenceCAPI->getattro;
	SetType.tp_setattro=cPersistenceCAPI->setattro;

	BTreeType.methods.link=cPersistenceCAPI->methods;
	BTreeType.tp_getattro=cPersistenceCAPI->getattro;
	BTreeType.tp_setattro=cPersistenceCAPI->setattro;

	TreeSetType.methods.link=cPersistenceCAPI->methods;
	TreeSetType.tp_getattro=cPersistenceCAPI->getattro;
	TreeSetType.tp_setattro=cPersistenceCAPI->setattro;
    }
  else return;
#else
  BTreeType.tp_getattro=PyExtensionClassCAPI->getattro;
  BucketType.tp_getattro=PyExtensionClassCAPI->getattro;
  SetType.tp_getattro=PyExtensionClassCAPI->getattro;
  TreeSetType.tp_getattro=PyExtensionClassCAPI->getattro;
#endif

  BTreeItemsType.ob_type=&PyType_Type;

#ifdef INTSET_H
  UNLESS(d = PyImport_ImportModule("intSet")) return;
  UNLESS(intSetType = PyObject_GetAttrString (d, "intSet")) return;
  Py_DECREF (d); 
#endif

  /* Create the module and add the functions */
  m = Py_InitModule4("_" MOD_NAME_PREFIX "BTree", module_methods,
		     BTree_module_documentation,
		     (PyObject*)NULL,PYTHON_API_VERSION);

  /* Add some symbolic constants to the module */
  d = PyModule_GetDict(m);

  PyDict_SetItemString(d, "__version__",
		       PyString_FromString("$Revision: 1.8 $"));

  PyExtensionClass_Export(d,MOD_NAME_PREFIX "Bucket", BucketType);
  PyExtensionClass_Export(d,MOD_NAME_PREFIX "BTree", BTreeType);
  PyExtensionClass_Export(d,MOD_NAME_PREFIX "Set", SetType);
  PyExtensionClass_Export(d,MOD_NAME_PREFIX "TreeSet", TreeSetType);
 
  /* Check for errors */
  if (PyErr_Occurred())
    Py_FatalError("can't initialize module " MOD_NAME_PREFIX "BTree");
}
