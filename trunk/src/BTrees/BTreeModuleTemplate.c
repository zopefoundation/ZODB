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
static PyObject *ConflictError = NULL;

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

/* Various kinds of BTree and Bucket structs are instances of
 * "sized containers", and have a common initial layout:
 *     The stuff needed for all Python objects, or all Persistent objects.
 *     int size:  The maximum number of things that could be contained
 *                without growing the container.
 *     int len:   The number of things currently contained.
 *
 * Invariant:  0 <= len <= size.
 *
 * A sized container typically goes on to declare one or more pointers
 * to contiguous arrays with 'size' elements each, the initial 'len' of
 * which are currently in use.
 */
#ifdef PERSISTENT
#define sizedcontainer_HEAD         \
    cPersistent_HEAD                \
    int size;                       \
    int len;
#else
#define sizedcontainer_HEAD         \
    PyObject_HEAD                   \
    int size;                       \
    int len;
#endif

/* Nothing is actually of type Sized, but (pointers to) BTree nodes and
 * Buckets can be cast to Sized* in contexts that only need to examine
 * the members common to all sized containers.
 */
typedef struct Sized_s {
    sizedcontainer_HEAD
} Sized;

#define SIZED(O) ((Sized*)(O))

/* A Bucket wraps contiguous vectors of keys and values.  Keys are unique,
 * and stored in sorted order.  The 'values' pointer may be NULL if the
 * Bucket is used to implement a set.  Buckets serving as leafs of BTrees
 * are chained together via 'next', so that the entire BTree contents
 * can be traversed in sorted order quickly and easily.
 */
typedef struct Bucket_s {
  sizedcontainer_HEAD
  struct Bucket_s *next;    /* the bucket with the next-larger keys */
  KEY_TYPE *keys;           /* 'len' keys, in increasing order */
  VALUE_TYPE *values;       /* 'len' corresponding values; NULL if a set */
} Bucket;

#define BUCKET(O) ((Bucket*)(O))

static void PyVar_AssignB(Bucket **v, Bucket *e) { Py_XDECREF(*v); *v=e;}
#define ASSIGNB(V,E) PyVar_AssignB(&(V),(E))
#define ASSIGNBC(V,E) (Py_INCREF((E)), PyVar_AssignB(&(V),(E)))

/* A BTree is complicated.  See Maintainer.txt.
 */

typedef struct BTreeItem_s {
  KEY_TYPE key;
  Sized *child; /* points to another BTree, or to a Bucket of some sort */
} BTreeItem;

typedef struct BTree_s {
  sizedcontainer_HEAD

  /* firstbucket points to the bucket containing the smallest key in
   * the BTree.  This is found by traversing leftmost child pointers
   * (data[0].child) until reaching a Bucket.
   */
  Bucket *firstbucket;

  /* The BTree points to 'len' children, via the "child" fields of the data
   * array.  There are len-1 keys in the 'key' fields, stored in increasing
   * order.  data[0].key is unused.  For i in 0 .. len-1, all keys reachable
   * from data[i].child are >= data[i].key and < data[i+1].key, at the
   * endpoints pretending that data[0].key is minus infinity and
   * data[len].key is positive infinity.
   */
  BTreeItem *data;
} BTree;

staticforward PyExtensionClass BTreeType;

#define BTREE(O) ((BTree*)(O))

/* Use BTREE_SEARCH to find which child pointer to follow.
 * RESULT   An int lvalue to hold the index i such that SELF->data[i].child
 *          is the correct node to search next.
 * SELF     A pointer to a BTree node.
 * KEY      The key you're looking for, of type KEY_TYPE.
 * ONERROR  What to do if key comparison raises an exception; for example,
 *          perhaps 'return NULL'.
 *
 * See Maintainer.txt for discussion:  this is optimized in subtle ways.
 * It's recommended that you call this at the start of a routine, waiting
 * to check for self->len == 0 after.
 */
#define BTREE_SEARCH(RESULT, SELF, KEY, ONERROR) {          \
    int _lo = 0;                                            \
    int _hi = (SELF)->len;                                  \
    int _i, _cmp;                                           \
    for (_i = _hi >> 1; _i > _lo; _i = (_lo + _hi) >> 1) {  \
        TEST_KEY_SET_OR(_cmp, (SELF)->data[_i].key, (KEY))  \
            ONERROR;                                        \
        if      (_cmp < 0) _lo = _i;                        \
        else if (_cmp > 0) _hi = _i;                        \
        else   /* equal */ break;                           \
    }                                                       \
    (RESULT) = _i;                                          \
}

/* SetIteration structs are used in the internal set iteration protocol.
 * When you want to iterate over a set or bucket or BTree (even an
 * individual key!),
 * 1. Declare a new iterator and a "merge" int:
 *        SetIteration si = {0,0,0};
 *        int merge = 0;
 *    Using "{0,0,0}" or "{0,0}" appear most common.  Only one {0} is
 *    necssary.  At least one must be given so that finiSetIteration() works
 *    correctly even if you don't get around to calling initSetIteration().
 * 2. Initialize it via
 *        initSetIteration(&si, PyObject *s, int weight, &merge)
 *    It's an error if that returns an int < 0.  In case of error on the
 *    init call, calling finiSetIteration(&si) is optional.  But if the
 *    init call succeeds, you must eventually call finiSetIteration(),
 *    and whether or not subsequent calls to si.next() fail.
 * 3. Get the first element:
 *        if (si.next(&si) < 0) { there was an error }
 *    If the set isn't empty, this sets si.position to an int >= 0,
 *    si.key to the element's key (of type KEY_TYPE), and maybe si.value to
 *    the element's value (of type VALUE_TYPE).  si.value is defined
 *    iff merge was set to true by the initSetIteration() call, which is
 *    equivalent to whether si.usesValue is true.
 * 4. Process all the elements:
 *        while (si.position >= 0) {
 *            do something with si.key and/or si.value;
 *            if (si.next(&si) < 0) { there was an error; }
 *        }
 * 5. Finalize the SetIterator:
 *        finiSetIteration(&si);
 *    This is mandatory!  si may contain references to iterator objects,
 *    keys and values, and they must be cleaned up else they'll leak.  If
 *    this were C++ we'd hide that in the destructor, but in C you have to
 *    do it by hand.
 */
typedef struct SetIteration_s
{
  PyObject *set;    /* the set, bucket, BTree, ..., being iterated */
  int position;     /* initialized to 0; set to -1 by next() when done */
  int usesValue;    /* true iff 'set' has values & we iterate them */
  int hasValue;     /* true iff 'set' has values (as well as keys) */
  KEY_TYPE key;     /* next() sets to next key */
  VALUE_TYPE value; /* next() may set to next value */
  int (*next)(struct SetIteration_s*);  /* function to get next key+value */
} SetIteration;

/* Finish the set iteration protocol.  This MUST be called by everyone
 * who starts a set iteration, unless the initial call to initSetIteration
 * failed; in that case, and only that case, calling finiSetIteration is
 * optional.
 */
static void
finiSetIteration(SetIteration *i)
{
    assert(i != NULL);
    if (i->set == NULL)
        return;
    Py_DECREF(i->set);
    i->set = NULL;      /* so it doesn't hurt to call this again */

    if (i->position > 0) {
        /* next() was called at least once, but didn't finish iterating
         * (else position would be negative).  So the cached key and
         * value need to be cleaned up.
         */
        DECREF_KEY(i->key);
        if (i->usesValue) {
            DECREF_VALUE(i->value);
        }
    }
    i->position = -1;   /* stop any stray next calls from doing harm */
}

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
  return 1;
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
  return 1;
}

static void *
PyMalloc(size_t sz)
{
  void *r;

  ASSERT(sz > 0, "non-positive size malloc", NULL);

  if ((r=PyMem_Malloc(sz))) return r;

  PyErr_NoMemory();
  return NULL;
}

static void *
PyRealloc(void *p, size_t sz)
{
  void *r;

  ASSERT(sz > 0, "non-positive size realloc", NULL);

  if (p) r=PyMem_Realloc(p,sz);
  else r=PyMem_Malloc(sz);

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
#ifdef MULTI_INT_UNION
  {"multiunion", (PyCFunction) multiunion_m, METH_VARARGS,
   "multiunion(seq) -- compute union of a sequence of integer sets.\n"
   "\n"
   "Each element of seq must be an integer set, or convertible to one\n"
   "via the set iteration protocol.  The union returned is an IISet."
  },
#endif
  {NULL,		NULL}		/* sentinel */
};

static char BTree_module_documentation[] =
"\n"
MASTER_ID
BTREEITEMSTEMPLATE_C
"$Id: BTreeModuleTemplate.c,v 1.31 2002/06/10 04:57:43 tim_one Exp $\n"
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
INITMODULE (void)
{
  PyObject *m, *d, *c;

  UNLESS (sort_str=PyString_FromString("sort")) return;
  UNLESS (reverse_str=PyString_FromString("reverse")) return;
  UNLESS (items_str=PyString_FromString("items")) return;
  UNLESS (__setstate___str=PyString_FromString("__setstate__")) return;

  UNLESS (PyExtensionClassCAPI=PyCObject_Import("ExtensionClass","CAPI"))
      return;

#ifdef PERSISTENT
  if ((cPersistenceCAPI=PyCObject_Import("cPersistence","CAPI")))
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

  /* Grab the ConflictError class */

  m = PyImport_ImportModule("ZODB.POSException");

  if (m != NULL) {
  	c = PyObject_GetAttrString(m, "BTreesConflictError");
  	if (c != NULL)
  		ConflictError = c;
	Py_DECREF(m);
  }

  if (ConflictError == NULL) {
  	Py_INCREF(PyExc_ValueError);
	ConflictError=PyExc_ValueError;
  }

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

  PyExtensionClass_Export(d,MOD_NAME_PREFIX "Bucket", BucketType);
  PyExtensionClass_Export(d,MOD_NAME_PREFIX "BTree", BTreeType);
  PyExtensionClass_Export(d,MOD_NAME_PREFIX "Set", SetType);
  PyExtensionClass_Export(d,MOD_NAME_PREFIX "TreeSet", TreeSetType);
}
