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

#include "Python.h"
/* include structmember.h for offsetof */
#include "structmember.h"

#ifdef PERSISTENT
#include "cPersistence.h"
#else
#define PER_USE_OR_RETURN(self, NULL)
#define PER_ALLOW_DEACTIVATION(self)
#define PER_PREVENT_DEACTIVATION(self)
#define PER_DEL(self)
#define PER_USE(O) 1
#define PER_ACCESSED(O) 1
#endif

/* So sue me.  This pair gets used all over the place, so much so that it
 * interferes with understanding non-persistence parts of algorithms.
 * PER_UNUSE can be used after a successul PER_USE or PER_USE_OR_RETURN.
 * It allows the object to become ghostified, and tells the persistence
 * machinery that the object's fields were used recently.
 */
#define PER_UNUSE(OBJ) do {             \
    PER_ALLOW_DEACTIVATION(OBJ);        \
    PER_ACCESSED(OBJ);                  \
} while (0)

/*
  The tp_name slots of the various BTree types contain the fully
  qualified names of the types, e.g. zodb.btrees.OOBTree.OOBTree.
  The full name is usd to support pickling and because it is not
  possible to modify the __module__ slot of a type dynamically.  (This
  may be a bug in Python 2.2).
*/

#define MODULE_NAME "BTrees._" MOD_NAME_PREFIX "BTree."

static PyObject *sort_str, *reverse_str, *__setstate___str,
    *_bucket_type_str;
static PyObject *ConflictError = NULL;

static void PyVar_Assign(PyObject **v, PyObject *e) { Py_XDECREF(*v); *v=e;}
#define ASSIGN(V,E) PyVar_Assign(&(V),(E))
#define UNLESS(E) if (!(E))
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

static PyTypeObject BTreeType;
static PyTypeObject BucketType;

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
 * 1. Declare a new iterator:
 *        SetIteration si = {0,0,0};
 *    Using "{0,0,0}" or "{0,0}" appear most common.  Only one {0} is
 *    necssary.  At least one must be given so that finiSetIteration() works
 *    correctly even if you don't get around to calling initSetIteration().
 * 2. Initialize it via
 *        initSetIteration(&si, PyObject *s, useValues)
 *    It's an error if that returns an int < 0.  In case of error on the
 *    init call, calling finiSetIteration(&si) is optional.  But if the
 *    init call succeeds, you must eventually call finiSetIteration(),
 *    and whether or not subsequent calls to si.next() fail.
 * 3. Get the first element:
 *        if (si.next(&si) < 0) { there was an error }
 *    If the set isn't empty, this sets si.position to an int >= 0,
 *    si.key to the element's key (of type KEY_TYPE), and maybe si.value to
 *    the element's value (of type VALUE_TYPE).  si.value is defined
 *    iff si.usesValue is true.
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

    v = PyInt_FromLong(i);
    if (!v) {
	v = Py_None;
	Py_INCREF(v);
    }
    PyErr_SetObject(PyExc_IndexError, v);
    Py_DECREF(v);
    return NULL;
}

/* Search for the bucket immediately preceding *current, in the bucket chain
 * starting at first.  current, *current and first must not be NULL.
 *
 * Return:
 *     1    *current holds the correct bucket; this is a borrowed reference
 *     0    no such bucket exists; *current unaltered
 *    -1    error; *current unaltered
 */
static int
PreviousBucket(Bucket **current, Bucket *first)
{
    Bucket *trailing = NULL;    /* first travels; trailing follows it */
    int result = 0;

    assert(current && *current && first);
    if (first == *current)
        return 0;

    do {
        trailing = first;
	PER_USE_OR_RETURN(first, -1);
        first = first->next;
	PER_UNUSE(trailing);

	if (first == *current) {
	    *current = trailing;
	    result = 1;
	    break;
	}
    } while (first);

    return result;
}

static void *
BTree_Malloc(size_t sz)
{
    void *r;

    ASSERT(sz > 0, "non-positive size malloc", NULL);

    r = malloc(sz);
    if (r)
	return r;

    PyErr_NoMemory();
    return NULL;
}

static void *
BTree_Realloc(void *p, size_t sz)
{
    void *r;

    ASSERT(sz > 0, "non-positive size realloc", NULL);

    if (p)
	r = realloc(p, sz);
    else
	r = malloc(sz);

    UNLESS (r)
	PyErr_NoMemory();

    return r;
}

/* Shared keyword-argument list for BTree/Bucket
 * (iter)?(keys|values|items)
 */
static char *search_keywords[] = {"min", "max",
				  "excludemin", "excludemax",
				  0};

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
"$Id: BTreeModuleTemplate.c,v 1.39 2004/01/14 19:16:46 jeremy Exp $\n"
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

int
init_persist_type(PyTypeObject *type)
{
    type->ob_type = &PyType_Type;
    type->tp_base = cPersistenceCAPI->pertype;

    if (PyType_Ready(type) < 0)
	return 0;

    return 1;
}

void
INITMODULE (void)
{
    PyObject *m, *d, *c;

    sort_str = PyString_InternFromString("sort");
    if (!sort_str)
	return;
    reverse_str = PyString_InternFromString("reverse");
    if (!reverse_str)
	return;
    __setstate___str = PyString_InternFromString("__setstate__");
    if (!__setstate___str)
	return;
    _bucket_type_str = PyString_InternFromString("_bucket_type");
    if (!_bucket_type_str)
	return;

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

    /* Initialize the PyPersist_C_API and the type objects. */
    cPersistenceCAPI = PyCObject_Import("persistent.cPersistence", "CAPI");
    if (cPersistenceCAPI == NULL)
	return;

    BTreeItemsType.ob_type = &PyType_Type;
    BTreeIter_Type.ob_type = &PyType_Type;
    BTreeIter_Type.tp_getattro = PyObject_GenericGetAttr;
    BucketType.tp_new = PyType_GenericNew;
    SetType.tp_new = PyType_GenericNew;
    BTreeType.tp_new = PyType_GenericNew;
    TreeSetType.tp_new = PyType_GenericNew;
    if (!init_persist_type(&BucketType))
	return;
    if (!init_persist_type(&BTreeType))
	return;
    if (!init_persist_type(&SetType))
	return;
    if (!init_persist_type(&TreeSetType))
	return;

    if (PyDict_SetItem(BTreeType.tp_dict, _bucket_type_str,
		       (PyObject *)&BucketType) < 0) {
	fprintf(stderr, "btree failed\n");
	return;
    }
    if (PyDict_SetItem(TreeSetType.tp_dict, _bucket_type_str,
		       (PyObject *)&SetType) < 0) {
	fprintf(stderr, "bucket failed\n");
	return;
    }

    /* Create the module and add the functions */
    m = Py_InitModule4("_" MOD_NAME_PREFIX "BTree",
		       module_methods, BTree_module_documentation,
		       (PyObject *)NULL, PYTHON_API_VERSION);

    /* Add some symbolic constants to the module */
    d = PyModule_GetDict(m);
    if (PyDict_SetItemString(d, MOD_NAME_PREFIX "Bucket",
			     (PyObject *)&BucketType) < 0)
	return;
    if (PyDict_SetItemString(d, MOD_NAME_PREFIX "BTree",
			     (PyObject *)&BTreeType) < 0)
	return;
    if (PyDict_SetItemString(d, MOD_NAME_PREFIX "Set",
			     (PyObject *)&SetType) < 0)
	return;
    if (PyDict_SetItemString(d, MOD_NAME_PREFIX "TreeSet",
			     (PyObject *)&TreeSetType) < 0)
	return;
}
