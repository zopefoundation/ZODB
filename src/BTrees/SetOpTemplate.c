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

/****************************************************************************
 Set operations
 ****************************************************************************/

#define SETOPTEMPLATE_C "$Id: SetOpTemplate.c,v 1.13 2002/05/30 21:00:30 tim_one Exp $\n"

#ifdef INTSET_H
static int 
nextIntSet(SetIteration *i)
{    
  if (i->position >= 0)
    {
      UNLESS(PER_USE(INTSET(i->set))) return -1;

      if (i->position < INTSET(i->set)->len)
        {
          i->key = INTSET(i->set)->data[i->position];
          i->position ++;
        }
      else
        {
          i->position = -1;
          PER_ACCESSED(INTSET(i->set));
        }

      PER_ALLOW_DEACTIVATION(INTSET(i->set));
    }

          
  return 0;
}
#endif

#ifdef KEY_CHECK
static int 
nextKeyAsSet(SetIteration *i)
{
  if (i->position >= 0)
    {
      if (i->position < 1)
        {
          i->position ++;
        }
      else
        i->position = -1;
    }
  return 0;
}
#endif

static int
initSetIteration(SetIteration *i, PyObject *s, int w, int *merge)
{
  i->position=0;

  if (ExtensionClassSubclassInstance_Check(s, &BucketType))
    {
      i->set = s;
      Py_INCREF(s);

      if (w >= 0) 
        {
          *merge=1;
          i->next=nextBucket;
        }
      else
        i->next=nextSet;

      i->hasValue=1;
    }
  else if (ExtensionClassSubclassInstance_Check(s, &SetType))
    {
      i->set = s;
      Py_INCREF(s);

      i->next=nextSet;
      i->hasValue=0;
    }
  else if (ExtensionClassSubclassInstance_Check(s, &BTreeType))
    {
      i->set=BTree_rangeSearch(BTREE(s), NULL, 'i');
      UNLESS(i->set) return -1;

      if (w >= 0) 
        {
          *merge=1;
          i->next=nextBTreeItems;
        }
      else
        i->next=nextTreeSetItems;
      i->hasValue=1;
    }
  else if (ExtensionClassSubclassInstance_Check(s, &TreeSetType))
    {
      i->set=BTree_rangeSearch(BTREE(s), NULL, 'k');
      UNLESS(i->set) return -1;

      i->next=nextTreeSetItems;
      i->hasValue=0;
    }
#ifdef INTSET_H
  else if (s->ob_type==(PyTypeObject*)intSetType)
    {
      i->set = s;
      Py_INCREF(s);

      i->next=nextIntSet;
      i->hasValue=0;
    }
#endif
#ifdef KEY_CHECK
  else if (KEY_CHECK(s))
    {
      int copied=1;

      i->set = s;
      Py_INCREF(s);
      i->next=nextKeyAsSet;
      i->hasValue=0;
      COPY_KEY_FROM_ARG(i->key, s, copied);
      UNLESS (copied) return -1;
    }
#endif
  else
    {
      PyErr_SetString(PyExc_TypeError, "invalid argument");
      return -1;
    }

  return 0;
}

#ifndef MERGE_WEIGHT
#define MERGE_WEIGHT(O, w) (O)
#endif

static int 
copyRemaining(Bucket *r, SetIteration *i, int merge, int w)
{
  while (i->position >= 0)
    {
      if(r->len >= r->size && Bucket_grow(r, ! merge) < 0) return -1;
      COPY_KEY(r->keys[r->len], i->key);
      INCREF_KEY(r->keys[r->len]);

      if (merge)
        {
          COPY_VALUE(r->values[r->len], MERGE_WEIGHT(i->value, w));
          INCREF_VALUE(r->values[r->len]);
        }
      r->len++;
      if (i->next(i) < 0) return -1;
    }

  return 0;
}

static PyObject *
set_operation(PyObject *s1, PyObject *s2, 
              int w1, int w2,
              int c1, int c12, int c2)
{
  Bucket *r=0;
  SetIteration i1 = {0,0,0}, i2 = {0,0,0};
  int cmp, merge=0;

  if (initSetIteration(&i1, s1, w1, &merge) < 0) return NULL;
  if (initSetIteration(&i2, s2, w2, &merge) < 0) return NULL;

  if (merge)
    {
#ifndef MERGE
      if (c12 && i1.hasValue && i2.hasValue) goto invalid_set_operation;
#endif
      if (! i1.hasValue && i2.hasValue)
        {
          SetIteration t;
          int i;

          t=i1; i1=i2; i2=t;
          i=c1; c1=c2; c2=i;
          i=w1; w1=w2; w2=i;
        }
#ifdef MERGE_DEFAULT
      i1.value=MERGE_DEFAULT;
      i2.value=MERGE_DEFAULT;
#else
      if (i1.hasValue)
        {
          if (! i2.hasValue && c2) goto invalid_set_operation;
        }
      else
        {
          if (c1 || c12) goto invalid_set_operation;
        }
#endif

      UNLESS(r=BUCKET(PyObject_CallObject(OBJECT(&BucketType), NULL)))
        goto err;
    }
  else
    {
      UNLESS(r=BUCKET(PyObject_CallObject(OBJECT(&SetType), NULL)))
        goto err;
    }

  if (i1.next(&i1) < 0) return NULL;
  if (i2.next(&i2) < 0) return NULL;

  while (i1.position >= 0 && i2.position >= 0)
    {
      cmp=TEST_KEY(i1.key, i2.key);
      if(cmp < 0)
	{
	  if(c1)
	    {
	      if(r->len >= r->size && Bucket_grow(r, ! merge) < 0) goto err;
              COPY_KEY(r->keys[r->len], i1.key);
              INCREF_KEY(r->keys[r->len]);
              if (merge)
                {
                  COPY_VALUE(r->values[r->len], MERGE_WEIGHT(i1.value, w1));
                  INCREF_VALUE(r->values[r->len]);
                }
	      r->len++;
	    }
          if (i1.next(&i1) < 0) goto err;
	}
      else if(cmp==0)
	{
	  if(c12)
	    {
	      if(r->len >= r->size && Bucket_grow(r, ! merge) < 0) goto err;
              COPY_KEY(r->keys[r->len], i1.key);
              INCREF_KEY(r->keys[r->len]);
              if (merge)
                {
#ifdef MERGE
                  r->values[r->len] = MERGE(i1.value, w1, i2.value, w2);
#else
                  COPY_VALUE(r->values[r->len], i1.value);
                  INCREF_VALUE(r->values[r->len]);
#endif                    
                }
	      r->len++;
	    }
          if (i1.next(&i1) < 0) goto err;
          if (i2.next(&i2) < 0) goto err;
	}
      else
	{
	  if(c2)
	    {
	      if(r->len >= r->size && Bucket_grow(r, ! merge) < 0) goto err;
              COPY_KEY(r->keys[r->len], i2.key);
              INCREF_KEY(r->keys[r->len]);
              if (merge)
                {
                  COPY_VALUE(r->values[r->len], MERGE_WEIGHT(i2.value, w2));
                  INCREF_VALUE(r->values[r->len]);
                }
	      r->len++;
	    }
          if (i2.next(&i2) < 0) goto err;
	}
    }
  if(c1 && copyRemaining(r, &i1, merge, w1) < 0) goto err;
  if(c2 && copyRemaining(r, &i2, merge, w2) < 0) goto err;

  Py_DECREF(i1.set);
  Py_DECREF(i2.set);

  return OBJECT(r);

#ifndef MERGE_DEFAULT
invalid_set_operation:
  PyErr_SetString(PyExc_TypeError, "invalid set operation");
#endif

err:
  Py_XDECREF(i1.set);
  Py_XDECREF(i2.set);
  Py_XDECREF(r);
  return NULL;
}

static PyObject *
difference_m(PyObject *ignored, PyObject *args)
{
  PyObject *o1, *o2;

  UNLESS(PyArg_ParseTuple(args, "OO", &o1, &o2)) return NULL;


  if (o1==Py_None || o2==Py_None) 
    {
      Py_INCREF(Py_None);
      return Py_None;
    }
  
  return set_operation(o1, o2, 1, -1, 1, 0, 0);
}         

static PyObject *
union_m(PyObject *ignored, PyObject *args)
{
  PyObject *o1, *o2;

  UNLESS(PyArg_ParseTuple(args, "OO", &o1, &o2)) return NULL;

  if (o1==Py_None)
    {
      Py_INCREF(o2);
      return o2;
    }
  else if (o2==Py_None)
    {
      Py_INCREF(o1);
      return o1;
    }
  
  return set_operation(o1, o2, -1, -1, 1, 1, 1);
}         

static PyObject *
intersection_m(PyObject *ignored, PyObject *args)
{
  PyObject *o1, *o2;

  UNLESS(PyArg_ParseTuple(args, "OO", &o1, &o2)) return NULL;

  if (o1==Py_None)
    {
      Py_INCREF(o2);
      return o2;
    }
  else if (o2==Py_None)
    {
      Py_INCREF(o1);
      return o1;
    }
  
  return set_operation(o1, o2, -1, -1, 0, 1, 0);
}         

#ifdef MERGE

static PyObject *
wunion_m(PyObject *ignored, PyObject *args)
{
  PyObject *o1, *o2;
  int w1=1, w2=1;

  UNLESS(PyArg_ParseTuple(args, "OO|ii", &o1, &o2, &w1, &w2)) return NULL;

  if (o1==Py_None)
    return Py_BuildValue("iO", (o2==Py_None ? 0 : w2), o2);
  else if (o2==Py_None)
    return Py_BuildValue("iO", w1, o1);
  
  o1=set_operation(o1, o2, w1, w2, 1, 1, 1);
  if (o1) ASSIGN(o1, Py_BuildValue("iO", 1, o1));

  return o1;
}         

static PyObject *
wintersection_m(PyObject *ignored, PyObject *args)
{
  PyObject *o1, *o2;
  int w1=1, w2=1;

  UNLESS(PyArg_ParseTuple(args, "OO|ii", &o1, &o2, &w1, &w2)) return NULL;

  if (o1==Py_None)
    return Py_BuildValue("iO", (o2==Py_None ? 0 : w2), o2);
  else if (o2==Py_None)
    return Py_BuildValue("iO", w1, o1);
  
  o1=set_operation(o1, o2, w1, w2, 0, 1, 0);
  if (o1) 
    ASSIGN(o1, Py_BuildValue("iO", 
            ((o1->ob_type == (PyTypeObject*)(&SetType)) ? w2+w1 : 1),
                             o1));

  return o1;
}         

#endif

#ifdef MULTI_INT_UNION
#include "sorters.c"

/* Input is a sequence of integer sets (or convertible to sets by the
   set iteration protocol).  Output is the union of the sets.  The point
   is to run much faster than doing pairs of unions.
*/
static PyObject *
multiunion_m(PyObject *ignored, PyObject *args)
{
    PyObject *seq;          /* input sequence */
    int n;                  /* length of input sequence */
    PyObject *set = NULL;   /* an element of the input sequence */
    Bucket *result;         /* result set */
    int i;

    UNLESS(PyArg_ParseTuple(args, "O", &seq))
        return NULL;

    n = PyObject_Length(seq);
    if (n < 0)
        return NULL;

    /* Construct an empty result set. */
    result = BUCKET(PyObject_CallObject(OBJECT(&SetType), NULL));
    if (result == NULL)
        return NULL;

    /* For each set in the input sequence, append its elements to the result
       set.  At this point, we ignore the possibility of duplicates. */
    for (i = 0; i < n; ++i) {
        SetIteration setiter = {0, 0, 0};
        int merge;  /* dummy needed for initSetIteration */

        set = PySequence_GetItem(seq, i);
        if (set == NULL)
            goto Error;

        /* XXX TODO: If set is a bucket, do a straight resize+memcpy instead.
        */
        if (initSetIteration(&setiter, set, 1, &merge) < 0)
            goto Error;
        if (setiter.next(&setiter) < 0)
            goto Error;
        while (setiter.position >= 0) {
            if (result->len >= result->size && Bucket_grow(result, 1) < 0)
                goto Error;
            COPY_KEY(result->keys[result->len], setiter.key);
            ++result->len;
            /* We know the key is an int, so no need to incref it. */
            if (setiter.next(&setiter) < 0)
                goto Error;
        }
        Py_DECREF(set);
        set = NULL;
    }

    /* Combine, sort, remove duplicates, and reset the result's len.
       If the set shrinks (which happens if and only if there are
       duplicates), no point to realloc'ing the set smaller, as we
       expect the result set to be short-lived.
    */
    if (result->len > 0) {
        size_t newlen;          /* number of elements in final result set */
        newlen = sort_int4_nodups(result->keys, (size_t)result->len);
        result->len = (int)newlen;
    }
    return (PyObject *)result;

Error:
    Py_DECREF(result);
    Py_XDECREF(set);
    return NULL;
}

#endif
