/*****************************************************************************

  Copyright (c) 2001 Zope Corporation and Contributors. All Rights Reserved.
  
  This software is subject to the provisions of the Zope Public License,
  Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
  THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
  WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
  FOR A PARTICULAR PURPOSE
  
 ****************************************************************************/

#define MERGETEMPLATE_C "$Id: MergeTemplate.c,v 1.8 2001/11/28 15:50:54 matt Exp $\n"

/****************************************************************************
 Set operations
 ****************************************************************************/

static int
merge_output(Bucket *r, SetIteration *i, int mapping)
{
  if(r->len >= r->size && Bucket_grow(r, ! mapping) < 0) return -1;
  COPY_KEY(r->keys[r->len], i->key);
  INCREF_KEY(r->keys[r->len]);
  if (mapping)
    {
      COPY_VALUE(r->values[r->len], i->value);
      INCREF_VALUE(r->values[r->len]);
    }
  r->len++;
  return 0;
}

static PyObject *
merge_error(int p1, int p2, int p3, int reason)
{
  PyObject *r;

  UNLESS (r=Py_BuildValue("iiii", p1, p2, p3, reason)) r=Py_None;
  if (ConflictError == NULL) {
  	ConflictError=PyExc_ValueError;
	Py_INCREF(ConflictError);
  }
  PyErr_SetObject(ConflictError, r);
  if (r != Py_None) 
    {
      Py_DECREF(r);
    }

  return NULL;
}

static PyObject *
bucket_merge(Bucket *s1, Bucket *s2, Bucket *s3)
{
  Bucket *r=0;
  PyObject *s;
  SetIteration i1 = {0,0,0}, i2 = {0,0,0}, i3 = {0,0,0};
  int cmp12, cmp13, cmp23, mapping=0, set;

  if (initSetIteration(&i1, OBJECT(s1), 0, &mapping) < 0) return NULL;
  if (initSetIteration(&i2, OBJECT(s2), 0, &mapping) < 0) return NULL;
  if (initSetIteration(&i3, OBJECT(s3), 0, &mapping) < 0) return NULL;

  set = ! mapping;

  if (mapping)
    {
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
  if (i3.next(&i3) < 0) return NULL;

  while (i1.position >= 0 && i2.position >= 0 && i3.position >= 0)
    {
      cmp12=TEST_KEY(i1.key, i2.key);
      cmp13=TEST_KEY(i1.key, i3.key);
      if (cmp12==0)
        {
          if (cmp13==0)
            {
              if (set || (TEST_VALUE(i1.value, i2.value) == 0))
                {               /* change in i3 or all same */
                  if (merge_output(r, &i3, mapping) < 0) goto err;
                }
              else if (set || (TEST_VALUE(i1.value, i3.value) == 0))
                {               /* change in i2 */
                  if (merge_output(r, &i2, mapping) < 0) goto err;
                }
              else
                {               /* conflicting changes in i2 and i3 */
                  merge_error(i1.position, i2.position, i3.position, 1);
                  goto err;
                }
              if (i1.next(&i1) < 0) goto err;
              if (i2.next(&i2) < 0) goto err;
              if (i3.next(&i3) < 0) goto err;
            }
          else if (cmp13 > 0)
            {                   /* insert i3 */
              if (merge_output(r, &i3, mapping) < 0) goto err;
              if (i3.next(&i3) < 0) goto err;
            }
          else if (set || (TEST_VALUE(i1.value, i2.value) == 0))
            {                   /* delete i3 */
              if (i1.next(&i1) < 0) goto err;
              if (i2.next(&i2) < 0) goto err;
            }
          else
            {                   /* conflicting del in i3 and change in i2 */
              merge_error(i1.position, i2.position, i3.position, 2);
              goto err;
            }
        }
      else if (cmp13 == 0)
        {
          if (cmp12 > 0)
            {                   /* insert i2 */
              if (merge_output(r, &i2, mapping) < 0) goto err;
              if (i2.next(&i2) < 0) goto err;
            }
          else if (set || (TEST_VALUE(i1.value, i3.value) == 0))
            {                   /* delete i2 */
              if (i1.next(&i1) < 0) goto err;
              if (i3.next(&i3) < 0) goto err;
            }
          else
            {                   /* conflicting del in i2 and change in i3 */
              merge_error(i1.position, i2.position, i3.position, 3);
              goto err;
            }
        }
      else
        {                       /* Both keys changed */
          cmp23=TEST_KEY(i2.key, i3.key);
          if (cmp23==0)
            {                   /* dualing inserts or deletes */
              merge_error(i1.position, i2.position, i3.position, 4);
              goto err;
            }
          if (cmp12 > 0)
            {                   /* insert i2 */
              if (cmp23 > 0)
                {               /* insert i3 first */
                  if (merge_output(r, &i3, mapping) < 0) goto err;
                  if (i3.next(&i3) < 0) goto err;
                }
              else
                {               /* insert i2 first */
                  if (merge_output(r, &i2, mapping) < 0) goto err;
                  if (i2.next(&i2) < 0) goto err;
                }
            }
          else if (cmp13 > 0)
            {                   /* Insert i3 */
              if (merge_output(r, &i3, mapping) < 0) goto err;
              if (i3.next(&i3) < 0) goto err;
            }
          else
            {                   /* Dueling deletes */
              merge_error(i1.position, i2.position, i3.position, 5);
              goto err;
            }
        }
    }

  while (i2.position >= 0 && i3.position >= 0)
    {                           /* New inserts */
      cmp23=TEST_KEY(i2.key, i3.key);
      if (cmp23==0)
        {                       /* dualing inserts */
          merge_error(i1.position, i2.position, i3.position, 6);
          goto err;
        }
      if (cmp23 > 0)
        {                       /* insert i3 */
          if (merge_output(r, &i3, mapping) < 0) goto err;
          if (i3.next(&i3) < 0) goto err;
        }
      else
        {                       /* insert i2 */
          if (merge_output(r, &i2, mapping) < 0) goto err;
          if (i2.next(&i2) < 0) goto err;
        }
    }

  while (i1.position >= 0 && i2.position >= 0)
    {                           /* deleting i3 */
      cmp12=TEST_KEY(i1.key, i2.key);
      if (cmp12 > 0)
        {                       /* insert i2 */
          if (merge_output(r, &i2, mapping) < 0) goto err;
          if (i2.next(&i2) < 0) goto err;
        }
      else if (cmp12==0 && (set || (TEST_VALUE(i1.value, i2.value) == 0)))
        {                       /* delete i3 */
          if (i1.next(&i1) < 0) goto err;
          if (i2.next(&i2) < 0) goto err;
        }
      else
        {                       /* Dualing deletes or delete and change */
          merge_error(i1.position, i2.position, i3.position, 7);
          goto err;
        }
    }

  while (i1.position >= 0 && i3.position >= 0)
    {                           /* deleting i2 */
      cmp13=TEST_KEY(i1.key, i3.key);
      if (cmp13 > 0)
        {                       /* insert i3 */
          if (merge_output(r, &i3, mapping) < 0) goto err;
          if (i3.next(&i3) < 0) goto err;
        }
      else if (cmp13==0 && (set || (TEST_VALUE(i1.value, i3.value) == 0)))
        {                       /* delete i2 */
          if (i1.next(&i1) < 0) goto err;
          if (i3.next(&i3) < 0) goto err;
        }
      else
        {                       /* Dualing deletes or delete and change */
          merge_error(i1.position, i2.position, i3.position, 8);
          goto err;
        }
    }

  if (i1.position >= 0)
    {                           /* Dueling deletes */
      merge_error(i1.position, i2.position, i3.position, 9);
      goto err;
    }

  while (i2.position >= 0)
    {                           /* Inserting i2 at end */
      if (merge_output(r, &i2, mapping) < 0) goto err;
      if (i2.next(&i2) < 0) goto err;
    }

  while (i3.position >= 0)
    {                           /* Inserting i2 at end */
      if (merge_output(r, &i3, mapping) < 0) goto err;
      if (i3.next(&i3) < 0) goto err;
    }
  
  Py_DECREF(i1.set);
  Py_DECREF(i2.set);
  Py_DECREF(i3.set);

  if (s1->next)
    {
      Py_INCREF(s1->next);
      r->next = s1->next;
    }
  s=bucket_getstate(r, NULL);
  Py_DECREF(r);

  return s;

 err:
  Py_XDECREF(i1.set);
  Py_XDECREF(i2.set);
  Py_XDECREF(i3.set);
  Py_XDECREF(r);
  return NULL;
}
