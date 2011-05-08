/*****************************************************************************

  Copyright (c) 2001, 2002 Zope Foundation and Contributors.
  All Rights Reserved.

  This software is subject to the provisions of the Zope Public License,
  Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
  THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
  WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
  FOR A PARTICULAR PURPOSE

****************************************************************************/

#define MERGETEMPLATE_C "$Id$\n"

/****************************************************************************
 Set operations
****************************************************************************/

static int
merge_output(Bucket *r, SetIteration *i, int mapping)
{
  if (r->len >= r->size && Bucket_grow(r, -1, !mapping) < 0)
    return -1;
  COPY_KEY(r->keys[r->len], i->key);
  INCREF_KEY(r->keys[r->len]);
  if (mapping) {
    COPY_VALUE(r->values[r->len], i->value);
    INCREF_VALUE(r->values[r->len]);
  }
  r->len++;
  return 0;
}

/* The "reason" argument is a little integer giving "a reason" for the
 * error.  In the Zope3 codebase, these are mapped to explanatory strings
 * via zodb/btrees/interfaces.py.
 */
static PyObject *
merge_error(int p1, int p2, int p3, int reason)
{
  PyObject *r;

  UNLESS (r=Py_BuildValue("iiii", p1, p2, p3, reason)) r=Py_None;
  if (ConflictError == NULL) {
  	ConflictError = PyExc_ValueError;
    Py_INCREF(ConflictError);
  }
  PyErr_SetObject(ConflictError, r);
  if (r != Py_None)
    {
      Py_DECREF(r);
    }

  return NULL;
}

/* It's hard to explain "the rules" for bucket_merge, in large part because
 * any automatic conflict-resolution scheme is going to be incorrect for
 * some endcases of *some* app.  The scheme here is pretty conservative,
 * and should be OK for most apps.  It's easier to explain what the code
 * allows than what it forbids:
 *
 * Leaving things alone:  it's OK if both s2 and s3 leave a piece of s1
 * alone (don't delete the key, and don't change the value).
 *
 * Key deletion:  a transaction (s2 or s3) can delete a key (from s1), but
 * only if the other transaction (of s2 and s3) doesn't delete the same key.
 * However, it's not OK for s2 and s3 to, between them, end up deleting all
 * the keys.  This is a higher-level constraint, due to that the caller of
 * bucket_merge() doesn't have enough info to unlink the resulting empty
 * bucket from its BTree correctly.  It's also not OK if s2 or s3 are empty,
 * because the transaction that emptied the bucket unlinked the bucket from
 * the tree, and nothing we do here can get it linked back in again.
 *
 * Key insertion:  s2 or s3 can add a new key, provided the other transaction
 * doesn't insert the same key.  It's not OK even if they insert the same
 * <key, value> pair.
 *
 * Mapping value modification:  s2 or s3 can modify the value associated
 * with a key in s1, provided the other transaction doesn't make a
 * modification of the same key to a different value.  It's OK if s2 and s3
 * both give the same new value to the key while it's hard to be precise about
 * why, this doesn't seem consistent with that it's *not* OK for both to add
 * a new key mapping to the same value).
 */
static PyObject *
bucket_merge(Bucket *s1, Bucket *s2, Bucket *s3)
{
  Bucket *r=0;
  PyObject *s;
  SetIteration i1 = {0,0,0}, i2 = {0,0,0}, i3 = {0,0,0};
  int cmp12, cmp13, cmp23, mapping, set;

  /* If either "after" bucket is empty, punt. */
  if (s2->len == 0 || s3->len == 0)
    {
      merge_error(-1, -1, -1, 12);
      goto err;
    }

  if (initSetIteration(&i1, OBJECT(s1), 1) < 0)
    goto err;
  if (initSetIteration(&i2, OBJECT(s2), 1) < 0)
    goto err;
  if (initSetIteration(&i3, OBJECT(s3), 1) < 0)
    goto err;

  mapping = i1.usesValue | i2.usesValue | i3.usesValue;
  set = !mapping;

  if (mapping)
    r = (Bucket *)PyObject_CallObject((PyObject *)&BucketType, NULL);
  else
    r = (Bucket *)PyObject_CallObject((PyObject *)&SetType, NULL);
  if (r == NULL)
    goto err;

  if (i1.next(&i1) < 0)
    goto err;
  if (i2.next(&i2) < 0)
    goto err;
  if (i3.next(&i3) < 0)
    goto err;

  /* Consult zodb/btrees/interfaces.py for the meaning of the last
   * argument passed to merge_error().
   */
  /* TODO:  This isn't passing on errors raised by value comparisons. */
  while (i1.position >= 0 && i2.position >= 0 && i3.position >= 0)
    {
      TEST_KEY_SET_OR(cmp12, i1.key, i2.key) goto err;
      TEST_KEY_SET_OR(cmp13, i1.key, i3.key) goto err;
      if (cmp12==0)
        {
          if (cmp13==0)
            {
              if (set || (TEST_VALUE(i1.value, i2.value) == 0))
                {               /* change in i3 value or all same */
                  if (merge_output(r, &i3, mapping) < 0) goto err;
                }
              else if (set || (TEST_VALUE(i1.value, i3.value) == 0))
                {               /* change in i2 value */
                  if (merge_output(r, &i2, mapping) < 0) goto err;
                }
              else
                {               /* conflicting value changes in i2 and i3 */
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
            {                   /* deleted in i3 */
              if (i3.position == 1)
                {
                  /* Deleted the first item.  This will modify the
                     parent node, so we don't know if merging will be
                     safe
                  */
                  merge_error(i1.position, i2.position, i3.position, 13);
                  goto err;
                }
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
            {                   /* deleted in i2 */
              if (i2.position == 1)
                {
                  /* Deleted the first item.  This will modify the
                     parent node, so we don't know if merging will be
                     safe
                  */
                  merge_error(i1.position, i2.position, i3.position, 13);
                  goto err;
                }
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
          TEST_KEY_SET_OR(cmp23, i2.key, i3.key) goto err;
          if (cmp23==0)
            {                   /* dueling inserts or deletes */
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
            {                   /* 1<2 and 1<3:  both deleted 1.key */
              merge_error(i1.position, i2.position, i3.position, 5);
              goto err;
            }
        }
    }

  while (i2.position >= 0 && i3.position >= 0)
    {                           /* New inserts */
      TEST_KEY_SET_OR(cmp23, i2.key, i3.key) goto err;
      if (cmp23==0)
        {                       /* dueling inserts */
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
    {                           /* remainder of i1 deleted in i3 */
      TEST_KEY_SET_OR(cmp12, i1.key, i2.key) goto err;
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
        {                       /* Dueling deletes or delete and change */
          merge_error(i1.position, i2.position, i3.position, 7);
          goto err;
        }
    }

  while (i1.position >= 0 && i3.position >= 0)
    {                           /* remainder of i1 deleted in i2 */
      TEST_KEY_SET_OR(cmp13, i1.key, i3.key) goto err;
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
        {                       /* Dueling deletes or delete and change */
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
    {                           /* Inserting i3 at end */
      if (merge_output(r, &i3, mapping) < 0) goto err;
      if (i3.next(&i3) < 0) goto err;
    }

  /* If the output bucket is empty, conflict resolution doesn't have
   * enough info to unlink it from its containing BTree correctly.
   */
  if (r->len == 0)
    {
      merge_error(-1, -1, -1, 10);
      goto err;
    }

  finiSetIteration(&i1);
  finiSetIteration(&i2);
  finiSetIteration(&i3);

  if (s1->next)
    {
      Py_INCREF(s1->next);
      r->next = s1->next;
    }
  s = bucket_getstate(r);
  Py_DECREF(r);

  return s;

 err:
  finiSetIteration(&i1);
  finiSetIteration(&i2);
  finiSetIteration(&i3);
  Py_XDECREF(r);
  return NULL;
}
