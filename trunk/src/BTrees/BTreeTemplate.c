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

#define BTREETEMPLATE_C "$Id: BTreeTemplate.c,v 1.54 2002/06/14 14:39:07 jeremy Exp $\n"

/*
** _BTree_get
**
** Search a BTree.
**
** Arguments
**      self        a pointer to a BTree
**      keyarg      the key to search for, as a Python object
**      has_key     true/false; when false, try to return the associated
**                  value; when true, return a boolean
** Return
**      When has_key false:
**          If key exists, its associated value.
**          If key doesn't exist, NULL and KeyError is set.
**      When has_key true:
**          A Python int is returned in any case.
**          If key exists, the depth of the bucket in which it was found.
**          If key doesn't exist, 0.
*/
static PyObject *
_BTree_get(BTree *self, PyObject *keyarg, int has_key)
{
  KEY_TYPE key;
  int min;              /* index of child to search */
  PyObject *r = NULL;   /* result object */
  int copied = 1;

  COPY_KEY_FROM_ARG(key, keyarg, copied);
  UNLESS (copied) return NULL;

  PER_USE_OR_RETURN(self, NULL);

  BTREE_SEARCH(min, self, key, goto Error);
  if (self->len)
    {
      if (SameType_Check(self, self->data[min].child))
        r = _BTree_get(BTREE(self->data[min].child), keyarg,
                        has_key ? has_key + 1: 0);
      else
        r = _bucket_get(BUCKET(self->data[min].child), keyarg,
                        has_key ? has_key + 1: 0);
    }
  else
    {  /* No data */
      if (has_key)
        r = PyInt_FromLong(0);
      else
        PyErr_SetObject(PyExc_KeyError, keyarg);
    }

Error:
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  return r;
}

static PyObject *
BTree_get(BTree *self, PyObject *key)
{
  return _BTree_get(self, key, 0);
}

/*
  Copy data from the current BTree to the newly created BTree, next.
  Reset length to reflect the fact that we've given up some data.
*/
static int
BTree_split(BTree *self, int index, BTree *next)
{
  int next_size;
  ASSERT(self->len > 1, "split of empty tree", -1);

  if (index < 0 || index >= self->len) index=self->len/2;

  next_size=self->len-index;
  ASSERT(next_size > 0, "split creates empty tree", -1);

  UNLESS (next->data=PyMalloc(sizeof(BTreeItem)*next_size)) return -1;
  memcpy(next->data, self->data+index, sizeof(BTreeItem)*next_size);
  next->size=next->len=next_size;

  self->len = index;

  if (SameType_Check(self, next->data->child))
    {
      PER_USE_OR_RETURN(BTREE(next->data->child), -1);
      next->firstbucket = BTREE(next->data->child)->firstbucket;
      Py_XINCREF(next->firstbucket);
      PER_ALLOW_DEACTIVATION(BTREE(next->data->child));
      PER_ACCESSED(BTREE(next->data->child));
    }
  else
    {
      next->firstbucket = BUCKET(next->data->child);
      Py_XINCREF(next->firstbucket);
    }

  if (PER_CHANGED(self) < 0)
    return -1;
  return 0;
}

/* Split out data among two newly created BTrees, which become
   out children.
*/
static int
BTree_clone(BTree *self)
{
  /* We've grown really big without anybody splitting us.
     We should split ourselves.
   */
  BTree *n1=0, *n2=0;
  BTreeItem *d=0;

  /* Create two BTrees to hold ourselves after split */
  UNLESS (n1=BTREE(PyObject_CallObject(OBJECT(self->ob_type), NULL)))
    return -1;
  UNLESS (n2=BTREE(PyObject_CallObject(OBJECT(self->ob_type), NULL)))
    goto err;

  /* Create a new data buffer to hold two BTrees */
  UNLESS (d=PyMalloc(sizeof(BTreeItem)*2)) goto err;

  /* Split ourself */
  if (BTree_split(self,-1,n2) < 0) goto err;

  /* Move our data to new BTree */
  n1->size=self->size;
  n1->len=self->len;
  n1->data=self->data;
  n1->firstbucket = self->firstbucket;
  Py_XINCREF(n1->firstbucket);

  /* Initialize our data to hold split data */
  self->data = d;
  self->len = 2;
  self->size = 2;
  self->data->child = SIZED(n1);
  COPY_KEY(self->data[1].key, n2->data->key);

  /* We take the unused reference from n2, so there's no reason to INCREF! */
  /* INCREF_KEY(self->data[1].key); */

  self->data[1].child = SIZED(n2);

  return 0;

err:
  Py_XDECREF(n1);
  Py_XDECREF(n2);
  if (d) free(d);
  return -1;
}

/*
** BTree_grow
**
** Grow a BTree
**
** Arguments:	self	The BTree
**		index	the index item to insert at
**
** Returns:	 0	on success
**		-1	on failure
*/
static int
BTree_grow(BTree *self, int index, int noval)
{
  int i;
  Sized *v, *e=0;
  BTreeItem *d;

  if (self->len == self->size)
    {
      if (self->size)
        {
          UNLESS (d=PyRealloc(self->data, sizeof(BTreeItem)*self->size*2))
            return -1;
          self->data=d;
          self->size *= 2;
        }
      else
        {
          UNLESS (d=PyMalloc(sizeof(BTreeItem)*2))
            return -1;
          self->data=d;
          self->size = 2;
        }
    }

  d=self->data+index;
  if (self->len)
    {
      v = d->child;
      /* Create a new object of the same type as the target value */
      e = SIZED(PyObject_CallObject(OBJECT(v->ob_type), NULL));
      UNLESS (e) return -1;

      UNLESS(PER_USE(v))
        {
          Py_DECREF(e);
          return -1;
        }

      /* Now split between the original (v) and the new (e) at the midpoint*/
      if (SameType_Check(self, v))
        {
          i=BTree_split(BTREE(v), -1,   BTREE(e));
        }
      else
        {
          i=bucket_split(BUCKET(v), -1, BUCKET(e));
        }
      PER_ALLOW_DEACTIVATION(v);

      if (i < 0)
        {
          Py_DECREF(e);
          return -1;
        }

      index++;
      d++;
      if (self->len > index)	/* Shift up the old values one array slot */
        memmove(d+1, d, sizeof(BTreeItem)*(self->len-index));

      if (SameType_Check(self, v))
        {
          COPY_KEY(d->key, BTREE(e)->data->key);

          /* We take the unused reference from e, so there's no
             reason to INCREF!
          */
          /* INCREF_KEY(self->data[1].key); */
        }
      else
        {
          COPY_KEY(d->key, BUCKET(e)->keys[0]);
          INCREF_KEY(d->key);
        }
      d->child=e;

      self->len++;

      if (self->len >= MAX_BTREE_SIZE(self) * 2) return BTree_clone(self);
    }
  else
    {
      if (noval)
        {
          d->child = SIZED(PyObject_CallObject(OBJECT(&SetType), NULL));
          UNLESS (d->child) return -1;
        }
      else
        {
          d->child = SIZED(PyObject_CallObject(OBJECT(&BucketType), NULL));
          UNLESS (d->child) return -1;
        }
      self->len=1;
      Py_INCREF(d->child);
      self->firstbucket = BUCKET(d->child);
    }

  return 0;
}

/* Return the rightmost bucket reachable from following child pointers
 * from self.  The caller gets a new reference to this bucket.  Note that
 * bucket 'next' pointers are not followed:  if self is an interior node
 * of a BTree, this returns the rightmost bucket in that node's subtree.
 * In case of error, returns NULL.
 *
 * self must not be a ghost; this isn't checked.  The result may be a ghost.
 *
 * Pragmatics:  Note that the rightmost bucket's last key is the largest
 * key in self's subtree.
 */
static Bucket *
BTree_lastBucket(BTree *self)
{
    Sized *pchild;
    Bucket *result;

    UNLESS (self->data && self->len) {
        IndexError(-1); /*XXX*/
        return NULL;
    }

    pchild = self->data[self->len - 1].child;
    if (SameType_Check(self, pchild)) {
        self = BTREE(pchild);
        PER_USE_OR_RETURN(self, NULL);
        result = BTree_lastBucket(self);
        PER_ALLOW_DEACTIVATION(self);
        PER_ACCESSED(self);
    }
    else {
        Py_INCREF(pchild);
        result = BUCKET(pchild);
    }
    return result;
}

static int
BTree_deleteNextBucket(BTree *self)
{
  Bucket *b;

  PER_USE_OR_RETURN(self, -1);

  UNLESS (b=BTree_lastBucket(self)) goto err;
  if (Bucket_deleteNextBucket(b) < 0) goto err;

  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);

  return 0;

 err:
  PER_ALLOW_DEACTIVATION(self);
  return -1;
}

/*
  Set (value != 0) or delete (value=0) a tree item.

  If unique is non-zero, then only change if the key is
  new.

  If noval is non-zero, then don't set a value (the tree
  is a set).

  Return 1 on successful change, 0 is no change, -1 on error.
*/
static int
_BTree_set(BTree *self, PyObject *keyarg, PyObject *value,
           int unique, int noval)
{
    int min, grew, copied=1, changed=0, bchanged=0;
    int childlength;
    BTreeItem *d;
    KEY_TYPE key;

    COPY_KEY_FROM_ARG(key, keyarg, copied);
    UNLESS (copied) return -1;

    PER_USE_OR_RETURN(self, -1);

    if (!self->len) {
	if (value) {
	    if (BTree_grow(self, 0, noval) < 0)
		goto err;
	}
	else {
	    PyErr_SetObject(PyExc_KeyError, keyarg);
	    goto err;
	}
    }

    BTREE_SEARCH(min, self, key, goto err);
    d = self->data + min;
    if (SameType_Check(self, d->child))
	grew = _BTree_set((BTree *)d->child, keyarg, value, unique, noval);
    else
	grew = _bucket_set((Bucket *)d->child, keyarg, value, unique, noval,
			   &bchanged);
    if (grew < 0)
	goto err;
    if (grew == 0)
        goto Done;

    /* A bucket changed size. */
    bchanged = 1;

    UNLESS(PER_USE(d->child))
        goto err;
    childlength = d->child->len;
    PER_ALLOW_DEACTIVATION(d->child);
    PER_ACCESSED(d->child);

    if (value) {
        /* A bucket got bigger -- if it's "too big", split it. */
        int toobig;

        if (SameType_Check(self, d->child))
            toobig = childlength > MAX_BTREE_SIZE(d->child);
        else
            toobig = childlength > MAX_BUCKET_SIZE(d->child);

        if (toobig) {
            if (BTree_grow(self, min, noval) < 0)
                goto err;
            changed = 1;
        }
        goto Done;
    }

    /* A bucket got smaller. */
    if (min && grew > 1) {
        /* Somebody below us deleted their first bucket and */
        /* an intermediate tree couldn't handle it. */
        if (BTree_deleteNextBucket(BTREE(d[-1].child)) < 0)
            goto err;
        grew = 1; /* Reset flag, since we handled it */
    }
    if (childlength > 0)
        goto Done;

    /* The child became empty. */
    if (!SameType_Check(self, d->child)) {
        /* We are about to delete a bucket. */
        if (min) {
            /* If it's not our first bucket, we can tell the
               previous bucket to adjust it's reference to it. */
            if (Bucket_deleteNextBucket(BUCKET(d[-1].child)) < 0)
                goto err;
        }
        else {
            /* If it's the first bucket, we can't adjust the
               reference to it ourselves, so we'll just
               increment the grew flag to indicate to a
               parent node that it's last bucket should
               adjust its reference. If there is no parent,
               then there's nothing to do. */
            grew++;
        }
    }
    self->len--;
    Py_DECREF(d->child);
    if (min) {
        DECREF_KEY(d->key);
    }
    if (min < self->len)
        memmove(d, d+1, (self->len-min)*sizeof(BTreeItem));
    if (!min) {
        if (self->len) {
            /* We just deleted our first child, so we need to
               adjust our first bucket. */
            if (SameType_Check(self, self->data->child)) {
                UNLESS (PER_USE(BTREE(self->data->child)))
                    goto err;
                ASSIGNB(self->firstbucket,
                        BTREE(self->data->child)->firstbucket);
                Py_XINCREF(self->firstbucket);
                PER_ALLOW_DEACTIVATION(BTREE(self->data->child));
                PER_ACCESSED(BTREE(self->data->child));
            }
            else {
                ASSIGNB(self->firstbucket, BUCKET(self->data->child));
                Py_INCREF(self->firstbucket);
            }
            /* We can toss our first key now */
            DECREF_KEY(self->data->key);
        }
        else {
            Py_XDECREF(self->firstbucket);
            self->firstbucket = 0;
        }
    }
    changed = 1;

Done:
#ifdef PERSISTENT
    if (changed
	|| (bchanged                                    /* The bucket changed */
	    && self->len == 1                            /* We have only one */
	    && ! SameType_Check(self, self->data->child) /* It's our child */
            && BUCKET(self->data->child)->oid == NULL      /* It's in our record*/
	    )
	)
	if (PER_CHANGED(self) < 0)
	    goto err;
#endif

    PER_ALLOW_DEACTIVATION(self);
    PER_ACCESSED(self);
    return grew;

err:
    PER_ALLOW_DEACTIVATION(self);
    PER_ACCESSED(self);
    return -1;
}

/*
** BTree_setitem
**
** wrapper for _BTree_set
**
** Arguments:	self	The BTree
**		key	The key to insert
**		v	The value to insert
**
** Returns	-1	on failure
**		 0	on success
*/
static int
BTree_setitem(BTree *self, PyObject *key, PyObject *v)
{
  if (_BTree_set(self, key, v, 0, 0) < 0) return -1;
  return 0;
}

/*
** _BTree_clear
**
** Clears out all of the values in the BTree
**
** Arguments:	self	The BTree
**
** Returns:	 0	on success
**		-1	on failure
*/
static int
_BTree_clear(BTree *self)
{
  int i, l;

  /* The order in which we dealocate, from "top to bottom" is critical
     to prevent memory memory errors when the deallocation stack
     becomes huge when dealocating use linked lists of buckets.
  */

  if (self->firstbucket)
    {
      ASSERT(self->firstbucket->ob_refcnt > 0,
             "Invalid firstbucket pointer", -1);
      Py_DECREF(self->firstbucket);
      self->firstbucket=NULL;
    }

  for (l=self->len, i=0; i < l; i++)
    {
      if (i)
        {
          DECREF_KEY(self->data[i].key);
        }
      Py_DECREF(self->data[i].child);
    }
  self->len=0;

  if (self->data)
    {
      free(self->data);
      self->data=0;
      self->size=0;
    }

  return 0;
}

#ifdef PERSISTENT
static PyObject *
BTree__p_deactivate(BTree *self, PyObject *args)
{
  if (self->state==cPersistent_UPTODATE_STATE && self->jar)
    {
      if (_BTree_clear(self) < 0) return NULL;
      PER_GHOSTIFY(self);
    }

  Py_INCREF(Py_None);
  return Py_None;
}
#endif

static PyObject *
BTree_clear(BTree *self, PyObject *args)
{
  PER_USE_OR_RETURN(self, NULL);

  if (self->len)
    {
      if (_BTree_clear(self) < 0) goto err;
      if (PER_CHANGED(self) < 0) goto err;
    }

  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);

  Py_INCREF(Py_None);
  return Py_None;

err:
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  return NULL;
}

/*
 * Return:
 *
 * For an empty BTree (self->len == 0), None.
 *
 * For a BTree with one child (self->len == 1), and that child is a bucket,
 * and that bucket has a NULL oid, a one-tuple containing a one-tuple
 * containing the bucket's state:
 *
 *     (
 *         (
 *              child[0].__getstate__(),
 *         ),
 *     )
 *
 * Else a two-tuple.  The first element is a tuple interleaving the BTree's
 * keys and direct children, of size 2*self->len - 1 (key[0] is unused and
 * is not saved).  The second element is the firstbucket:
 *
 *     (
 *          (child[0], key[1], child[1], key[2], child[2], ...,
 *                                       key[len-1], child[len-1]),
 *          self->firstbucket
 *     )
 *
 * In the above, key[i] means self->data[i].key, and similarly for child[i].
 */
static PyObject *
BTree_getstate(BTree *self, PyObject *args)
{
  PyObject *r=0, *o;
  int i, l;

  PER_USE_OR_RETURN(self, NULL);

  if (self->len)
    {
      UNLESS (r=PyTuple_New(self->len*2-1)) goto err;

      if (self->len == 1
          && self->data->child->ob_type != self->ob_type
#ifdef PERSISTENT
          && BUCKET(self->data->child)->oid == NULL
#endif
          )
        {
          /* We have just one bucket. Save its data directly. */
          UNLESS(o=bucket_getstate(BUCKET(self->data->child), NULL)) goto err;
          PyTuple_SET_ITEM(r,0,o);
          ASSIGN(r, Py_BuildValue("(O)", r));
        }
      else
        {
          for (i=0, l=0; i < self->len; i++)
            {
              if (i)
                {
                  COPY_KEY_TO_OBJECT(o, self->data[i].key);
                  PyTuple_SET_ITEM(r,l,o);
                  l++;
                }
              o = OBJECT(self->data[i].child);
              Py_INCREF(o);
              PyTuple_SET_ITEM(r,l,o);
              l++;
            }
          ASSIGN(r, Py_BuildValue("OO", r, self->firstbucket));
        }

    }
  else
    {
      r = Py_None;
      Py_INCREF(r);
    }

  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);

  return r;

err:
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  return NULL;
}

static int
_BTree_setstate(BTree *self, PyObject *state, int noval)
{
  PyObject *items, *firstbucket=0;
  BTreeItem *d;
  int len, l, i, copied=1;

  if (_BTree_clear(self) < 0) return -1;

  if (state != Py_None)
    {

      if (!PyArg_ParseTuple(state,"O|O",&items, &firstbucket))
        return -1;

      if ((len=PyTuple_Size(items)) < 0) return -1;
      len=(len+1)/2;

      if (len > self->size)
        {
          UNLESS (d=PyRealloc(self->data, sizeof(BTreeItem)*len)) return -1;
          self->data=d;
          self->size=len;
        }

      for (i=0, d=self->data, l=0; i < len; i++, d++)
        {
          if (i)
            {
              COPY_KEY_FROM_ARG(d->key, PyTuple_GET_ITEM(items,l), copied);
              l++;
              UNLESS (copied) return -1;
              INCREF_KEY(d->key);
            }
          d->child = SIZED(PyTuple_GET_ITEM(items,l));
          if (PyTuple_Check(d->child))
            {
              if (noval)
                {
                  d->child = SIZED(PyObject_CallObject(OBJECT(&SetType),
                                                       NULL));
                  UNLESS (d->child) return -1;
                  if (_set_setstate(BUCKET(d->child),
                                    PyTuple_GET_ITEM(items,l))
                      < 0) return -1;
                }
              else
                {
                  d->child = SIZED(PyObject_CallObject(OBJECT(&BucketType),
                                                       NULL));
                  UNLESS (d->child) return -1;
                  if (_bucket_setstate(BUCKET(d->child),
                                       PyTuple_GET_ITEM(items,l))
                      < 0) return -1;
                }
            }
          else
            {
              Py_INCREF(d->child);
            }
          l++;
        }

      if (len)
        {
          if (! firstbucket)
            firstbucket = OBJECT(self->data->child);

          UNLESS (ExtensionClassSubclassInstance_Check(
                    firstbucket,
                    noval ? &SetType : &BucketType))
            {
              PyErr_SetString(PyExc_TypeError,
                              "No firstbucket in non-empty BTree");
              return -1;
            }

          self->firstbucket = BUCKET(firstbucket);
          Py_INCREF(firstbucket);
        }

      self->len=len;
    }

  return 0;
}

static PyObject *
BTree_setstate(BTree *self, PyObject *args)
{
  int r;

  if (!PyArg_ParseTuple(args,"O",&args)) return NULL;

  PER_PREVENT_DEACTIVATION(self);
  r=_BTree_setstate(self, args, 0);
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);

  if (r < 0) return NULL;
  Py_INCREF(Py_None);
  return Py_None;
}

#ifdef PERSISTENT
static PyObject *
BTree__p_resolveConflict(BTree *self, PyObject *args)
{
  PyObject *s[3], *r;
  int i;

  r = NULL;

  UNLESS (PyArg_ParseTuple(args, "OOO", s, s+1, s+2)) goto err;

                                /* for each state, detuplefy it twice */
  for (i=0; i < 3; i++)
    UNLESS (s[i]==Py_None || PyArg_ParseTuple(s[i], "O", s+i)) goto err;
  for (i=0; i < 3; i++)
    UNLESS (s[i]==Py_None || PyArg_ParseTuple(s[i], "O", s+i)) goto err;

  for (i=0; i < 3; i++)         /* Now make sure detupled thing is a tuple */
    UNLESS (s[i]==Py_None || PyTuple_Check(s[i]))
      return merge_error(-100, -100, -100, -100);

  if (ExtensionClassSubclassInstance_Check(self, &BTreeType))
      r = _bucket__p_resolveConflict(OBJECT(&BucketType), s);
  else
      r = _bucket__p_resolveConflict(OBJECT(&SetType), s);

err:

  if (r) {
  	ASSIGN(r, Py_BuildValue("((O))", r));
  } else {
  	PyObject *error;
	PyObject *value;
	PyObject *traceback;
  	/* Change any errors to ConflictErrors */

	PyErr_Fetch(&error, &value, &traceback);
	Py_INCREF(ConflictError);
	Py_XDECREF(error);
	PyErr_Restore(ConflictError, value, traceback);
  }

  return r;
}
#endif

/*
 BTree_findRangeEnd -- Find one end, expressed as a bucket and
 position, for a range search.

 If low, return bucket and index of the smallest item >= key,
 otherwise return bucket and index of the largest item <= key.

 Return:
    -1      Error; offset and bucket unchanged
     0      Not found; offset and bucket unchanged
     1      Correct bucket and offset stored; the caller owns a new reference
            to the bucket.

 Internal:
    We do binary searches in BTree nodes downward, at each step following
    C(i) where K(i) <= key < K(i+1).  As always, K(i) <= C(i) < K(i+1) too.
    (See Maintainer.txt for the meaning of that notation.)  That eventually
    leads to a bucket where we do Bucket_findRangeEnd.  That usually works,
    but there are two cases where it can fail to find the correct answer:

    1. On a low search, we find a bucket with keys >= K(i), but that doesn't
       imply there are keys in the bucket >= key.  For example, suppose
       a bucket has keys in 1..100, its successor's keys are in 200..300, and
       we're doing a low search on 150.  We'll end up in the first bucket,
       but there are no keys >= 150 in it.  K(i+1) > key, though, and all
       the keys in C(i+1) >= K(i+1) > key, so the first key in the next
       bucket (if any) is the correct result.  This is easy to find by
       following the bucket 'next' pointer.

    2. On a high search, again that the keys in the bucket are >= K(i)
       doesn't imply that any key in the bucket is <= key, but it's harder
       for this to fail (and an earlier version of this routine didn't
       catch it):  if K(i) itself is in the bucket, it works (then
       K(i) <= key is *a* key in the bucket that's in the desired range).
       But when keys get deleted from buckets, they aren't also deleted from
       BTree nodes, so there's no guarantee that K(i) is in the bucket.
       For example, delete the smallest key S from some bucket, and S
       remains in the interior BTree nodes.  Do a high search for S, and
       the BTree nodes direct the search to the bucket S used to be in,
       but all keys remaining in that bucket are > S.  The largest key in
       the *preceding* bucket (if any) is < K(i), though, and K(i) <= key,
       so the largest key in the preceding bucket is < key and so is the
       proper result.

       This is harder to get at efficiently, as buckets are linked only in
       the increasing direction.  While we're searching downward,
       deepest_smaller is set to the  node deepest in the tree where
       we *could* have gone to the left of C(i).  The rightmost bucket in
       deepest_smaller's subtree is the bucket preceding the bucket we find
       at first.  This is clumsy to get at, but efficient.
*/
static int
BTree_findRangeEnd(BTree *self, PyObject *keyarg, int low,
                   Bucket **bucket, int *offset) {
    Sized *deepest_smaller = NULL;      /* last possibility to move left */
    int deepest_smaller_is_btree = 0;   /* Boolean; if false, it's a bucket */
    Bucket *pbucket;
    int self_got_rebound = 0;   /* Boolean; when true, deactivate self */
    int result = -1;            /* Until proven innocent */
    int i;
    KEY_TYPE key;
    int copied = 1;

    COPY_KEY_FROM_ARG(key, keyarg, copied);
    UNLESS (copied) return -1;

    /* We don't need to: PER_USE_OR_RETURN(self, -1);
       because the caller does. */
    UNLESS (self->data && self->len) return 0;

    /* Search downward until hitting a bucket, stored in pbucket. */
    for (;;) {
        Sized *pchild;
        int pchild_is_btree;

        BTREE_SEARCH(i, self, key, goto Done);
        pchild = self->data[i].child;
        pchild_is_btree = SameType_Check(self, pchild);
        if (i) {
            deepest_smaller = self->data[i-1].child;
            deepest_smaller_is_btree = pchild_is_btree;
        }

        if (pchild_is_btree) {
            if (self_got_rebound) {
                PER_ALLOW_DEACTIVATION(self);
                PER_ACCESSED(self);
            }
            self = BTREE(pchild);
            self_got_rebound = 1;
            PER_USE_OR_RETURN(self, -1);
        }
        else {
            pbucket = BUCKET(pchild);
            break;
        }
    }

    /* Search the bucket for a suitable key. */
    i = Bucket_findRangeEnd(pbucket, keyarg, low, offset);
    if (i < 0)
        goto Done;
    if (i > 0) {
        Py_INCREF(pbucket);
        *bucket = pbucket;
        result = 1;
        goto Done;
    }
    /* This may be one of the two difficult cases detailed in the comments. */
    if (low) {
        Bucket *next;

        UNLESS(PER_USE(pbucket)) goto Done;
        next = pbucket->next;
        if (next) {
                result = 1;
                Py_INCREF(next);
                *bucket = next;
                *offset = 0;
        }
        else
                result = 0;
        PER_ALLOW_DEACTIVATION(pbucket);
        PER_ACCESSED(pbucket);
    }
    /* High-end search:  if it's possible to go left, do so. */
    else if (deepest_smaller) {
        if (deepest_smaller_is_btree) {
            UNLESS(PER_USE(deepest_smaller)) goto Done;
            /* We own the reference this returns. */
            pbucket = BTree_lastBucket(BTREE(deepest_smaller));
            PER_ALLOW_DEACTIVATION(deepest_smaller);
            PER_ACCESSED(deepest_smaller);
            if (pbucket == NULL) goto Done;   /* error */
        }
        else {
            pbucket = BUCKET(deepest_smaller);
            Py_INCREF(pbucket);
        }
        UNLESS(PER_USE(pbucket)) goto Done;
        result = 1;
        *bucket = pbucket;  /* transfer ownership to caller */
        *offset = pbucket->len - 1;
        PER_ALLOW_DEACTIVATION(pbucket);
        PER_ACCESSED(pbucket);
    }
    else
        result = 0;     /* simply not found */

Done:
    if (self_got_rebound) {
        PER_ALLOW_DEACTIVATION(self);
        PER_ACCESSED(self);
    }
    return result;
}

static PyObject *
BTree_maxminKey(BTree *self, PyObject *args, int min)
{
  PyObject *key=0;
  Bucket *bucket = NULL;
  int offset, rc;

  UNLESS (PyArg_ParseTuple(args, "|O", &key)) return NULL;

  PER_USE_OR_RETURN(self, NULL);

  UNLESS (self->data && self->len) goto empty;

  /* Find the  range */

  if (key)
    {
      if ((rc = BTree_findRangeEnd(self, key, min, &bucket, &offset)) <= 0)
        {
          if (rc < 0) goto err;
          goto empty;
        }
      PER_ALLOW_DEACTIVATION(self);
      PER_ACCESSED(self);
      UNLESS (PER_USE(bucket))
        {
          Py_DECREF(bucket);
          return NULL;
        }
    }
  else if (min)
    {
      bucket = self->firstbucket;
      PER_ALLOW_DEACTIVATION(self);
      PER_ACCESSED(self);
      UNLESS (PER_USE(bucket)) return NULL;
      Py_INCREF(bucket);
      offset = 0;
      if (offset >= bucket->len)
        {
          switch (firstBucketOffset(&bucket, &offset))
            {
            case 0:  goto empty;
            case -1: goto err;
            }
        }
    }
  else
    {
      bucket = BTree_lastBucket(self);
      PER_ALLOW_DEACTIVATION(self);
      PER_ACCESSED(self);
      UNLESS (PER_USE(bucket))
        {
          Py_DECREF(bucket);
          return NULL;
        }
      if (bucket->len)
        offset = bucket->len - 1;
      else
        {
          switch (lastBucketOffset(&bucket, &offset, self->firstbucket, -1))
            {
            case 0:  goto empty;
            case -1: goto err;
            }
        }
    }

  COPY_KEY_TO_OBJECT(key, bucket->keys[offset]);
  PER_ALLOW_DEACTIVATION(bucket);
  PER_ACCESSED(bucket);
  Py_DECREF(bucket);

  return key;

 empty:
  PyErr_SetString(PyExc_ValueError, "empty tree");

 err:
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  if (bucket)
    {
      PER_ALLOW_DEACTIVATION(bucket);
      PER_ACCESSED(bucket);
      Py_DECREF(bucket);
    }
  return NULL;
}

static PyObject *
BTree_minKey(BTree *self, PyObject *args)
{
  return BTree_maxminKey(self, args, 1);
}

static PyObject *
BTree_maxKey(BTree *self, PyObject *args)
{
  return BTree_maxminKey(self, args, 0);
}

/*
** BTree_rangeSearch
**
** Generates a BTreeItems object based on the two indexes passed in,
** being the range between them.
**
*/
static PyObject *
BTree_rangeSearch(BTree *self, PyObject *args, char type)
{
  PyObject *f=0, *l=0;
  int rc;
  Bucket *lowbucket = NULL;
  Bucket *highbucket = NULL;
  int lowoffset;
  int highoffset;

  UNLESS (! args || PyArg_ParseTuple(args,"|OO",&f, &l)) return NULL;

  PER_USE_OR_RETURN(self, NULL);

  UNLESS (self->data && self->len) goto empty;

  /* Find the low range */

  if (f && f != Py_None)
    {
      if ((rc = BTree_findRangeEnd(self, f, 1, &lowbucket, &lowoffset)) <= 0)
        {
          if (rc < 0) goto err;
          goto empty;
        }
    }
  else
    {
      lowbucket = self->firstbucket;
      Py_INCREF(lowbucket);
      lowoffset = 0;
    }

  /* Find the high range */

  if (l && l != Py_None)
    {
      if ((rc = BTree_findRangeEnd(self, l, 0, &highbucket, &highoffset)) <= 0)
        {
          Py_DECREF(lowbucket);
          if (rc < 0) goto err;
          goto empty;
        }
    }
  else
    {
      highbucket = BTree_lastBucket(self);
      assert(highbucket != NULL);  /* we know self isn't empty */
      UNLESS (PER_USE(highbucket))
        {
          Py_DECREF(lowbucket);
          Py_DECREF(highbucket);
          goto err;
        }
      highoffset = highbucket->len - 1;
      PER_ALLOW_DEACTIVATION(highbucket);
      PER_ACCESSED(highbucket);
    }

  /* It's still possible that the range is empty, even if f < l.  For
   * example, if f=3 and l=4, and 3 and 4 aren't in the BTree, but 2 and
   * 5 are, then the low position points to the 5 now and the high position
   * points to the 2 now.  They're not necessarily even in the same bucket,
   * so there's no trick we can play with pointer compares to get out
   * cheap in general.
   */
  if (lowbucket == highbucket && lowoffset > highoffset)
    goto empty_and_decref_buckets;      /* definitely empty */

  /* The buckets differ, or they're the same and the offsets show a non-
   * empty range.
   */
  if (f && f != Py_None             /* both args user-supplied */
      && l && l != Py_None
      && lowbucket != highbucket)   /* and different buckets */
    {
      KEY_TYPE first;
      KEY_TYPE last;
      int cmp;

      /* Have to check the hard way:  see how the endpoints compare. */
      UNLESS (PER_USE(lowbucket)) goto err_and_decref_buckets;
      COPY_KEY(first, lowbucket->keys[lowoffset]);
      PER_ALLOW_DEACTIVATION(lowbucket);
      PER_ACCESSED(lowbucket);

      UNLESS (PER_USE(highbucket)) goto err_and_decref_buckets;
      COPY_KEY(last, highbucket->keys[highoffset]);
      PER_ALLOW_DEACTIVATION(highbucket);
      PER_ACCESSED(highbucket);

      TEST_KEY_SET_OR(cmp, first, last) goto err_and_decref_buckets;
      if (cmp > 0) goto empty_and_decref_buckets;
    }

  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);

  f = newBTreeItems(type, lowbucket, lowoffset, highbucket, highoffset);
  Py_DECREF(lowbucket);
  Py_DECREF(highbucket);
  return f;

 err_and_decref_buckets:
  Py_DECREF(lowbucket);
  Py_DECREF(highbucket);

 err:
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  return NULL;

 empty_and_decref_buckets:
  Py_DECREF(lowbucket);
  Py_DECREF(highbucket);

 empty:
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  return newBTreeItems(type, 0, 0, 0, 0);
}

/*
** BTree_keys
*/
static PyObject *
BTree_keys(BTree *self, PyObject *args)
{
  return BTree_rangeSearch(self,args, 'k');
}

/*
** BTree_values
*/
static PyObject *
BTree_values(BTree *self, PyObject *args)
{
  return BTree_rangeSearch(self,args,'v');
}

/*
** BTree_items
*/
static PyObject *
BTree_items(BTree *self, PyObject *args)
{
  return BTree_rangeSearch(self,args,'i');
}

static PyObject *
BTree_byValue(BTree *self, PyObject *args)
{
  PyObject *r=0, *o=0, *item=0, *omin;
  VALUE_TYPE min;
  VALUE_TYPE v;
  int copied=1;
  SetIteration it = {0, 0, 1};

  PER_USE_OR_RETURN(self, NULL);

  UNLESS (PyArg_ParseTuple(args, "O", &omin)) return NULL;
  COPY_VALUE_FROM_ARG(min, omin, copied);
  UNLESS(copied) return NULL;

  UNLESS (r=PyList_New(0)) goto err;

  it.set=BTree_rangeSearch(self, NULL, 'i');
  UNLESS(it.set) goto err;

  if (nextBTreeItems(&it) < 0) goto err;

  while (it.position >= 0)
    {
      if (TEST_VALUE(it.value, min) >= 0)
        {
          UNLESS (item = PyTuple_New(2)) goto err;

          COPY_KEY_TO_OBJECT(o, it.key);
          UNLESS (o) goto err;
          PyTuple_SET_ITEM(item, 1, o);

          COPY_VALUE(v, it.value);
          NORMALIZE_VALUE(v, min);
          COPY_VALUE_TO_OBJECT(o, v);
          DECREF_VALUE(v);
          UNLESS (o) goto err;
          PyTuple_SET_ITEM(item, 0, o);

          if (PyList_Append(r, item) < 0) goto err;
          Py_DECREF(item);
          item = 0;
        }
      if (nextBTreeItems(&it) < 0) goto err;
    }

  item=PyObject_GetAttr(r,sort_str);
  UNLESS (item) goto err;
  ASSIGN(item, PyObject_CallObject(item, NULL));
  UNLESS (item) goto err;
  ASSIGN(item, PyObject_GetAttr(r, reverse_str));
  UNLESS (item) goto err;
  ASSIGN(item, PyObject_CallObject(item, NULL));
  UNLESS (item) goto err;
  Py_DECREF(item);

  finiSetIteration(&it);
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  return r;

 err:
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  Py_XDECREF(r);
  finiSetIteration(&it);
  Py_XDECREF(item);
  return NULL;
}

/*
** BTree_getm
*/
static PyObject *
BTree_getm(BTree *self, PyObject *args)
{
  PyObject *key, *d=Py_None, *r;

  UNLESS (PyArg_ParseTuple(args, "O|O", &key, &d)) return NULL;
  if ((r=_BTree_get(self, key, 0))) return r;
  UNLESS (PyErr_ExceptionMatches(PyExc_KeyError)) return NULL;
  PyErr_Clear();
  Py_INCREF(d);
  return d;
}

/*
** BTree_has_key
*/
static PyObject *
BTree_has_key(BTree *self, PyObject *args)
{
  PyObject *key;

  UNLESS (PyArg_ParseTuple(args,"O",&key)) return NULL;
  return _BTree_get(self, key, 1);
}

static PyObject *
BTree_addUnique(BTree *self, PyObject *args)
{
  int grew;
  PyObject *key, *v;

  UNLESS (PyArg_ParseTuple(args, "OO", &key, &v)) return NULL;

  if ((grew=_BTree_set(self, key, v, 1, 0)) < 0) return NULL;
  return PyInt_FromLong(grew);
}


static struct PyMethodDef BTree_methods[] = {
  {"__getstate__", (PyCFunction) BTree_getstate,	METH_VARARGS,
   "__getstate__() -- Return the picklable state of the object"},
  {"__setstate__", (PyCFunction) BTree_setstate,	METH_VARARGS,
   "__setstate__() -- Set the state of the object"},
  {"has_key",	(PyCFunction) BTree_has_key,	METH_VARARGS,
     "has_key(key) -- Test whether the bucket contains the given key"},
  {"keys",	(PyCFunction) BTree_keys,	METH_VARARGS,
     "keys([min, max]) -- Return the keys"},
  {"values",	(PyCFunction) BTree_values,	METH_VARARGS,
     "values([min, max]) -- Return the values"},
  {"items",	(PyCFunction) BTree_items,	METH_VARARGS,
     "items([min, max]) -- Return the items"},
  {"byValue",	(PyCFunction) BTree_byValue,	METH_VARARGS,
   "byValue(min) -- "
   "Return value-keys with values >= min and reverse sorted by values"
  },
  {"get",	(PyCFunction) BTree_getm,	METH_VARARGS,
   "get(key[,default]) -- Look up a value\n\n"
   "Return the default (or None) if the key is not found."
  },
  {"maxKey", (PyCFunction) BTree_maxKey,	METH_VARARGS,
   "maxKey([key]) -- Fine the maximum key\n\n"
   "If an argument is given, find the maximum <= the argument"},
  {"minKey", (PyCFunction) BTree_minKey,	METH_VARARGS,
   "minKey([key]) -- Fine the minimum key\n\n"
   "If an argument is given, find the minimum >= the argument"},
  {"clear",	(PyCFunction) BTree_clear,	METH_VARARGS,
   "clear() -- Remove all of the items from the BTree"},
  {"insert", (PyCFunction)BTree_addUnique, METH_VARARGS,
   "insert(key, value) -- Add an item if the key is not already used.\n\n"
   "Return 1 if the item was added, or 0 otherwise"
  },
  {"update",	(PyCFunction) Mapping_update,	METH_VARARGS,
   "update(collection) -- Add the items from the given collection"},
  {"__init__",	(PyCFunction) Mapping_update,	METH_VARARGS,
   "__init__(collection) -- Initialize with items from the given collection"},
#ifdef PERSISTENT
  {"_p_resolveConflict", (PyCFunction) BTree__p_resolveConflict, METH_VARARGS,
   "_p_resolveConflict() -- Reinitialize from a newly created copy"},
  {"_p_deactivate", (PyCFunction) BTree__p_deactivate,	METH_VARARGS,
   "_p_deactivate() -- Reinitialize from a newly created copy"},
#endif
  {NULL,		NULL}		/* sentinel */
};

static void
BTree_dealloc(BTree *self)
{
  if (self->state != cPersistent_GHOST_STATE)
    _BTree_clear(self);

  PER_DEL(self);

  Py_DECREF(self->ob_type);
  PyObject_Del(self);
}

static int
BTree_length_or_nonzero(BTree *self, int nonzero)
{
  int c=0;
  Bucket *b, *n;

  PER_USE_OR_RETURN(self, -1);
  b = self->firstbucket;
  Py_XINCREF(b);
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);

  while (b != NULL)
    {
      PER_USE_OR_RETURN(b, -1);
      c += b->len;
      if (nonzero && c)
        {
          /* Short-circuit if all we care about is nonempty */
          PER_ALLOW_DEACTIVATION(b);
          PER_ACCESSED(b);
          Py_DECREF(b);
          return 1;
        }
      n = b->next;
      Py_XINCREF(n);
      PER_ALLOW_DEACTIVATION(b);
      PER_ACCESSED(b);
      ASSIGNB(b, n);
    }

  return c;
}

static int
BTree_length( BTree *self)
{
  return BTree_length_or_nonzero(self, 0);
}

static PyMappingMethods BTree_as_mapping = {
  (inquiry)BTree_length,		/*mp_length*/
  (binaryfunc)BTree_get,		/*mp_subscript*/
  (objobjargproc)BTree_setitem,	        /*mp_ass_subscript*/
};

static int
BTree_nonzero( BTree *self)
{
  return BTree_length_or_nonzero(self, 1);
}

static PyNumberMethods BTree_as_number_for_nonzero = {
  0,0,0,0,0,0,0,0,0,0,
  (inquiry)BTree_nonzero};

static PyExtensionClass BTreeType = {
  PyObject_HEAD_INIT(NULL)
  0,				/*ob_size*/
  MOD_NAME_PREFIX "BTree",			/*tp_name*/
  sizeof(BTree),		/*tp_basicsize*/
  0,				/*tp_itemsize*/
  /************* methods ********************/
  (destructor) BTree_dealloc,/*tp_dealloc*/
  (printfunc)0,			/*tp_print*/
  (getattrfunc)0,		/*obsolete tp_getattr*/
  (setattrfunc)0,		/*obsolete tp_setattr*/
  (cmpfunc)0,			/*tp_compare*/
  (reprfunc)0,			/*tp_repr*/
  &BTree_as_number_for_nonzero,	/*tp_as_number*/
  0,				/*tp_as_sequence*/
  &BTree_as_mapping,	/*tp_as_mapping*/
  (hashfunc)0,			/*tp_hash*/
  (ternaryfunc)0,		/*tp_call*/
  (reprfunc)0,			/*tp_str*/
  (getattrofunc)0,
  0,				/*tp_setattro*/

  /* Space for future expansion */
  0L,0L,
  "Mapping type implemented as sorted list of items",
  METHOD_CHAIN(BTree_methods),
  EXTENSIONCLASS_BASICNEW_FLAG
#ifdef PERSISTENT
  | PERSISTENT_TYPE_FLAG
#endif
  | EXTENSIONCLASS_NOINSTDICT_FLAG,
};
