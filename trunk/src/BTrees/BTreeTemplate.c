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

#define BTREETEMPLATE_C "$Id: BTreeTemplate.c,v 1.72 2002/10/05 00:39:56 gvanrossum Exp $\n"

/* Sanity-check a BTree.  This is a private helper for BTree_check.  Return:
 *      -1         Error.  If it's an internal inconsistency in the BTree,
 *                 AssertionError is set.
 *       0         No problem found.
 *
 * nextbucket is the bucket "one beyond the end" of the BTree; the last bucket
 * directly reachable from following right child pointers *should* be linked
 * to nextbucket (and this is checked).
 */
static int
BTree_check_inner(BTree *self, Bucket *nextbucket)
{
    int i;
    Bucket *bucketafter;
    Sized *child;
    char *errormsg = "internal error";  /* someone should have overriden */
    Sized *activated_child = NULL;
    int result = -1;    /* until proved innocent */

#define CHECK(CONDITION, ERRORMSG)          \
    if (!(CONDITION)) {                     \
        errormsg = (ERRORMSG);              \
        goto Error;                         \
    }

    PER_USE_OR_RETURN(self, -1);
    CHECK(self->len >= 0, "BTree len < 0");
    CHECK(self->len <= self->size, "BTree len > size");
    if (self->len == 0) {
        /* Empty BTree. */
        CHECK(self->firstbucket == NULL,
              "Empty BTree has non-NULL firstbucket");
        result = 0;
        goto Done;
    }
    /* Non-empty BTree. */
    CHECK(self->firstbucket != NULL, "Non-empty BTree has NULL firstbucket");

    /* Obscure:  The first bucket is pointed to at least by self->firstbucket
     * and data[0].child of whichever BTree node it's a child of.  However,
     * if persistence is enabled then the latter BTree node may be a ghost
     * at this point, and so its pointers "don't count":  we can only rely
     * on self's pointers being intact.
     */
#ifdef PERSISTENT
    CHECK(self->firstbucket->ob_refcnt >= 1,
          "Non-empty BTree firstbucket has refcount < 1");
#else
    CHECK(self->firstbucket->ob_refcnt >= 2,
          "Non-empty BTree firstbucket has refcount < 2");
#endif

    for (i = 0; i < self->len; ++i) {
        CHECK(self->data[i].child != NULL, "BTree has NULL child");
    }

    if (SameType_Check(self, self->data[0].child)) {
        /* Our children are also BTrees. */
        child = self->data[0].child;
        UNLESS (PER_USE(child)) goto Done;
        activated_child = child;
        CHECK(self->firstbucket == BTREE(child)->firstbucket,
               "BTree has firstbucket different than "
               "its first child's firstbucket");
        PER_ALLOW_DEACTIVATION(child);
        activated_child = NULL;
        for (i = 0; i < self->len; ++i) {
            child = self->data[i].child;
            CHECK(SameType_Check(self, child),
                  "BTree children have different types");
            if (i == self->len - 1)
                bucketafter = nextbucket;
            else {
                BTree *child2 = BTREE(self->data[i+1].child);
                UNLESS (PER_USE(child2)) goto Done;
                bucketafter = child2->firstbucket;
                PER_ALLOW_DEACTIVATION(child2);
            }
            if (BTree_check_inner(BTREE(child), bucketafter) < 0) goto Done;
        }
    }
    else {
        /* Our children are buckets. */
        CHECK(self->firstbucket == BUCKET(self->data[0].child),
              "Bottom-level BTree node has inconsistent firstbucket belief");
        for (i = 0; i < self->len; ++i) {
            child = self->data[i].child;
            UNLESS (PER_USE(child)) goto Done;
            activated_child = child;
            CHECK(!SameType_Check(self, child),
                  "BTree children have different types");
            CHECK(child->len >= 1, "Bucket length < 1"); /* no empty buckets! */
            CHECK(child->len <= child->size, "Bucket len > size");
#ifdef PERSISTENT
            CHECK(child->ob_refcnt >= 1, "Bucket has refcount < 1");
#else
            CHECK(child->ob_refcnt >= 2, "Bucket has refcount < 2");
#endif
            if (i == self->len - 1)
                bucketafter = nextbucket;
            else
                bucketafter = BUCKET(self->data[i+1].child);
            CHECK(BUCKET(child)->next == bucketafter,
                  "Bucket next pointer is damaged");
            PER_ALLOW_DEACTIVATION(child);
            activated_child = NULL;
        }
    }
    result = 0;
    goto Done;

Error:
    PyErr_SetString(PyExc_AssertionError, errormsg);
    result = -1;
Done:
    /* No point updating access time -- this isn't a "real" use. */
    PER_ALLOW_DEACTIVATION(self);
    if (activated_child) {
        PER_ALLOW_DEACTIVATION(activated_child);
    }
    return result;

#undef CHECK
}

/* Sanity-check a BTree.  This is the ._check() method.  Return:
 *      NULL       Error.  If it's an internal inconsistency in the BTree,
 *                 AssertionError is set.
 *      Py_None    No problem found.
 */
static PyObject*
BTree_check(BTree *self, PyObject *args)
{
    PyObject *result = NULL;
    int i = BTree_check_inner(self, NULL);

    if (i >= 0) {
        result = Py_None;
        Py_INCREF(result);
    }
    return result;
}

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
    PyObject *result = NULL;    /* guilty until proved innocent */
    int copied = 1;

    COPY_KEY_FROM_ARG(key, keyarg, copied);
    UNLESS (copied) return NULL;

    PER_USE_OR_RETURN(self, NULL);
    if (self->len == 0) {
        /* empty BTree */
        if (has_key)
            result = PyInt_FromLong(0);
        else
            PyErr_SetObject(PyExc_KeyError, keyarg);
    }
    else {
        for (;;) {
            int i;
            Sized *child;

            BTREE_SEARCH(i, self, key, goto Done);
            child = self->data[i].child;
            has_key += has_key != 0;    /* bump depth counter, maybe */
            if (SameType_Check(self, child)) {
                PER_UNUSE(self);
                self = BTREE(child);
                PER_USE_OR_RETURN(self, NULL);
            }
            else {
                result = _bucket_get(BUCKET(child), keyarg, has_key);
                break;
            }
        }
    }

Done:
    PER_UNUSE(self);
    return result;
}

static PyObject *
BTree_get(BTree *self, PyObject *key)
{
  return _BTree_get(self, key, 0);
}

/*
 * Move data from the current BTree, from index onward, to the newly created
 * BTree 'next'.  self and next must both be activated.  If index is OOB (< 0
 * or >= self->len), use self->len / 2 as the index (i.e., split at the
 * midpoint).  self must have at least 2 children on entry, and index must
 * be such that self and next each have at least one child at exit.  self's
 * accessed time is updated.
 *
 * Return:
 *    -1    error
 *     0    OK
 */
static int
BTree_split(BTree *self, int index, BTree *next)
{
    int next_size;
    Sized *child;

    if (index < 0 || index >= self->len)
	index = self->len / 2;

    next_size = self->len - index;
    ASSERT(index > 0, "split creates empty tree", -1);
    ASSERT(next_size > 0, "split creates empty tree", -1);

    next->data = PyMalloc(sizeof(BTreeItem) * next_size);
    if (!next->data)
	return -1;
    memcpy(next->data, self->data + index, sizeof(BTreeItem) * next_size);
    next->size = next_size;  /* but don't set len until we succeed */

    /* Set next's firstbucket.  self->firstbucket is still correct. */
    child = next->data[0].child;
    if (SameType_Check(self, child)) {
        PER_USE_OR_RETURN(child, -1);
	next->firstbucket = BTREE(child)->firstbucket;
	PER_UNUSE(child);
    }
    else
	next->firstbucket = BUCKET(child);
    Py_INCREF(next->firstbucket);

    next->len = next_size;
    self->len = index;
    return PER_CHANGED(self) < 0 ? -1 : 0;
}


/* Fwd decl -- BTree_grow and BTree_split_root reference each other. */
static int BTree_grow(BTree *self, int index, int noval);

/* Split the root.  This is a little special because the root isn't a child
 * of anything else, and the root needs to retain its object identity.  So
 * this routine moves the root's data into a new child, and splits the
 * latter.  This leaves the root with two children.
 *
 * Return:
 *      0   OK
 *     -1   error
 *
 * CAUTION:  The caller must call PER_CHANGED on self.
 */
static int
BTree_split_root(BTree *self, int noval)
{
    BTree *child;
    BTreeItem *d;

    /* Create a child BTree, and a new data vector for self. */
    child = BTREE(PyObject_CallObject(OBJECT(self->ob_type), NULL));
    if (!child) return -1;

    d = PyMalloc(sizeof(BTreeItem) * 2);
    if (!d) {
        Py_DECREF(child);
        return -1;
    }

    /* Move our data to new BTree. */
    child->size = self->size;
    child->len = self->len;
    child->data = self->data;
    child->firstbucket = self->firstbucket;
    Py_INCREF(child->firstbucket);

    /* Point self to child and split the child. */
    self->data = d;
    self->len = 1;
    self->size = 2;
    self->data[0].child = SIZED(child); /* transfers reference ownership */
    return BTree_grow(self, 0, noval);
}

/*
** BTree_grow
**
** Grow a BTree
**
** Arguments:	self	The BTree
**		index	self->data[index].child needs to be split.  index
**                      must be 0 if self is empty (len == 0), and a new
**                      empty bucket is created then.
**              noval   Boolean; is this a set (true) or mapping (false)?
**
** Returns:	 0	on success
**		-1	on failure
**
** CAUTION:  If self is empty on entry, this routine adds an empty bucket.
** That isn't a legitimate BTree; if the caller doesn't put something in
** in the bucket (say, because of a later error), the BTree must be cleared
** to get rid of the empty bucket.
*/
static int
BTree_grow(BTree *self, int index, int noval)
{
  int i;
  Sized *v, *e = 0;
  BTreeItem *d;

  if (self->len == self->size)
    {
      if (self->size)
        {
          d = PyRealloc(self->data, sizeof(BTreeItem) * self->size * 2);
          if (d == NULL)
            return -1;
          self->data = d;
          self->size *= 2;
        }
      else
        {
          d = PyMalloc(sizeof(BTreeItem) * 2);
          if (d == NULL)
            return -1;
          self->data = d;
          self->size = 2;
        }
    }

  if (self->len)
    {
      d = self->data + index;
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
          i = BTree_split(BTREE(v), -1,   BTREE(e));
        }
      else
        {
          i = bucket_split(BUCKET(v), -1, BUCKET(e));
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
      d->child = e;

      self->len++;

      if (self->len >= MAX_BTREE_SIZE(self) * 2)    /* the root is huge */
        return BTree_split_root(self, noval);
    }
  else
    {
      /* The BTree is empty.  Create an empty bucket.  See CAUTION in
       * the comments preceding.
       */
      assert(index == 0);
      d = self->data;
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
      self->len = 1;
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
** _BTree_clear
**
** Clears out all of the values in the BTree (firstbucket, keys, and children);
** leaving self an empty BTree.
**
** Arguments:	self	The BTree
**
** Returns:	 0	on success
**		-1	on failure
**
** Internal:  Deallocation order is important.  The danger is that a long
** list of buckets may get freed "at once" via decref'ing the first bucket,
** in which case a chain of consequenct Py_DECREF calls may blow the stack.
** Luckily, every bucket has a refcount of at least two, one due to being a
** BTree node's child, and another either because it's not the first bucket in
** the chain (so the preceding bucket points to it), or because firstbucket
** points to it.  By clearing in the natural depth-first, left-to-right
** order, the BTree->bucket child pointers prevent Py_DECREF(bucket->next)
** calls from freeing bucket->next, and the maximum stack depth is equal
** to the height of the tree.
**/
static int
_BTree_clear(BTree *self)
{
    const int len = self->len;

    if (self->firstbucket) {
        /* Obscure:  The first bucket is pointed to at least by
         * self->firstbucket and data[0].child of whichever BTree node it's
         * a child of.  However, if persistence is enabled then the latter
         * BTree node may be a ghost at this point, and so its pointers "don't
         * count":  we can only rely on self's pointers being intact.
         */
#ifdef PERSISTENT
	ASSERT(self->firstbucket->ob_refcnt > 0,
	       "Invalid firstbucket pointer", -1);
#else
	ASSERT(self->firstbucket->ob_refcnt > 1,
	       "Invalid firstbucket pointer", -1);
#endif
	Py_DECREF(self->firstbucket);
	self->firstbucket = NULL;
    }

    if (self->data) {
        int i;
        if (len > 0) { /* 0 is special because key 0 is trash */
            Py_DECREF(self->data[0].child);
	}

        for (i = 1; i < len; i++) {
#ifdef KEY_TYPE_IS_PYOBJECT
	    DECREF_KEY(self->data[i].key);
#endif
            Py_DECREF(self->data[i].child);
        }
	free(self->data);
	self->data = NULL;
    }

    self->len = self->size = 0;
    return 0;
}

/*
  Set (value != 0) or delete (value=0) a tree item.

  If unique is non-zero, then only change if the key is
  new.

  If noval is non-zero, then don't set a value (the tree
  is a set).

  Return:
    -1  error
     0  successful, and number of entries didn't change
    >0  successful, and number of entries did change

  Internal
     There are two distinct return values > 0:

     1  Successful, number of entries changed, but firstbucket did not go away.

     2  Successful, number of entries changed, firstbucket did go away.
        This can only happen on a delete (value == NULL).  The caller may
        need to change its own firstbucket pointer, and in any case *someone*
        needs to adjust the 'next' pointer of the bucket immediately preceding
        the bucket that went away (it needs to point to the bucket immediately
        following the bucket that went away).
*/
static int
_BTree_set(BTree *self, PyObject *keyarg, PyObject *value,
           int unique, int noval)
{
    int changed = 0;    /* did I mutate? */
    int min;            /* index of child I searched */
    BTreeItem *d;       /* self->data[min] */
    int childlength;    /* len(self->data[min].child) */
    int status;         /* our return value; and return value from callee */
    int self_was_empty; /* was self empty at entry? */

    KEY_TYPE key;
    int copied = 1;

    COPY_KEY_FROM_ARG(key, keyarg, copied);
    UNLESS (copied) return -1;

    PER_USE_OR_RETURN(self, -1);

    self_was_empty = self->len == 0;
    if (self_was_empty) {
        /* We're empty.  Make room. */
	if (value) {
	    if (BTree_grow(self, 0, noval) < 0)
		goto Error;
	}
	else {
	    /* Can't delete a key from an empty BTree. */
	    PyErr_SetObject(PyExc_KeyError, keyarg);
	    goto Error;
	}
    }

    /* Find the right child to search, and hand the work off to it. */
    BTREE_SEARCH(min, self, key, goto Error);
    d = self->data + min;

    if (SameType_Check(self, d->child))
	status = _BTree_set(BTREE(d->child), keyarg, value, unique, noval);
    else {
        int bucket_changed = 0;
	status = _bucket_set(BUCKET(d->child), keyarg,
	                     value, unique, noval, &bucket_changed);
#ifdef PERSISTENT
	/* If a BTree contains only a single bucket, BTree.__getstate__()
	 * includes the bucket's entire state, and the bucket doesn't get
	 * an oid of its own.  So if we have a single oid-less bucket that
	 * changed, it's *our* oid that should be marked as changed.
	 */
	if (bucket_changed
	    && self->len == 1
	    && self->data[0].child->oid == NULL)
	{
	    changed = 1;
	}
#endif
    }
    if (status == 0) goto Done;
    if (status < 0) goto Error;
    assert(status == 1 || status == 2);

    /* The child changed size.  Get its new size.  Note that since the tree
     * rooted at the child changed size, so did the tree rooted at self:
     * our status must be >= 1 too.
     */
    UNLESS(PER_USE(d->child)) goto Error;
    childlength = d->child->len;
    PER_UNUSE(d->child);

    if (value) {
        /* A bucket got bigger -- if it's "too big", split it. */
        int toobig;

        assert(status == 1);    /* can be 2 only on deletes */
        if (SameType_Check(self, d->child))
            toobig = childlength > MAX_BTREE_SIZE(d->child);
        else
            toobig = childlength > MAX_BUCKET_SIZE(d->child);

        if (toobig) {
            if (BTree_grow(self, min, noval) < 0) goto Error;
            changed = 1;        /* BTree_grow mutated self */
        }
        goto Done;      /* and status still == 1 */
    }

    /* A bucket got smaller.  This is much harder, and despite that we
     * don't try to rebalance the tree.
     */
    if (status == 2) {  /*  this is the last reference to child status */
        /* Two problems to solve:  May have to adjust our own firstbucket,
         * and the bucket that went away needs to get unlinked.
         */
        if (min) {
            /* This wasn't our firstbucket, so no need to adjust ours (note
             * that it can't be the firstbucket of any node above us either).
             * Tell "the tree to the left" to do the unlinking.
             */
            if (BTree_deleteNextBucket(BTREE(d[-1].child)) < 0) goto Error;
            status = 1;     /* we solved the child's firstbucket problem */
        }
        else {
            /* This was our firstbucket.  Update to new firstbucket value. */
            Bucket *nextbucket;
            UNLESS(PER_USE(d->child)) goto Error;
            nextbucket = BTREE(d->child)->firstbucket;
            PER_UNUSE(d->child);

            Py_XINCREF(nextbucket);
            Py_DECREF(self->firstbucket);
            self->firstbucket = nextbucket;
            changed = 1;

            /* The caller has to do the unlinking -- we can't.  Also, since
             * it was our firstbucket, it may also be theirs.
             */
            assert(status == 2);
        }
    }

    /* If the child isn't empty, we're done!  We did all that was possible for
     * us to do with the firstbucket problems the child gave us, and since the
     * child isn't empty don't create any new firstbucket problems of our own.
     */
    if (childlength) goto Done;

    /* The child became empty:  we need to remove it from self->data.
     * But first, if we're a bottom-level node, we've got more bucket-fiddling
     * to set up.
     */
    if (!SameType_Check(self, d->child)) {
        /* We're about to delete a bucket. */
        if (min) {
            /* It's not our first bucket, so we can tell the previous
             * bucket to adjust its reference to it.  It can't be anyone
             * else's first bucket either, so the caller needn't do anything.
             */
            if (Bucket_deleteNextBucket(BUCKET(d[-1].child)) < 0) goto Error;
            /* status should be 1, and already is:  if it were 2, the
             * block above would have set it to 1 in its min != 0 branch.
             */
            assert(status == 1);
        }
        else {
            Bucket *nextbucket;
            /* It's our first bucket.  We can't unlink it directly. */
            /* 'changed' will be set true by the deletion code following. */
            UNLESS(PER_USE(d->child)) goto Error;
            nextbucket = BUCKET(d->child)->next;
            PER_UNUSE(d->child);

            Py_XINCREF(nextbucket);
            Py_DECREF(self->firstbucket);
            self->firstbucket = nextbucket;

            status = 2; /* we're giving our caller a new firstbucket problem */
         }
    }

    /* Remove the child from self->data. */
    Py_DECREF(d->child);
    if (min) {
        DECREF_KEY(d->key);
    }
    --self->len;
    if (min < self->len)
        memmove(d, d+1, (self->len - min) * sizeof(BTreeItem));
    changed = 1;

Done:
#ifdef PERSISTENT
    if (changed) {
        if (PER_CHANGED(self) < 0) goto Error;
    }
#endif
    PER_UNUSE(self);
    return status;

Error:
    if (self_was_empty) {
        /* BTree_grow may have left the BTree in an invalid state.  Make
         * sure the tree is a legitimate empty tree.
         */
        _BTree_clear(self);
    }
    PER_UNUSE(self);
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

    if (_BTree_clear(self) < 0)
	return -1;

    /* The state of a BTree can be one of the following:
       None -- an empty BTree
       A one-tuple -- a single bucket btree
       A two-tuple -- a BTree with more than one bucket
       See comments for BTree_getstate() for the details.
    */

    if (state == Py_None)
	return 0;

    if (!PyArg_ParseTuple(state, "O|O:__setstate__", &items, &firstbucket))
        return -1;

    len = PyTuple_Size(items);
    if (len < 0)
	return -1;
    len = (len + 1) / 2;
    assert(len > 0);

    assert(self->size == 0); /* XXX we called _BTree_clear() above! */
    assert(self->data == NULL); /* ditto */
    self->data = PyMalloc(sizeof(BTreeItem) * len);
    if (self->data == NULL)
	return -1;
    self->size = len;

    for (i = 0, d = self->data, l = 0; i < len; i++, d++) {
	PyObject *v;
	if (i) { /* skip the first key slot */
	    COPY_KEY_FROM_ARG(d->key, PyTuple_GET_ITEM(items,l), copied);
	    l++;
	    if (!copied)
		return -1;
	    INCREF_KEY(d->key);
	}
	v = PyTuple_GET_ITEM(items, l);
	if (PyTuple_Check(v)) {
	    /* Handle the special case in __getstate__() for a BTree
	       with a single bucket. */
	    if (noval) {
		d->child = SIZED(PyObject_CallObject(OBJECT(&SetType),
						     NULL));
		UNLESS (d->child) return -1;
		if (_set_setstate(BUCKET(d->child), v) < 0)
		    return -1;
	    }
	    else {
		d->child = SIZED(PyObject_CallObject(OBJECT(&BucketType),
						     NULL));
		UNLESS (d->child) return -1;
		if (_bucket_setstate(BUCKET(d->child), v) < 0)
		    return -1;
	    }
	}
	else {
	    d->child = (Sized *)v;
	    Py_INCREF(v);
	}
	l++;
    }

    if (!firstbucket)
	firstbucket = OBJECT(self->data->child);

    if (!ExtensionClassSubclassInstance_Check(
	    firstbucket, noval ? &SetType : &BucketType)) {
	PyErr_SetString(PyExc_TypeError, "No firstbucket in non-empty BTree");
	return -1;
    }

    self->firstbucket = BUCKET(firstbucket);
    Py_INCREF(firstbucket);
#ifndef PERSISTENT
    /* firstbucket is also the child of some BTree node, but that node may
     * be a ghost if persistence is enabled.
     */
    assert(self->firstbucket->ob_refcnt > 1);
#endif

    self->len = len;

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
  }
  else {
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
  PyObject *key = 0;
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
      PER_USE_OR_RETURN(bucket, NULL);
      Py_INCREF(bucket);
      offset = 0;
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
      assert(bucket->len);
      offset = bucket->len - 1;
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
   "maxKey([key]) -- Find the maximum key\n\n"
   "If an argument is given, find the maximum <= the argument"},
  {"minKey", (PyCFunction) BTree_minKey,	METH_VARARGS,
   "minKey([key]) -- Find the minimum key\n\n"
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
  {"_check", (PyCFunction) BTree_check,         METH_VARARGS,
   "Perform sanity check on BTree, and raise exception if flawed."},
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

/*
 * Return the number of elements in a BTree.  nonzero is a Boolean, and
 * when true requests just a non-empty/empty result.  Testing for emptiness
 * is efficient (constant-time).  Getting the true length takes time
 * proportional to the number of leaves (buckets).
 *
 * Return:
 *     When nonzero true:
 *          -1  error
 *           0  empty
 *           1  not empty
 *     When nonzero false (possibly expensive!):
 *          -1  error
 *        >= 0  number of elements.
 */
static int
BTree_length_or_nonzero(BTree *self, int nonzero)
{
    int result;
    Bucket *b;
    Bucket *next;

    PER_USE_OR_RETURN(self, -1);
    b = self->firstbucket;
    PER_UNUSE(self);
    if (nonzero)
        return b != NULL;

    result = 0;
    while (b) {
        PER_USE_OR_RETURN(b, -1);
        result += b->len;
        next = b->next;
        PER_UNUSE(b);
        b = next;
    }
    return result;
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
