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

#define BUCKETTEMPLATE_C "$Id: BucketTemplate.c,v 1.50 2003/04/07 21:43:14 jeremy Exp $\n"

/* Use BUCKET_SEARCH to find the index at which a key belongs.
 * INDEX    An int lvalue to hold the index i such that KEY belongs at
 *          SELF->keys[i].  Note that this will equal SELF->len if KEY
 *          is larger than the bucket's largest key.  Else it's the
 *          smallest i such that SELF->keys[i] >= KEY.
 * ABSENT   An int lvalue to hold a Boolean result, true (!= 0) if the
 *          key is absent, false (== 0) if the key is at INDEX.
 * SELF     A pointer to a Bucket node.
 * KEY      The key you're looking for, of type KEY_TYPE.
 * ONERROR  What to do if key comparison raises an exception; for example,
 *          perhaps 'return NULL'.
 *
 * See Maintainer.txt for discussion:  this is optimized in subtle ways.
 * It's recommended that you call this at the start of a routine, waiting
 * to check for self->len == 0 after (if an empty bucket is special in
 * context; INDEX becomes 0 and ABSENT becomes true if this macro is run
 * with an empty SELF, and that may be all the invoker needs to know).
 */
#define BUCKET_SEARCH(INDEX, ABSENT, SELF, KEY, ONERROR) {  \
    int _lo = 0;                                            \
    int _hi = (SELF)->len;                                  \
    int _i;                                                 \
    int _cmp = 1;                                           \
    for (_i = _hi >> 1; _lo < _hi; _i = (_lo + _hi) >> 1) { \
        TEST_KEY_SET_OR(_cmp, (SELF)->keys[_i], (KEY))      \
            ONERROR;                                        \
        if      (_cmp < 0)  _lo = _i + 1;                   \
        else if (_cmp == 0) break;                          \
        else                _hi = _i;                       \
    }                                                       \
    (INDEX) = _i;                                           \
    (ABSENT) = _cmp;                                        \
}

/*
** _bucket_get
**
** Search a bucket for a given key.
**
** Arguments
**     self	The bucket
**     keyarg	The key to look for
**     has_key	Boolean; if true, return a true/false result; else return
**              the value associated with the key.
**
** Return
**     If has_key:
**         Returns the Python int 0 if the key is absent, else returns
**         has_key itself as a Python int.  A BTree caller generally passes
**         the depth of the bucket for has_key, so a true result returns
**         the bucket depth then.
**         Note that has_key should be tree when searching set buckets.
**     If not has_key:
**         If the key is present, returns the associated value, and the
**         caller owns the reference.  Else returns NULL and sets KeyError.
**     Whether or not has_key:
**         If a comparison sets an exception, returns NULL.
*/
static PyObject *
_bucket_get(Bucket *self, PyObject *keyarg, int has_key)
{
    int i, cmp;
    KEY_TYPE key;
    PyObject *r = NULL;
    int copied = 1;

    COPY_KEY_FROM_ARG(key, keyarg, copied);
    UNLESS (copied) return NULL;

    PER_USE_OR_RETURN(self, NULL);

    BUCKET_SEARCH(i, cmp, self, key, goto Done);
    if (has_key)
    	r = PyInt_FromLong(cmp ? 0 : has_key);
    else {
        if (cmp == 0) {
            COPY_VALUE_TO_OBJECT(r, self->values[i]);
        }
        else
            PyErr_SetObject(PyExc_KeyError, keyarg);
    }

Done:
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  return r;
}

static PyObject *
bucket_getitem(Bucket *self, PyObject *key)
{
  return _bucket_get(self, key, 0);
}

/*
** Bucket_grow
**
** Resize a bucket.
**
** Arguments:   self    The bucket.
**              newsize The new maximum capacity.  If < 0, double the
**                      current size unless the bucket is currently empty,
**                      in which case use MIN_BUCKET_ALLOC.
**              noval   Boolean; if true, allocate only key space and not
**                      value space
**
** Returns:     -1      on error, and MemoryError exception is set
**               0      on success
*/
static int
Bucket_grow(Bucket *self, int newsize, int noval)
{
  KEY_TYPE *keys;
  VALUE_TYPE *values;

  if (self->size)
    {
      if (newsize < 0)
        newsize = self->size * 2;
      if (newsize < 0)    /* int overflow */
        goto Overflow;
      UNLESS (keys = PyRealloc(self->keys, sizeof(KEY_TYPE) * newsize))
        return -1;

      UNLESS (noval)
        {
          values = PyRealloc(self->values, sizeof(VALUE_TYPE) * newsize);
          if (values == NULL)
            {
              free(keys);
              return -1;
            }
          self->values = values;
        }
      self->keys = keys;
    }
  else
    {
      if (newsize < 0)
        newsize = MIN_BUCKET_ALLOC;
      UNLESS (self->keys = PyMalloc(sizeof(KEY_TYPE) * newsize))
        return -1;
      UNLESS (noval)
        {
          self->values = PyMalloc(sizeof(VALUE_TYPE) * newsize);
          if (self->values == NULL)
            {
              free(self->keys);
              self->keys = NULL;
              return -1;
            }
        }
    }
  self->size = newsize;
  return 0;

Overflow:
  PyErr_NoMemory();
  return -1;
}

/* So far, bucket_append is called only by multiunion_m(), so is called
 * only when MULTI_INT_UNION is defined.  Flavors of BTree/Bucket that
 * don't support MULTI_INT_UNION don't call bucket_append (yet), and
 * gcc complains if bucket_append is compiled in those cases.  So only
 * compile bucket_append if it's going to be used.
 */
#ifdef MULTI_INT_UNION
/*
 * Append a slice of the "from" bucket to self.
 *
 * self         Append (at least keys) to this bucket.  self must be activated
 *              upon entry, and remains activated at exit.  If copyValues
 *              is true, self must be empty or already have a non-NULL values
 *              pointer.  self's access and modification times aren't updated.
 * from         The bucket from which to take keys, and possibly values.  from
 *              must be activated upon entry, and remains activated at exit.
 *              If copyValues is true, from must have a non-NULL values
 *              pointer.  self and from must not be the same.  from's access
 *              time isn't updated.
 * i, n         The slice from[i : i+n] is appended to self.  Must have
 *              i >= 0, n > 0 and i+n <= from->len.
 * copyValues   Boolean.  If true, copy values from the slice as well as keys.
 *              In this case, from must have a non-NULL values pointer, and
 *              self must too (unless self is empty, in which case a values
 *              vector will be allocated for it).
 * overallocate Boolean.  If self doesn't have enough room upon entry to hold
 *              all the appended stuff, then if overallocate is false exactly
 *              enough room will be allocated to hold the new stuff, else if
 *              overallocate is true an excess will be allocated.  overallocate
 *              may be a good idea if you expect to append more stuff to self
 *              later; else overallocate should be false.
 *
 * CAUTION:  If self is empty upon entry (self->size == 0), and copyValues is
 * false, then no space for values will get allocated.  This can be a trap if
 * the caller intends to copy values itself.
 *
 * Return
 *    -1        Error.
 *     0        OK.
 */
static int
bucket_append(Bucket *self, Bucket *from, int i, int n,
              int copyValues, int overallocate)
{
    int newlen;

    assert(self && from && self != from);
    assert(i >= 0);
    assert(n > 0);
    assert(i+n <= from->len);

    /* Make room. */
    newlen = self->len + n;
    if (newlen > self->size) {
        int newsize = newlen;
        if (overallocate)   /* boost by 25% -- pretty arbitrary */
            newsize += newsize >> 2;
        if (Bucket_grow(self, newsize, ! copyValues) < 0)
            return -1;
    }
    assert(newlen <= self->size);

    /* Copy stuff. */
    memcpy(self->keys + self->len, from->keys + i, n * sizeof(KEY_TYPE));
    if (copyValues) {
        assert(self->values);
        assert(from->values);
        memcpy(self->values + self->len, from->values + i,
                n * sizeof(VALUE_TYPE));
    }
    self->len = newlen;

    /* Bump refcounts. */
#ifdef KEY_TYPE_IS_PYOBJECT
    {
        int j;
        PyObject **p = from->keys + i;
        for (j = 0; j < n; ++j, ++p) {
            Py_INCREF(*p);
        }
    }
#endif
#ifdef VALUE_TYPE_IS_PYOBJECT
    if (copyValues) {
        int j;
        PyObject **p = from->values + i;
        for (j = 0; j < n; ++j, ++p) {
            Py_INCREF(*p);
        }
    }
#endif
    return 0;
}
#endif /* MULTI_INT_UNION */

/*
** _bucket_set: Assign a value to a key in a bucket, delete a key+value
**  pair, or just insert a key.
**
** Arguments
**     self     The bucket
**     keyarg   The key to look for
**     v        The value to associate with key; NULL means delete the key.
**              If NULL, it's an error (KeyError) if the key isn't present.
**              Note that if this is a set bucket, and you want to insert
**              a new set element, v must be non-NULL although its exact
**              value will be ignored.  Passing Py_None is good for this.
**     unique   Boolean; when true, don't replace the value if the key is
**              already present.
**     noval    Boolean; when true, operate on keys only (ignore values)
**     changed  ignored on input
**
** Return
**     -1       on error
**      0       on success and the # of bucket entries didn't change
**      1       on success and the # of bucket entries did change
**  *changed    If non-NULL, set to 1 on any mutation of the bucket.
*/
static int
_bucket_set(Bucket *self, PyObject *keyarg, PyObject *v,
            int unique, int noval, int *changed)
{
    int i, cmp;
    KEY_TYPE key;
    int result = -1;    /* until proven innocent */
    int copied = 1;

    COPY_KEY_FROM_ARG(key, keyarg, copied);
    UNLESS(copied) return -1;

    PER_USE_OR_RETURN(self, -1);

    BUCKET_SEARCH(i, cmp, self, key, goto Done);
    if (cmp == 0) {
        VALUE_TYPE value;
        /* The key exists, at index i. */

        if (v) {
            /* The key exists at index i, and there's a new value.
             * If unique, we're not supposed to replace it.  If noval, or this
             * is a set bucket (self->values is NULL), there's nothing to do.
             */
            if (unique || noval || self->values == NULL) {
                result = 0;
                goto Done;
            }

            /* The key exists at index i, and we need to replace the value. */
            COPY_VALUE_FROM_ARG(value, v, copied);
            UNLESS(copied) goto Done;
#ifdef VALUE_SAME
            /* short-circuit if no change */
            if (VALUE_SAME(self->values[i], value)) {
                result = 0;
                goto Done;
            }
#endif
            if (changed)
                *changed = 1;
            DECREF_VALUE(self->values[i]);
            COPY_VALUE(self->values[i], value);
            INCREF_VALUE(self->values[i]);
            if (PER_CHANGED(self) >= 0)
                result = 0;
            goto Done;
        }

        /* The key exists at index i, and should be deleted. */
        DECREF_KEY(self->keys[i]);
        self->len--;
        if (i < self->len)
            memmove(self->keys + i, self->keys + i+1,
                    sizeof(KEY_TYPE)*(self->len - i));

        if (self->values) {
            DECREF_VALUE(self->values[i]);
            if (i < self->len)
                memmove(self->values + i, self->values + i+1,
                        sizeof(VALUE_TYPE)*(self->len - i));
        }

        if (! self->len) {
            self->size = 0;
            free(self->keys);
            self->keys = NULL;
            if (self->values) {
                free(self->values);
                self->values = NULL;
            }
        }

        if (changed)
            *changed = 1;
        if (PER_CHANGED(self) >= 0)
            result = 1;
        goto Done;
    }

    /* The key doesn't exist, and belongs at index i. */
    if (!v) {
        /* Can't delete a non-existent key. */
        PyErr_SetObject(PyExc_KeyError, keyarg);
        goto Done;
    }

    /* The key doesn't exist and should be inserted at index i. */
    if (self->len == self->size && Bucket_grow(self, -1, noval) < 0)
        goto Done;

    if (self->len > i) {
        memmove(self->keys + i+1, self->keys + i,
                sizeof(KEY_TYPE)*(self->len - i));
        if (self->values) {
            memmove(self->values + i+1, self->values + i,
                    sizeof(VALUE_TYPE)*(self->len - i));
        }
    }

    COPY_KEY(self->keys[i], key);
    INCREF_KEY(self->keys[i]);

    if (! noval) {
        COPY_VALUE_FROM_ARG(self->values[i], v, copied);
        UNLESS(copied) return -1;
        INCREF_VALUE(self->values[i]);
    }

    self->len++;
    if (changed)
        *changed = 1;
    if (PER_CHANGED(self) >= 0)
        result = 1;

Done:
    PER_ALLOW_DEACTIVATION(self);
    PER_ACCESSED(self);
    return result;
}

/*
** bucket_setitem
**
** wrapper for _bucket_setitem (eliminates +1 return code)
**
** Arguments:	self	The bucket
**		key	The key to insert under
**		v	The value to insert
**
** Returns	 0 	on success
**		-1	on failure
*/
static int
bucket_setitem(Bucket *self, PyObject *key, PyObject *v)
{
  if (_bucket_set(self, key, v, 0, 0, 0) < 0) return -1;
  return 0;
}

/**
 ** Mapping_update()
 **
 ** Accepts a sequence of 2-tuples or any object with an items()
 ** method that returns a sequence of 2-tuples.
 **
 */

static PyObject *
Mapping_update(PyObject *self, PyObject *args)
{
  PyObject *seq=0, *o, *t, *v, *tb, *k, *items = NULL;
  int i, ind;

  UNLESS(PyArg_ParseTuple(args, "|O:update", &seq)) return NULL;

  if (!seq)
    {
      Py_INCREF(Py_None);
      return Py_None;
    }

  if (!PySequence_Check(seq))
    {
      items = PyObject_GetAttr(seq, items_str);
      UNLESS(items) return NULL;
      ASSIGN(items, PyObject_CallObject(items, NULL));
      UNLESS(items) return NULL;
      /* items is DECREFed on exit, seq is not */
      seq = items;
    }

  for (i=0; ; i++)
    {
      o = PySequence_GetItem(seq, i);
      UNLESS (o)
	{
	  PyErr_Fetch(&t, &v, &tb);
	  if (t != PyExc_IndexError)
	    {
	      PyErr_Restore(t, v, tb);
	      goto err;
	    }
	  Py_XDECREF(t);
	  Py_XDECREF(v);
	  Py_XDECREF(tb);
	  break;
	}
      ind = PyArg_ParseTuple(o, "OO;items must be 2-item tuples", &k, &v);
      if (ind)
	ind = PyObject_SetItem(self, k, v);
      else
	ind = -1;
      Py_DECREF(o);
      if (ind < 0) {
        PyErr_SetString(PyExc_TypeError,"Sequence must contain 2-item tuples");
        goto err;
        }
    }

  Py_XDECREF(items);
  Py_INCREF(Py_None);
  return Py_None;

 err:
  Py_XDECREF(items);
  return NULL;
}


/*
** bucket_split
**
** Splits one bucket into two
**
** Arguments:	self	The bucket
**		index	the index of the key to split at (O.O.B use midpoint)
**		next	the new bucket to split into
**
** Returns:	 0	on success
**		-1	on failure
*/
static int
bucket_split(Bucket *self, int index, Bucket *next)
{
  int next_size;

  ASSERT(self->len > 1, "split of empty bucket", -1);

  if (index < 0 || index >= self->len)
    index = self->len / 2;

  next_size = self->len - index;

  next->keys = PyMalloc(sizeof(KEY_TYPE) * next_size);
  if (!next->keys)
      return -1;
  memcpy(next->keys, self->keys + index, sizeof(KEY_TYPE) * next_size);
  if (self->values)
    {
      next->values = PyMalloc(sizeof(VALUE_TYPE) * next_size);
      if (!next->values)
        {
          free(next->keys);
          next->keys = NULL;
          return -1;
        }
      memcpy(next->values, self->values + index,
             sizeof(VALUE_TYPE) * next_size);
    }
  next->size = next_size;
  next->len = next_size;
  self->len = index;

  next->next = self->next;

  Py_INCREF(next);
  self->next = next;

  if (PER_CHANGED(self) < 0)
    return -1;

  return 0;
}

/* Set self->next to self->next->next, i.e. unlink self's successor from
 * the chain.
 *
 * Return:
 *     -1       error
 *      0       OK
 */
static int
Bucket_deleteNextBucket(Bucket *self)
{
    int result = -1;    /* until proven innocent */
    Bucket *successor;

    PER_USE_OR_RETURN(self, -1);
    successor = self->next;
    if (successor) {
        Bucket *next;
        /* Before:  self -> successor -> next
         * After:   self --------------> next
         */
        UNLESS (PER_USE(successor)) goto Done;
        next = successor->next;
        PER_UNUSE(successor);

        Py_XINCREF(next);       /* it may be NULL, of course */
        self->next = next;
        Py_DECREF(successor);
	if (PER_CHANGED(self) < 0)
	    goto Done;
    }
    result = 0;

Done:
    PER_UNUSE(self);
    return result;
}

/*
 Bucket_findRangeEnd -- Find the index of a range endpoint
 (possibly) contained in a bucket.

 Arguments:     self        The bucket
                keyarg      The key to match against
                low         Boolean; true for low end of range, false for high
                offset      The output offset

 If low true, *offset <- index of the smallest item >= key,
 if low false the index of the largest item <= key.  In either case, if there
 is no such index, *offset is left alone and 0 is returned.

 Return:
      0     No suitable index exists; *offset has not been changed
      1     The correct index was stored into *offset
     -1     Error

 Example:  Suppose the keys are [2, 4].  Searching for 2 sets *offset to 0 and
 returns 1 regardless of low.  Searching for 4 sets *offset to 1 and returns
 1 regardless of low.
 Searching for 1:
     If low true, sets *offset to 0, returns 1.
     If low false, returns 0.
 Searching for 3:
     If low true, sets *offset to 1, returns 1.
     If low false, sets *offset to 0, returns 1.
 Searching for 5:
     If low true, returns 0.
     If low false, sets *offset to 1, returns 1.
 */
static int
Bucket_findRangeEnd(Bucket *self, PyObject *keyarg, int low, int *offset)
{
    int i, cmp;
    int result = -1;    /* until proven innocent */
    KEY_TYPE key;
    int copied = 1;

    COPY_KEY_FROM_ARG(key, keyarg, copied);
    UNLESS (copied) return -1;

    PER_USE_OR_RETURN(self, -1);

    BUCKET_SEARCH(i, cmp, self, key, goto Done);
    if (cmp == 0)   /* exact match at index i */
        result = 1;

    /* Else keys[i-1] < key < keys[i], picturing infinities at OOB indices */
    else if (low)   /* i has the smallest item > key, unless i is OOB */
        result = i < self->len;

    else {          /* i-1 has the largest item < key, unless i-1 is 0OB */
        --i;
        result = i >= 0;
    }

    if (result)
        *offset = i;

Done:
    PER_ALLOW_DEACTIVATION(self);
    PER_ACCESSED(self);
    return result;
}

static PyObject *
Bucket_maxminKey(Bucket *self, PyObject *args, int min)
{
  PyObject *key=0;
  int rc, offset;

  if (args && ! PyArg_ParseTuple(args, "|O", &key)) return NULL;

  PER_USE_OR_RETURN(self, NULL);

  UNLESS (self->len) goto empty;

  /* Find the low range */
  if (key)
    {
      if ((rc = Bucket_findRangeEnd(self, key, min, &offset)) <= 0)
        {
          if (rc < 0) return NULL;
          goto empty;
        }
    }
  else if (min) offset = 0;
  else offset = self->len -1;

  COPY_KEY_TO_OBJECT(key, self->keys[offset]);
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);

  return key;

 empty:
  PyErr_SetString(PyExc_ValueError, "empty bucket");
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  return NULL;
}

static PyObject *
Bucket_minKey(Bucket *self, PyObject *args)
{
  return Bucket_maxminKey(self, args, 1);
}

static PyObject *
Bucket_maxKey(Bucket *self, PyObject *args)
{
  return Bucket_maxminKey(self, args, 0);
}

static int
Bucket_rangeSearch(Bucket *self, PyObject *args, int *low, int *high)
{
  PyObject *f=0, *l=0;
  int rc;

  if (args && ! PyArg_ParseTuple(args,"|OO",&f, &l)) return -1;

  UNLESS (self->len) goto empty;

  /* Find the low range */
  if (f && f != Py_None)
    {
      UNLESS (rc = Bucket_findRangeEnd(self, f, 1, low))
        {
          if (rc < 0) return -1;
          goto empty;
        }
    }
  else *low = 0;

  /* Find the high range */
  if (l && l != Py_None)
    {
      UNLESS (rc = Bucket_findRangeEnd(self, l, 0, high))
        {
          if (rc < 0) return -1;
          goto empty;
        }
    }
  else *high = self->len - 1;

  /* If f < l to begin with, it's quite possible that low > high now. */
  if (*low <= *high)
    return 0;

 empty:
  *low = 0;
  *high = -1;
  return 0;
}

/*
** bucket_keys
**
** Generate a list of all keys in the bucket
**
** Arguments:	self	The Bucket
**		args	(unused)
**
** Returns:	list of bucket keys
*/
static PyObject *
bucket_keys(Bucket *self, PyObject *args)
{
  PyObject *r=0, *key;
  int i, low, high;

  PER_USE_OR_RETURN(self, NULL);

  if (Bucket_rangeSearch(self, args, &low, &high) < 0) goto err;

  UNLESS (r=PyList_New(high-low+1)) goto err;

  for (i=low; i <= high; i++)
    {
      COPY_KEY_TO_OBJECT(key, self->keys[i]);
      if (PyList_SetItem(r, i-low , key) < 0) goto err;
    }

  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  return r;

 err:
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  Py_XDECREF(r);
  return NULL;
}

/*
** bucket_values
**
** Generate a list of all values in the bucket
**
** Arguments:	self	The Bucket
**		args	(unused)
**
** Returns	list of values
*/
static PyObject *
bucket_values(Bucket *self, PyObject *args)
{
  PyObject *r=0, *v;
  int i, low, high;

  PER_USE_OR_RETURN(self, NULL);

  if (Bucket_rangeSearch(self, args, &low, &high) < 0) goto err;

  UNLESS (r=PyList_New(high-low+1)) goto err;

  for (i=low; i <= high; i++)
    {
      COPY_VALUE_TO_OBJECT(v, self->values[i]);
      UNLESS (v) goto err;
      if (PyList_SetItem(r, i-low, v) < 0) goto err;
    }

  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  return r;

 err:
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  Py_XDECREF(r);
  return NULL;
}

/*
** bucket_items
**
** Returns a list of all items in a bucket
**
** Arguments:	self	The Bucket
**		args	(unused)
**
** Returns:	list of all items in the bucket
*/
static PyObject *
bucket_items(Bucket *self, PyObject *args)
{
  PyObject *r=0, *o=0, *item=0;
  int i, low, high;

  PER_USE_OR_RETURN(self, NULL);

  if (Bucket_rangeSearch(self, args, &low, &high) < 0) goto err;

  UNLESS (r=PyList_New(high-low+1)) goto err;

  for (i=low; i <= high; i++)
    {
      UNLESS (item = PyTuple_New(2)) goto err;

      COPY_KEY_TO_OBJECT(o, self->keys[i]);
      UNLESS (o) goto err;
      PyTuple_SET_ITEM(item, 0, o);

      COPY_VALUE_TO_OBJECT(o, self->values[i]);
      UNLESS (o) goto err;
      PyTuple_SET_ITEM(item, 1, o);

      if (PyList_SetItem(r, i-low, item) < 0) goto err;

      item = 0;
    }

  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  return r;

 err:
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  Py_XDECREF(r);
  Py_XDECREF(item);
  return NULL;
}

static PyObject *
bucket_byValue(Bucket *self, PyObject *args)
{
  PyObject *r=0, *o=0, *item=0, *omin;
  VALUE_TYPE min;
  VALUE_TYPE v;
  int i, l, copied=1;

  PER_USE_OR_RETURN(self, NULL);

  UNLESS (PyArg_ParseTuple(args, "O", &omin)) return NULL;
  COPY_VALUE_FROM_ARG(min, omin, copied);
  UNLESS(copied) return NULL;

  for (i=0, l=0; i < self->len; i++)
    if (TEST_VALUE(self->values[i], min) >= 0)
      l++;

  UNLESS (r=PyList_New(l)) goto err;

  for (i=0, l=0; i < self->len; i++)
    {
      if (TEST_VALUE(self->values[i], min) < 0) continue;

      UNLESS (item = PyTuple_New(2)) goto err;

      COPY_KEY_TO_OBJECT(o, self->keys[i]);
      UNLESS (o) goto err;
      PyTuple_SET_ITEM(item, 1, o);

      COPY_VALUE(v, self->values[i]);
      NORMALIZE_VALUE(v, min);
      COPY_VALUE_TO_OBJECT(o, v);
      DECREF_VALUE(v);
      UNLESS (o) goto err;
      PyTuple_SET_ITEM(item, 0, o);

      if (PyList_SetItem(r, l, item) < 0) goto err;
      l++;

      item = 0;
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

  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  return r;

 err:
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  Py_XDECREF(r);
  Py_XDECREF(item);
  return NULL;
}

static int
_bucket_clear(Bucket *self)
{
    const int len = self->len;
    /* Don't declare i at this level.  If neither keys nor values are
     * PyObject*, i won't be referenced, and you'll get a nuisance compiler
     * wng for declaring it here.
     */
    self->len = self->size = 0;

    if (self->next) {
        Py_DECREF(self->next);
        self->next = NULL;
    }

    /* Silence compiler warning about unused variable len for the case
       when neither key nor value is an object, i.e. II. */
    (void)len;

    if (self->keys) {
#ifdef KEY_TYPE_IS_PYOBJECT
        int i;
        for (i = 0; i < len; ++i)
            DECREF_KEY(self->keys[i]);
#endif
        free(self->keys);
        self->keys = NULL;
    }

    if (self->values) {
#ifdef VALUE_TYPE_IS_PYOBJECT
        int i;
        for (i = 0; i < len; ++i)
            DECREF_VALUE(self->values[i]);
#endif
        free(self->values);
        self->values = NULL;
    }
    return 0;
}

#ifdef PERSISTENT
static PyObject *
bucket__p_deactivate(Bucket *self, PyObject *args)
{
  if (self->state==cPersistent_UPTODATE_STATE && self->jar)
    {
      if (_bucket_clear(self) < 0) return NULL;
      PER_GHOSTIFY(self);
    }

  Py_INCREF(Py_None);
  return Py_None;
}
#endif

static PyObject *
bucket_clear(Bucket *self, PyObject *args)
{
  PER_USE_OR_RETURN(self, NULL);

  if (self->len)
    {
      if (_bucket_clear(self) < 0) return NULL;
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
 * For a set bucket (self->values is NULL), a one-tuple or two-tuple.  The
 * first element is a tuple of keys, of length self->len.  The second element
 * is the next bucket, present if and only if next is non-NULL:
 *
 *     (
 *          (keys[0], keys[1], ..., keys[len-1]),
 *          <self->next iff non-NULL>
 *     )
 *
 * For a mapping bucket (self->values is not NULL), a one-tuple or two-tuple.
 * The first element is a tuple interleaving keys and values, of length
 * 2 * self->len.  The second element is the next bucket, present iff next is
 * non-NULL:
 *
 *     (
 *          (keys[0], values[0], keys[1], values[1], ...,
 *                               keys[len-1], values[len-1]),
 *          <self->next iff non-NULL>
 *     )
 */
static PyObject *
bucket_getstate(Bucket *self, PyObject *args)
{
  PyObject *o=0, *items=0;
  int i, len, l;

  if (args && ! PyArg_ParseTuple(args, "")) return NULL;

  PER_USE_OR_RETURN(self, NULL);

  len=self->len;

  if (self->values)
    {                           /* Bucket */
      UNLESS (items=PyTuple_New(len*2)) goto err;
      for (i=0, l=0; i < len; i++)
        {
          COPY_KEY_TO_OBJECT(o, self->keys[i]);
          UNLESS (o) goto err;
          PyTuple_SET_ITEM(items, l, o);
          l++;

          COPY_VALUE_TO_OBJECT(o, self->values[i]);
          UNLESS (o) goto err;
          PyTuple_SET_ITEM(items, l, o);
          l++;
        }
    }
  else
    {                           /* Set */
      UNLESS (items=PyTuple_New(len)) goto err;
      for (i=0; i < len; i++)
        {
          COPY_KEY_TO_OBJECT(o, self->keys[i]);
          UNLESS (o) goto err;
          PyTuple_SET_ITEM(items, i, o);
        }
    }

  if (self->next)
    ASSIGN(items, Py_BuildValue("OO", items, self->next));
  else
    ASSIGN(items, Py_BuildValue("(O)", items));

  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);

  return items;

err:
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  Py_XDECREF(items);
  return NULL;
}

static int
_bucket_setstate(Bucket *self, PyObject *args)
{
  PyObject *k, *v, *items;
  Bucket *next=0;
  int i, l, len, copied=1;
  KEY_TYPE *keys;
  VALUE_TYPE *values;

  UNLESS (PyArg_ParseTuple(args, "O|O", &items, &next))
    return -1;

  if ((len=PyTuple_Size(items)) < 0) return -1;
  len /= 2;

  for (i=self->len; --i >= 0; )
    {
      DECREF_KEY(self->keys[i]);
      DECREF_VALUE(self->values[i]);
    }
  self->len=0;

  if (self->next)
    {
      Py_DECREF(self->next);
      self->next=0;
    }

  if (len > self->size)
    {
      UNLESS (keys=PyRealloc(self->keys, sizeof(KEY_TYPE)*len))
        return -1;
      UNLESS (values=PyRealloc(self->values, sizeof(VALUE_TYPE)*len))
        return -1;
      self->keys=keys;
      self->values=values;
      self->size=len;
    }

  for (i=0, l=0; i<len; i++)
    {
      k=PyTuple_GET_ITEM(items, l);
      l++;
      v=PyTuple_GET_ITEM(items, l);
      l++;

      COPY_KEY_FROM_ARG(self->keys[i], k, copied);
      UNLESS (copied) return -1;
      COPY_VALUE_FROM_ARG(self->values[i], v, copied);
      UNLESS (copied) return -1;
      INCREF_KEY(self->keys[i]);
      INCREF_VALUE(self->values[i]);
    }

  self->len=len;

  if (next)
    {
      self->next=next;
      Py_INCREF(next);
    }

  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);

  return 0;
}

static PyObject *
bucket_setstate(Bucket *self, PyObject *args)
{
  int r;

  UNLESS (PyArg_ParseTuple(args, "O", &args)) return NULL;

  PER_PREVENT_DEACTIVATION(self);
  r=_bucket_setstate(self, args);
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);

  if (r < 0) return NULL;
  Py_INCREF(Py_None);
  return Py_None;
}

/*
** bucket_has_key
**
*/
static PyObject *
bucket_has_key(Bucket *self, PyObject *args)
{
  PyObject *key;

  UNLESS (PyArg_ParseTuple(args,"O",&key)) return NULL;
  return _bucket_get(self, key, 1);
}

/*
** bucket_getm
**
*/
static PyObject *
bucket_getm(Bucket *self, PyObject *args)
{
  PyObject *key, *d=Py_None, *r;

  UNLESS (PyArg_ParseTuple(args, "O|O", &key, &d)) return NULL;
  if ((r=_bucket_get(self, key, 0))) return r;
  UNLESS (PyErr_ExceptionMatches(PyExc_KeyError)) return NULL;
  PyErr_Clear();
  Py_INCREF(d);
  return d;
}

#ifdef PERSISTENT
static PyObject *merge_error(int p1, int p2, int p3, int reason);
static PyObject *bucket_merge(Bucket *s1, Bucket *s2, Bucket *s3);

static PyObject *
_bucket__p_resolveConflict(PyObject *ob_type, PyObject *s[3])
{
  PyObject *r=0, *a;
  Bucket *b[3];
  int i;

  for (i=0; i < 3; i++)
    {
      if ((b[i]=(Bucket*)PyObject_CallObject(OBJECT(ob_type), NULL)))
        {
          if ((s[i] == Py_None))  /* None is equivalent to empty, for BTrees */
            continue;
          ASSIGN(r, PyObject_GetAttr(OBJECT(b[i]), __setstate___str));
          if ((a=PyTuple_New(1)))
            {
              if (r)
                {
                  PyTuple_SET_ITEM(a, 0, s[i]);
                  Py_INCREF(s[i]);
                  ASSIGN(r, PyObject_CallObject(r, a));
                }
              Py_DECREF(a);
              if (r) continue;
            }
        }
      /* This is not the expected path.  It's the error exit path! */
      Py_XDECREF(r);
      while (--i >= 0)
        {
          Py_DECREF(b[i]);
        }
      return NULL;
    }
  Py_DECREF(r);
  r=NULL;

  if (b[0]->next != b[1]->next || b[0]->next != b[2]->next)
    {
      merge_error(-1, -1, -1, -1);
      goto err;
    }

  r=bucket_merge(b[0], b[1], b[2]);

 err:
  Py_DECREF(b[0]);
  Py_DECREF(b[1]);
  Py_DECREF(b[2]);

  if (r == NULL) {
  	PyObject *error;
  	PyObject *value;
  	PyObject *traceback;

  	PyErr_Fetch(&error, &value, &traceback);
	Py_INCREF(ConflictError);
	Py_XDECREF(error);
	PyErr_Restore(ConflictError, value, traceback);
  }

  return r;
}

static PyObject *
bucket__p_resolveConflict(Bucket *self, PyObject *args)
{
  PyObject *s[3];

  UNLESS(PyArg_ParseTuple(args, "OOO", &s[0], &s[1], &s[2])) return NULL;

  return _bucket__p_resolveConflict(OBJECT(self->ob_type), s);
}
#endif

static struct PyMethodDef Bucket_methods[] = {
  {"__getstate__", (PyCFunction) bucket_getstate,	METH_VARARGS,
   "__getstate__() -- Return the picklable state of the object"},
  {"__setstate__", (PyCFunction) bucket_setstate,	METH_VARARGS,
   "__setstate__() -- Set the state of the object"},
  {"keys",	(PyCFunction) bucket_keys,	METH_VARARGS,
     "keys([min, max]) -- Return the keys"},
  {"has_key",	(PyCFunction) bucket_has_key,	METH_VARARGS,
     "has_key(key) -- Test whether the bucket contains the given key"},
  {"clear",	(PyCFunction) bucket_clear,	METH_VARARGS,
   "clear() -- Remove all of the items from the bucket"},
  {"update",	(PyCFunction) Mapping_update,	METH_VARARGS,
   "update(collection) -- Add the items from the given collection"},
  {"__init__",	(PyCFunction) Mapping_update,	METH_VARARGS,
   "__init__(collection) -- Initialize with items from the given collection"},
  {"maxKey", (PyCFunction) Bucket_maxKey,	METH_VARARGS,
   "maxKey([key]) -- Find the maximum key\n\n"
   "If an argument is given, find the maximum <= the argument"},
  {"minKey", (PyCFunction) Bucket_minKey,	METH_VARARGS,
   "minKey([key]) -- Find the minimum key\n\n"
   "If an argument is given, find the minimum >= the argument"},
  {"values",	(PyCFunction) bucket_values,	METH_VARARGS,
     "values([min, max]) -- Return the values"},
  {"items",	(PyCFunction) bucket_items,	METH_VARARGS,
     "items([min, max])) -- Return the items"},
  {"byValue",	(PyCFunction) bucket_byValue,	METH_VARARGS,
   "byValue(min) -- "
   "Return value-keys with values >= min and reverse sorted by values"
  },
  {"get",	(PyCFunction) bucket_getm,	METH_VARARGS,
   "get(key[,default]) -- Look up a value\n\n"
   "Return the default (or None) if the key is not found."
  },
#ifdef PERSISTENT
  {"_p_resolveConflict", (PyCFunction) bucket__p_resolveConflict, METH_VARARGS,
   "_p_resolveConflict() -- Reinitialize from a newly created copy"},
  {"_p_deactivate", (PyCFunction) bucket__p_deactivate, METH_VARARGS,
   "_p_deactivate() -- Reinitialize from a newly created copy"},
#endif
  {NULL,		NULL}		/* sentinel */
};

static void
Bucket_dealloc(Bucket *self)
{
    if (self->state != cPersistent_GHOST_STATE)
	_bucket_clear(self);

    PER_DEL(self);

    Py_DECREF(self->ob_type);
    PyObject_Del(self);
}

/* Code to access Bucket objects as mappings */
static int
Bucket_length( Bucket *self)
{
  int r;
  PER_USE_OR_RETURN(self, -1);
  r=self->len;
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  return r;
}

static PyMappingMethods Bucket_as_mapping = {
  (inquiry)Bucket_length,		/*mp_length*/
  (binaryfunc)bucket_getitem,		/*mp_subscript*/
  (objobjargproc)bucket_setitem,	/*mp_ass_subscript*/
};

static PyObject *
bucket_repr(Bucket *self)
{
  static PyObject *format;
  PyObject *r, *t;

  UNLESS (format) UNLESS (format=PyString_FromString(MOD_NAME_PREFIX "Bucket(%s)"))
    return NULL;
  UNLESS (t=PyTuple_New(1)) return NULL;
  UNLESS (r=bucket_items(self,NULL)) goto err;
  PyTuple_SET_ITEM(t,0,r);
  r=t;
  ASSIGN(r,PyString_Format(format,r));
  return r;
err:
  Py_DECREF(t);
  return NULL;
}

static PyExtensionClass BucketType = {
  PyObject_HEAD_INIT(NULL)
  0,				/*ob_size*/
  MOD_NAME_PREFIX "Bucket",			/*tp_name*/
  sizeof(Bucket),		/*tp_basicsize*/
  0,				/*tp_itemsize*/
  /*********** methods ***********************/
  (destructor) Bucket_dealloc,	/*tp_dealloc*/
  (printfunc)0,			/*tp_print*/
  (getattrfunc)0,		/*obsolete tp_getattr*/
  (setattrfunc)0,		/*obsolete tp_setattr*/
  (cmpfunc)0,			/*tp_compare*/
  (reprfunc) bucket_repr,	/*tp_repr*/
  0,				/*tp_as_number*/
  0,				/*tp_as_sequence*/
  &Bucket_as_mapping,		/*tp_as_mapping*/
  (hashfunc)0,			/*tp_hash*/
  (ternaryfunc)0,		/*tp_call*/
  (reprfunc)0,			/*tp_str*/
  (getattrofunc)0,		/*tp_getattro*/
  0,				/*tp_setattro*/

  /* Space for future expansion */
  0L,0L,
  "Mapping type implemented as sorted list of items",
  METHOD_CHAIN(Bucket_methods),
  EXTENSIONCLASS_BASICNEW_FLAG
#ifdef PERSISTENT
  | PERSISTENT_TYPE_FLAG
#endif
  | EXTENSIONCLASS_NOINSTDICT_FLAG,
};


static int
nextBucket(SetIteration *i)
{
  if (i->position >= 0)
    {
      UNLESS(PER_USE(BUCKET(i->set))) return -1;

      if (i->position)
        {
          DECREF_KEY(i->key);
          DECREF_VALUE(i->value);
        }

      if (i->position < BUCKET(i->set)->len)
        {
          COPY_KEY(i->key, BUCKET(i->set)->keys[i->position]);
          INCREF_KEY(i->key);
          COPY_VALUE(i->value, BUCKET(i->set)->values[i->position]);
          INCREF_VALUE(i->value);
          i->position ++;
        }
      else
        {
          i->position = -1;
          PER_ACCESSED(BUCKET(i->set));
        }

      PER_ALLOW_DEACTIVATION(BUCKET(i->set));
    }


  return 0;
}
