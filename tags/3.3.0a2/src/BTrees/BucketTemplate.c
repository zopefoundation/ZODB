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

#define BUCKETTEMPLATE_C "$Id: BucketTemplate.c,v 1.55 2003/11/28 16:44:44 jim Exp $\n"

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
**         Note that has_key should be true when searching set buckets.
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

    UNLESS (PER_USE(self)) return NULL;

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
    PER_UNUSE(self);
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

    if (self->size) {
        if (newsize < 0)
            newsize = self->size * 2;
        if (newsize < 0)    /* int overflow */
            goto Overflow;
        UNLESS (keys = BTree_Realloc(self->keys, sizeof(KEY_TYPE) * newsize))
            return -1;

        UNLESS (noval) {
            values = BTree_Realloc(self->values, sizeof(VALUE_TYPE) * newsize);
            if (values == NULL) {
                free(keys);
                return -1;
            }
            self->values = values;
        }
        self->keys = keys;
    }
    else {
        if (newsize < 0)
            newsize = MIN_BUCKET_ALLOC;
        UNLESS (self->keys = BTree_Malloc(sizeof(KEY_TYPE) * newsize))
            return -1;
        UNLESS (noval) {
            self->values = BTree_Malloc(sizeof(VALUE_TYPE) * newsize);
            if (self->values == NULL) {
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

    /* Subtle:  there may or may not be a value.  If there is, we need to
     * check its type early, so that in case of error we can get out before
     * mutating the bucket.  But because value isn't used on all paths, if
     * we don't initialize value then gcc gives a nuisance complaint that
     * value may be used initialized (it can't be, but gcc doesn't know
     * that).  So we initialize it.  However, VALUE_TYPE can be various types,
     * including int, PyObject*, and char[6], so it's a puzzle to spell
     * initialization.  It so happens that {0} is a valid initializer for all
     * these types.
     */
    VALUE_TYPE value = {0};	/* squash nuisance warning */
    int result = -1;    /* until proven innocent */
    int copied = 1;

    COPY_KEY_FROM_ARG(key, keyarg, copied);
    UNLESS(copied) return -1;

    /* Copy the value early (if needed), so that in case of error a
     * pile of bucket mutations don't need to be undone.
     */
    if (v && !noval) {
    	COPY_VALUE_FROM_ARG(value, v, copied);
    	UNLESS(copied) return -1;
    }

    UNLESS (PER_USE(self)) return -1;

    BUCKET_SEARCH(i, cmp, self, key, goto Done);
    if (cmp == 0) {
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
        memmove(self->keys + i + 1, self->keys + i,
                sizeof(KEY_TYPE) * (self->len - i));
        if (self->values) {
            memmove(self->values + i + 1, self->values + i,
                    sizeof(VALUE_TYPE) * (self->len - i));
        }
    }

    COPY_KEY(self->keys[i], key);
    INCREF_KEY(self->keys[i]);

    if (! noval) {
        COPY_VALUE(self->values[i], value);
        INCREF_VALUE(self->values[i]);
    }

    self->len++;
    if (changed)
        *changed = 1;
    if (PER_CHANGED(self) >= 0)
        result = 1;

Done:
    PER_UNUSE(self);
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
    if (_bucket_set(self, key, v, 0, 0, 0) < 0)
	return -1;
    return 0;
}

/**
 ** Accepts a sequence of 2-tuples, or any object with an items()
 ** method that returns an iterable object producing 2-tuples.
 */
static int
update_from_seq(PyObject *map, PyObject *seq)
{
    PyObject *iter, *o, *k, *v;
    int err = -1;

    /* One path creates a new seq object.  The other path has an
       INCREF of the seq argument.  So seq must always be DECREFed on
       the way out.
     */
    if (!PySequence_Check(seq)) {
	PyObject *items;
	items = PyObject_GetAttrString(seq, "items");
	if (items == NULL)
	    return -1;
	seq = PyObject_CallObject(items, NULL);
	Py_DECREF(items);
	if (seq == NULL)
	    return -1;
    } else
	Py_INCREF(seq);

    iter = PyObject_GetIter(seq);
    if (iter == NULL)
	goto err;
    while (1) {
	o = PyIter_Next(iter);
	if (o == NULL) {
	    if (PyErr_Occurred())
		goto err;
	    else
		break;
	}
	if (!PyTuple_Check(o) || PyTuple_GET_SIZE(o) != 2) {
	    Py_DECREF(o);
	    PyErr_SetString(PyExc_TypeError,
			    "Sequence must contain 2-item tuples");
	    goto err;
	}
	k = PyTuple_GET_ITEM(o, 0);
	v = PyTuple_GET_ITEM(o, 1);
	if (PyObject_SetItem(map, k, v) < 0) {
	    Py_DECREF(o);
	    goto err;
	}
	Py_DECREF(o);
    }

    err = 0;
 err:
    Py_DECREF(iter);
    Py_DECREF(seq);
    return err;
}

static PyObject *
Mapping_update(PyObject *self, PyObject *seq)
{
    if (update_from_seq(self, seq) < 0)
	return NULL;
    Py_INCREF(Py_None);
    return Py_None;
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

    next->keys = BTree_Malloc(sizeof(KEY_TYPE) * next_size);
    if (!next->keys)
	return -1;
    memcpy(next->keys, self->keys + index, sizeof(KEY_TYPE) * next_size);
    if (self->values) {
	next->values = BTree_Malloc(sizeof(VALUE_TYPE) * next_size);
	if (!next->values) {
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
                exclude_equal  Boolean; if true, don't accept an exact match,
                	       and if there is one then move right if low and
                	       left if !low.
                offset      The output offset

 If low true, *offset <- index of the smallest item >= key,
 if low false the index of the largest item <= key.  In either case, if there
 is no such index, *offset is left alone and 0 is returned.

 Return:
      0     No suitable index exists; *offset has not been changed
      1     The correct index was stored into *offset
     -1     Error

 Example:  Suppose the keys are [2, 4], and exclude_equal is false.  Searching
 for 2 sets *offset to 0 and returns 1 regardless of low.  Searching for 4
 sets *offset to 1 and returns 1 regardless of low.
 Searching for 1:
     If low true, sets *offset to 0, returns 1.
     If low false, returns 0.
 Searching for 3:
     If low true, sets *offset to 1, returns 1.
     If low false, sets *offset to 0, returns 1.
 Searching for 5:
     If low true, returns 0.
     If low false, sets *offset to 1, returns 1.

 The 1, 3 and 5 examples are the same when exclude_equal is true.
 */
static int
Bucket_findRangeEnd(Bucket *self, PyObject *keyarg, int low, int exclude_equal,
		    int *offset)
{
    int i, cmp;
    int result = -1;    /* until proven innocent */
    KEY_TYPE key;
    int copied = 1;

    COPY_KEY_FROM_ARG(key, keyarg, copied);
    UNLESS (copied) return -1;

    UNLESS (PER_USE(self)) return -1;

    BUCKET_SEARCH(i, cmp, self, key, goto Done);
    if (cmp == 0) {
    	/* exact match at index i */
    	if (exclude_equal) {
	    /* but we don't want an exact match */
            if (low)
                ++i;
            else
                --i;
        }
    }
    /* Else keys[i-1] < key < keys[i], picturing infinities at OOB indices,
     * and i has the smallest item > key, which is correct for low.
     */
    else if (! low)
        /* i-1 has the largest item < key (unless i-1 is 0OB) */
        --i;

    result = 0 <= i && i < self->len;
    if (result)
        *offset = i;

Done:
  PER_UNUSE(self);
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
      if ((rc = Bucket_findRangeEnd(self, key, min, 0, &offset)) <= 0)
        {
          if (rc < 0) return NULL;
          goto empty;
        }
    }
  else if (min) offset = 0;
  else offset = self->len -1;

  COPY_KEY_TO_OBJECT(key, self->keys[offset]);
  PER_UNUSE(self);

  return key;

 empty:
  PyErr_SetString(PyExc_ValueError, "empty bucket");
  PER_UNUSE(self);
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
Bucket_rangeSearch(Bucket *self, PyObject *args, PyObject *kw,
		   int *low, int *high)
{
    PyObject *min = Py_None;
    PyObject *max = Py_None;
    int excludemin = 0;
    int excludemax = 0;
    int rc;

    if (args) {
        if (! PyArg_ParseTupleAndKeywords(args, kw, "|OOii", search_keywords,
        				  &min,
        				  &max,
        				  &excludemin,
        				  &excludemax))
	    return -1;
    }

    UNLESS (self->len) goto empty;

    /* Find the low range */
    if (min != Py_None) {
        UNLESS (rc = Bucket_findRangeEnd(self, min, 1, excludemin, low)) {
            if (rc < 0) return -1;
            goto empty;
        }
    }
    else {
    	*low = 0;
    	if (excludemin) {
    	    if (self->len < 2)
    	    	goto empty;
    	    ++*low;
    	}
    }

    /* Find the high range */
    if (max != Py_None) {
        UNLESS (rc = Bucket_findRangeEnd(self, max, 0, excludemax, high)) {
            if (rc < 0) return -1;
            goto empty;
	}
    }
    else {
	*high = self->len - 1;
	if (excludemax) {
	    if (self->len < 2)
	    	goto empty;
	    --*high;
	}
    }

    /* If min < max to begin with, it's quite possible that low > high now. */
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
bucket_keys(Bucket *self, PyObject *args, PyObject *kw)
{
  PyObject *r = NULL, *key;
  int i, low, high;

  PER_USE_OR_RETURN(self, NULL);

  if (Bucket_rangeSearch(self, args, kw, &low, &high) < 0)
      goto err;

  r = PyList_New(high-low+1);
  if (r == NULL)
      goto err;

  for (i=low; i <= high; i++) {
      COPY_KEY_TO_OBJECT(key, self->keys[i]);
      if (PyList_SetItem(r, i-low , key) < 0)
	  goto err;
  }

  PER_UNUSE(self);
  return r;

 err:
  PER_UNUSE(self);
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
bucket_values(Bucket *self, PyObject *args, PyObject *kw)
{
  PyObject *r=0, *v;
  int i, low, high;

  PER_USE_OR_RETURN(self, NULL);

  if (Bucket_rangeSearch(self, args, kw, &low, &high) < 0) goto err;

  UNLESS (r=PyList_New(high-low+1)) goto err;

  for (i=low; i <= high; i++)
    {
      COPY_VALUE_TO_OBJECT(v, self->values[i]);
      UNLESS (v) goto err;
      if (PyList_SetItem(r, i-low, v) < 0) goto err;
    }

  PER_UNUSE(self);
  return r;

 err:
  PER_UNUSE(self);
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
bucket_items(Bucket *self, PyObject *args, PyObject *kw)
{
  PyObject *r=0, *o=0, *item=0;
  int i, low, high;

  PER_USE_OR_RETURN(self, NULL);

  if (Bucket_rangeSearch(self, args, kw, &low, &high) < 0) goto err;

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

  PER_UNUSE(self);
  return r;

 err:
  PER_UNUSE(self);
  Py_XDECREF(r);
  Py_XDECREF(item);
  return NULL;
}

static PyObject *
bucket_byValue(Bucket *self, PyObject *omin)
{
  PyObject *r=0, *o=0, *item=0;
  VALUE_TYPE min;
  VALUE_TYPE v;
  int i, l, copied=1;

  PER_USE_OR_RETURN(self, NULL);

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

  PER_UNUSE(self);
  return r;

 err:
  PER_UNUSE(self);
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
bucket__p_deactivate(Bucket *self, PyObject *args, PyObject *keywords)
{
    int ghostify = 1;
    PyObject *force = NULL;

    if (args && PyTuple_GET_SIZE(args) > 0) {
	PyErr_SetString(PyExc_TypeError,
			"_p_deactivate takes not positional arguments");
	return NULL;
    }
    if (keywords) {
	int size = PyDict_Size(keywords);
	force = PyDict_GetItemString(keywords, "force");
	if (force)
	    size--;
	if (size) {
	    PyErr_SetString(PyExc_TypeError,
			    "_p_deactivate only accepts keyword arg force");
	    return NULL;
	}
    }

    if (self->jar && self->oid) {
	ghostify = self->state == cPersistent_UPTODATE_STATE;
	if (!ghostify && force) {
	    if (PyObject_IsTrue(force))
		ghostify = 1;
	    if (PyErr_Occurred())
		return NULL;
	}
	if (ghostify) {
	    if (_bucket_clear(self) < 0)
		return NULL;
	    PER_GHOSTIFY(self);
	}
    }
    Py_INCREF(Py_None);
    return Py_None;
}
#endif

static PyObject *
bucket_clear(Bucket *self, PyObject *args)
{
  PER_USE_OR_RETURN(self, NULL);

  if (self->len) {
      if (_bucket_clear(self) < 0)
	  return NULL;
      if (PER_CHANGED(self) < 0)
	  goto err;
  }
  PER_UNUSE(self);
  Py_INCREF(Py_None);
  return Py_None;

err:
  PER_UNUSE(self);
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
bucket_getstate(Bucket *self)
{
    PyObject *o = NULL, *items = NULL, *state;
    int i, len, l;

    PER_USE_OR_RETURN(self, NULL);

    len = self->len;

    if (self->values) { /* Bucket */
	items = PyTuple_New(len * 2);
	if (items == NULL)
	    goto err;
	for (i = 0, l = 0; i < len; i++) {
	    COPY_KEY_TO_OBJECT(o, self->keys[i]);
	    if (o == NULL)
		goto err;
	    PyTuple_SET_ITEM(items, l, o);
	    l++;

	    COPY_VALUE_TO_OBJECT(o, self->values[i]);
	    if (o == NULL)
		goto err;
	    PyTuple_SET_ITEM(items, l, o);
	    l++;
        }
    } else { /* Set */
	items = PyTuple_New(len);
	if (items == NULL)
	    goto err;
	for (i = 0; i < len; i++) {
	    COPY_KEY_TO_OBJECT(o, self->keys[i]);
	    if (o == NULL)
		goto err;
	    PyTuple_SET_ITEM(items, i, o);
        }
    }

    if (self->next)
	state = Py_BuildValue("OO", items, self->next);
    else
	state = Py_BuildValue("(O)", items);
    Py_DECREF(items);

    PER_UNUSE(self);
    return state;

 err:
    PER_UNUSE(self);
    Py_XDECREF(items);
    return NULL;
}

static int
_bucket_setstate(Bucket *self, PyObject *state)
{
    PyObject *k, *v, *items;
    Bucket *next = NULL;
    int i, l, len, copied=1;
    KEY_TYPE *keys;
    VALUE_TYPE *values;

    if (!PyArg_ParseTuple(state, "O|O:__setstate__", &items, &next))
	return -1;

    len = PyTuple_Size(items);
    if (len < 0)
	return -1;
    len /= 2;

    for (i = self->len; --i >= 0; ) {
	DECREF_KEY(self->keys[i]);
	DECREF_VALUE(self->values[i]);
    }
    self->len = 0;

    if (self->next) {
	Py_DECREF(self->next);
	self->next = NULL;
    }

    if (len > self->size) {
	keys = BTree_Realloc(self->keys, sizeof(KEY_TYPE)*len);
	if (keys == NULL)
	    return -1;
	values = BTree_Realloc(self->values, sizeof(VALUE_TYPE)*len);
	if (values == NULL)
	    return -1;
	self->keys = keys;
	self->values = values;
	self->size = len;
    }

    for (i=0, l=0; i < len; i++) {
	k = PyTuple_GET_ITEM(items, l);
	l++;
	v = PyTuple_GET_ITEM(items, l);
	l++;

	COPY_KEY_FROM_ARG(self->keys[i], k, copied);
	if (!copied)
	    return -1;
	COPY_VALUE_FROM_ARG(self->values[i], v, copied);
	if (!copied)
	    return -1;
	INCREF_KEY(self->keys[i]);
	INCREF_VALUE(self->values[i]);
    }

    self->len = len;

    if (next) {
	self->next = next;
	Py_INCREF(next);
    }

    return 0;
}

static PyObject *
bucket_setstate(Bucket *self, PyObject *state)
{
    int r;

    PER_PREVENT_DEACTIVATION(self);
    r = _bucket_setstate(self, state);
    PER_UNUSE(self);

    if (r < 0)
	return NULL;
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
bucket_has_key(Bucket *self, PyObject *key)
{
    return _bucket_get(self, key, 1);
}


/* Search bucket self for key.  This is the sq_contains slot of the
 * PySequenceMethods.
 *
 * Return:
 *     -1     error
 *      0     not found
 *      1     found
 */
static int
bucket_contains(Bucket *self, PyObject *key)
{
    PyObject *asobj = _bucket_get(self, key, 1);
    int result = -1;

    if (asobj != NULL) {
        result = PyInt_AsLong(asobj) ? 1 : 0;
        Py_DECREF(asobj);
    }
    return result;
}

/*
** bucket_getm
**
*/
static PyObject *
bucket_getm(Bucket *self, PyObject *args)
{
    PyObject *key, *d=Py_None, *r;

    if (!PyArg_ParseTuple(args, "O|O:get", &key, &d))
	return NULL;
    r = _bucket_get(self, key, 0);
    if (r)
	return r;
    if (!PyErr_ExceptionMatches(PyExc_KeyError))
	return NULL;
    PyErr_Clear();
    Py_INCREF(d);
    return d;
}

/**************************************************************************/
/* Iterator support. */

/* A helper to build all the iterators for Buckets and Sets.
 * If args is NULL, the iterator spans the entire structure.  Else it's an
 * argument tuple, with optional low and high arguments.
 * kind is 'k', 'v' or 'i'.
 * Returns a BTreeIter object, or NULL if error.
 */
static PyObject *
buildBucketIter(Bucket *self, PyObject *args, PyObject *kw, char kind)
{
    BTreeItems *items;
    int lowoffset, highoffset;
    BTreeIter *result = NULL;

    PER_USE_OR_RETURN(self, NULL);
    if (Bucket_rangeSearch(self, args, kw, &lowoffset, &highoffset) < 0)
        goto Done;

    items = (BTreeItems *)newBTreeItems(kind, self, lowoffset,
                                              self, highoffset);
    if (items == NULL) goto Done;

    result = BTreeIter_new(items);      /* win or lose, we're done */
    Py_DECREF(items);

Done:
    PER_UNUSE(self);
    return (PyObject *)result;
}

/* The implementation of iter(Bucket_or_Set); the Bucket tp_iter slot. */
static PyObject *
Bucket_getiter(Bucket *self)
{
    return buildBucketIter(self, NULL, NULL, 'k');
}

/* The implementation of Bucket.iterkeys(). */
static PyObject *
Bucket_iterkeys(Bucket *self, PyObject *args, PyObject *kw)
{
    return buildBucketIter(self, args, kw, 'k');
}

/* The implementation of Bucket.itervalues(). */
static PyObject *
Bucket_itervalues(Bucket *self, PyObject *args, PyObject *kw)
{
    return buildBucketIter(self, args, kw, 'v');
}

/* The implementation of Bucket.iteritems(). */
static PyObject *
Bucket_iteritems(Bucket *self, PyObject *args, PyObject *kw)
{
    return buildBucketIter(self, args, kw, 'i');
}

/* End of iterator support. */

#ifdef PERSISTENT
static PyObject *merge_error(int p1, int p2, int p3, int reason);
static PyObject *bucket_merge(Bucket *s1, Bucket *s2, Bucket *s3);

static PyObject *
_bucket__p_resolveConflict(PyObject *ob_type, PyObject *s[3])
{
    PyObject *result = NULL;	/* guilty until proved innocent */
    Bucket *b[3] = {NULL, NULL, NULL};
    PyObject *meth = NULL;
    PyObject *a = NULL;
    int i;

    for (i = 0; i < 3; i++) {
	PyObject *r;

	b[i] = (Bucket*)PyObject_CallObject((PyObject *)ob_type, NULL);
	if (b[i] == NULL)
	    goto Done;
	if (s[i] == Py_None) /* None is equivalent to empty, for BTrees */
	    continue;
	meth = PyObject_GetAttr((PyObject *)b[i], __setstate___str);
	if (meth == NULL)
	    goto Done;
	a = PyTuple_New(1);
	if (a == NULL)
	    goto Done;
	PyTuple_SET_ITEM(a, 0, s[i]);
	Py_INCREF(s[i]);
	r = PyObject_CallObject(meth, a);  /* b[i].__setstate__(s[i]) */
	if (r == NULL)
	    goto Done;
	Py_DECREF(r);
	Py_DECREF(a);
	Py_DECREF(meth);
	a = meth = NULL;
    }

    if (b[0]->next != b[1]->next || b[0]->next != b[2]->next)
	merge_error(-1, -1, -1, 0);
    else
	result = bucket_merge(b[0], b[1], b[2]);

Done:
    Py_XDECREF(meth);
    Py_XDECREF(a);
    Py_XDECREF(b[0]);
    Py_XDECREF(b[1]);
    Py_XDECREF(b[2]);

    return result;
}

static PyObject *
bucket__p_resolveConflict(Bucket *self, PyObject *args)
{
    PyObject *s[3];

    if (!PyArg_ParseTuple(args, "OOO", &s[0], &s[1], &s[2]))
	return NULL;

    return _bucket__p_resolveConflict((PyObject *)self->ob_type, s);
}
#endif

/* XXX Even though the _next attribute is read-only, a program could
   probably do arbitrary damage to a the btree internals.  For
   example, it could call clear() on a bucket inside a BTree.

   We need to decide if the convenience for inspecting BTrees is worth
   the risk.
*/

static struct PyMemberDef Bucket_members[] = {
    {"_next", T_OBJECT, offsetof(Bucket, next)},
    {NULL}
};

static struct PyMethodDef Bucket_methods[] = {
    {"__getstate__", (PyCFunction) bucket_getstate,	METH_NOARGS,
     "__getstate__() -- Return the picklable state of the object"},

    {"__setstate__", (PyCFunction) bucket_setstate,	METH_O,
     "__setstate__() -- Set the state of the object"},

    {"keys",	(PyCFunction) bucket_keys,	METH_KEYWORDS,
     "keys([min, max]) -- Return the keys"},

    {"has_key",	(PyCFunction) bucket_has_key,	METH_O,
     "has_key(key) -- Test whether the bucket contains the given key"},

    {"clear",	(PyCFunction) bucket_clear,	METH_VARARGS,
     "clear() -- Remove all of the items from the bucket"},

    {"update",	(PyCFunction) Mapping_update,	METH_O,
     "update(collection) -- Add the items from the given collection"},

    {"maxKey", (PyCFunction) Bucket_maxKey,	METH_VARARGS,
     "maxKey([key]) -- Find the maximum key\n\n"
     "If an argument is given, find the maximum <= the argument"},

    {"minKey", (PyCFunction) Bucket_minKey,	METH_VARARGS,
     "minKey([key]) -- Find the minimum key\n\n"
     "If an argument is given, find the minimum >= the argument"},

    {"values",	(PyCFunction) bucket_values,	METH_KEYWORDS,
     "values([min, max]) -- Return the values"},

    {"items",	(PyCFunction) bucket_items,	METH_KEYWORDS,
     "items([min, max])) -- Return the items"},

    {"byValue",	(PyCFunction) bucket_byValue,	METH_O,
     "byValue(min) -- "
     "Return value-keys with values >= min and reverse sorted by values"},

    {"get",	(PyCFunction) bucket_getm,	METH_VARARGS,
     "get(key[,default]) -- Look up a value\n\n"
     "Return the default (or None) if the key is not found."},

    {"iterkeys", (PyCFunction) Bucket_iterkeys,  METH_KEYWORDS,
     "B.iterkeys([min[,max]]) -> an iterator over the keys of B"},

    {"itervalues", (PyCFunction) Bucket_itervalues,  METH_KEYWORDS,
     "B.itervalues([min[,max]]) -> an iterator over the values of B"},

    {"iteritems", (PyCFunction) Bucket_iteritems,    METH_KEYWORDS,
     "B.iteritems([min[,max]]) -> an iterator over the (key, value) items of B"},

#ifdef PERSISTENT
    {"_p_resolveConflict", (PyCFunction) bucket__p_resolveConflict,
     METH_VARARGS,
     "_p_resolveConflict() -- Reinitialize from a newly created copy"},

    {"_p_deactivate", (PyCFunction) bucket__p_deactivate, METH_KEYWORDS,
     "_p_deactivate() -- Reinitialize from a newly created copy"},
#endif
    {NULL, NULL}
};

static int
Bucket_init(PyObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *v = NULL;

    if (!PyArg_ParseTuple(args, "|O:" MOD_NAME_PREFIX "Bucket", &v))
	return -1;

    if (v)
	return update_from_seq(self, v);
    else
	return 0;
}

static void
bucket_dealloc(Bucket *self)
{
    if (self->state != cPersistent_GHOST_STATE)
	_bucket_clear(self);

    cPersistenceCAPI->pertype->tp_dealloc((PyObject *)self);
}

static int
bucket_traverse(Bucket *self, visitproc visit, void *arg)
{
    int err = 0;
    int i, len;

#define VISIT(SLOT)                             \
    if (SLOT) {                                 \
        err = visit((PyObject *)(SLOT), arg);   \
        if (err)                                \
            goto Done;                          \
    }

    /* Call our base type's traverse function.  Because buckets are
     * subclasses of Peristent, there must be one.
     */
    err = cPersistenceCAPI->pertype->tp_traverse((PyObject *)self, visit, arg);
    if (err)
	goto Done;

    /* If this is registered with the persistence system, cleaning up cycles
     * is the database's problem.  It would be horrid to unghostify buckets
     * here just to chase pointers every time gc runs.
     */
    if (self->state == cPersistent_GHOST_STATE)
        goto Done;

    len = self->len;
    (void)i;    /* if neither keys nor values are PyObject*, "i" is otherwise
                   unreferenced and we get a nuisance compiler wng */
#ifdef KEY_TYPE_IS_PYOBJECT
    /* Keys are Python objects so need to be traversed. */
    for (i = 0; i < len; i++)
        VISIT(self->keys[i]);
#endif

#ifdef VALUE_TYPE_IS_PYOBJECT
    if (self->values != NULL) {
        /* self->values exists (this is a mapping bucket, not a set bucket),
         * and are Python objects, so need to be traversed. */
        for (i = 0; i < len; i++)
            VISIT(self->values[i]);
    }
#endif

    VISIT(self->next);

Done:
    return err;

#undef VISIT
}

static int
bucket_tp_clear(Bucket *self)
{
    if (self->state != cPersistent_GHOST_STATE)
	_bucket_clear(self);
    return 0;
}

/* Code to access Bucket objects as mappings */
static int
Bucket_length( Bucket *self)
{
    int r;
    UNLESS (PER_USE(self)) return -1;
    r = self->len;
    PER_UNUSE(self);
    return r;
}

static PyMappingMethods Bucket_as_mapping = {
  (inquiry)Bucket_length,		/*mp_length*/
  (binaryfunc)bucket_getitem,		/*mp_subscript*/
  (objobjargproc)bucket_setitem,	/*mp_ass_subscript*/
};

static PySequenceMethods Bucket_as_sequence = {
    (inquiry)0,                     /* sq_length */
    (binaryfunc)0,                  /* sq_concat */
    (intargfunc)0,                  /* sq_repeat */
    (intargfunc)0,                  /* sq_item */
    (intintargfunc)0,               /* sq_slice */
    (intobjargproc)0,               /* sq_ass_item */
    (intintobjargproc)0,            /* sq_ass_slice */
    (objobjproc)bucket_contains,    /* sq_contains */
    0,                              /* sq_inplace_concat */
    0,                              /* sq_inplace_repeat */
};

static PyObject *
bucket_repr(Bucket *self)
{
    PyObject *i, *r;
    char repr[10000];
    int rv;

    i = bucket_items(self, NULL, NULL);
    if (!i)
	return NULL;
    r = PyObject_Repr(i);
    Py_DECREF(i);
    if (!r) {
	return NULL;
    }
    rv = PyOS_snprintf(repr, sizeof(repr),
		       "%s(%s)", self->ob_type->tp_name,
		       PyString_AS_STRING(r));
    if (rv > 0 && rv < sizeof(repr)) {
	Py_DECREF(r);
	return PyString_FromStringAndSize(repr, strlen(repr));
    }
    else {
	/* The static buffer wasn't big enough */
	int size;
	PyObject *s;

	/* 3 for the parens and the null byte */
	size = strlen(self->ob_type->tp_name) + PyString_GET_SIZE(r) + 3;
	s = PyString_FromStringAndSize(NULL, size);
	if (!s) {
	    Py_DECREF(r);
	    return r;
	}
	PyOS_snprintf(PyString_AS_STRING(s), size,
		      "%s(%s)", self->ob_type->tp_name, PyString_AS_STRING(r));
	Py_DECREF(r);
	return s;
    }
}

static PyTypeObject BucketType = {
    PyObject_HEAD_INIT(NULL) /* PyPersist_Type */
    0,					/* ob_size */
    MODULE_NAME MOD_NAME_PREFIX "Bucket",/* tp_name */
    sizeof(Bucket),			/* tp_basicsize */
    0,					/* tp_itemsize */
    (destructor)bucket_dealloc,		/* tp_dealloc */
    0,					/* tp_print */
    0,					/* tp_getattr */
    0,					/* tp_setattr */
    0,					/* tp_compare */
    (reprfunc)bucket_repr,		/* tp_repr */
    0,					/* tp_as_number */
    &Bucket_as_sequence,		/* tp_as_sequence */
    &Bucket_as_mapping,			/* tp_as_mapping */
    0,					/* tp_hash */
    0,					/* tp_call */
    0,					/* tp_str */
    0,					/* tp_getattro */
    0,					/* tp_setattro */
    0,					/* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC |
	    Py_TPFLAGS_BASETYPE, 	/* tp_flags */
    0,					/* tp_doc */
    (traverseproc)bucket_traverse,	/* tp_traverse */
    (inquiry)bucket_tp_clear,		/* tp_clear */
    0,					/* tp_richcompare */
    0,					/* tp_weaklistoffset */
    (getiterfunc)Bucket_getiter,	/* tp_iter */
    0,					/* tp_iternext */
    Bucket_methods,			/* tp_methods */
    Bucket_members,			/* tp_members */
    0,					/* tp_getset */
    0,					/* tp_base */
    0,					/* tp_dict */
    0,					/* tp_descr_get */
    0,					/* tp_descr_set */
    0,					/* tp_dictoffset */
    Bucket_init,			/* tp_init */
    0,					/* tp_alloc */
    0, /*PyType_GenericNew,*/		/* tp_new */
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
