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

#define BUCKETTEMPLATE_C "$Id: BucketTemplate.c,v 1.27 2002/02/21 21:41:17 jeremy Exp $\n"

/*
** _bucket_get
**
** Get the bucket item with the matching key
**
** Arguments:	self	The bucket
**		key	The key to match against
**		has_key	Just return object "1" if key found, object "0" if not
**
** Returns:	object	matching object or 0/1 object
*/


static PyObject *
_bucket_get(Bucket *self, PyObject *keyarg, int has_key)
{
  int min, max, i, l, cmp, copied=1;
  PyObject *r;
  KEY_TYPE key;
  
  COPY_KEY_FROM_ARG(key, keyarg, copied);
  UNLESS (copied) return NULL;

  PER_USE_OR_RETURN(self, NULL);

  for (min=0, max=self->len, i=max/2, l=max; i != l; l=i, i=(min+max)/2)
    {
      cmp=TEST_KEY(self->keys[i], key);
      if (PyErr_Occurred()) goto err;

      if (cmp < 0) min=i;
      else if (cmp == 0)
	{
	  if (has_key) r=PyInt_FromLong(has_key);
	  else
	    {
              COPY_VALUE_TO_OBJECT(r, self->values[i]);
	    }
	  PER_ALLOW_DEACTIVATION(self);
          PER_ACCESSED(self);
	  return r;
	}
      else max=i;
    }

  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  if (has_key) return PyInt_FromLong(0);
  PyErr_SetObject(PyExc_KeyError, keyarg);

err:
  return NULL;
}

static PyObject *
bucket_getitem(Bucket *self, PyObject *key)
{
  return _bucket_get(self, key, 0);
}

static int 
Bucket_grow(Bucket *self, int noval)
{
  KEY_TYPE *keys;
  VALUE_TYPE *values;

  if (self->size)
    {
      UNLESS (keys=PyRealloc(self->keys, sizeof(KEY_TYPE)*self->size*2))
        return -1;
      
      UNLESS (noval)
        {
          UNLESS (values=PyRealloc(self->values,
                                   sizeof(VALUE_TYPE)*self->size*2))
            return -1;
          self->values=values;
        }
      self->keys=keys;
      self->size*=2;
    }
  else
    {
      UNLESS (self->keys=PyMalloc(sizeof(KEY_TYPE)*MIN_BUCKET_ALLOC))
        return -1;
      UNLESS (noval || 
              (self->values=PyMalloc(sizeof(VALUE_TYPE)*MIN_BUCKET_ALLOC))
              )
        return -1;
      self->size=MIN_BUCKET_ALLOC;
    }
  
  return 0;
}

/*
** _bucket_set
**
** Assign a value into a bucket
**
** Arguments:	self	The bucket
**		key	The key of the object to insert
**		v	The value of the object to insert
**              unique  Inserting a unique key
**
** Returns:	-1 	on error
**		 0	on success with a replacement
**		 1	on success with a new value (growth)
*/
static int
_bucket_set(Bucket *self, PyObject *keyarg, PyObject *v, 
            int unique, int noval, int *changed)
{
  int min, max, i, l, cmp, copied=1;
  KEY_TYPE key;
  
  COPY_KEY_FROM_ARG(key, keyarg, copied);
  UNLESS(copied) return -1;

  PER_USE_OR_RETURN(self, -1);

  for (min=0, max=l=self->len, i=max/2; i != l; l=i, i=(min+max)/2)
    {
      if ((cmp=TEST_KEY(self->keys[i], key)) < 0) min=i;
      else if (cmp==0)
	{
	  if (v)			/* Assign value to key */
	    {
              if (! unique && ! noval && self->values)
                {
                  VALUE_TYPE value;

                  COPY_VALUE_FROM_ARG(value, v, copied);
                  UNLESS(copied) return -1;

#ifdef VALUE_SAME
                  if (VALUE_SAME(self->values[i], value))
                    { /* short-circuit if no change */
                      PER_ALLOW_DEACTIVATION(self);
                      PER_ACCESSED(self);
                      return 0;
                    }
#endif
                  if (changed) *changed=1;
                  DECREF_VALUE(self->values[i]);
                  COPY_VALUE(self->values[i], value);
                  INCREF_VALUE(self->values[i]);
                  if (PER_CHANGED(self) < 0) goto err;
                }
	      PER_ALLOW_DEACTIVATION(self);
              PER_ACCESSED(self);
	      return 0;
	    }
	  else			/* There's no value so remove the item */
	    {
	      self->len--;

	      DECREF_KEY(self->keys[i]);
	      if (i < self->len)	
                memmove(self->keys+i, self->keys+i+1,
                        sizeof(KEY_TYPE)*(self->len-i));

              if (self->values && ! noval)
                {
                  DECREF_VALUE(self->values[i]);
                  if (i < self->len)	
                    memmove(self->values+i, self->values+i+1,
                            sizeof(VALUE_TYPE)*(self->len-i));

                }
	      
              if (! self->len)
		{
		  self->size=0;
		  free(self->keys);
		  self->keys=NULL;
                  if (self->values)
                    {
                      free(self->values);
                      self->values=NULL;
                    }
		}

	      if (PER_CHANGED(self) < 0) goto err;
	      PER_ALLOW_DEACTIVATION(self);
              PER_ACCESSED(self);
	      return 1;
	    }
	}
      else max=i;
    }

  if (!v)
    {
      PyErr_SetObject(PyExc_KeyError, keyarg);
      goto err;
    }

  if (self->len==self->size && Bucket_grow(self, noval) < 0) goto err;

  if (max != i) i++;

  if (self->len > i)
    {
      memmove(self->keys+i+1, self->keys+i,
              sizeof(KEY_TYPE)*(self->len-i));
      UNLESS (noval)
        memmove(self->values+i+1, self->values+i,
                sizeof(VALUE_TYPE)*(self->len-i));
    }


  COPY_KEY(self->keys[i], key);
  INCREF_KEY(self->keys[i]);

  UNLESS (noval)
    {
      COPY_VALUE_FROM_ARG(self->values[i], v, copied);
      UNLESS(copied) return -1;
      INCREF_VALUE(self->values[i]);
    }

  self->len++;

  if (PER_CHANGED(self) < 0) goto err;
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  return 1;

err:
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  return -1;
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

  if (index < 0 || index >= self->len) index=self->len/2;

  next_size=self->len-index;

  UNLESS (next->keys=PyMalloc(sizeof(KEY_TYPE)*next_size)) return -1;
  memcpy(next->keys, self->keys+index, sizeof(KEY_TYPE)*next_size);
  if (self->values)
    {
      UNLESS (next->values=PyMalloc(sizeof(VALUE_TYPE)*next_size))
        return -1;
      memcpy(next->values, self->values+index, sizeof(VALUE_TYPE)*next_size);
    }
  next->size = next_size;
  next->len= next_size;
  self->len=index;

  next->next = self->next;

  Py_INCREF(next);
  self->next = next;

  PER_CHANGED(self);

  return 0;
}

static int
Bucket_nextBucket(Bucket *self, Bucket **r)
{
  PER_USE_OR_RETURN(self, -1);
  *r=self->next;
  Py_XINCREF(*r);
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  return 0;
}

static int 
Bucket_deleteNextBucket(Bucket *self)
{
  PER_USE_OR_RETURN(self, -1);
  if (self->next)
    {
      Bucket *n;
      if (Bucket_nextBucket(self->next, &n) < 0) goto err;
      ASSIGNB(self->next, n);
      PER_CHANGED(self);
    }
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  return 0;
 err:
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);
  return -1;
}
/*
 Bucket_findRangeEnd -- Find the index of a range endpoint 
 (possibly) contained in a bucket.

 Arguments:	self		The bucket
		key		the key to match against
		low             end flag
                offset          The output offset
	

 If low, return bucket and index of the smallest item >= key,
 otherwise return bucket and index of the largest item <= key.

 Return: 0 -- Not found, 1 -- found, -1 -- error.
*/
static int
Bucket_findRangeEnd(Bucket *self, PyObject *keyarg, int low, int *offset)
{
  int min, max, i, l, cmp, copied=1;
  KEY_TYPE key;

  COPY_KEY_FROM_ARG(key, keyarg, copied);
  UNLESS (copied) return -1;

  PER_USE_OR_RETURN(self, -1);

  for (min=0, max=self->len, i=max/2, l=max; i != l; l=i, i=(min+max)/2) 
    {
      cmp=TEST_KEY(self->keys[i], key);
      if (cmp < 0)
	min=i;
      else if (cmp == 0)
        {
          PER_ALLOW_DEACTIVATION(self);
          PER_ACCESSED(self);
          *offset=i;
          return 1;
        } 
      else
        max=i;
  }

  /* OK, no matches, pick max or min, depending on whether
     we want an upper or low end.
  */
  if (low) 
    {
      if (max == self->len) i=0;
      else 
        {
          i=1;
          *offset=max;
        }
    }
  else
    {
      if (max == 0) i=0;
      else 
        {
          i=1;
          *offset=min;
        }
    }

  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);

  return i;
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
  else *high=self->len - 1;

  return 0;

 empty:
  *low=0;
  *high=-1;
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
  int i;

  if (self->next) 
    {
      Py_DECREF(self->next);
      self->next=0;
    }

  for (i=self->len; --i >= 0; )
    {
      DECREF_KEY(self->keys[i]);
      if (self->values) 
        {
          DECREF_VALUE(self->values[i]);
        }
    }
  self->len=0;
  if (self->values) 
    {
      free(self->values);
      self->values=0;
    }
  if (self->keys)
    {
      free(self->keys);
      self->keys=0;
    }
  self->size=0;

  return 0;
}

#ifdef PERSISTENT
static PyObject *
bucket__p_deactivate(Bucket *self, PyObject *args)
{
  if (self->state==cPersistent_UPTODATE_STATE && self->jar)
    {
      if (_bucket_clear(self) < 0) return NULL;
      self->state=cPersistent_GHOST_STATE;
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
   "maxKey([key]) -- Fine the maximum key\n\n"
   "If an argument is given, find the maximum <= the argument"},
  {"minKey", (PyCFunction) Bucket_minKey,	METH_VARARGS,
   "minKey([key]) -- Fine the minimum key\n\n"
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
  _bucket_clear(self);

  PER_DEL(self);

  Py_DECREF(self->ob_type);
  PyMem_DEL(self);
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
