/*****************************************************************************
  
  Zope Public License (ZPL) Version 1.0
  -------------------------------------
  
  Copyright (c) Digital Creations.  All rights reserved.
  
  This license has been certified as Open Source(tm).
  
  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are
  met:
  
  1. Redistributions in source code must retain the above copyright
     notice, this list of conditions, and the following disclaimer.
  
  2. Redistributions in binary form must reproduce the above copyright
     notice, this list of conditions, and the following disclaimer in
     the documentation and/or other materials provided with the
     distribution.
  
  3. Digital Creations requests that attribution be given to Zope
     in any manner possible. Zope includes a "Powered by Zope"
     button that is installed by default. While it is not a license
     violation to remove this button, it is requested that the
     attribution remain. A significant investment has been put
     into Zope, and this effort will continue if the Zope community
     continues to grow. This is one way to assure that growth.
  
  4. All advertising materials and documentation mentioning
     features derived from or use of this software must display
     the following acknowledgement:
  
       "This product includes software developed by Digital Creations
       for use in the Z Object Publishing Environment
       (http://www.zope.org/)."
  
     In the event that the product being advertised includes an
     intact Zope distribution (with copyright and license included)
     then this clause is waived.
  
  5. Names associated with Zope or Digital Creations must not be used to
     endorse or promote products derived from this software without
     prior written permission from Digital Creations.
  
  6. Modified redistributions of any form whatsoever must retain
     the following acknowledgment:
  
       "This product includes software developed by Digital Creations
       for use in the Z Object Publishing Environment
       (http://www.zope.org/)."
  
     Intact (re-)distributions of any official Zope release do not
     require an external acknowledgement.
  
  7. Modifications are encouraged but must be packaged separately as
     patches to official Zope releases.  Distributions that do not
     clearly separate the patches from the original work must be clearly
     labeled as unofficial distributions.  Modifications which do not
     carry the name Zope may be packaged in any form, as long as they
     conform to all of the clauses above.
  
  
  Disclaimer
  
    THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS ``AS IS'' AND ANY
    EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
    IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
    PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL CREATIONS OR ITS
    CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
    LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
    USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND    ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
    OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
    OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
    SUCH DAMAGE.
  
  
  This software consists of contributions made by Digital Creations and
  many individuals on behalf of Digital Creations.  Specific
  attributions are listed in the accompanying credits file.
  
 ****************************************************************************/

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
  
  COPY_KEY_FROM_ARG(key, keyarg, &copied);
  UNLESS (copied) return NULL;

  PER_USE_OR_RETURN(self, NULL);

  for (min=0, max=self->len, i=max/2, l=max; i != l; l=i, i=(min+max)/2)
    {
      cmp=TEST_KEY(self->keys[i], key);
      if (cmp < 0) min=i;
      else if (cmp == 0)
	{
	  if (has_key) r=PyInt_FromLong(1);
	  else
	    {
              COPY_VALUE_TO_OBJECT(r, self->values[i]);
	    }
	  PER_ALLOW_DEACTIVATION(self);
	  return r;
	}
      else max=i;
    }

  PER_ALLOW_DEACTIVATION(self);
  if (has_key) return PyInt_FromLong(0);
  PyErr_SetObject(PyExc_KeyError, keyarg);
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
_bucket_set(Bucket *self, PyObject *keyarg, PyObject *v, int unique, int noval)
{
  int min, max, i, l, cmp, copied=1;
  KEY_TYPE key;
  
  COPY_KEY_FROM_ARG(key, keyarg, &copied);
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
                  DECREF_VALUE(self->values[i]);

                  COPY_VALUE_FROM_ARG(self->values[i], v, &copied);
                  UNLESS(copied) return -1;

                  INCREF_VALUE(self->values[i]);
                  if (PER_CHANGED(self) < 0) goto err;
                }
	      PER_ALLOW_DEACTIVATION(self);
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
      COPY_VALUE_FROM_ARG(self->values[i], v, &copied);
      UNLESS(copied) return -1;
      INCREF_VALUE(self->values[i]);
    }

  self->len++;

  if (PER_CHANGED(self) < 0) goto err;
  PER_ALLOW_DEACTIVATION(self);
  return 1;

err:
  PER_ALLOW_DEACTIVATION(self);
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
  if (_bucket_set(self, key, v, 0, 0) < 0) return -1;
  return 0;
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

  return 0;
}

static int
Bucket_nextBucket(Bucket *self, Bucket **r)
{
  PER_USE_OR_RETURN(self, -1);
  *r=self->next;
  Py_XINCREF(*r);
  PER_ALLOW_DEACTIVATION(self);
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
  return 0;
 err:
  PER_ALLOW_DEACTIVATION(self);
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
  Bucket *chase;
  Bucket *release = NULL;
  KEY_TYPE key;

  COPY_KEY_FROM_ARG(key, keyarg, &copied);
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

  return key;
  
 empty:
  PyErr_SetString(PyExc_ValueError, "empty bucket");

 err:
  PER_ALLOW_DEACTIVATION(self);
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
      if (PyList_SetItem(r, i, key) < 0) goto err;
    }

  PER_ALLOW_DEACTIVATION(self);
  return r;

 err:
  PER_ALLOW_DEACTIVATION(self);
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
      COPY_KEY_TO_OBJECT(v, self->keys[i]);
      UNLESS (v) goto err;
      if (PyList_SetItem(r, i, v) < 0) goto err;
    }

  PER_ALLOW_DEACTIVATION(self);
  return r;

 err:
  PER_ALLOW_DEACTIVATION(self);
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
      
      if (PyList_SetItem(r, i, item) < 0) goto err;

      item = 0;
    }

  PER_ALLOW_DEACTIVATION(self);
  return r;

 err:
  PER_ALLOW_DEACTIVATION(self);
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
  COPY_VALUE_FROM_ARG(min, omin, &copied);
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
  return r;

 err:
  PER_ALLOW_DEACTIVATION(self);
  Py_XDECREF(r);
  Py_XDECREF(item);
  return NULL;
}

#ifdef PERSISTENT
static PyObject *
bucket__p_deactivate(Bucket *self, PyObject *args)
{
  if (self->state==cPersistent_UPTODATE_STATE)
    {
      int i;

      for (i=self->len; --i >= 0; )
	{
	  DECREF_KEY(self->keys[i]);
	  if (self->values) DECREF_VALUE(self->values[i]);
	}
      Py_XDECREF(self->next);
      self->len=0;
      self->state=cPersistent_GHOST_STATE;
    }

  Py_INCREF(Py_None);
  return Py_None;
}
#endif

static PyObject *
bucket_clear(Bucket *self, PyObject *args)
{
  int i;

  PER_USE_OR_RETURN(self, NULL);

  for (i=self->len; --i >= 0; )
    {
      DECREF_KEY(self->keys[i]);
      if (self->values) DECREF_VALUE(self->values[i]);
    }
  self->len=0;
  if (PER_CHANGED(self) < 0) goto err;
  PER_ALLOW_DEACTIVATION(self);
  Py_INCREF(Py_None); 
  return Py_None;

err:
  PER_ALLOW_DEACTIVATION(self);
  return NULL;
}

static PyObject *
bucket_getstate(Bucket *self, PyObject *args)
{
  PyObject *o=0, *items=0;
  int i, len, l;

  PER_USE_OR_RETURN(self, NULL);

  len=self->len;

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

  if (self->next) 
    ASSIGN(items, Py_BuildValue("OO", items, self->next));
  else
    ASSIGN(items, Py_BuildValue("(O)", items));
  
  PER_ALLOW_DEACTIVATION(self);

  return items;

err:
  PER_ALLOW_DEACTIVATION(self);
  Py_XDECREF(items);
  return NULL;
}

/*
** bucket_setstate
**
** bulk set of all items in bucket
**
** Arguments:	self	The Bucket
**		args	The object pointng to the two lists of tuples
**
** Returns:	None	on success
**		NULL	on error
*/
static PyObject *
bucket_setstate(Bucket *self, PyObject *args)
{
  PyObject *k, *v, *r, *items;
  Bucket *next=0;
  int i, l, len, copied=1;
  KEY_TYPE *keys;
  VALUE_TYPE *values;

  PER_PREVENT_DEACTIVATION(self); 

  UNLESS (PyArg_ParseTuple(args, "O", &args)) goto err;

  UNLESS (PyArg_ParseTuple(args, "O|O!", &items, self->ob_type, &next))
    goto err;

  if ((len=PyTuple_Size(items)) < 0) goto err;

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
      UNLESS (keys=PyRealloc(self->keys, sizeof(KEY_TYPE)*len)) goto err;
      UNLESS (values=PyRealloc(self->values, sizeof(KEY_TYPE)*len)) goto err;
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

      COPY_KEY_FROM_ARG(self->keys[i], k, &copied);
      UNLESS (copied) return NULL;
      COPY_VALUE_FROM_ARG(self->values[i], v, &copied);
      UNLESS (copied) return NULL;
      INCREF_KEY(k);
      INCREF_VALUE(v);
    }

  self->len=l;

  PER_ALLOW_DEACTIVATION(self);
  Py_INCREF(Py_None);
  return Py_None;

 perr:
  self->len=i;
 err:
  PER_ALLOW_DEACTIVATION(self);
  return NULL;
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
  PyErr_Clear();
  Py_INCREF(d);
  return d;
}

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
  {"_p_deactivate", (PyCFunction) bucket__p_deactivate, METH_VARARGS,
   "_p_deactivate() -- Reinitialize from a newly created copy"},
#endif
  {NULL,		NULL}		/* sentinel */
};

static void
Bucket_dealloc(Bucket *self)
{
  int i;

  for (i=self->len; --i >= 0; )
    {
      DECREF_KEY(self->keys[i]);
      if (self->values) DECREF_VALUE(self->values[i]);
    }
  free(self->keys);
  free(self->values);
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

  UNLESS (format) UNLESS (format=PyString_FromString(PREFIX "Bucket(%s)")) 
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
  PREFIX "Bucket",			/*tp_name*/
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
  UNLESS(PER_USE(BUCKET(i->set))) return -1;
          
  if (i->position >= 0)
    {
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
        i->position = -1;
    }

  PER_ALLOW_DEACTIVATION(BUCKET(i->set));
          
  return 0;
}
